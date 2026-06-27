package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"velox-server/internal/ingest"
	"velox-server/internal/store"
	"velox-server/internal/taskattempts"
	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// handleTaskAccepted processes typed TaskAccepted — PR #4 task-native push mode.
// The lease was already created by ClaimNextReadyTask; we promote LEASED→RUNNING,
// create a TaskAttempt record, and grant the lease.
func (h *Handler) handleTaskAccepted(workerID string, ta *pb.TaskAccepted) {
	if !h.config.PushMode {
		return
	}
	taskID := ta.GetTaskId()

	sess := h.getSession(workerID)
	if sess == nil {
		return
	}

	// Lock claimMu to safely read and clear pendingTaskOffer.
	sess.claimMu.Lock()
	offer := sess.pendingTaskOffer
	var offerTaskID, offerLeaseID string
	if offer != nil {
		offerTaskID = offer.ID
		offerLeaseID = offer.LeaseID
	}
	sess.claimMu.Unlock()

	if offer == nil || offerTaskID != taskID {
		log.Printf("[GRPC] Worker %s accepted task %s but no matching pending offer", workerID, taskID)
		return
	}

	// Verify the lease_id the worker is accepting matches what we offered.
	declaredLeaseID := ta.GetLeaseId()
	if declaredLeaseID != "" && declaredLeaseID != offerLeaseID {
		log.Printf("[GRPC] Worker %s accepted task %s with mismatched lease_id: got %s, want %s",
			workerID, taskID, declaredLeaseID, offerLeaseID)
		return
	}

	// PR-2 (canonical-attempt-identity): the canonical attempt_id was
	// minted at Claim time (in ClaimNextWithAttemptAtomic inside the
	// same tx as the LEASED CAS + PENDING TaskAttempt INSERT). handleTaskAccepted
	// now CONSUMES the canonical attempt_id from the pending offer rather
	// than minting a new UUID, AND closes the canonical TaskAttempt
	// PENDING → RUNNING inside AcceptTaskAtomic's atomic tx.
	//
	// PR-04 / §9.5 invariant preserved: Task LEASED → RUNNING AND Attempt
	// PENDING → RUNNING commit in ONE transaction (AcceptTaskAtomic). The
	// pre-PR-2 INSERT pattern (Start + Create) had a crash window; PR-2's
	// earlier-minted PENDING row + this UPDATE path closes it.
	ctx := context.Background()
	attemptNum := offer.AttemptNumber
	attempt := &taskattempts.TaskAttempt{
		ID:            offer.AttemptID,
		TaskID:        taskID,
		JobID:         offer.JobID,
		WorkerID:      workerID,
		AttemptNumber: attemptNum,
		LeaseID:       declaredLeaseID,
		Status:        taskattempts.AttemptStatusRunning,
	}
	if err := h.taskRepo.AcceptTaskAtomic(ctx, attempt, offer.Revision); err != nil {
		if errors.Is(err, store.ErrTransitionConflict) {
			log.Printf("[GRPC] Worker %s accepted task %s but lease is stale or canonical attempt drift (offer.attempt_id=%s offer.attempt_number=%d attempt_id=%s attempt_number=%d) rev=%d — dropping TaskAccepted",
				workerID, taskID, offer.AttemptID, offer.AttemptNumber, attempt.ID, attemptNum, offer.Revision)
			// Stale lease: clear the pending offer so the next
			// ClaimNextReadyTask can re-offer this task.
			sess.claimMu.Lock()
			if sess.pendingTaskOffer != nil && sess.pendingTaskOffer.ID == taskID {
				sess.pendingTaskOffer = nil
			}
			sess.claimMu.Unlock()
		} else {
			log.Printf("[GRPC] AcceptTaskAtomic (LEASED→RUNNING + Attempt PENDING→RUNNING) failed for %s (worker %s): %v — keeping pending offer for retry",
				taskID, workerID, err)
			// Non-stale error: keep pendingTaskOffer so the next
			// TaskAccepted from the worker can retry the same offer
			// without a fresh ClaimNextReadyTask roundtrip.
		}
		return
	}

	// Send typed TaskLeaseGranted via sendCh.
	env := &pb.MasterToWorkerEnvelope{
		MessageId:       fmt.Sprintf("taskgrant-%s-%s", workerID, taskID),
		WorkerId:        workerID,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Msg: &pb.MasterToWorkerEnvelope_TaskLeaseGranted{
			TaskLeaseGranted: &pb.TaskLeaseGranted{
				TaskId:    taskID,
				JobId:     offer.JobID,
				LeaseId:   offer.LeaseID,
				AttemptId: attempt.ID,
			},
		},
	}

	if !safeSend(sess.sendCh, &outboundMessage{Envelope: env}) {
		log.Printf("[GRPC] sendCh full/closed for TaskLeaseGranted to worker %s — releasing claim for task %s",
			workerID, taskID)
		if releaseErr := h.taskRepo.ReleaseLease(ctx, taskID, workerID, offer.LeaseID); releaseErr != nil {
			log.Printf("[GRPC] Failed to release claim for task %s after grant send failure: %v", taskID, releaseErr)
		}
		sess.claimMu.Lock()
		if sess.pendingTaskOffer != nil && sess.pendingTaskOffer.ID == taskID {
			sess.pendingTaskOffer = nil
		}
		sess.claimMu.Unlock()
		return
	}

	// Clear pending offer under claimMu — task is now running on this worker.
	sess.claimMu.Lock()
	if sess.pendingTaskOffer != nil && sess.pendingTaskOffer.ID == taskID {
		sess.pendingTaskOffer = nil
	}
	sess.claimMu.Unlock()
}

// handleTaskRejected processes typed TaskRejected — PR #4 task-native push mode.
// Releases the claimed task back to READY for another worker.
//
// fix/task-rejected-lease-identity: validates the full identity tuple
// (task_id, attempt_id, lease_id, worker_id) against the stored task
// state BEFORE calling ReleaseLease. The pre-validation provides clear
// log diagnostics for stale rejects. The TOCTOU gap between taskRepo.Get()
// and taskRepo.ReleaseLease() is now CLOSED by ReleaseLease's CAS gate
// on (worker_id, lease_id) — a stale reject cannot release a task already
// reassigned to a different worker/lease.
//
// The revision field from the proto is not used as a CAS gate because
// the worker sends revision=0 (it does not know the master-side revision
// at reject time — the offer was never accepted). This is documented and
// expected; the (worker_id, lease_id) tuple is the primary gate.
func (h *Handler) handleTaskRejected(workerID string, tr *pb.TaskRejected) {
	taskID := tr.GetTaskId()
	reason := tr.GetReason()
	if taskID == "" {
		return
	}

	// fix/task-rejected-lease-identity: read the full identity tuple
	// from the typed proto and validate against the authoritative
	// task state BEFORE calling ReleaseLease.
	attemptID := tr.GetAttemptId()
	leaseID := tr.GetLeaseId()

	// h.taskRepo is wired at construction by bootstrap.go; nil here is a
	// programmer error (bootstrap must reject nil taskRepo before
	// creating the Handler).
	ctx := context.Background()
	t, err := h.taskRepo.Get(ctx, taskID)
	if err != nil || t == nil {
		log.Printf("[GRPC] TaskRejected from worker %s for task %s — task not found (reason=%q)",
			workerID, taskID, reason)
		h.clearPendingOfferForTask(workerID, taskID)
		return
	}

	// Guard 1: ownership — the rejecting worker must still own the task.
	// A stale reject arriving after the master's reaper reassigned the
	// task to a different worker must be silently dropped.
	if t.WorkerID != workerID {
		log.Printf("[GRPC] TaskRejected from worker %s for task %s refused — task now owned by %s (reason=%q)",
			workerID, taskID, t.WorkerID, reason)
		h.clearPendingOfferForTask(workerID, taskID)
		return
	}

	// Guard 2: lease identity — the reported lease_id must match the
	// stored lease_id. A stale reject from a previous lease cycle must
	// NOT release a task that was re-leased to a different lease.
	if leaseID != "" && t.LeaseID != "" && t.LeaseID != leaseID {
		log.Printf("[GRPC] TaskRejected from worker %s for task %s refused — lease mismatch (reported=%s stored=%s, reason=%q)",
			workerID, taskID, leaseID, t.LeaseID, reason)
		h.clearPendingOfferForTask(workerID, taskID)
		return
	}

	// Guard 3: attempt identity — the reported attempt_id must match the
	// canonical attempt stamped at Claim time (defense-in-depth; the
	// lease_id gate above is the primary defence).
	if attemptID != "" && t.AttemptID != "" && t.AttemptID != attemptID {
		log.Printf("[GRPC] TaskRejected from worker %s for task %s refused — attempt mismatch (reported=%s stored=%s, reason=%q)",
			workerID, taskID, attemptID, t.AttemptID, reason)
		h.clearPendingOfferForTask(workerID, taskID)
		return
	}

	log.Printf("[GRPC] Worker %s rejected task %s (attempt=%s lease=%s): %s",
		workerID, taskID, attemptID, leaseID, reason)

	if err := h.taskRepo.ReleaseLease(ctx, taskID, workerID, leaseID); err != nil {
		log.Printf("[GRPC] Failed to release rejected task %s: %v", taskID, err)
	}

	// Clear pending offer under claimMu.
	h.clearPendingOfferForTask(workerID, taskID)
}

// clearPendingOfferForTask removes the pending offer for a task if the
// worker still holds it. Safe to call when sess is nil (no-op). Extracted
// from handleTaskRejected so every early-return path clears the offer
// without duplicating the claimMu lock dance.
func (h *Handler) clearPendingOfferForTask(workerID, taskID string) {
	sess := h.getSession(workerID)
	if sess == nil {
		return
	}
	sess.claimMu.Lock()
	if sess.pendingTaskOffer != nil && sess.pendingTaskOffer.ID == taskID {
		sess.pendingTaskOffer = nil
	}
	sess.claimMu.Unlock()
}

// handleTaskResult processes typed TaskResult — feat/task-report-ingestion.
//
// PR-29 (`feat/task-report-ingestion`): the handler is now a thin relay
// to TaskReportIngestionService.Ingest, which centralizes the audit-cloused
// sequence:
//
//  1. atomic Task + Attempt close (TransitionTaskToTerminalAtomic)
//  2. worker-declared output_artifacts registration (with idempotent
//     skip on duplicate (task_id, artifact_id))
//  3. Job roll-up to AWAITING_ARTIFACT (all sibling tasks SUCCEEDED) or
//     FAILED (any sibling task FAILED) when the roll-up condition holds
//
// The handler pre-validates the identity tuple (presence of task_id,
// attempt_id, lease_id) before delegating. A nil ingestionSvc is treated
// as a misconfiguration and surfaces as a structured error log rather
// than a silent no-op — better to fail loud than to leak TaskResults
// without ever closing the Attempt.
func (h *Handler) handleTaskResult(workerID string, tr *pb.TaskResult) {
	taskID := tr.GetTaskId()
	if taskID == "" {
		log.Printf("[GRPC] TaskResult from worker %s missing task_id — dropping", workerID)
		return
	}
	attemptID := tr.GetAttemptId()
	leaseID := tr.GetLeaseId()
	if attemptID == "" || leaseID == "" {
		log.Printf("[GRPC] TaskResult from worker %s refused — missing identity (task=%q attempt=%q lease=%q)",
			workerID, taskID, attemptID, leaseID)
		return
	}

	log.Printf("[GRPC] Worker %s reported task %s (attempt %s): status=%s code=%q detail=%q, %d output artifacts",
		workerID, taskID, attemptID, tr.GetStatus(), tr.GetErrorCode(), tr.GetErrorDetail(), len(tr.GetOutputArtifacts()))

	if h.ingestionSvc == nil {
		log.Printf("[GRPC] TaskResult from worker %s REJECTED — ingestionSvc not wired (boot misconfig)", workerID)
		return
	}

	// Translate protobuf output_artifacts (Struct items) into the typed
	// DeclaredArtifact slice. Metadata is best-effort JSON.
	declared := make([]ingest.DeclaredArtifact, 0, len(tr.GetOutputArtifacts()))
	for _, item := range tr.GetOutputArtifacts() {
		m := item.AsMap()
		artID, _ := m["artifact_id"].(string)
		if artID == "" {
			continue
		}
		artType, _ := m["artifact_type"].(string)
		path, _ := m["artifact_path"].(string)
		var size int64
		if v, ok := m["size_bytes"].(float64); ok {
			size = int64(v)
		} else if v, ok := m["artifact_size"].(float64); ok {
			size = int64(v)
		}
		sha, _ := m["sha256"].(string)
		d := ingest.DeclaredArtifact{
			ArtifactID:   artID,
			ArtifactType: artType,
			Path:         path,
			Size:         size,
			SHA256:       sha,
			Metadata:     m,
		}
		declared = append(declared, d)
	}

	// Scorecard v1 / F1 — typed execution-metrics hoisting. Build the
	// 3 typed Go structs from the wire payload (see handler_jobs_metrics.go
	// for derivation rules + logs). They flow through IngestCommand to
	// IngestTaskResult, which persists them under the per-task mutex
	// immediately after the atomic close-write.
	typedMetrics := executionMetricsToAttemptMetrics(attemptID, tr.GetExecutionMetrics())
	typedCache := deriveCacheStats(attemptID, typedMetrics)
	typedCost := executionMetricsToCostBasis(attemptID, tr.GetExecutionMetrics())

	ctx := context.Background()

	// PR-2 / attempt_number wire-strict-compare — the canonical attempt
	// exists in task_attempts for (task_id, worker_id, lease_id); we
	// resolve its attempt_number here so ValidateIdentityTuple's cheap
	// field-presence check "AttemptNumber must be >0" no longer fires
	// on the wire. If the canonical lookup misses (pseudo-spoof /
	// impersonation), we pass 0 — the validator will surface the lookup-
	// miss sentinel or the cheap-check error and the audit-closure path
	// short-circuits cleanly. A LOOKUP ERROR is logged (distinct from
	// "no row found") so a future DB outage doesn't masquerade as a
	// wire-spoof in operator logs.
	var attemptNumber int32
	if h.taskAttemptRepo != nil {
		att, lookErr := h.taskAttemptRepo.GetByTaskIDAndWorkerAndLease(
			ctx, taskID, workerID, leaseID,
		)
		switch {
		case lookErr != nil:
			log.Printf("[GRPC] canonical attempt lookup failed for task=%s worker=%s lease=%s: %v (validator will likely fail cheap-check)", taskID, workerID, leaseID, lookErr)
		case att == nil:
			// No canonical row — handle as before (validator drops).
		default:
			attemptNumber = int32(att.AttemptNumber)
		}
	}

	res, err := h.ingestionSvc.IngestTaskResult(ctx, ingest.IngestCommand{
		TaskID:          taskID,
		AttemptID:       attemptID,
		AttemptNumber:   attemptNumber,
		LeaseID:         leaseID,
		WorkerID:        workerID,
		JobID:           tr.GetJobId(),
		Status:          tr.GetStatus(),
		ErrorCode:       tr.GetErrorCode(),
		ErrorDetail:     tr.GetErrorDetail(),
		OutputArtifacts: declared,
		TypedMetrics:    typedMetrics,
		CacheStats:      typedCache,
		CostBasis:       typedCost,
	})
	if err != nil {
		log.Printf("[GRPC] TaskResult ingest for task=%s attempt=%s FAILED: %v", taskID, attemptID, err)
		return
	}
	log.Printf("[GRPC] TaskResult ingest for task=%s done: closed=%v artNew=%d artSkip=%d jobXn=%v jobStatus=%q",
		taskID, res.AttemptClosed, res.ArtifactsNew, res.ArtifactsSkips, res.JobTransitioned, res.JobNewStatus)
}

// handleTaskRenewal processes a typed TaskLeaseRenewal via gRPC stream.
// PR-03 / fix/task-lease-renewal-protocol: canonical task-native
// renewal path. The worker sends the lease identity tuple
// (task_id, lease_id) plus a hint expiry. We look up the current
// task to obtain the live revision, then issue taskRepo.RenewLease
// with the CAS tuple (task_id, worker_id, lease_id, revision,
// status IN ('LEASED','RUNNING')) — no attempt_id predicate: PR-2's
// AcceptTaskAtomic is the SOLE writer of tasks.attempt_id and a
// worker cannot hold two different attempt_ids for the same task
// concurrently. The (worker_id, lease_id) gate closes the TOCTOU
// race against reaper-reset alone.
//
// No session lock is required: the audit invariant (CAS gates the
// database-side race) holds without serializing against the offer
// pipeline, and read-then-CAS is a safe pattern under SQLite's
// optimistic concurrency.
func (h *Handler) handleTaskRenewal(workerID string, tr *pb.TaskLeaseRenewal) {
	ctx := context.Background()
	taskID := tr.GetTaskId()
	leaseID := tr.GetLeaseId()

	if taskID == "" || leaseID == "" {
		log.Printf("[GRPC] TaskLeaseRenewal from worker %s refused — missing identity (task=%q lease=%q)",
			workerID, taskID, leaseID)
		return
	}

	t, err := h.taskRepo.Get(ctx, taskID)
	if err != nil || t == nil {
		log.Printf("[GRPC] TaskLeaseRenewal task %s not found: %v", taskID, err)
		return
	}

	expiry := time.Now().UTC().Add(30 * time.Minute)
	if tr.GetRequestedExpiry() != nil {
		expiry = tr.GetRequestedExpiry().AsTime()
	}

	if err := h.taskRepo.RenewLease(ctx, taskID, workerID, leaseID, expiry, t.Revision); err != nil {
		log.Printf("[GRPC] TaskLeaseRenewal failed for %s (worker %s lease %s): %v",
			taskID, workerID, leaseID, err)
		return
	}
	log.Printf("[GRPC] TaskLeaseRenewal extended task %s for worker %s lease=%s", taskID, workerID, leaseID)
}


