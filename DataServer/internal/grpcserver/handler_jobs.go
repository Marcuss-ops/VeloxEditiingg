package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"velox-server/internal/ingest"
	"velox-server/internal/placement"
	"velox-server/internal/store"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// handleTaskAccepted processes typed TaskAccepted — PR #4 task-native push mode.
// The lease was already created by ClaimNextReadyTask; we promote LEASED→RUNNING,
// create a TaskAttempt record, and grant the lease.
//
// fix/identity-tuple-mandatory: the full 6-field identity tuple
// (task_id, job_id, attempt_id, lease_id, attempt_number, revision) is
// now MANDATORY. The handler rejects any TaskAccepted with missing or
// zero-valued identity fields BEFORE touching the session or taskRepo.
func (h *Handler) handleTaskAccepted(workerID string, ta *pb.TaskAccepted) {
	if !h.config.PushMode {
		return
	}
	taskID := ta.GetTaskId()
	jobID := ta.GetJobId()
	attemptID := ta.GetAttemptId()
	leaseID := ta.GetLeaseId()
	attemptNumber := ta.GetAttemptNumber()
	revision := ta.GetRevision()

	if taskID == "" || jobID == "" || attemptID == "" || leaseID == "" || attemptNumber <= 0 {
		log.Printf("[GRPC] TaskAccepted from worker %s refused — incomplete identity (task=%q job=%q attempt=%q lease=%q attempt_num=%d rev=%d)",
			workerID, taskID, jobID, attemptID, leaseID, attemptNumber, revision)
		return
	}

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
	log.Printf("[GRPC] TaskAccepted received from worker %s: task=%s job=%s attempt=%s lease=%s offer_attempt=%s offer_lease=%s rev=%d",
		workerID, taskID, jobID, attemptID, leaseID, offer.AttemptID, offerLeaseID, revision)

	// Verify the lease_id and attempt_id the worker is accepting match
	// what we offered. The offer carries the canonical (attempt_id, lease_id)
	// pair minted by ClaimNextWithAttemptAtomic; the worker must echo both
	// back exactly.
	if leaseID != offerLeaseID {
		log.Printf("[GRPC] Worker %s accepted task %s with mismatched lease_id: got %s, want %s",
			workerID, taskID, leaseID, offerLeaseID)
		return
	}
	if attemptID != offer.AttemptID {
		log.Printf("[GRPC] Worker %s accepted task %s with mismatched attempt_id: got %s, want %s",
			workerID, taskID, attemptID, offer.AttemptID)
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
	attempt := &taskattempts.TaskAttempt{
		ID:            attemptID,
		TaskID:        taskID,
		JobID:         jobID,
		WorkerID:      workerID,
		AttemptNumber: int(attemptNumber),
		LeaseID:       leaseID,
		Status:        taskattempts.AttemptStatusRunning,
	}
	if err := h.taskRepo.AcceptTaskAtomic(ctx, attempt, int(revision)); err != nil {
		if errors.Is(err, store.ErrTransitionConflict) {
			log.Printf("[GRPC] Worker %s accepted task %s but lease is stale or canonical attempt drift (offer.attempt_id=%s offer.attempt_number=%d attempt_id=%s attempt_number=%d) rev=%d — dropping TaskAccepted",
				workerID, taskID, offer.AttemptID, offer.AttemptNumber, attempt.ID, attemptNumber, offer.Revision)
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

	// Send typed TaskLeaseGranted via sendCh with the full identity tuple.
	env := &pb.MasterToWorkerEnvelope{
		MessageId:       fmt.Sprintf("taskgrant-%s-%s", workerID, taskID),
		WorkerId:        workerID,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Msg: &pb.MasterToWorkerEnvelope_TaskLeaseGranted{
			TaskLeaseGranted: &pb.TaskLeaseGranted{
				TaskId:        taskID,
				JobId:         jobID,
				LeaseId:       offer.LeaseID,
				AttemptId:     attemptID,
				AttemptNumber: attemptNumber,
				Revision:      revision,
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
// fix/identity-tuple-mandatory: the full 6-field identity tuple is now
// MANDATORY. Every field must be present and non-empty / non-zero.
// Permissive "if x != """ guards replaced by strict field-presence checks.
func (h *Handler) handleTaskRejected(workerID string, tr *pb.TaskRejected) {
	taskID := tr.GetTaskId()
	jobID := tr.GetJobId()
	attemptID := tr.GetAttemptId()
	leaseID := tr.GetLeaseId()
	attemptNumber := tr.GetAttemptNumber()
	revision := tr.GetRevision()
	reason := tr.GetReason()

	if taskID == "" || jobID == "" || attemptID == "" || leaseID == "" || attemptNumber <= 0 {
		log.Printf("[GRPC] TaskRejected from worker %s refused — incomplete identity (task=%q job=%q attempt=%q lease=%q attempt_num=%d rev=%d reason=%q)",
			workerID, taskID, jobID, attemptID, leaseID, attemptNumber, revision, reason)
		h.clearPendingOfferForTask(workerID, taskID)
		return
	}

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
	if t.WorkerID != workerID {
		log.Printf("[GRPC] TaskRejected from worker %s for task %s refused — task now owned by %s (reason=%q)",
			workerID, taskID, t.WorkerID, reason)
		h.clearPendingOfferForTask(workerID, taskID)
		return
	}

	// Guard 2: lease identity — strict compare (both fields are mandatory).
	if t.LeaseID != leaseID {
		log.Printf("[GRPC] TaskRejected from worker %s for task %s refused — lease mismatch (reported=%s stored=%s, reason=%q)",
			workerID, taskID, leaseID, t.LeaseID, reason)
		h.clearPendingOfferForTask(workerID, taskID)
		return
	}

	// Guard 3: attempt identity — strict compare (both fields are mandatory).
	if t.AttemptID != attemptID {
		log.Printf("[GRPC] TaskRejected from worker %s for task %s refused — attempt mismatch (reported=%s stored=%s, reason=%q)",
			workerID, taskID, attemptID, t.AttemptID, reason)
		h.clearPendingOfferForTask(workerID, taskID)
		return
	}

	log.Printf("[GRPC] Worker %s rejected task %s (attempt=%s lease=%s): %s",
		workerID, taskID, attemptID, leaseID, reason)

	// Special-case: unsupported_executor — the worker rejected a task
	// it cannot execute because the executor is not in its registry.
	// This is a capability inconsistency between the placement snapshot
	// and the worker's actual runtime state. The session's executor
	// map is invalidated so the matcher won't pick this pair again.
	if reason == "unsupported_executor" {
		h.handleUnsupportedExecutorRejection(ctx, workerID, t)
		h.clearPendingOfferForTask(workerID, taskID)
		return
	}

	if err := h.taskRepo.ReleaseLease(ctx, taskID, workerID, leaseID); err != nil {
		log.Printf("[GRPC] Failed to release rejected task %s: %v", taskID, err)
	}

	// Clear pending offer under claimMu.
	h.clearPendingOfferForTask(workerID, taskID)
}

// handleUnsupportedExecutorRejection handles a task rejected with
// reason="unsupported_executor". The placement snapshot claimed the
// worker supported this executor but the worker disagreed at runtime.
//
// The handler:
//  1. Logs the capability inconsistency.
//  2. Invalidates the (executor_id, executor_version) pair in the
//     worker's session so the matcher won't offer it again.
//  3. Releases the lease — returns the task to READY without
//     consuming retry budget (PENDING attempts don't count).
//  4. Records a placement rejection metric placeholder.
func (h *Handler) handleUnsupportedExecutorRejection(
	ctx context.Context,
	workerID string,
	t *taskgraph.Task,
) {
	executorKey := placement.ExecutorKey{ID: t.ExecutorID, Version: t.ExecutorVersion}

	log.Printf("[PLACEMENT] Worker %s rejected task %s as unsupported_executor (executor=%s@%d) — capability inconsistency, invalidating for session",
		workerID, t.ID, t.ExecutorID, t.ExecutorVersion)

	// Invalidate this executor/version pair so the matcher won't
	// select another task with the same requirement for this session.
	sess := h.getSession(workerID)
	if sess != nil {
		sess.invalidateExecutor(executorKey)
	}

	// Release the lease. ReleaseLease sets the task back to READY
	// and removes the PENDING attempt. The attempt_count is NOT
	// consumed: PENDING attempts that never started don't count
	// toward the retry budget.
	if err := h.taskRepo.ReleaseLease(ctx, t.ID, workerID, t.LeaseID); err != nil {
		log.Printf("[PLACEMENT] ReleaseLease for unsupported_executor task %s failed: %v", t.ID, err)
	}

	// TODO: increment velox_placement_rejections_total{reason="unsupported_executor"}
	// when the Prometheus counter family is wired.
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
	jobID := tr.GetJobId()
	attemptID := tr.GetAttemptId()
	leaseID := tr.GetLeaseId()
	attemptNumber := tr.GetAttemptNumber()
	revision := tr.GetRevision()

	if taskID == "" || jobID == "" || attemptID == "" || leaseID == "" || attemptNumber <= 0 {
		log.Printf("[GRPC] TaskResult from worker %s refused — incomplete identity (task=%q job=%q attempt=%q lease=%q attempt_num=%d rev=%d)",
			workerID, taskID, jobID, attemptID, leaseID, attemptNumber, revision)
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

	// PR-2 / attempt_number wire-strict-compare — now sourced directly
	// from the proto (no longer resolved via a canonical lookup because
	// the worker sends the canonical attempt_number on the wire). The
	// revision is also consumed from the proto for CAS validation.
	//
	// A zero attempt_number (legacy worker) is rejected at the
	// field-presence check above; a non-zero value that mismatches the
	// canonical row triggers ErrIdentityMismatch in the ingestion
	// service's ValidateIdentityTuple.

	res, err := h.ingestionSvc.IngestTaskResult(ctx, ingest.IngestCommand{
		TaskID:          taskID,
		AttemptID:       attemptID,
		AttemptNumber:   attemptNumber,
		LeaseID:         leaseID,
		WorkerID:        workerID,
		JobID:           jobID,
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
// fix/identity-tuple-mandatory: the worker sends the full 6-field
// identity tuple on every renewal. We validate all fields are present
// then issue the CAS-backed RenewLease against the live DB revision.
func (h *Handler) handleTaskRenewal(workerID string, tr *pb.TaskLeaseRenewal) {
	ctx := context.Background()
	taskID := tr.GetTaskId()
	jobID := tr.GetJobId()
	attemptID := tr.GetAttemptId()
	leaseID := tr.GetLeaseId()
	attemptNumber := tr.GetAttemptNumber()
	renewalRevision := tr.GetRevision()

	if taskID == "" || jobID == "" || attemptID == "" || leaseID == "" || attemptNumber <= 0 {
		log.Printf("[GRPC] TaskLeaseRenewal from worker %s refused — incomplete identity (task=%q job=%q attempt=%q lease=%q attempt_num=%d rev=%d)",
			workerID, taskID, jobID, attemptID, leaseID, attemptNumber, renewalRevision)
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
