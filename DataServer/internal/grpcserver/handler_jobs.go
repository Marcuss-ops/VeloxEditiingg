package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"velox-server/internal/ingest"
	"velox-server/internal/jobs"
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
		} else {
			log.Printf("[GRPC] AcceptTaskAtomic (LEASED→RUNNING + Attempt PENDING→RUNNING) failed for %s (worker %s): %v — keeping pending offer for retry",
				taskID, workerID, err)
		}
		sess.claimMu.Lock()
		if sess.pendingTaskOffer != nil && sess.pendingTaskOffer.ID == taskID {
			sess.pendingTaskOffer = nil
		}
		sess.claimMu.Unlock()
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
		if releaseErr := h.taskRepo.ReleaseLease(ctx, taskID); releaseErr != nil {
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
func (h *Handler) handleTaskRejected(workerID string, tr *pb.TaskRejected) {
	taskID := tr.GetTaskId()
	reason := tr.GetReason()
	if taskID == "" {
		return
	}

	// Verify the worker owns this task before releasing.
	if !h.verifyTaskOwnership(workerID, taskID) {
		log.Printf("[GRPC] TaskRejected from worker %s for task %s refused — ownership mismatch (reason=%q)",
			workerID, taskID, reason)
		return
	}
	log.Printf("[GRPC] Worker %s rejected task %s: %s", workerID, taskID, reason)

	ctx := context.Background()
	if err := h.taskRepo.ReleaseLease(ctx, taskID); err != nil {
		log.Printf("[GRPC] Failed to release rejected task %s: %v", taskID, err)
	}

	// Clear pending offer under claimMu.
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

	ctx := context.Background()
	res, err := h.ingestionSvc.IngestTaskResult(ctx, ingest.IngestCommand{
		TaskID:          taskID,
		AttemptID:       attemptID,
		LeaseID:         leaseID,
		WorkerID:        workerID,
		JobID:           tr.GetJobId(),
		Status:          tr.GetStatus(),
		ErrorCode:       tr.GetErrorCode(),
		ErrorDetail:     tr.GetErrorDetail(),
		OutputArtifacts: declared,
	})
	if err != nil {
		log.Printf("[GRPC] TaskResult ingest for task=%s attempt=%s FAILED: %v", taskID, attemptID, err)
		return
	}
	log.Printf("[GRPC] TaskResult ingest for task=%s done: closed=%v artNew=%d artSkip=%d jobXn=%v jobStatus=%q",
		taskID, res.AttemptClosed, res.ArtifactsNew, res.ArtifactsSkips, res.JobTransitioned, res.JobNewStatus)
}

// maybeTransitionJob checks whether all tasks for a job are terminal and,
// if so, transitions the Job downstream. The terminal-flip path is
// intentionally split:
//
//   - All tasks SUCCEEDED ⇒ write jobs.StatusAwaitingArtifact (NOT
//     SUCCEEDED). The verified-finalization path in
//     artifacts.FinalizeVerified is the SOLE legal writer of
//     jobs.StatusSucceeded — it dispatches the actual flip once the
//     worker-reported artifact is verified and registered. See
//     internal/artifacts/scan_test.go for the audit lock.
//
//   - Any task FAILED or the Job is in a terminal-broken state ⇒
//     write jobs.StatusFailed directly. FAILED is a terminal that
//     can be written by this handler because it never had a
//     counterpart concurrent writer — there is only one place that
//     fails a Job intentionally (this helper), and there is no
//     artifact path that flips a Job to FAILED.
//
//   - Already at AWAITING_ARTIFACT or terminal ⇒ no-op (idempotent
//     re-call; the artifact path will close it).
//
// PR-02 closes audit §P0.2 (two competing writers of
// jobs.StatusSucceeded) by reserving that flip exclusively for the
// verified-finalization contract.
//
// PR #5: this helper runs asynchronously (via `go maybeTransitionJob`
// from handleTaskResult) to avoid blocking the gRPC loop.
func (h *Handler) maybeTransitionJob(ctx context.Context, jobID string) {
	tasks, err := h.taskRepo.List(ctx, taskgraph.Filter{JobIDs: []string{jobID}})
	if err != nil || len(tasks) == 0 {
		log.Printf("[GRPC] maybeTransitionJob: cannot list tasks for job %s: %v", jobID, err)
		return
	}

	allTerminal := true
	allSucceeded := true
	for _, t := range tasks {
		if !t.Status.IsTerminal() {
			allTerminal = false
			break
		}
		if t.Status != taskgraph.StatusSucceeded {
			allSucceeded = false
		}
	}

	if !allTerminal {
		return // Some tasks still running; nothing to do.
	}

	// Determine the Job's downstream status. PR-02: SUCCEEDED is
	// reserved for the verified-finalization path. This helper writes
	// only AWAITING_ARTIFACT (success path) or FAILED (failure path).
	var newStatus jobs.Status
	if allSucceeded {
		newStatus = jobs.StatusAwaitingArtifact
	} else {
		newStatus = jobs.StatusFailed
	}

	// Read the job's current revision for CAS.
	job, err := h.jobsRepo.Get(ctx, jobID)
	if err != nil || job == nil {
		log.Printf("[GRPC] maybeTransitionJob: cannot get job %s: %v", jobID, err)
		return
	}
	if job.Status.IsTerminal() {
		return // Already terminal; nothing to do.
	}
	// PR-02 idempotency: if the Job is already AWAITING_ARTIFACT (e.g.
	// a sibling task result fired this helper first), the verified-
	// finalization path will close it. Avoid a spurious re-write that
	// would trigger an unnecessary revision bump or a recurrence of
	// pre-PR-02 sometimes-flicker behavior on rapid-fire results.
	if job.Status == jobs.StatusAwaitingArtifact && newStatus == jobs.StatusAwaitingArtifact {
		return
	}

	if err := h.jobsRepo.SetStatus(ctx, jobID, job.Status, newStatus); err != nil {
		log.Printf("[GRPC] maybeTransitionJob: failed to transition job %s to %s: %v", jobID, newStatus, err)
		return
	}
	log.Printf("[GRPC] maybeTransitionJob: job %s transitioned to %s (all tasks terminal)", jobID, newStatus)
}

// verifyTaskOwnership checks that taskID currently belongs to workerID.
func (h *Handler) verifyTaskOwnership(workerID, taskID string) bool {
	if workerID == "" || taskID == "" {
		return false
	}
	t, err := h.taskRepo.Get(context.Background(), taskID)
	if err != nil || t == nil {
		return false
	}
	return t.WorkerID == workerID
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

// verifyJobOwnership checks that `jobID` currently belongs to `workerID`
// by querying the task table (PR #7). The Job struct no longer carries
// WorkerID — tasks carry the per-execution state.
func (h *Handler) verifyJobOwnership(workerID, jobID string) bool {
	if workerID == "" || jobID == "" {
		return false
	}
	// Check if any task for this job is currently held by workerID.
	tasks, err := h.taskRepo.List(context.Background(), taskgraph.Filter{JobIDs: []string{jobID}})
	if err != nil || len(tasks) == 0 {
		return false
	}
	for _, t := range tasks {
		if t.WorkerID == workerID && !t.Status.IsTerminal() {
			return true
		}
	}
	return false
}

// lookupJobCASFields fetches the (revision, attempt) tuple required for the
// StartJob CAS. Uses the canonical jobs.Job via Jobs().Get() — revision is
// on the domain model (Ondata 3 PR3 final), attempt maps to Attempts/RetryCount.
// No more map-based reads from dbStore.GetJob.
func (h *Handler) lookupJobCASFields(jobID string) (revision, attempt int, err error) {
	j, err := h.jobsRepo.Get(context.Background(), jobID)
	if err != nil {
		return 0, 0, err
	}
	if j == nil {
		return 0, 0, fmt.Errorf("job %s not found", jobID)
	}
	return j.Revision, j.Attempts, nil
}
