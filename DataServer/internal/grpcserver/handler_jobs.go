package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"velox-server/internal/jobs"
	"velox-server/internal/store"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// handleJobResult processes typed JobResult via gRPC stream.
//
// Artifact success gate (PR 1): a worker reporting status=success does NOT
// transition the job to SUCCEEDED. RecordRenderFinished logs the event while
// the job stays RUNNING. The actual SUCCEEDED transition is gated on the
// artifact service verifying and registering the artifact (see handleArtifactUploaded).
func (h *Handler) handleJobResult(workerID string, jr *pb.JobResult) {
	ctx := context.Background()
	jobID := jr.GetJobId()
	status := jr.GetStatus()
	errMsg := jr.GetError()

	if jobID == "" {
		log.Printf("[GRPC] JobResult from worker %s missing job_id — dropping", workerID)
		return
	}
	if !h.verifyJobOwnership(workerID, jobID) {
		log.Printf("[GRPC] JobResult from worker %s for job %s refused — ownership mismatch", workerID, jobID)
		return
	}

	if status == "success" {
		// Reject if lease_id or attempt is missing — these are required
		// for the identity CAS.
		leaseID := jr.GetLeaseId()
		attempt := int(jr.GetAttempt())
		if leaseID == "" {
			log.Printf("[GRPC] JobResult success from worker %s for job %s refused — missing lease_id", workerID, jobID)
			return
		}
		if attempt == 0 {
			log.Printf("[GRPC] JobResult success from worker %s for job %s refused — missing attempt", workerID, jobID)
			return
		}

		// Look up revision from the DB (protobuf does not carry it).
		currentRev, _, revErr := h.lookupJobCASFields(jobID)
		if revErr != nil {
			log.Printf("[GRPC] JobResult success from worker %s for job %s — cannot read revision: %v", workerID, jobID, revErr)
			return
		}

		cmd := store.RecordRenderFinishedCommand{
			JobID:            jobID,
			WorkerID:         workerID,
			LeaseID:          leaseID,
			AttemptNumber:    attempt,
			ExpectedRevision: currentRev,
			FinishedAt:       time.Now().UTC(),
		}
		if err := h.jobsRepo.RecordRenderFinished(ctx, cmd.JobID, cmd.WorkerID, cmd.LeaseID, cmd.AttemptNumber, cmd.ExpectedRevision); err != nil {
			log.Printf("[GRPC] RecordRenderFinished failed for %s: %v", jobID, err)
			return
		}
		log.Printf("[GRPC] Worker %s reported render finished for job %s — awaiting artifact", workerID, jobID)
	} else if status == "failed" {
		if err := h.jobsRepo.FailWithRetry(context.Background(), jobID, "", errMsg, true, 0); err != nil {
			log.Printf("[GRPC] Job failure transition failed for %s: %v", jobID, err)
		}
	}
}// handleJobAccepted processes typed JobAccepted — legacy job-based push mode.
// Kept for backward compat with pre-PR #4 workers. New workers use TaskAccepted.
func (h *Handler) handleJobAccepted(workerID string, ja *pb.JobAccepted) {
	if !h.config.PushMode {
		return
	}
	jobID := ja.GetJobId()

	sess := h.getSession(workerID)
	if sess == nil {
		return
	}

	// Legacy path: no pendingJobOffer on task-native sessions.
	log.Printf("[GRPC] Worker %s sent JobAccepted for job %s — legacy path (no-op in task-native dispatch)", workerID, jobID)
}

// handleJobRejected processes typed JobRejected — legacy job-based push mode.
// Kept for backward compat with pre-PR #4 workers. New workers use TaskRejected.
func (h *Handler) handleJobRejected(workerID string, jr *pb.JobRejected) {
	jobID := jr.GetJobId()
	reason := jr.GetReason()
	if jobID == "" {
		return
	}
	log.Printf("[GRPC] Worker %s rejected job %s — legacy path (no-op in task-native dispatch, reason=%q)", workerID, jobID, reason)
}

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

	// Promote LEASED → RUNNING atomically.
	ctx := context.Background()
	attemptNum := offer.AttemptCount + 1
	if err := h.taskRepo.Start(ctx, taskID, workerID, declaredLeaseID, attemptNum, offer.Revision); err != nil {
		if errors.Is(err, store.ErrTransitionConflict) {
			log.Printf("[GRPC] Worker %s accepted task %s but lease is stale (rev=%d) — rejecting",
				workerID, taskID, offer.Revision)
			sess.claimMu.Lock()
			if sess.pendingTaskOffer != nil && sess.pendingTaskOffer.ID == taskID {
				sess.pendingTaskOffer = nil
			}
			sess.claimMu.Unlock()
			return
		}
		log.Printf("[GRPC] StartTask (LEASED→RUNNING) failed for %s (worker %s): %v — keeping pending offer for retry",
			taskID, workerID, err)
		return
	}

	// Create TaskAttempt record.
	attempt := &taskattempts.TaskAttempt{
		ID:            uuid.NewString(),
		TaskID:        taskID,
		JobID:         offer.JobID,
		WorkerID:      workerID,
		AttemptNumber: attemptNum,
		LeaseID:       declaredLeaseID,
		Status:        taskattempts.AttemptStatusRunning,
	}
	if err := h.taskAttemptRepo.Create(ctx, attempt); err != nil {
		log.Printf("[GRPC] Failed to create TaskAttempt for task %s: %v (task is RUNNING, attempt record missing)", taskID, err)
		// Non-fatal: task is already RUNNING. Continue.
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

// handleTaskResult processes typed TaskResult — PR #5 task-native reporting.
// Receives the worker's execution report and transitions the TaskAttempt
// and Task accordingly.
func (h *Handler) handleTaskResult(workerID string, tr *pb.TaskResult) {
	taskID := tr.GetTaskId()
	if taskID == "" {
		log.Printf("[GRPC] TaskResult from worker %s missing task_id — dropping", workerID)
		return
	}

	status := tr.GetStatus()
	log.Printf("[GRPC] Worker %s reported task %s: status=%s code=%q detail=%q",
		workerID, taskID, status, tr.GetErrorCode(), tr.GetErrorDetail())

	ctx := context.Background()
	jobID := tr.GetJobId()

	// Log output artifacts if present (PR #5).
	if artifacts := tr.GetOutputArtifacts(); len(artifacts) > 0 {
		log.Printf("[GRPC] TaskResult for %s includes %d output artifacts", taskID, len(artifacts))
	}

	if status == "succeeded" {
		// Mark task SUCCEEDED.
		t, err := h.taskRepo.Get(ctx, taskID)
		if err != nil || t == nil {
			log.Printf("[GRPC] TaskResult for unknown task %s: %v", taskID, err)
			return
		}
		if err := h.taskRepo.SetStatus(ctx, taskID, taskgraph.StatusRunning, taskgraph.StatusSucceeded, t.Revision); err != nil {
			log.Printf("[GRPC] TaskResult success transition failed for %s: %v", taskID, err)
			return
		}
		// Update the attempt record.
		if activeAttempt, err := h.taskAttemptRepo.GetActiveAttempt(ctx, taskID); err == nil && activeAttempt != nil {
			_ = h.taskAttemptRepo.CompleteFinal(ctx, activeAttempt.ID, workerID, activeAttempt.LeaseID, taskattempts.AttemptStatusSucceeded, "", "", activeAttempt.ReportVersion)
		}
	} else {
		// Task failed.
		t, err := h.taskRepo.Get(ctx, taskID)
		if err != nil || t == nil {
			log.Printf("[GRPC] TaskResult for unknown task %s: %v", taskID, err)
			return
		}
		if err := h.taskRepo.Fail(ctx, taskID, tr.GetErrorDetail(), t.Revision); err != nil {
			log.Printf("[GRPC] TaskResult fail transition failed for %s: %v", taskID, err)
			return
		}
		// Update the attempt record.
		if activeAttempt, err := h.taskAttemptRepo.GetActiveAttempt(ctx, taskID); err == nil && activeAttempt != nil {
			_ = h.taskAttemptRepo.CompleteFinal(ctx, activeAttempt.ID, workerID, activeAttempt.LeaseID, taskattempts.AttemptStatusFailed, tr.GetErrorCode(), tr.GetErrorDetail(), activeAttempt.ReportVersion)
		}
	}

	// PR #5: after task transition, check if ALL tasks for this job are terminal.
	// If so, transition the job to SUCCEEDED (all tasks succeeded) or FAILED (any failed).
	if jobID != "" {
		go h.maybeTransitionJob(ctx, jobID)
	}
}

// maybeTransitionJob checks whether all tasks for a job are terminal and,
// if so, transitions the job to SUCCEEDED (all succeeded) or FAILED (any failed).
// PR #5: runs asynchronously after each task result to avoid blocking the gRPC loop.
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

	// Determine the job's terminal status.
	var newStatus jobs.Status
	if allSucceeded {
		newStatus = jobs.StatusSucceeded
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

// handleJobProgress tracks per-job progress (typed, forwarded via heartbeat for now).
//
// Phase 3.3: relaxed ownership gate — progress is informational and is
// only dropped when the worker definitely does not own the job. Mismatch
// scenarios are logged but not rejected (a worker mid-reconnect might
// legitimately race a late JobProgress against a freshly-reassigned job).
func (h *Handler) handleJobProgress(workerID string, jp *pb.JobProgress) {
	jobID := jp.GetJobId()
	if jobID == "" {
		return
	}
	if !h.verifyJobOwnership(workerID, jobID) {
		log.Printf("[GRPC] JobProgress from worker %s for job %s ignored — ownership mismatch",
			workerID, jobID)
		return
	}
	log.Printf("[GRPC] Worker %s progress on job %s: stage=%s %d%% (scene %d/%d)",
		workerID, jobID, jp.GetStage(), jp.GetProgressPercent(), jp.GetScene(), jp.GetTotalScenes())
}

// handleLeaseRenewal processes a typed LeaseRenewal via gRPC stream.
//
// Phase 3.3: verify the worker owns the job before extending the lease.
// Without this gate a malicious worker could renew (and thus keep alive
// forever) a job assigned to a different worker.
func (h *Handler) handleLeaseRenewal(workerID string, lr *pb.LeaseRenewal) {
	jobID := lr.GetJobId()
	if jobID == "" {
		return
	}
	if !h.verifyJobOwnership(workerID, jobID) {
		log.Printf("[GRPC] LeaseRenewal from worker %s for job %s refused — ownership mismatch", workerID, jobID)
		return
	}
	leaseExpiry := time.Now().UTC().Add(30 * time.Minute)
	if lr.GetLeaseExpiresAt() != nil {
		leaseExpiry = lr.GetLeaseExpiresAt().AsTime()
	}
	if err := h.jobsRepo.RenewLease(context.Background(), lr.GetJobId(), workerID, lr.GetLeaseId(), leaseExpiry, true, 0); err != nil {
		log.Printf("[GRPC] Lease renewal failed for job %s worker %s: %v", lr.GetJobId(), workerID, err)
	}
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
