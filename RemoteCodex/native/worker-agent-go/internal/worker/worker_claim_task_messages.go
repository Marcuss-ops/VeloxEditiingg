package worker

import (
	"context"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/pkg/api/renderplan"
)

// handleTaskOfferMessage validates a task-native offer, admits it against the
// worker capacity/executor contract, and stores it until the canonical lease
// grant arrives. Execution remains strictly gated on MsgTaskLeaseGranted.
func (w *Worker) handleTaskOfferMessage(ctx context.Context, msg controltransport.ControlMessage) {
	taskOffer, ok := msg.TypedPayload.(*pb.TaskOffer)
	if !ok || taskOffer == nil {
		w.logger.Warn("[RECEIVE] TaskOffer without typed payload")
		return
	}

	taskID := taskOffer.GetTaskId()
	attemptID := taskOffer.GetAttemptId()
	jobID := taskOffer.GetJobId()
	leaseID := taskOffer.GetLeaseId()
	attemptNumber := taskOffer.GetAttemptNumber()
	revision := taskOffer.GetRevision()
	if taskID == "" || jobID == "" || attemptID == "" || leaseID == "" || attemptNumber <= 0 {
		w.logger.Warn("[RECEIVE] TaskOffer refused — incomplete identity tuple (task=%q job=%q attempt=%q lease=%q attempt_num=%d rev=%d) — dropping",
			taskID, jobID, attemptID, leaseID, attemptNumber, revision)
		return
	}
	if w.IsStopped() || w.drainMode.Load() {
		if err := w.sendTaskReject(ctx, taskID, jobID, attemptID, leaseID, "draining", attemptNumber, revision); err != nil {
			w.logger.Warn("[RECEIVE] Failed to send TaskRejected: %v", err)
		}
		return
	}

	executorID := normalizeOfferedExecutorID(taskOffer.GetExecutorId())
	executorVersion := int(taskOffer.GetExecutorVersion())

	// Per-phase capacity check: classify the executor and enforce
	// phase-specific slot limits when configured. The flat
	// activeCount+pendingCount check remains as the fallback.
	taskPhase := classifyExecutor(executorID)
	capacity := w.capacitySnapshot(taskPhase)
	if !capacity.PhaseAvailable {
		w.logCapacityFull(capacity, "phase_gate")
		if err := w.sendTaskReject(ctx, taskID, jobID, attemptID, leaseID, "capacity_full", attemptNumber, revision); err != nil {
			w.logger.Warn("[RECEIVE] Failed to send TaskRejected (phase capacity %s): %v", taskPhase, err)
		}
		return
	}

	// Flat fallback: also check total active + pending against MaxActiveJobs.
	if !capacity.FlatAvailable {
		w.logCapacityFull(capacity, "flat_gate")
		if err := w.sendTaskReject(ctx, taskID, jobID, attemptID, leaseID, "capacity_full", attemptNumber, revision); err != nil {
			w.logger.Warn("[RECEIVE] Failed to send TaskRejected (capacity): %v", err)
		}
		return
	}

	if !w.executorRegistry.Has(executorID, executorVersion) {
		if err := w.sendTaskReject(ctx, taskID, jobID, attemptID, leaseID, "unsupported_executor", attemptNumber, revision); err != nil {
			w.logger.Warn("[RECEIVE] Failed to send TaskRejected (unsupported executor): %v", err)
		}
		return
	}

	// RenderPlan admission is intentionally before TaskAccepted: the worker
	// must execute a versioned compiled plan, not infer a timeline from
	// arbitrary legacy keys.
	admissionPayload := map[string]interface{}{}
	if tsp := taskOffer.GetTaskSpec(); tsp != nil {
		for key, value := range tsp.AsMap() {
			admissionPayload[key] = value
		}
	}
	admissionPayload["executor_id"] = executorID
	admissionPayload["executor_version"] = executorVersion
	if outputContract := taskOffer.GetOutputContract(); outputContract != nil {
		admissionPayload["output_contract"] = outputContract.AsMap()
	}
	if err := renderplan.ValidateTaskPayload(admissionPayload); err != nil {
		w.logger.Warn("[RECEIVE] TaskOffer refused — invalid versioned RenderPlan (task=%s): %v", taskID, err)
		if rejectErr := w.sendTaskReject(ctx, taskID, jobID, attemptID, leaseID, "invalid_render_plan", attemptNumber, revision); rejectErr != nil {
			w.logger.Warn("[RECEIVE] Failed to send TaskRejected (invalid render plan): %v", rejectErr)
		}
		return
	}

	w.logger.Info("[RECEIVE] TaskOffer received: task=%s attempt=%s job=%s executor=%s@%d — deferring executeTask to TaskLeaseGranted",
		taskID, attemptID, jobID, executorID, taskOffer.GetExecutorVersion())

	if err := w.sendTaskAccepted(ctx, taskOffer); err != nil {
		w.logger.Warn("[RECEIVE] Failed to send TaskAccepted: %v", err)
		return
	}

	var specPayload map[string]interface{}
	if tsp := taskOffer.GetTaskSpec(); tsp != nil {
		specPayload = tsp.AsMap()
	}
	pte := &PendingTaskExecution{
		TaskID:          taskID,
		JobID:           jobID,
		JobRevision:     int(taskOffer.GetJobRevision()),
		AttemptID:       attemptID,
		AttemptNumber:   int(attemptNumber),
		LeaseID:         leaseID,
		ExecutorID:      executorID,
		ExecutorVersion: int(taskOffer.GetExecutorVersion()),
		Revision:        int(revision),
		Spec: executor.TaskSpec{
			Version:    int(taskOffer.GetExecutorVersion()),
			JobID:      jobID,
			ExecutorID: executorID,
			Payload:    specPayload,
		},
	}

	decision := w.storePendingTask(taskID, pte)
	switch decision {
	case OfferDuplicate:
		w.logger.Warn("[RECEIVE] TaskOffer for task=%s is a duplicate of pending entry — re-sending TaskAccepted", taskID)
		if err := w.sendTaskAccepted(ctx, taskOffer); err != nil {
			w.logger.Warn("[RECEIVE] Failed to re-send TaskAccepted for duplicate: %v", err)
		}
	case OfferReplaced:
		w.logger.Info("[RECEIVE] TaskOffer for task=%s replaced stale pending entry (newer attempt)", taskID)
	case OfferStale:
		w.logger.Warn("[RECEIVE] TaskOffer for task=%s rejected — older than existing pending entry", taskID)
		if err := w.sendTaskReject(ctx, taskID, jobID, attemptID, leaseID, "stale_offer", attemptNumber, revision); err != nil {
			w.logger.Warn("[RECEIVE] Failed to send TaskRejected (stale): %v", err)
		}
	case OfferIdentityConflict:
		w.logger.Warn("[RECEIVE] TaskOffer for task=%s identity conflict (same attempt_number, different lease/revision)", taskID)
	case OfferInserted:
		// Happy path — already stored.
	}
}

// handleTaskLeaseGrantedMessage cross-validates the canonical lease identity
// against the pending task and starts execution only after the Master has
// committed the attempt to RUNNING.
func (w *Worker) handleTaskLeaseGrantedMessage(ctx context.Context, msg controltransport.ControlMessage) {
	taskGrant, ok := msg.TypedPayload.(*pb.TaskLeaseGranted)
	if !ok || taskGrant == nil {
		w.logger.Warn("[RECEIVE] TaskLeaseGranted without typed payload")
		return
	}
	taskID := taskGrant.GetTaskId()
	if taskID == "" {
		w.logger.Warn("[RECEIVE] TaskLeaseGranted without task_id — dropping")
		return
	}

	grantJobID := taskGrant.GetJobId()
	grantAttemptID := taskGrant.GetAttemptId()
	grantLeaseID := taskGrant.GetLeaseId()
	grantAttemptNumber := taskGrant.GetAttemptNumber()
	grantRevision := taskGrant.GetRevision()
	if grantJobID == "" || grantAttemptID == "" || grantLeaseID == "" || grantAttemptNumber <= 0 {
		w.logger.Warn("[RECEIVE] TaskLeaseGranted for task %s refused — incomplete identity (job=%q attempt=%q lease=%q attempt_num=%d rev=%d)",
			taskID, grantJobID, grantAttemptID, grantLeaseID, grantAttemptNumber, grantRevision)
		return
	}

	pte := w.takePendingTask(taskID)
	if pte == nil {
		w.logger.Warn("[RECEIVE] TaskLeaseGranted for unknown task %s — dropping", taskID)
		return
	}
	if grantJobID != pte.JobID || grantAttemptID != pte.AttemptID || grantLeaseID != pte.LeaseID || int(grantAttemptNumber) != pte.AttemptNumber {
		w.logger.Warn("[RECEIVE] TaskLeaseGranted for task %s identity mismatch against pending task (grant: job=%q attempt=%q lease=%q num=%d) vs (pending: job=%q attempt=%q lease=%q num=%d) — dropping",
			taskID, grantJobID, grantAttemptID, grantLeaseID, grantAttemptNumber, pte.JobID, pte.AttemptID, pte.LeaseID, pte.AttemptNumber)
		return
	}
	if grantJobRevision := int(taskGrant.GetJobRevision()); grantJobRevision > 0 {
		pte.JobRevision = grantJobRevision
	}

	w.AddActiveTaskLease(taskID, grantJobID, grantAttemptID, grantLeaseID, int(grantAttemptNumber), int(grantRevision))
	w.logger.Info("[RECEIVE] TaskLeaseGranted for task=%s attempt=%s job=%s lease=%s num=%d rev=%d — starting execution",
		taskID, grantAttemptID, grantJobID, grantLeaseID, grantAttemptNumber, grantRevision)

	go func() {
		defer w.RemoveActiveTaskLease(taskID)
		w.executeTask(w.taskContext(ctx), pte, taskID, grantAttemptID)
	}()
}
