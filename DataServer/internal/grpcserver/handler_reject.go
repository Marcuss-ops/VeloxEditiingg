// Package grpcserver / handler_reject.go
//
// TaskRejected handler + rejection helpers, sliced out of
// handler_jobs.go so each task-lifecycle message type owns a file.
package grpcserver

import (
	"context"

	"velox-server/internal/logging"
	"velox-server/internal/placement"
	"velox-server/internal/taskgraph"
	pb "velox-shared/controltransport/pb"
)

// handleTaskRejected processes typed TaskRejected — PR #4 task-native push mode.
// Releases the claimed task back to READY for another worker.
//
// fix/identity-tuple-mandatory: the full 6-field identity tuple is now
// MANDATORY. Every field must be present and non-empty / non-zero.
// Permissive "if x != """ guards replaced by strict field-presence checks.
func (h *Handler) handleTaskRejected(workerID string, tr *pb.TaskRejected, sess *workerSession) {
	if tr == nil || h.taskRepo == nil {
		return
	}
	taskID := tr.GetTaskId()
	jobID := tr.GetJobId()
	attemptID := tr.GetAttemptId()
	leaseID := tr.GetLeaseId()
	attemptNumber := tr.GetAttemptNumber()
	revision := tr.GetRevision()
	reason := tr.GetReason()

	ctx := context.Background()
	t, err := h.taskRepo.Get(ctx, taskID)
	if err != nil || t == nil {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCTaskRejectRefused, "[GRPC] TaskRejected from worker %s for task %s — task not found (reason=%q)", workerID, taskID, reason)
		return
	}
	masterIdentity := taskIdentityFromTask(t)
	if t.Status != taskgraph.StatusLeased {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCTaskRejectRefused, "[GRPC] TaskRejected from worker %s refused — task %s is not LEASED (status=%s)", workerID, taskID, t.Status)
		return
	}
	if sess == nil || sess.workerID != workerID {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCTaskRejectRefused, "[GRPC] TaskRejected from worker %s refused — stale or missing session for task %s", workerID, taskID)
		return
	}
	sess.claimMu.Lock()
	offer := sess.pendingTaskOffer
	sess.claimMu.Unlock()
	if offer == nil || offer.ID != taskID {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCTaskRejectRefused, "[GRPC] TaskRejected from worker %s refused — no matching pending offer for task %s", workerID, taskID)
		return
	}
	wireIdentity := taskIdentityFromWire(taskID, jobID, attemptID, leaseID, int(attemptNumber), int(revision), workerID)
	if err := validateTaskIdentity(wireIdentity, masterIdentity); err != nil {
		// Do not clear the pending offer on an invalid message: clearing is a
		// mutation and would let a replay/takeover message alter live state.
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCTaskRejectRefused, "[GRPC] TaskRejected from worker %s refused — identity validation failed for task %s: %v (reason=%q)", workerID, taskID, err, reason)
		return
	}
	if err := validateTaskIdentity(taskIdentityFromTask(&offer.Task), masterIdentity); err != nil {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCTaskRejectRefused, "[GRPC] TaskRejected from worker %s refused — pending offer for task %s is stale: %v", workerID, taskID, err)
		return
	}

	logGRPCf(ctx, logging.LevelInfo, logging.CodeGRPCTaskRejected, "[GRPC] Worker %s rejected task %s (attempt=%s lease=%s): %s", workerID, taskID, attemptID, leaseID, reason)

	// Hold the session registry read lock across the durable CAS and all
	// in-memory cleanup. A reconnect cannot replace this session between
	// ownership validation and release/offer mutation.
	h.mu.RLock()
	defer h.mu.RUnlock()
	if !h.isCurrentSessionLocked(workerID, sess) {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCTaskRejectRefused, "[GRPC] TaskRejected from worker %s refused — session was replaced before release for task %s", workerID, taskID)
		return
	}

	// Special-case: unsupported_executor — the worker rejected a task
	// it cannot execute because the executor is not in its registry.
	// This is a capability inconsistency between the placement snapshot
	// and the worker's actual runtime state. The session's executor
	// map is invalidated so the matcher won't pick this pair again.
	if reason == "unsupported_executor" {
		if h.handleUnsupportedExecutorRejection(ctx, workerID, t, sess) {
			h.clearPendingOfferForTask(sess, taskID)
		}
		return
	}

	if err := h.taskRepo.ReleaseLease(ctx, taskID, workerID, leaseID); err != nil {
		// A failed CAS means ownership was not proven. Do not mutate the
		// session offer; the current owner/session must reconcile it.
		logGRPCf(ctx, logging.LevelError, logging.CodeGRPCTaskRejectFailed, "[GRPC] Failed to release rejected task %s: %v", taskID, err)
		return
	}

	// Clear pending offer only after the lease release committed.
	h.clearPendingOfferForTask(sess, taskID)
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
	sess *workerSession,
) bool {
	executorKey := placement.NormalizeExecutorKey(t.ExecutorID, t.ExecutorVersion)

	logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCPlacement, "[PLACEMENT] Worker %s rejected task %s as unsupported_executor (executor=%s@%d) — capability inconsistency, invalidating for session", workerID, t.ID, t.ExecutorID, t.ExecutorVersion)

	// Release the lease first. ReleaseLease sets the task back to READY
	// and removes the PENDING attempt. The attempt_count is NOT
	// consumed: PENDING attempts that never started don't count
	// toward the retry budget. A failed CAS proves ownership was lost,
	// so no session capability state may be changed.
	if err := h.taskRepo.ReleaseLease(ctx, t.ID, workerID, t.LeaseID); err != nil {
		logGRPCf(ctx, logging.LevelError, logging.CodeGRPCPlacementFailed, "[PLACEMENT] ReleaseLease for unsupported_executor task %s failed: %v", t.ID, err)
		return false
	}

	// Invalidate this executor/version pair only after the durable release
	// succeeds, so a stale rejection cannot poison the live session.
	if sess == nil || sess.workerID != workerID {
		return false
	}
	sess.invalidateExecutor(executorKey)

	// Increment velox_placement_rejections_total{reason="unsupported_executor"}
	// via the PlacementRejectionSink (when wired).
	if h.placementRejectionSink != nil {
		h.placementRejectionSink.RecordPlacementRejection("unsupported_executor")
	}
	return true
}

// clearPendingOfferForTask removes the pending offer for a task if the
// worker still holds it. Safe to call when sess is nil (no-op). Extracted
// from handleTaskRejected so every early-return path clears the offer
// without duplicating the claimMu lock dance.
func (h *Handler) clearPendingOfferForTask(sess *workerSession, taskID string) {
	if sess == nil {
		return
	}
	sess.claimMu.Lock()
	if sess.pendingTaskOffer != nil && sess.pendingTaskOffer.ID == taskID {
		sess.pendingTaskOffer = nil
	}
	sess.claimMu.Unlock()
}
