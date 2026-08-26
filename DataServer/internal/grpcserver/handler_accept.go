// Package grpcserver / handler_accept.go
//
// TaskAccepted handler + lease-grant helpers, sliced out of
// handler_jobs.go so each task-lifecycle message type owns a file.
package grpcserver

import (
	"context"
	"errors"
	"fmt"

	"velox-server/internal/forwardingstore"
	"velox-server/internal/logging"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-server/internal/telemetry"
	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"go.opentelemetry.io/otel/attribute"
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
func (h *Handler) handleTaskAccepted(workerID string, ta *pb.TaskAccepted, sess *workerSession) {
	if h.config == nil || !h.config.PushMode || ta == nil || sess == nil || h.taskRepo == nil || sess.workerID != workerID {
		return
	}
	taskID := ta.GetTaskId()
	jobID := ta.GetJobId()
	attemptID := ta.GetAttemptId()
	leaseID := ta.GetLeaseId()
	attemptNumber := ta.GetAttemptNumber()
	revision := ta.GetRevision()

	// Read-only session access happens before validation. No session or task
	// mutation is allowed until the authenticated worker and every wire field
	// match the current master-owned task row.
	sess.claimMu.Lock()
	offer := sess.pendingTaskOffer
	sess.claimMu.Unlock()

	masterTask, err := h.taskRepo.Get(context.Background(), taskID)
	if err != nil || masterTask == nil {
		logGRPCf(ctxForTaskSession(sess), logging.LevelWarn, logging.CodeGRPCTaskAcceptRefused, "[GRPC] TaskAccepted from worker %s refused — task %s not found", workerID, taskID)
		return
	}
	wireIdentity := taskIdentityFromWire(taskID, jobID, attemptID, leaseID, int(attemptNumber), int(revision), workerID)
	masterIdentity := taskIdentityFromTask(masterTask)
	if masterTask.Status == taskgraph.StatusRunning &&
		validateTaskIdentityShape(wireIdentity, "wire") == nil &&
		validateTaskIdentityShape(masterIdentity, "master task") == nil &&
		wireIdentity.TaskID == masterIdentity.TaskID &&
		wireIdentity.JobID == masterIdentity.JobID &&
		wireIdentity.AttemptID == masterIdentity.AttemptID &&
		wireIdentity.LeaseID == masterIdentity.LeaseID &&
		wireIdentity.AttemptNumber == masterIdentity.AttemptNumber &&
		wireIdentity.WorkerID == masterIdentity.WorkerID &&
		wireIdentity.Revision+1 == masterIdentity.Revision {
		// Idempotent replay after AcceptTaskAtomic committed but the
		// original grant was lost. This path sends only an ACK-like grant;
		// it never mutates the task, attempt, lease, or pending offer.
		h.mu.RLock()
		if h.isCurrentSessionLocked(workerID, sess) {
			h.sendTaskLeaseGranted(ctxForTaskSession(sess), sess, masterTask, jobID)
		}
		h.mu.RUnlock()
		return
	}
	if masterTask.Status != taskgraph.StatusLeased {
		logGRPCf(ctxForTaskSession(sess), logging.LevelWarn, logging.CodeGRPCTaskAcceptRefused, "[GRPC] TaskAccepted from worker %s refused — task %s is not LEASED (status=%s)", workerID, taskID, masterTask.Status)
		return
	}
	if offer == nil || offer.ID != taskID {
		logGRPCf(ctxForTaskSession(sess), logging.LevelWarn, logging.CodeGRPCTaskAcceptRefused, "[GRPC] Worker %s accepted task %s but no matching pending offer", workerID, taskID)
		return
	}
	if err := validateTaskIdentity(wireIdentity, masterIdentity); err != nil {
		logGRPCf(ctxForTaskSession(sess), logging.LevelWarn, logging.CodeGRPCTaskAcceptRefused, "[GRPC] TaskAccepted from worker %s refused — identity validation failed for task %s: %v", workerID, taskID, err)
		return
	}
	if err := validateTaskIdentity(taskIdentityFromTask(&offer.Task), masterIdentity); err != nil {
		logGRPCf(ctxForTaskSession(sess), logging.LevelWarn, logging.CodeGRPCTaskAcceptRefused, "[GRPC] TaskAccepted from worker %s refused — pending offer for task %s is stale: %v", workerID, taskID, err)
		return
	}
	logGRPCf(ctxForTaskSession(sess), logging.LevelInfo, logging.CodeGRPCTaskAccepted, "[GRPC] TaskAccepted received from worker %s: task=%s job=%s attempt=%s lease=%s offer_attempt=%s offer_lease=%s rev=%d", workerID, taskID, jobID, attemptID, leaseID, offer.AttemptID, offer.LeaseID, revision)

	// PR-2 (canonical-attempt-identity): the canonical attempt_id was
	// minted at Claim time (in ClaimNextWithAttemptAtomic inside the
	// same tx as the LEASED CAS + PENDING TaskAttempt INSERT). handleTaskAccepted
	// now CONSUMES the canonical attempt_id from the pending offer rather
	// than minting a new UUID, AND closes the canonical TaskAttempt
	// PENDING → RUNNING inside AcceptTaskAtomic's atomic tx.
	//
	// §9.5 invariant preserved: Task LEASED → RUNNING AND Attempt
	// PENDING → RUNNING commit in ONE transaction (AcceptTaskAtomic). The
	// pre-PR-2 INSERT pattern (Start + Create) had a crash window; PR-2's
	// earlier-minted PENDING row + this UPDATE path closes it.
	//
	// Scorecard v2 / Step 15: start a "claim_task" span for distributed
	// tracing. The trace context flows from the gRPC metadata through
	// the otelgrpc stats handler + W3C propagator.
	// Scorecard v2 / Step 15c: use session.ctx (derived from stream.Context())
	// so the span inherits the parent trace context from the worker.
	gRPCctx := context.Background()
	if sess != nil && sess.ctx != nil {
		gRPCctx = sess.ctx
	}
	ctx, span := telemetry.StartSpan(gRPCctx, "claim_task",
		attribute.String("velox.task_id", taskID),
		attribute.String("velox.worker_id", workerID),
		attribute.String("velox.attempt_id", attemptID),
	)
	defer span.End()

	attempt := &taskattempts.TaskAttempt{
		ID:            attemptID,
		TaskID:        taskID,
		JobID:         jobID,
		WorkerID:      workerID,
		AttemptNumber: int(attemptNumber),
		LeaseID:       leaseID,
		Status:        taskattempts.AttemptStatusRunning,
	}
	// Fence the durable transition to the authenticated stream session. The
	// read lock prevents closeOldSessionLocked from replacing this session
	// between the ownership check and the CAS; the pending offer is then
	// re-read so a concurrent claim update cannot be mistaken for this one.
	h.mu.RLock()
	defer h.mu.RUnlock()
	if !h.isCurrentSessionLocked(workerID, sess) {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCTaskAcceptRefused, "[GRPC] Worker %s accepted task %s refused — session was replaced before CAS", workerID, taskID)
		return
	}
	sess.claimMu.Lock()
	offer = sess.pendingTaskOffer
	sess.claimMu.Unlock()
	if offer == nil || offer.ID != taskID {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCTaskAcceptRefused, "[GRPC] Worker %s accepted task %s refused — pending offer disappeared before CAS", workerID, taskID)
		return
	}
	if err := validateTaskIdentity(taskIdentityFromTask(&offer.Task), masterIdentity); err != nil {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCTaskAcceptRefused, "[GRPC] Worker %s accepted task %s refused — pending offer changed before CAS: %v", workerID, taskID, err)
		return
	}

	if err := h.taskRepo.AcceptTaskAtomic(ctx, attempt, int(revision)); err != nil {
		if errors.Is(err, forwardingstore.ErrTransitionConflict) {
			logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCTaskAcceptRefused, "[GRPC] Worker %s accepted task %s but lease is stale or canonical attempt drift (offer.attempt_id=%s offer.attempt_number=%d attempt_id=%s attempt_number=%d) rev=%d — dropping TaskAccepted", workerID, taskID, offer.AttemptID, offer.AttemptNumber, attempt.ID, attemptNumber, offer.Revision)
			// Stale lease: clear the pending offer so the next
			// ClaimNextReadyTask can re-offer this task.
			sess.claimMu.Lock()
			if sess.pendingTaskOffer != nil && sess.pendingTaskOffer.ID == taskID {
				sess.pendingTaskOffer = nil
			}
			sess.claimMu.Unlock()
		} else {
			logGRPCf(ctx, logging.LevelError, logging.CodeGRPCTaskAcceptFailed, "[GRPC] AcceptTaskAtomic (LEASED→RUNNING + Attempt PENDING→RUNNING) failed for %s (worker %s): %v — keeping pending offer for retry", taskID, workerID, err)
			// Non-stale error: keep pendingTaskOffer so the next
			// TaskAccepted from the worker can retry the same offer
			// without a fresh ClaimNextReadyTask roundtrip.
		}
		return
	}
	// Send typed TaskLeaseGranted via sendCh with the full identity tuple.
	grantTask := *masterTask
	grantTask.Status = taskgraph.StatusRunning
	grantTask.Revision = int(revision) + 1
	grantTask.WorkerID = workerID
	grantTask.LeaseID = leaseID
	grantTask.AttemptID = attemptID
	grantTask.AttemptNumber = int(attemptNumber)
	if !h.sendTaskLeaseGranted(ctx, sess, &grantTask, jobID) {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCTaskAcceptFailed, "[GRPC] sendCh full/closed for TaskLeaseGranted to worker %s — releasing claim for task %s", workerID, taskID)
		if releaseErr := h.taskRepo.ReleaseLease(ctx, taskID, workerID, offer.LeaseID); releaseErr != nil {
			logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCTaskAcceptFailed, "[GRPC] Failed to release claim for task %s after grant send failure: %v", taskID, releaseErr)
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

func ctxForTaskSession(sess *workerSession) context.Context {
	if sess != nil && sess.ctx != nil {
		return sess.ctx
	}
	return context.Background()
}

// sendTaskLeaseGranted emits the post-transition task identity. It is used
// both after a successful accept and for a replay-only ACK; it never mutates
// the task or the pending offer itself.
func (h *Handler) sendTaskLeaseGranted(ctx context.Context, sess *workerSession, task *taskgraph.Task, jobID string) bool {
	if sess == nil || task == nil {
		return false
	}
	jobRevision := int32(0)
	if h.jobsRepo != nil {
		job, err := h.jobsRepo.Get(ctx, jobID)
		if err != nil {
			logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCTaskAcceptFailed, "[GRPC] Failed to load job revision for TaskLeaseGranted task=%s job=%s: %v", task.ID, jobID, err)
		} else if job != nil {
			jobRevision = int32(job.Revision)
		}
	}
	env := &pb.MasterToWorkerEnvelope{
		MessageId:       fmt.Sprintf("taskgrant-%s-%s", sess.workerID, task.ID),
		WorkerId:        sess.workerID,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Msg: &pb.MasterToWorkerEnvelope_TaskLeaseGranted{
			TaskLeaseGranted: &pb.TaskLeaseGranted{
				TaskId:        task.ID,
				JobId:         task.JobID,
				LeaseId:       task.LeaseID,
				AttemptId:     task.AttemptID,
				AttemptNumber: int32(task.AttemptNumber),
				Revision:      int32(task.Revision),
				JobRevision:   jobRevision,
			},
		},
	}
	return safeSend(sess.sendCh, &outboundMessage{Envelope: env})
}
