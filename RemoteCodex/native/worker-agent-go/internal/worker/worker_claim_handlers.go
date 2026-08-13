package worker

import (
	"context"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/pkg/api"
)

// worker_claim_handlers.go owns the per-message dispatch helpers of
// the receive loop: command conversion (msgToCommand / getIntParam),
// the typed TaskAccepted / TaskRejected senders, and the pending-task
// map store/take primitives. The receive-loop orchestrator lives in
// worker_claimloop.go.

/* PR-protobuf-refactor: msgToJob + msgToJobFromProto removed — pb.JobOffer
   no longer exists. TaskOffer is now the canonical dispatch path. */

// msgToCommand converts a ControlMessage (MsgCommand) to an api.WorkerCommand using typed proto fields.
func msgToCommand(msg controltransport.ControlMessage) api.WorkerCommand {
	cmd, ok := msg.TypedPayload.(*pb.Command)
	if !ok || cmd == nil {
		return api.WorkerCommand{}
	}

	ts := ""
	if cmd.GetTimestamp() != nil {
		ts = cmd.GetTimestamp().AsTime().UTC().Format(time.RFC3339)
	}

	wc := api.WorkerCommand{
		CommandID: cmd.GetCommandId(),
		Command:   cmd.GetCommand(),
		Timestamp: ts,
	}
	if p := cmd.GetParams(); p != nil {
		wc.Payload = p.AsMap()
	}
	return wc
}

/* PR-protobuf-refactor: sendAccept + sendReject removed — legacy
   JobAccepted/JobRejected messages no longer have transport encoding.
   Task-native sendTaskAccepted + sendTaskReject are the canonical path. */

// sendTaskAccepted sends a typed TaskAccepted message via the transport.
func (w *Worker) sendTaskAccepted(ctx context.Context, offer *pb.TaskOffer) error {
	acceptMsg := controltransport.NewTypedMessage(
		controltransport.MsgTaskAccepted,
		w.config.WorkerID,
		w.config.ProtocolVersion,
		&pb.TaskAccepted{
			TaskId:        offer.GetTaskId(),
			JobId:         offer.GetJobId(),
			AttemptId:     offer.GetAttemptId(),
			LeaseId:       offer.GetLeaseId(),
			AttemptNumber: offer.GetAttemptNumber(),
			Revision:      offer.GetRevision(),
		},
	)
	return w.transport.Send(ctx, acceptMsg)
}

// sendTaskReject sends a typed TaskRejected message via the transport.
func (w *Worker) sendTaskReject(ctx context.Context, taskID, jobID, attemptID, leaseID, reason string, attemptNumber, revision int32) error {
	rejectMsg := controltransport.NewTypedMessage(
		controltransport.MsgTaskRejected,
		w.config.WorkerID,
		w.config.ProtocolVersion,
		&pb.TaskRejected{
			TaskId:        taskID,
			JobId:         jobID,
			AttemptId:     attemptID,
			LeaseId:       leaseID,
			Reason:        reason,
			AttemptNumber: attemptNumber,
			Revision:      revision,
		},
	)
	return w.transport.Send(ctx, rejectMsg)
}

// storePendingTask records a TaskOffer-accepted task awaiting
// TaskLeaseGranted before executeTask dispatch (PR-2 canonical-attempt-
// identity). Keyed by task_id via pendingTasks / pendingTasksMu.
func (w *Worker) storePendingTask(taskID string, pte *PendingTaskExecution) {
	w.pendingTasksMu.Lock()
	defer w.pendingTasksMu.Unlock()
	if w.IsStopped() {
		return
	}
	w.pendingTasks[taskID] = pte
}

// takePendingTask retrieves and removes a pending task by task_id.
// Returns nil if the task was not found.
func (w *Worker) takePendingTask(taskID string) *PendingTaskExecution {
	w.pendingTasksMu.Lock()
	defer w.pendingTasksMu.Unlock()
	if w.IsStopped() {
		return nil
	}
	pte := w.pendingTasks[taskID]
	delete(w.pendingTasks, taskID)
	return pte
}
