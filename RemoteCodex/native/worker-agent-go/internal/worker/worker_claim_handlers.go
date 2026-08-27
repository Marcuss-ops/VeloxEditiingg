package worker

import (
	"context"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/api"
)

// worker_claim_handlers.go owns the per-message dispatch helpers of
// the receive loop: command conversion (msgToCommand / getIntParam),
// the typed TaskAccepted / TaskRejected senders, and the pending-task
// map store/take primitives. The receive-loop orchestrator lives in
// worker_claimloop.go.

// PendingOfferDecision classifies how a TaskOffer maps against the
// existing pending task for the same task_id. Returned by storePendingTask
// so the caller can emit the correct metric and log without duplicating
// the comparison logic.
type PendingOfferDecision int

const (
	// OfferInserted means no previous pending task existed — the offer
	// was stored as a new entry.
	OfferInserted PendingOfferDecision = iota

	// OfferDuplicate means the incoming offer is identity-equal to the
	// existing pending task (same attempt_id, attempt_number, lease_id,
	// revision). The existing entry is kept and the caller should
	// re-send TaskAccepted without touching the pending map.
	OfferDuplicate

	// OfferReplaced means the incoming offer has a strictly newer
	// attempt_number (or same attempt_number with a higher revision) and
	// the existing entry was replaced.
	OfferReplaced

	// OfferStale means the incoming offer has a lower attempt_number than
	// the existing pending task. The offer is rejected without modifying
	// the map.
	OfferStale

	// OfferIdentityConflict means the incoming offer has the same
	// attempt_number but an incompatible lease_id or revision, indicating
	// a master-side identity conflict. The existing entry is kept.
	OfferIdentityConflict
)

// comparePendingOffer evaluates an incoming offer against an existing
// pending task and returns the appropriate PendingOfferDecision.
func comparePendingOffer(existing, incoming *PendingTaskExecution) PendingOfferDecision {
	if incoming.AttemptNumber > existing.AttemptNumber {
		return OfferReplaced
	}
	if incoming.AttemptNumber < existing.AttemptNumber {
		return OfferStale
	}
	// Same attempt_number — check lease_id and revision.
	if incoming.LeaseID == existing.LeaseID && incoming.Revision == existing.Revision {
		return OfferDuplicate
	}
	// Same attempt_number but mismatched lease or revision — identity conflict.
	return OfferIdentityConflict
}

// recordOfferDecision emits the appropriate Prometheus counter for a
// PendingOfferDecision. Call it inside the pendingTasksMu critical section
// or immediately after, while the decision is still fresh.
func recordOfferDecision(decision PendingOfferDecision) {
	m := telemetry.GetPrometheusMetrics()
	switch decision {
	case OfferInserted:
		// No counter needed for the happy path — the normal flow handles it.
	case OfferDuplicate:
		m.RecordOfferDuplicate()
	case OfferReplaced:
		m.RecordOfferReplaced()
	case OfferStale:
		m.RecordOfferStale()
	case OfferIdentityConflict:
		m.RecordOfferIdentityConflict()
	}
}

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
//
// If a pending task already exists for taskID, the decision is classified
// by comparePendingOffer: identical offers are deduplicated (the caller
// re-sends TaskAccepted), stale offers are rejected, newer offers replace
// the existing entry, and identity-conflicting offers are logged but left
// untouched.
func (w *Worker) storePendingTask(taskID string, pte *PendingTaskExecution) PendingOfferDecision {
	w.pendingTasksMu.Lock()
	defer w.pendingTasksMu.Unlock()
	if w.IsStopped() {
		return OfferStale
	}
	existing, ok := w.pendingTasks[taskID]
	if !ok {
		w.pendingTasks[taskID] = pte
		recordOfferDecision(OfferInserted)
		return OfferInserted
	}

	decision := comparePendingOffer(existing, pte)
	recordOfferDecision(decision)

	switch decision {
	case OfferInserted:
		w.pendingTasks[taskID] = pte
	case OfferDuplicate:
		// Keep existing — caller re-sends TaskAccepted.
	case OfferReplaced:
		w.pendingTasks[taskID] = pte
	case OfferStale:
		// Reject — existing is newer.
	case OfferIdentityConflict:
		// Keep existing — log at call site.
	}
	return decision
}

// takePendingTask retrieves and removes a pending task by task_id.
// Returns nil if the task was not found. Successful retrieval increments
// the offer_reconciled_total counter — this is the point where a previously
// accepted offer transitions from pending to execution.
func (w *Worker) takePendingTask(taskID string) *PendingTaskExecution {
	w.pendingTasksMu.Lock()
	defer w.pendingTasksMu.Unlock()
	if w.IsStopped() {
		return nil
	}
	pte := w.pendingTasks[taskID]
	delete(w.pendingTasks, taskID)
	if pte != nil {
		telemetry.GetPrometheusMetrics().RecordOfferReconciled()
	}
	return pte
}
