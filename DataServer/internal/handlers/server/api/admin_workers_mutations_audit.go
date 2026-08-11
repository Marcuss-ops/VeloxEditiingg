package api

import (
	"context"
	"encoding/json"
	"time"

	"velox-server/internal/fleet"
	"velox-server/internal/store"
)

// newMutationOperation constructs the queued audit operation without
// changing the operation schema or payload contract.
func newMutationOperation(workerID, kind string, req MutationRequest, now time.Time, operationID string) *store.Operation {
	payload := json.RawMessage("{}")
	if kind == fleet.OperationKindUpdate {
		payload, _ = json.Marshal(map[string]string{"target_digest": req.TargetDigest})
	} else if kind == fleet.OperationKindRestart {
		payload, _ = json.Marshal(map[string]any{
			"audio_mix_strategy": req.AudioMixStrategy,
			"audio_mix_profile":  req.AudioMixProfile,
		})
	}
	return &store.Operation{
		OperationID: operationID,
		WorkerID:    workerID,
		Op:          kind,
		RequestedBy: "admin",
		Reason:      req.Reason,
		Payload:     payload,
		QueuedAt:    now,
	}
}

// publishMutation publishes the audit operation and, for resume, releases
// only the CAS claim owned by this operation when publication fails.
func (h *AdminWorkersMutationsHandler) publishMutation(ctx context.Context, kind, workerID string, op *store.Operation) (publishErr, cleanupErr error) {
	publishErr = h.publisher.PublishOperation(ctx, op)
	if publishErr != nil && kind == fleet.OperationKindResume {
		cleanupErr = h.reg.ClearWorkerResumingIfOwner(ctx, workerID, op.OperationID)
	}
	return publishErr, cleanupErr
}
