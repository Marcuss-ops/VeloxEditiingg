package worker

// worker_artifacts.go: typed pending-msg dispatcher for the Artifact
// Commit Protocol v1 (Fase 3.4 / 3.6). The executeTask pipeline
// registers a per-task buffered channel via registerPendingArtifactAck,
// blocks on waitForArtifactAck until the master's typed reply arrives,
// and the receiveLoop (worker_claimloop.go) routes MsgArtifactUploadPlan
// and MsgTaskCommitAck replies into that channel via
// dispatchTypedPlanOrAck. Stop() drains the entire map via
// drainPendingArtifactAcks so no goroutine leaks across sessions.
//
// Extracted from worker.go (commit 02d9dd6 → next).

import (
	"context"
	"errors"
	"time"

	"velox-shared/controltransport"
)

// ── Artifact Commit Protocol v1 — typed pending-msg dispatcher ──
//
// MsgArtifactUploadPlan and MsgTaskCommitAck are request/response
// replies to the executor pipeline. The pipeline registers a
// per-task channel before issuing TaskOutputDeclared /
// ArtifactUploadCompleted; the receive loop dispatches the master's
// reply into that channel and the pipeline unblocks.
//
// Timeout: the pipeline MUST call waitForArtifactAck with a
// deadline (typically lease_deadline - safety margin) — otherwise
// a master that never replies would leak the goroutine forever.
// The receive loop non-blocking-sends; if the channel is full
// (pipeline already abandoned the slot) the message is dropped
// with a WARN so a slow pipeline is observable.
//
// Concurrency: pendingArtifactAcks is protected by
// pendingArtifactAcksMu. Channels are buffered (cap 1) so the
// receive loop never blocks on a slow pipeline. Stale entries
// (channels the pipeline has already abandoned) are GC'd by
// unregisterPendingArtifactAck. Stop() drains the entire map.

func (w *Worker) registerPendingArtifactAck(taskID string) chan controltransport.ControlMessage {
	w.pendingArtifactAcksMu.Lock()
	defer w.pendingArtifactAcksMu.Unlock()
	if w.pendingArtifactAcks == nil {
		w.pendingArtifactAcks = make(map[string]chan controltransport.ControlMessage)
	}
	ch := make(chan controltransport.ControlMessage, 1)
	w.pendingArtifactAcks[taskID] = ch
	return ch
}

// waitForArtifactAck blocks until a message for taskID arrives on
// the dispatcher channel OR the deadline elapses. Returns the
// typed reply on success; nil + a timeout error on deadline. The
// caller MUST call unregisterPendingArtifactAck in a defer to keep
// the map tidy.
func (w *Worker) waitForArtifactAck(ctx context.Context, taskID string, deadline time.Time) (controltransport.ControlMessage, error) {
	ch := w.registerPendingArtifactAck(taskID)
	defer w.unregisterPendingArtifactAck(taskID)

	timeout := time.NewTimer(time.Until(deadline))
	defer timeout.Stop()

	select {
	case msg := <-ch:
		return msg, nil
	case <-timeout.C:
		return controltransport.ControlMessage{}, ErrArtifactAckTimeout
	case <-ctx.Done():
		return controltransport.ControlMessage{}, ctx.Err()
	}
}

// ErrArtifactAckTimeout is returned by waitForArtifactAck when the
// master fails to deliver the expected ArtifactUploadPlan /
// TaskCommitAck before the lease deadline. The pipeline surfaces
// this as a TaskResult with error_code="artifact_ack_timeout".
var ErrArtifactAckTimeout = errors.New("worker: timed out waiting for artifact ack")

func (w *Worker) dispatchTypedPlanOrAck(msg controltransport.ControlMessage) bool {
	taskID := extractTaskIDFromTyped(msg)
	if taskID == "" {
		w.logger.Warn("[ARTIFACT-ACK] typed reply with no GetTaskId (concrete=%T) — dropping msg=%s",
			msg.TypedPayload, msg.Type)
		return false
	}
	w.pendingArtifactAcksMu.RLock()
	ch, ok := w.pendingArtifactAcks[taskID]
	w.pendingArtifactAcksMu.RUnlock()
	if !ok {
		return false
	}
	// Non-blocking send. If the pipeline already abandoned the slot
	// the channel buffer holds the message and is GC'd on
	// unregister. We use select-default to avoid blocking the
	// receive loop on a stuck pipeline.
	select {
	case ch <- msg:
	default:
		w.logger.Warn("[ARTIFACT-ACK] dispatcher channel full for task=%s msg=%s — dropping",
			taskID, msg.Type)
	}
	return true
}

func (w *Worker) unregisterPendingArtifactAck(taskID string) {
	w.pendingArtifactAcksMu.Lock()
	defer w.pendingArtifactAcksMu.Unlock()
	if w.pendingArtifactAcks == nil {
		return
	}
	ch, ok := w.pendingArtifactAcks[taskID]
	if !ok {
		return
	}
	delete(w.pendingArtifactAcks, taskID)
	// Drain any leftover message so the channel can be GC'd.
	select {
	case <-ch:
	default:
	}
}

// drainPendingArtifactAcks is called by Stop() to release every
// registered channel + map entry. Prevents goroutine leaks on
// shutdown.
func (w *Worker) drainPendingArtifactAcks() {
	w.pendingArtifactAcksMu.Lock()
	defer w.pendingArtifactAcksMu.Unlock()
	cleared := len(w.pendingArtifactAcks)
	for taskID, ch := range w.pendingArtifactAcks {
		// Non-blocking drain to release any buffered message.
		select {
		case <-ch:
		default:
		}
		_ = taskID
	}
	w.pendingArtifactAcks = make(map[string]chan controltransport.ControlMessage)
	if cleared > 0 {
		w.logger.Info("[STOP] Drained pending artifact-ack dispatchers: %d", cleared)
	}
}

// extractTaskIDFromTyped pulls the (task_id) field from the typed
// proto payload of an ArtifactUploadPlan or TaskCommitAck message.
// Both messages satisfy the anonymous-interface check because the
// proto-generated `GetTaskId() string` accessor is on the pointer
// receiver.
func extractTaskIDFromTyped(msg controltransport.ControlMessage) string {
	switch p := msg.TypedPayload.(type) {
	case interface{ GetTaskId() string }:
		return p.GetTaskId()
	}
	return ""
}
