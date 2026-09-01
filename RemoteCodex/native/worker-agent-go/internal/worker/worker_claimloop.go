package worker

// worker_claimloop.go owns only the canonical receive-loop router for
// master→worker control messages. Per-domain message handling lives in:
//   - worker_claim_task_messages.go     — task offer + lease grant
//   - worker_claim_prefetch_messages.go — future-asset plan + cancellation
//   - worker_claim_control_messages.go  — commands/config/drain/acks
// Connection-state helpers live in worker_connection_state.go.

import (
	"context"

	"velox-shared/controltransport"
)

// receiveLoop processes incoming messages from the transport receive channel.
// This switch remains the single canonical master→worker routing surface; the
// case bodies are delegated by domain so admission, prefetch, and control-plane
// responsibilities do not accumulate in one monolithic function.
func (w *Worker) receiveLoop(ctx context.Context, recvCh <-chan controltransport.ControlMessage) {
	defer w.wg.Done()

	w.logger.Info("[RECEIVE] Receive loop started — waiting for messages from master")

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("[RECEIVE] Receive loop exiting (context done)")
			return
		case <-w.stopChan:
			w.logger.Info("[RECEIVE] Receive loop exiting (stop signal)")
			return
		case msg, ok := <-recvCh:
			if !ok {
				w.logger.Warn("[RECEIVE] Receive channel closed — transport disconnected")
				return
			}

			switch msg.Type {
			case controltransport.MsgTaskOffer:
				w.handleTaskOfferMessage(ctx, msg)
			case controltransport.MsgFutureAssetPlan:
				w.handleFutureAssetPlanMessage(ctx, msg)
			case controltransport.MsgCancelPrefetch:
				w.handleCancelPrefetchMessage(msg)
			case controltransport.MsgTaskLeaseGranted:
				w.handleTaskLeaseGrantedMessage(ctx, msg)
			case controltransport.MsgCommand:
				w.handleCommandMessage(ctx, msg)
			case controltransport.MsgCancelJob:
				w.handleCancelJobMessage(msg)
			case controltransport.MsgDrain:
				w.handleDrainMessage()
			case controltransport.MsgConfigurationUpdate:
				w.handleConfigurationUpdateMessage(msg)
			case controltransport.MsgLeaseRevoked:
				w.handleLeaseRevokedMessage(msg)
			case controltransport.MsgPing:
				w.sendHeartbeat(ctx)
			case controltransport.MsgHelloAck:
				w.logger.Debug("[RECEIVE] HelloAck received — session confirmed")
			case controltransport.MsgTaskResultAck:
				w.handleTaskResultAckMessage(msg)
			case controltransport.MsgArtifactUploadPlan, controltransport.MsgTaskCommitAck, controltransport.MsgArtifactEarlyUploadPlan:
				w.handleArtifactControlMessage(msg)
			default:
				w.logger.Debug("[RECEIVE] Unhandled message type: %s", msg.Type)
			}
		}
	}
}
