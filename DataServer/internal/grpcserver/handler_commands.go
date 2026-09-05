// Package grpcserver / handler_commands.go
//
// Command dispatch logic for the WorkerControl gRPC handler, sliced
// out of handler.go so the stream lifecycle + main message loop stay
// focused on transport concerns.
//
// dispatchCommands reads pending commands from SQLite for the
// worker, wraps each in a typed Command envelope with OnSent
// callback, queues them on the session's sendCh (sessionWriter
// drains), and only after a successful Stream.Send invokes
// MarkCommandDelivered on the CommandManager.
//
// Nil cmdMgr is safe: returns immediately — this lets protocol-level
// tests and boot-dry-run handlers operate without a command manager.
package grpcserver

import (
	"fmt"
	"time"

	"velox-server/internal/logging"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
)

// registerCommandWake attaches a wake channel for the given worker. The
// channel is buffered (cap 1) and non-blocking: a notify while a wake is
// already pending collapses into one dispatch. Sessions call this on stream
// start and must call the returned deregister func on teardown.
func (h *Handler) registerCommandWake(workerID string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	h.mu.Lock()
	if h.commandWake == nil {
		h.commandWake = make(map[string]map[chan struct{}]struct{})
	}
	if h.commandWake[workerID] == nil {
		h.commandWake[workerID] = make(map[chan struct{}]struct{})
	}
	h.commandWake[workerID][ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		if set, ok := h.commandWake[workerID]; ok {
			delete(set, ch)
			if len(set) == 0 {
				delete(h.commandWake, workerID)
			}
		}
		h.mu.Unlock()
	}
}

// NotifyCommand wakes command dispatch for every live session of workerID.
// Called by command producers (bootstrap wires workersreg.CommandManager's
// wake hook to this) immediately after a worker_commands row is persisted,
// so the command reaches a connected worker within milliseconds instead of
// waiting for the heartbeat cadence or the 1-second dispatch ticker (which
// remains purely as a loss-recovery backstop).
//
// Notify is non-blocking and lossy by design: a wake while a dispatch is
// already pending collapses into one dispatch, and a session that misses the
// wake still picks the command up on its next ticker/heartbeat cycle.
func (h *Handler) NotifyCommand(workerID string) {
	h.mu.RLock()
	set := h.commandWake[workerID]
	channels := make([]chan struct{}, 0, len(set))
	for ch := range set {
		channels = append(channels, ch)
	}
	h.mu.RUnlock()
	for _, ch := range channels {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// dispatchCommands reads pending commands from SQLite for the worker,
// sends each as a typed Command via sendCh, and marks only successfully
// sent commands as delivered. Commands that fail to send remain in pending
// state for retry on the next dispatch cycle.
//
// Nil cmdMgr is safe: returns immediately — this lets protocol-level
// tests and boot-dry-run handlers operate without a command manager.
func (h *Handler) dispatchCommands(workerID string, sess *workerSession) {
	if h.cmdMgr == nil {
		return
	}
	cmds := h.cmdMgr.GetPendingCommands(workerID)
	if len(cmds) == 0 {
		return
	}

	logGRPCf(ctxForTaskSession(sess), logging.LevelInfo, logging.CodeGRPCCommandDispatch, "[GRPC] Dispatching %d pending commands to worker %s", len(cmds), workerID)

	for _, cmd := range cmds {
		var params *structpb.Struct
		if cmd.Params != nil {
			params, _ = structpb.NewStruct(cmd.Params)
		}

		ts, err := time.Parse(time.RFC3339, cmd.Timestamp)
		if err != nil {
			ts = time.Now().UTC()
		}

		env := &pb.MasterToWorkerEnvelope{
			MessageId:       fmt.Sprintf("cmd-%s-%s", workerID, cmd.CommandID),
			WorkerId:        workerID,
			SessionId:       sess.sessionID,
			SentAt:          timestamppb.Now(),
			ProtocolVersion: controltransport.ProtocolVersionCurrent,
			Msg: &pb.MasterToWorkerEnvelope_Command{
				Command: &pb.Command{
					CommandId: cmd.CommandID,
					Command:   cmd.Command,
					Timestamp: timestamppb.New(ts),
					Params:    params,
				},
			},
		}

		// Issue 5 fix: send via sendCh — non-blocking (sessionWriter drains).
		// Issue 3 fix: only mark as delivered AFTER a successful stream.Send
		// via the OnSent callback (gap #1 fix — the real write happens in
		// sessionWriter, not here).
		cmdID := cmd.CommandID // capture for closure
		out := &outboundMessage{
			Envelope: env,
			OnSent: func() {
				if cmdID != "" {
					if err := h.cmdMgr.MarkCommandDelivered(cmdID); err != nil {
						logGRPCf(ctxForTaskSession(sess), logging.LevelWarn, logging.CodeGRPCCommandFailed, "[GRPC] Failed to mark command %s delivered: %v", cmdID, err)
					}
				}
			},
		}
		if !safeSend(sess.sendCh, out) {
			logGRPCf(ctxForTaskSession(sess), logging.LevelWarn, logging.CodeGRPCCommandFailed, "[GRPC] sendCh full/closed — dropping command %s for worker %s (will retry)", cmd.CommandID, workerID)
			continue
		}
	}
}
