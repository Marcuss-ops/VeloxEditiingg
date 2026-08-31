// Package grpcserver / handler_stream_session.go
//
// Session registry helpers and the single-writer output path for the
// WorkerControl bidi stream. Extracted from handler_stream.go so the
// Stream() lifecycle stays focused on handshake/admission/teardown.
package grpcserver

import (
	"context"
	"time"

	"velox-server/internal/logging"
	pb "velox-shared/controltransport/pb"
)

const disconnectedLeaseRecoveryGrace = 45 * time.Second

type sessionLeaseShortener interface {
	ShortenSessionLeases(context.Context, string, string, time.Time) (int, error)
}

func (h *Handler) shortenSessionLeases(sess *workerSession) {
	if sess == nil || h.taskRepo == nil {
		return
	}
	shortener, ok := h.taskRepo.(sessionLeaseShortener)
	if !ok {
		return
	}
	count, err := shortener.ShortenSessionLeases(context.Background(), sess.workerID, sess.sessionID, time.Now().UTC().Add(disconnectedLeaseRecoveryGrace))
	if err != nil {
		logGRPCf(context.Background(), logging.LevelWarn, logging.CodeGRPCSessionCleanupFailed, "[GRPC] Failed to shorten leases for disconnected worker %s session %s: %v", sess.workerID, sess.sessionID, err)
		return
	}
	if count > 0 {
		logGRPCf(context.Background(), logging.LevelInfo, logging.CodeGRPCSessionCleanupFailed, "[GRPC] Shortened %d active lease(s) for disconnected worker %s session %s; recovery grace=%s", count, sess.workerID, sess.sessionID, disconnectedLeaseRecoveryGrace)
	}
}

// closeOldSessionLocked removes any existing session for the given workerID
// and signals its notifier goroutine to stop. Must be called with h.mu held.
func (h *Handler) closeOldSessionLocked(workerID string) {
	oldSID, ok := h.workerSessions[workerID]
	if !ok {
		return
	}
	oldSess, exists := h.sessions[oldSID]
	if exists {
		logGRPCf(context.Background(), logging.LevelInfo, logging.CodeGRPCWorkerReconnecting, "[GRPC] Worker %s reconnecting — removing old session %s", workerID, oldSID)
		// P0 #6: close the done channel to stop the old notifier goroutine.
		// Messages from the old session's main loop are dropped by isCurrentSession().
		oldSess.doneOnce.Do(func() {
			close(oldSess.done)
		})
		// Issue 6 fix: cancel the old session's context to stop its goroutines.
		if oldSess.cancel != nil {
			oldSess.cancel()
		}
		h.shortenSessionLeases(oldSess)
		// PR #4: release any pendingTaskOffer held by the old session
		// so the claim is returned promptly on reconnect.
		oldSess.claimMu.Lock()
		if oldSess.pendingTaskOffer != nil {
			if releaseErr := h.taskRepo.ReleaseLease(context.Background(), oldSess.pendingTaskOffer.ID, oldSess.workerID, oldSess.pendingTaskOffer.LeaseID); releaseErr != nil {
				logGRPCf(context.Background(), logging.LevelWarn, logging.CodeGRPCSessionCleanupFailed, "[GRPC] Failed to release old pendingTaskOffer for task %s during reconnect: %v", oldSess.pendingTaskOffer.ID, releaseErr)
			}
			oldSess.pendingTaskOffer = nil
		}
		oldSess.claimMu.Unlock()
		// Issue 7 fix: revoke the old session in SQLite.
		if h.dbStore != nil {
			_ = h.dbStore.RevokeSession(oldSID)
		}
	}
	delete(h.sessions, oldSID)
	delete(h.workerSessions, workerID)
}

// isCurrentSession returns true if the given sessionID is still the active
// session for workerID. Used to drop messages from stale/zombie connections.
func (h *Handler) isCurrentSession(workerID, sessionID string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	sid, ok := h.workerSessions[workerID]
	return ok && sid == sessionID
}

// isCurrentSessionLocked is the lock-held variant used when a lifecycle CAS
// must be fenced against reconnect between ownership validation and mutation.
// It compares both the registry entry and pointer identity; the latter also
// keeps lightweight handler tests safe when a session has no generated ID.
// Callers must hold h.mu.RLock or h.mu.Lock.
func (h *Handler) isCurrentSessionLocked(workerID string, sess *workerSession) bool {
	if sess == nil || sess.workerID != workerID {
		return false
	}
	sid, ok := h.workerSessions[workerID]
	if !ok {
		return false
	}
	current, ok := h.sessions[sid]
	return ok && current == sess && (sess.sessionID == "" || sid == sess.sessionID)
}

// getSession returns the active session for a workerID, or nil if none.
func (h *Handler) getSession(workerID string) *workerSession {
	h.mu.RLock()
	defer h.mu.RUnlock()
	sid, ok := h.workerSessions[workerID]
	if !ok {
		return nil
	}
	return h.sessions[sid]
}

// sessionWriter is the sole goroutine allowed to call stream.Send().
// All message producers write outboundMessage values to sendCh; this
// goroutine drains and sends. OnSent callbacks are invoked after a
// successful stream.Send so that producers can confirm delivery only
// after the real network write.
//
// Exits when sendCh is closed (signaling session teardown) OR when a
// stream.Send() failure surfaces the error to the main loop via writerErr.
//
// Phase 4.2: a write failure MUST NOT be silently absorbed — publish to
// writerErr so the main loop tears the session down promptly. We also
// drain the channel before exiting so producers do not block on a full
// sendCh during the close sequence.
func (h *Handler) sessionWriter(sess *workerSession) {
	for out := range sess.sendCh {
		if err := sess.stream.Send(out.Envelope); err != nil {
			logGRPCf(sess.ctx, logging.LevelError, logging.CodeGRPCStreamWriterFailure, "[GRPC] sessionWriter send error for worker %s (session %s): %v", sess.workerID, sess.sessionID, err)
			// Best-effort publish (cap 1, non-blocking).
			select {
			case sess.writerErr <- err:
			default:
			}
			// Fast-drain remaining messages so producers attached to sendCh
			// are not blocked as the main loop winds down. We do NOT attempt
			// to resend them — they belong to a session that is about to die.
			// OnSent callbacks are NOT invoked for drained messages so
			// commands remain pending for retry on next dispatch cycle.
			for range sess.sendCh {
			}
			break
		}
		switch msg := out.Envelope.Msg.(type) {
		case *pb.MasterToWorkerEnvelope_TaskOffer:
			if msg.TaskOffer != nil {
				logGRPCf(sess.ctx, logging.LevelInfo, logging.CodeGRPCPlacement, "[GRPC] TaskOffer sent to worker %s (session %s): task=%s job=%s attempt=%s lease=%s", sess.workerID, sess.sessionID, msg.TaskOffer.GetTaskId(), msg.TaskOffer.GetJobId(), msg.TaskOffer.GetAttemptId(), msg.TaskOffer.GetLeaseId())
			}
		case *pb.MasterToWorkerEnvelope_TaskLeaseGranted:
			if msg.TaskLeaseGranted != nil {
				logGRPCf(sess.ctx, logging.LevelInfo, logging.CodeGRPCTaskAccepted, "[GRPC] TaskLeaseGranted sent to worker %s (session %s): task=%s job=%s attempt=%s lease=%s", sess.workerID, sess.sessionID, msg.TaskLeaseGranted.GetTaskId(), msg.TaskLeaseGranted.GetJobId(), msg.TaskLeaseGranted.GetAttemptId(), msg.TaskLeaseGranted.GetLeaseId())
			}
		}
		// Call OnSent callback after successful send (gap #1 fix).
		if out.OnSent != nil {
			out.OnSent()
		}
	}
	logGRPCf(sess.ctx, logging.LevelInfo, logging.CodeGRPCWorkerDisconnected, "[GRPC] sessionWriter exiting for worker %s (session %s)", sess.workerID, sess.sessionID)
}

// safeSend attempts to send an outboundMessage on the channel, returning true on success.
// Returns false if the channel is full or closed (uses recover to handle closed channel panic).
func safeSend(ch chan *outboundMessage, out *outboundMessage) bool {
	defer func() { recover() }()
	select {
	case ch <- out:
		return true
	default:
		return false
	}
}
