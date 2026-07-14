// Package grpcserver / handler_stream.go
//
// Stream lifecycle and session management for the WorkerControl gRPC handler.
// Extracted from handler.go to keep the core types file focused.
package grpcserver

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"velox-server/internal/store"
	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Stream handles a bidirectional gRPC stream from a single worker.
// Receives WorkerToMasterEnvelope, sends MasterToWorkerEnvelope.
func (h *Handler) Stream(stream grpc.BidiStreamingServer[pb.WorkerToMasterEnvelope, pb.MasterToWorkerEnvelope]) error {
	// P0 security: extract worker identity from client certificate (mTLS).
	certWorkerID := h.extractWorkerIDFromStream(stream)

	// Wait for Hello message to identify the worker
	env, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("stream: recv hello: %w", err)
	}

	hello := env.GetHello()
	if hello == nil {
		return fmt.Errorf("stream: expected hello, got %T", env.Msg)
	}

	// P0 security: validate the declared worker_id against the client certificate.
	declaredWorkerID := env.WorkerId
	if certWorkerID != "" {
		if certWorkerID != declaredWorkerID {
			return fmt.Errorf("stream: worker_id mismatch: cert=%s, declared=%s", certWorkerID, declaredWorkerID)
		}
		log.Printf("[GRPC] Worker authenticated via mTLS: %s", certWorkerID)
	} else if !h.config.AllowInsecure {
		return fmt.Errorf("stream: insecure connections not allowed (set VELOX_GRPC_ALLOW_INSECURE_DEV=true for dev)")
	}

	// P0 security: gate the worker against VELOX_ALLOWED_WORKERS before
	// credential validation. Workers not in the allowlist receive
	// PermissionDenied — the transport-level error is surfaced to the
	// worker agent as a gRPC status code, and the connection is refused.
	//
	// This check runs AFTER cert identity verification but BEFORE
	// credential_hash validation and session creation, because an
	// unlisted worker should never reach the credential store.
	//
	// Using gRPC status.Errorf(codes.PermissionDenied) so the worker
	// and operator can distinguish "not allowed" from internal errors.
	if !h.authorizer.IsAllowed(declaredWorkerID) {
		return status.Errorf(codes.PermissionDenied,
			"worker %q is not in VELOX_ALLOWED_WORKERS", declaredWorkerID)
	}

	// P0 security: validate credential_hash against stored worker credentials
	if err := h.validateCredentialHash(declaredWorkerID, hello.GetCredentialHash()); err != nil {
		return fmt.Errorf("stream: credential validation failed: %w", err)
	}

	workerID := declaredWorkerID
	sessionID := fmt.Sprintf("grpc-%s-%d", workerID, time.Now().UnixNano())

	// Issue 6 fix: create a cancellable context for the session.
	// Scorecard v2 / Step 15c: derive from stream.Context() so the
	// session inherits the trace context propagated by otelgrpc.
	sessionCtx, sessionCancel := context.WithCancel(stream.Context())

	// Issue 5 fix: create the send channel (buffered to avoid blocking producers).
	sendCh := make(chan *outboundMessage, 64)

	// Register session — keyed by sessionID to prevent defer from deleting a newer session.
	h.mu.Lock()
	h.closeOldSessionLocked(workerID)

	sess := &workerSession{
		workerID:  workerID,
		sessionID: sessionID,
		stream:    stream,
		done:      make(chan struct{}),
		cancel:    sessionCancel,
		ctx:       sessionCtx,
		sendCh:    sendCh,
		writerErr: make(chan error, 1),
	}
	h.sessions[sessionID] = sess
	h.workerSessions[workerID] = sessionID
	h.mu.Unlock()

	log.Printf("[GRPC] Worker %s connected (session: %s, name: %s)", workerID, sessionID, hello.GetWorkerName())

	// Extract supported_job_types from Hello capabilities for ClaimNext filtering.
	// Placement: parse typed executor capabilities from the worker's
	// capability report and store them on the session. A worker whose
	// executors block is missing or malformed is NOT eligible for
	// placement and must be rejected at registration.
	if hello.GetCapabilities() != nil {
		capsMap := hello.GetCapabilities().AsMap()
		if types := extractSupportedJobTypes(capsMap); len(types) > 0 {
			sess.supportedJobTypes.Store(types)
		}
		if mpj := maxParallelJobsFromCapabilities(capsMap); mpj > 0 {
			sess.maxParallelJobs.Store(int32(mpj))
		}
		executors, err := parseExecutorCapabilities(capsMap)
		if err != nil {
			log.Printf("[GRPC] Worker %s: failed to parse executor capabilities: %v", workerID, err)
			return fmt.Errorf("stream: invalid executor capabilities: %w", err)
		}
		sess.replaceCapabilities(executors, capabilitiesBoolMap(capsMap))
	}
	sess.ready.Store(true)
	sess.draining.Store(false)
	sess.lastHeartbeatUnix.Store(time.Now().UTC().Unix())

	// Issue 7 fix: persist the session to SQLite worker_sessions table.
	if h.dbStore != nil {
		_ = h.dbStore.InsertSession(&store.PersistedSession{
			SessionID: sessionID,
			WorkerID:  workerID,
			TokenHash: store.HashCredential(hello.GetCredentialHash()),
			IPAddress: h.extractPeerIP(stream),
			ExpiresAt: time.Now().UTC().Add(24 * time.Hour),
		})
	}

	// Issue 5 fix: start the dedicated session writer goroutine.
	// All stream.Send() calls go through sendCh → sessionWriter from this point on.
	go h.sessionWriter(sess)

	defer func() {
		// Issue 6 fix: cancel session context first to stop notifier goroutines
		// BEFORE closing sendCh (prevents panic on send to closed channel).
		sessionCancel()

		// Issue 5 fix: close sendCh to stop the sessionWriter goroutine.
		// At this point no goroutine should be sending on sendCh.
		close(sendCh)

		// PR #4: release any pending task offer on session teardown.
		sess.claimMu.Lock()
		if sess.pendingTaskOffer != nil {
			if releaseErr := h.taskRepo.ReleaseLease(context.Background(), sess.pendingTaskOffer.ID, sess.workerID, sess.pendingTaskOffer.LeaseID); releaseErr != nil {
				log.Printf("[GRPC] Failed to release pendingTaskOffer for task %s on session teardown: %v", sess.pendingTaskOffer.ID, releaseErr)
			}
			sess.pendingTaskOffer = nil
		}
		sess.claimMu.Unlock()

		h.mu.Lock()
		if currentSID, ok := h.workerSessions[workerID]; ok && currentSID == sessionID {
			delete(h.workerSessions, workerID)
		}
		delete(h.sessions, sessionID)
		h.mu.Unlock()

		// Issue 7 fix: revoke the session in SQLite on disconnect.
		if h.dbStore != nil {
			_ = h.dbStore.RevokeSession(sessionID)
		}

		// P0 #6: use doneOnce to avoid double-close when closeOldSessionLocked
		// already signalled the notifier goroutine to stop.
		sess.doneOnce.Do(func() {
			close(sess.done)
		})
		log.Printf("[GRPC] Worker %s disconnected (session: %s)", workerID, sessionID)
	}()

	// Protocol-version handshake validation — STRICT mode.
	// Only ProtocolVersionCurrent ("v3") is accepted. Empty strings and
	// legacy versions return FailedPrecondition.
	if !controltransport.IsSupportedProtocol(env.ProtocolVersion) {
		log.Printf("[GRPC] worker %s protocol version %q rejected — supported: %v",
			workerID, env.ProtocolVersion, controltransport.SupportedProtocolVersions)
		return status.Errorf(codes.FailedPrecondition,
			"worker %s protocol_version %q is not supported (supported: %v)",
			workerID, env.ProtocolVersion, controltransport.SupportedProtocolVersions)
	}

	// Send typed HelloAck via sendCh (sessionWriter handles the actual Send).
	ack := &pb.MasterToWorkerEnvelope{
		MessageId:       fmt.Sprintf("ack-%s-%d", workerID, time.Now().UnixNano()),
		WorkerId:        workerID,
		SessionId:       sessionID,
		SequenceNumber:  1,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Msg:             &pb.MasterToWorkerEnvelope_HelloAck{HelloAck: &pb.HelloAck{}},
	}
	if !safeSend(sendCh, &outboundMessage{Envelope: ack}) {
		return fmt.Errorf("stream: sendCh full for hello_ack")
	}

	// Dispatch any pending commands that arrived while worker was disconnected
	h.dispatchCommands(workerID, sess)

	// Start push-mode job notifier.
	// Issue 6 fix: use sessionCtx for cleanup so notifier goroutines stop
	// when the session is cancelled.
	var notifyCh chan struct{}
	var notifyStop context.CancelFunc
	if h.config.PushMode {
		notifyCtx, cancel := context.WithCancel(sessionCtx)
		notifyStop = cancel
		notifyCh = make(chan struct{}, 1)
		go h.notifyTasksAvailable(notifyCtx, workerID, notifyCh, sess.done)
	}

	if notifyStop != nil {
		defer notifyStop()
	}

	// Issue 6/Phase 4.2: wrap stream.Recv() in a goroutine so the main loop
	// can select on writerErr (cap-1) without blocking on Recv. The wrap
	// also makes session cancellation explicit: when sessionCtx is cancelled
	// the wrapper exits cleanly instead of leaking forever.
	recvCh := make(chan *pb.WorkerToMasterEnvelope, 16)
	recvErrCh := make(chan error, 1)
	go func() {
		for {
			env, err := stream.Recv()
			if err != nil {
				recvErrCh <- err
				return
			}
			select {
			case recvCh <- env:
			case <-sessionCtx.Done():
				return
			}
		}
	}()

	// Main message loop — type-switch on the oneof Msg field, while
	// also watching writerErr/sessionCtx to drive clean teardown.
	for {
		select {
		case <-sessionCtx.Done():
			return sessionCtx.Err()

		case err := <-sess.writerErr:
			// P0 teardown: stream.Write failed inside sessionWriter. Cancel
			// the session context and revoke the SQLite session so the worker
			// reconnects promptly and we don't leak the orphaned job.
			log.Printf("[GRPC] sessionWriter failure for worker %s (session %s): %v — tearing down",
				workerID, sessionID, err)
			sess.cancel()
			if h.dbStore != nil {
				_ = h.dbStore.RevokeSession(sessionID)
			}
			// PR #4: release pending task offer on writer failure.
			sess.claimMu.Lock()
			if sess.pendingTaskOffer != nil {
				if releaseErr := h.taskRepo.ReleaseLease(context.Background(), sess.pendingTaskOffer.ID, sess.workerID, sess.pendingTaskOffer.LeaseID); releaseErr != nil {
					log.Printf("[GRPC] Failed to release pendingTaskOffer for task %s on writer failure: %v", sess.pendingTaskOffer.ID, releaseErr)
				}
				sess.pendingTaskOffer = nil
			}
			sess.claimMu.Unlock()
			return fmt.Errorf("stream: writer failure: %w", err)

		case err := <-recvErrCh:
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("stream: recv: %w", err)

		case env := <-recvCh:
			// Issue 6 fix: drop messages from stale sessions (zombie connections after reconnect).
			if !h.isCurrentSession(workerID, sessionID) {
				continue
			}

			// Issue 7 fix: sequence number check for replay protection.
			if env.SequenceNumber > 0 {
				if env.SequenceNumber <= sess.lastRecvSeq {
					log.Printf("[GRPC] Duplicate or replayed message from worker %s: seq=%d, last=%d",
						workerID, env.SequenceNumber, sess.lastRecvSeq)
					continue
				}
				sess.lastRecvSeq = env.SequenceNumber
			}

			switch m := env.Msg.(type) {
			case *pb.WorkerToMasterEnvelope_Heartbeat:
				h.handleHeartbeat(workerID, sessionID, m.Heartbeat)
				h.dispatchCommands(workerID, sess)
				if notifyCh != nil {
					select {
					case notifyCh <- struct{}{}:
					default:
					}
				}
			case *pb.WorkerToMasterEnvelope_TaskLeaseRenewal:
				h.handleTaskRenewal(workerID, m.TaskLeaseRenewal)

			case *pb.WorkerToMasterEnvelope_TaskAccepted:
				h.handleTaskAccepted(workerID, m.TaskAccepted, sess)

			case *pb.WorkerToMasterEnvelope_TaskRejected:
				h.handleTaskRejected(workerID, m.TaskRejected)

			case *pb.WorkerToMasterEnvelope_TaskResult:
				h.handleTaskResult(workerID, m.TaskResult, sess)

			case *pb.WorkerToMasterEnvelope_CommandAck:
				h.handleCommandAck(workerID, m.CommandAck)

			case *pb.WorkerToMasterEnvelope_ArtifactUploaded:
				// Blocco 1 final-wire (P0 #2, #3, #4): invoke the
				// capability gate before delegating. ArtifactUploaded
				// is the on-the-wire "artifact.commit.v1" message and
				// the canonical write path through artifacts.Service.
				// A misconfigured/spool-broken/transport-empty master
				// MUST NOT accept a commit because that would yield a
				// SUCCEEDED job with no on-disk blob. See
				// handler_artifacts.go::checkArtifactCommitGate for
				// the fail-closed semantic (PermissionDenied).
				if gateErr := h.checkArtifactCommitGate(workerID); gateErr != nil {
					return gateErr
				}
				h.handleArtifactUploaded(workerID, m.ArtifactUploaded)

			case *pb.WorkerToMasterEnvelope_Goodbye:
				return nil

			default:
				log.Printf("[GRPC] Unknown message type from worker %s: %T", workerID, env.Msg)
			}
		}
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
		log.Printf("[GRPC] Worker %s reconnecting — removing old session %s", workerID, oldSID)
		// P0 #6: close the done channel to stop the old notifier goroutine.
		// Messages from the old session's main loop are dropped by isCurrentSession().
		oldSess.doneOnce.Do(func() {
			close(oldSess.done)
		})
		// Issue 6 fix: cancel the old session's context to stop its goroutines.
		if oldSess.cancel != nil {
			oldSess.cancel()
		}
		// PR #4: release any pendingTaskOffer held by the old session
		// so the claim is returned promptly on reconnect.
		oldSess.claimMu.Lock()
		if oldSess.pendingTaskOffer != nil {
			if releaseErr := h.taskRepo.ReleaseLease(context.Background(), oldSess.pendingTaskOffer.ID, oldSess.workerID, oldSess.pendingTaskOffer.LeaseID); releaseErr != nil {
				log.Printf("[GRPC] Failed to release old pendingTaskOffer for task %s during reconnect: %v", oldSess.pendingTaskOffer.ID, releaseErr)
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
			log.Printf("[GRPC] sessionWriter send error for worker %s (session %s): %v",
				sess.workerID, sess.sessionID, err)
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
				log.Printf("[GRPC] TaskOffer sent to worker %s (session %s): task=%s job=%s attempt=%s lease=%s",
					sess.workerID, sess.sessionID,
					msg.TaskOffer.GetTaskId(), msg.TaskOffer.GetJobId(),
					msg.TaskOffer.GetAttemptId(), msg.TaskOffer.GetLeaseId())
			}
		case *pb.MasterToWorkerEnvelope_TaskLeaseGranted:
			if msg.TaskLeaseGranted != nil {
				log.Printf("[GRPC] TaskLeaseGranted sent to worker %s (session %s): task=%s job=%s attempt=%s lease=%s",
					sess.workerID, sess.sessionID,
					msg.TaskLeaseGranted.GetTaskId(), msg.TaskLeaseGranted.GetJobId(),
					msg.TaskLeaseGranted.GetAttemptId(), msg.TaskLeaseGranted.GetLeaseId())
			}
		}
		// Call OnSent callback after successful send (gap #1 fix).
		if out.OnSent != nil {
			out.OnSent()
		}
	}
	log.Printf("[GRPC] sessionWriter exiting for worker %s (session %s)", sess.workerID, sess.sessionID)
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
