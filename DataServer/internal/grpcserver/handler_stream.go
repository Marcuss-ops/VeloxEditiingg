// Package grpcserver / handler_stream.go
//
// Stream lifecycle and session management for the WorkerControl gRPC handler.
// This file holds the Stream() entry point: mTLS identity extraction, Hello
// handshake, worker admission (allowlist, credential_hash, protocol version),
// runtime snapshot minting, session registration, and the main message loop.
//
// Message routing lives in handler_stream_messages.go (dispatchMessage) and
// session registry/writer helpers in handler_stream_session.go.
package grpcserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"velox-server/internal/logging"
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
		logGRPCf(stream.Context(), logging.LevelInfo, logging.CodeGRPCStreamAuthenticated, "[GRPC] Worker authenticated via mTLS: %s", certWorkerID)
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

	// Reject unsupported protocol versions before creating any durable
	// runtime identity row. A refused Hello must leave no snapshot behind.
	if !controltransport.IsSupportedProtocol(env.ProtocolVersion) {
		supportedProtocols := controltransport.SupportedProtocols()
		logGRPCf(stream.Context(), logging.LevelWarn, logging.CodeGRPCStreamRejected, "[GRPC] worker %s protocol version %q rejected — supported: %v", declaredWorkerID, env.ProtocolVersion, supportedProtocols)
		return status.Errorf(codes.FailedPrecondition,
			"worker %s protocol_version %q is not supported (supported: %v)",
			declaredWorkerID, env.ProtocolVersion, supportedProtocols)
	}

	workerID := declaredWorkerID
	sessionID := fmt.Sprintf("grpc-%s-%d", workerID, time.Now().UnixNano())

	// Validate the Hello capability payload before any durable admission.
	// A malformed worker must not create a session or runtime snapshot.
	caps := map[string]interface{}{}
	if hello.GetCapabilities() != nil {
		caps = hello.GetCapabilities().AsMap()
	}
	executorRegistry, err := parseExecutorCapabilities(caps)
	if err != nil {
		return fmt.Errorf("stream: invalid executor capabilities: %w", err)
	}
	if supported, ok := caps[controltransport.CapabilityCanonicalPayloadV2].(bool); !ok || !supported {
		logGRPCf(stream.Context(), logging.LevelWarn, logging.CodeGRPCStreamRejected, "[GRPC] worker %s rejected: canonical payload capability %q missing or false", declaredWorkerID, controltransport.CapabilityCanonicalPayloadV2)
		return status.Errorf(codes.FailedPrecondition,
			"worker %s does not support canonical payload contract %s", declaredWorkerID, controltransport.CapabilityCanonicalPayloadV2)
	}

	// Mint the immutable snapshot before admitting the durable control
	// session. InsertSession is transactional; if collision or any other
	// admission error occurs, this exact snapshot is deleted and the prior
	// session remains untouched.
	workerSnapshotID := ""
	if h.dbStore != nil {
		capabilitiesJSON, marshalErr := json.Marshal(caps)
		if marshalErr != nil {
			return fmt.Errorf("stream: encode worker runtime capabilities: %w", marshalErr)
		}
		snapshot, snapshotErr := h.dbStore.GetOrCreateWorkerRuntimeSnapshot(store.WorkerRuntimeSnapshot{
			WorkerID:          workerID,
			SessionID:         sessionID,
			Hostname:          hello.GetHostname(),
			WorkerName:        hello.GetWorkerName(),
			WorkerClass:       hello.GetWorkerClass(),
			RolloutGroup:      hello.GetRolloutGroup(),
			WorkerVersion:     hello.GetVersion(),
			BundleVersion:     hello.GetBundleVersion(),
			BundleHash:        hello.GetBundleHash(),
			EngineVersion:     hello.GetEngineVersion(),
			ProtocolVersion:   env.ProtocolVersion,
			CapabilitiesJSON:  string(capabilitiesJSON),
			LogicalCPUCount:   snapshotInt(caps, "logical_cpu_count", snapshotHostInt(caps, "cpu_count")),
			EffectiveCPUCount: snapshotInt(caps, "effective_cpu_count", snapshotHostInt(caps, "cpu_count")),
			TotalMemoryBytes:  snapshotInt64(caps, "total_memory_bytes", snapshotHostInt64(caps, "ram_bytes")),
			CPUQuota:          snapshotFloat(caps, "cpu_quota", 0),
			GPUModel:          snapshotString(caps, "gpu_model", ""),
			CPUModel:          snapshotString(caps, "cpu_model", ""),
			StorageClass:      snapshotString(caps, "storage_class", ""),
			ConfigHash:        snapshotString(caps, "config_hash", ""),
			DockerImageDigest: snapshotString(caps, "docker_image_digest", ""),
			FFmpegVersion:     snapshotString(caps, "ffmpeg_version", ""),
			GitSHA:            snapshotString(caps, "git_sha", ""),
		})
		if snapshotErr != nil {
			return fmt.Errorf("stream: create worker runtime snapshot: %w", snapshotErr)
		}
		if snapshot == nil || snapshot.SnapshotID == "" {
			return fmt.Errorf("stream: create worker runtime snapshot: empty snapshot")
		}
		workerSnapshotID = snapshot.SnapshotID

		newTokenHash := store.HashCredential(hello.GetCredentialHash())
		peerIP := h.extractPeerIP(stream)
		insertErr := h.dbStore.InsertSession(&store.PersistedSession{
			SessionID:   sessionID,
			WorkerID:    workerID,
			SessionType: "control",
			TokenHash:   newTokenHash,
			IPAddress:   peerIP,
			ExpiresAt:   time.Now().UTC().Add(24 * time.Hour),
		})
		if errors.Is(insertErr, store.ErrWorkerIDCollision) {
			if cleanupErr := h.dbStore.DeleteWorkerRuntimeSnapshotBySession(workerID, sessionID); cleanupErr != nil {
				logGRPCf(stream.Context(), logging.LevelWarn, logging.CodeGRPCSessionCleanupFailed, "[GRPC] Worker %s snapshot cleanup after collision failed: %v", workerID, cleanupErr)
			}
			logGRPCf(stream.Context(), logging.LevelWarn, logging.CodeGRPCStreamHelloCollision, "[GRPC] Worker %s hello COLLISION: rejecting incoming hello peer_ip=%s", workerID, peerIP)
			return status.Errorf(codes.AlreadyExists,
				"worker_id %q already connected on a different credential", workerID)
		}
		if insertErr != nil {
			if cleanupErr := h.dbStore.DeleteWorkerRuntimeSnapshotBySession(workerID, sessionID); cleanupErr != nil {
				logGRPCf(stream.Context(), logging.LevelWarn, logging.CodeGRPCSessionCleanupFailed, "[GRPC] Worker %s snapshot cleanup after session admission failure failed: %v", workerID, cleanupErr)
			}
			return fmt.Errorf("stream: persist worker session: %w", insertErr)
		}
		if existingTokenHash, probeErr := h.dbStore.CheckActiveSessionCollision(workerID, "control"); probeErr == nil && existingTokenHash == newTokenHash {
			logGRPCf(stream.Context(), logging.LevelInfo, logging.CodeGRPCWorkerConnected, "[GRPC] Worker %s hello admitted/reconnected (session: %s)", workerID, sessionID)
		}
	}

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
		workerID:         workerID,
		sessionID:        sessionID,
		workerSnapshotID: workerSnapshotID,
		stream:           stream,
		done:             make(chan struct{}),
		cancel:           sessionCancel,
		ctx:              sessionCtx,
		sendCh:           sendCh,
		writerErr:        make(chan error, 1),
	}
	h.sessions[sessionID] = sess
	h.workerSessions[workerID] = sessionID
	h.mu.Unlock()

	logGRPCf(stream.Context(), logging.LevelInfo, logging.CodeGRPCWorkerConnected, "[GRPC] Worker %s connected (session: %s, name: %s)", workerID, sessionID, hello.GetWorkerName())

	// Placement uses only the validated typed executor registry and the
	// canonical host capacity/cache projections. Legacy job-type flags
	// are not admitted or retained.
	if mpj := maxParallelJobsFromCapabilities(caps); mpj > 0 {
		sess.maxParallelJobs.Store(int32(mpj))
	}
	sess.replaceCapabilities(executorRegistry, capabilitiesBoolMap(caps))
	sess.replaceAssetCacheKeys(extractAssetCacheKeys(caps))
	sess.ready.Store(true)

	// Bridge the hello-declared capability report into the in-memory
	// registry so the read model (GET /api/v1/workers) surfaces the
	// canonical capacity (task_slots = host.max_parallel_jobs). The
	// legacy HTTP registration path did this via extra["capabilities"];
	// without the same bridge here, v3-only workers read task_slots=0
	// in the API even though they advertise max_parallel_jobs=1.
	h.registerHelloCapabilitiesInRegistry(context.Background(), workerID, hello.GetWorkerName(), h.extractPeerIP(stream), env.ProtocolVersion, caps, hello)
	sess.draining.Store(false)
	sess.lastHeartbeatUnix.Store(time.Now().UTC().Unix())

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
				logGRPCf(stream.Context(), logging.LevelWarn, logging.CodeGRPCSessionCleanupFailed, "[GRPC] Failed to release pendingTaskOffer for task %s on session teardown: %v", sess.pendingTaskOffer.ID, releaseErr)
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
		logGRPCf(stream.Context(), logging.LevelInfo, logging.CodeGRPCWorkerDisconnected, "[GRPC] Worker %s disconnected (session: %s)", workerID, sessionID)
	}()

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

	// Commands are durable and may be created while the stream is already
	// connected (notably operator cancellation). Heartbeats are not a
	// reliable delivery clock, so poll the outbox independently while this
	// session is alive. The initial dispatch above handles reconnects; this
	// loop closes the connected-session delivery gap.
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-sessionCtx.Done():
				return
			case <-ticker.C:
				h.dispatchCommands(workerID, sess)
			}
		}
	}()

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

	// Main message loop — routes envelopes via dispatchMessage while
	// also watching writerErr/sessionCtx to drive clean teardown.
	for {
		select {
		case <-sessionCtx.Done():
			return sessionCtx.Err()

		case err := <-sess.writerErr:
			// P0 teardown: stream.Write failed inside sessionWriter. Cancel
			// the session context and revoke the SQLite session so the worker
			// reconnects promptly and we don't leak the orphaned job.
			logGRPCf(stream.Context(), logging.LevelError, logging.CodeGRPCStreamWriterFailure, "[GRPC] sessionWriter failure for worker %s (session %s): %v — tearing down", workerID, sessionID, err)
			sess.cancel()
			if h.dbStore != nil {
				_ = h.dbStore.RevokeSession(sessionID)
			}
			// PR #4: release pending task offer on writer failure.
			sess.claimMu.Lock()
			if sess.pendingTaskOffer != nil {
				if releaseErr := h.taskRepo.ReleaseLease(context.Background(), sess.pendingTaskOffer.ID, sess.workerID, sess.pendingTaskOffer.LeaseID); releaseErr != nil {
					logGRPCf(stream.Context(), logging.LevelWarn, logging.CodeGRPCSessionCleanupFailed, "[GRPC] Failed to release pendingTaskOffer for task %s on writer failure: %v", sess.pendingTaskOffer.ID, releaseErr)
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
					logGRPCf(stream.Context(), logging.LevelWarn, logging.CodeGRPCStreamReplay, "[GRPC] Duplicate or replayed message from worker %s: seq=%d, last=%d", workerID, env.SequenceNumber, sess.lastRecvSeq)
					continue
				}
				sess.lastRecvSeq = env.SequenceNumber
			}

			if err := h.dispatchMessage(workerID, sessionID, env, sess, notifyCh); err != nil {
				// Goodbye is a clean end-of-stream: do not surface an error.
				if errors.Is(err, errStreamGoodbye) {
					return nil
				}
				return err
			}
		}
	}
}
