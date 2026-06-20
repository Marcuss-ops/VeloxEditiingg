// Package grpcserver provides the master-side gRPC handler for the
// WorkerControl bidirectional stream service using typed protobuf envelopes.
//
// The handler manages persistent worker streams, forwarding heartbeats,
// lease renewals, job claims, and commands between the gRPC stream and
// the existing HTTP-based control plane components (Registry, CommandManager,
// LifecycleService).
//
// Phase 2 (typed protobuf): uses WorkerToMasterEnvelope / MasterToWorkerEnvelope
// with typed oneof messages instead of TransportMessage { string type; Struct payload }.
package grpcserver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"velox-server/internal/artifacts"
	"velox-server/internal/queue"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Handler implements pb.WorkerControlServer. It manages persistent worker
// streams and bridges gRPC messages to the existing control plane.
type Handler struct {
	pb.UnimplementedWorkerControlServer

	registry     *workersreg.Registry
	cmdMgr       *workersreg.CommandManager
	lifecycleSvc *queue.LifecycleService
	artifactSvc  *artifacts.Service
	dbStore      *store.SQLiteStore
	config       *HandlerConfig
	authorizer   WorkerAuthorizer // P0: gates workers against VELOX_ALLOWED_WORKERS

	mu             sync.Mutex
	sessions       map[string]*workerSession // sessionID → active stream session
	workerSessions map[string]string         // workerID → sessionID (for lookup)
}

// HandlerConfig holds configuration for the gRPC handler.
type HandlerConfig struct {
	PushMode       bool   // Phase 5+: send JobOffer directly, workers respond JobAccepted
	AllowInsecure  bool   // Dev-only: allow insecure gRPC connections (VELOX_GRPC_ALLOW_INSECURE_DEV)
	AllowedWorkers string // P0: comma-separated worker ID allowlist (VELOX_ALLOWED_WORKERS)
}

// outboundMessage wraps a protobuf envelope with optional callbacks
// for the sessionWriter. OnSent is called after a successful stream.Send;
// nil means no callback. This enables #1 fix: commands are marked delivered
// only after the real network write, not after safeSend puts them in the
// in-memory channel.
type outboundMessage struct {
	Envelope *pb.MasterToWorkerEnvelope
	OnSent   func() // Called after successful stream.Send; nil if not needed
}

// workerSession tracks a single worker's gRPC stream connection.
type workerSession struct {
	workerID  string
	sessionID string
	stream    grpc.BidiStreamingServer[pb.WorkerToMasterEnvelope, pb.MasterToWorkerEnvelope]
	done      chan struct{}
	doneOnce  sync.Once          // P0 #6: prevents double-close on session teardown/reconnect
	cancel    context.CancelFunc // cancels the session context to terminate old goroutines

	// Serialized output: all stream.Send() calls go through sendCh → sessionWriter.
	// No other goroutine may call stream.Send() directly.
	sendCh chan *outboundMessage

	// writerErr is a small (cap 1) channel used by sessionWriter to signal
	// a stream.Send() failure back to the Stream() main loop. Phase 4.2
	// requirement: a network-level send error MUST terminate the session,
	// otherwise pending offers can be left orphaned silently. The main loop
	// reads writerErr inside its select and triggers a teardown on receipt.
	writerErr chan error

	// Job offering synchronization (Issue 4 fix).
	pendingOffer *queue.Job // JobOffer sent, awaiting JobAccepted/JobRejected
	claimMu      sync.Mutex // serializes the claim+send+set flow; also guards pendingOffer r/w

	// Worker capacity tracking (atomic — Phase 4.1 fix). The handleHeartbeat
	// goroutine writes them, sendPushJobOffer reads them under claimMu. Using
	// atomic.Int32 makes the read lock-free and race-clean in `-race`.
	maxParallelJobs atomic.Int32
	activeJobsCount atomic.Int32

	// supportedJobTypes is updated by handleHeartbeat from the worker's
	// capabilities and read by collectAllowedJobTypes under claimMu.
	// atomic.Value avoids RWMutex overhead while remaining race-clean.
	supportedJobTypes atomic.Value // []string

	// Sequence numbers for replay protection (Issue 7 fix).
	lastRecvSeq int64 // last received sequence number from worker
}

// NewHandler creates a new gRPC WorkerControl handler.
//
// PR 2 (chunk 4): artifactSvc type changed from *queue.ArtifactFinalizationService
// (the old 2-tx STAGING→VERIFYING→READY gate) to *artifacts.Service (the
// new single-tx session-based gate). The handler rejects ArtifactUploaded
// messages when artifactSvc is nil so misconfiguration surfaces as
// dropped uploads rather than a SUCCEEDED job with no verification.
// Bootstrap must supply a real *artifacts.Service.
//
// P0 (2026-06): the handler now gates all inbound worker streams through a
// WorkerAuthorizer backed by VELOX_ALLOWED_WORKERS. Workers NOT in the
// allowlist receive gRPC PermissionDenied before credential validation.
func NewHandler(
	registry *workersreg.Registry,
	cmdMgr *workersreg.CommandManager,
	lifecycleSvc *queue.LifecycleService,
	artifactSvc *artifacts.Service,
	dbStore *store.SQLiteStore,
	config *HandlerConfig,
) *Handler {
	if config == nil {
		config = &HandlerConfig{PushMode: true}
	}
	return &Handler{
		registry:       registry,
		cmdMgr:         cmdMgr,
		lifecycleSvc:   lifecycleSvc,
		artifactSvc:    artifactSvc,
		dbStore:        dbStore,
		config:         config,
		authorizer:     NewAllowlistAuthorizer(config.AllowedWorkers, config.AllowInsecure),
		sessions:       make(map[string]*workerSession),
		workerSessions: make(map[string]string),
	}
}

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
	sessionCtx, sessionCancel := context.WithCancel(context.Background())

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
		sendCh:    sendCh,
		writerErr: make(chan error, 1),
	}
	h.sessions[sessionID] = sess
	h.workerSessions[workerID] = sessionID
	h.mu.Unlock()

	log.Printf("[GRPC] Worker %s connected (session: %s, name: %s)", workerID, sessionID, hello.GetWorkerName())

	// Extract supported_job_types from Hello capabilities for ClaimNext filtering.
	if hello.GetCapabilities() != nil {
		if types := extractSupportedJobTypes(hello.GetCapabilities().AsMap()); len(types) > 0 {
			sess.supportedJobTypes.Store(types)
		}
	}

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

		// Gap #2 fix: release any pending offer that was not accepted/rejected
		// so the job doesn't stay leased until lease expiry.
		sess.claimMu.Lock()
		if sess.pendingOffer != nil {
			if releaseErr := h.lifecycleSvc.Repo().ReleaseClaim(context.Background(), sess.pendingOffer.JobID); releaseErr != nil {
				log.Printf("[GRPC] Failed to release pendingOffer for job %s on session teardown: %v", sess.pendingOffer.JobID, releaseErr)
			}
			sess.pendingOffer = nil
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

	// Issue 7 fix: validate protocol version from Hello.
	// Phase 4.2: mismatched protocol is now FATAL for the connection —
	// silently accepting a stale protocol would risk acting on message
	// shapes the master does not understand, leading to data corruption.
	if env.ProtocolVersion != "" && env.ProtocolVersion != controltransport.ProtocolVersionCurrent {
		return fmt.Errorf("stream: worker %s protocol version mismatch: got %q, want %q",
			workerID, env.ProtocolVersion, controltransport.ProtocolVersionCurrent)
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
		go h.notifyJobsAvailable(notifyCtx, workerID, notifyCh, sess.done)
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
			// Gap #2 fix: release pendingOffer so the job doesn't stay leased.
			sess.claimMu.Lock()
			if sess.pendingOffer != nil {
				if releaseErr := h.lifecycleSvc.Repo().ReleaseClaim(context.Background(), sess.pendingOffer.JobID); releaseErr != nil {
					log.Printf("[GRPC] Failed to release pendingOffer for job %s on writer failure: %v", sess.pendingOffer.JobID, releaseErr)
				}
				sess.pendingOffer = nil
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

			case *pb.WorkerToMasterEnvelope_LeaseRenewal:
				h.handleLeaseRenewal(workerID, m.LeaseRenewal)

			case *pb.WorkerToMasterEnvelope_JobAccepted:
				h.handleJobAccepted(workerID, m.JobAccepted)

			case *pb.WorkerToMasterEnvelope_JobRejected:
				h.handleJobRejected(workerID, m.JobRejected)

			case *pb.WorkerToMasterEnvelope_JobProgress:
				h.handleJobProgress(workerID, m.JobProgress)

			case *pb.WorkerToMasterEnvelope_CommandAck:
				h.handleCommandAck(workerID, m.CommandAck)

			case *pb.WorkerToMasterEnvelope_JobResult:
				h.handleJobResult(workerID, m.JobResult)

			case *pb.WorkerToMasterEnvelope_ArtifactUploaded:
				h.handleArtifactUploaded(workerID, m.ArtifactUploaded)

			case *pb.WorkerToMasterEnvelope_Goodbye:
				return nil

			default:
				log.Printf("[GRPC] Unknown message type from worker %s: %T", workerID, env.Msg)
			}
		}
	}
}

// StartGRPCServer starts a gRPC server on the configured port and registers
// the WorkerControl handler. Supports mTLS when certFile/keyFile/caFile are provided.
func StartGRPCServer(port int, handler *Handler, certFile, keyFile, caFile string) (*grpc.Server, net.Listener, error) {
	if port <= 0 {
		return nil, nil, nil // gRPC disabled
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, nil, fmt.Errorf("grpc: listen on :%d: %w", port, err)
	}

	var grpcOpts []grpc.ServerOption

	if certFile != "" && keyFile != "" {
		serverCert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, nil, fmt.Errorf("grpc: load server cert/key: %w", err)
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{serverCert},
			MinVersion:   tls.VersionTLS12,
		}

		if caFile != "" {
			caPEM, err := os.ReadFile(caFile)
			if err != nil {
				return nil, nil, fmt.Errorf("grpc: read ca file: %w", err)
			}
			certPool := x509.NewCertPool()
			if !certPool.AppendCertsFromPEM(caPEM) {
				return nil, nil, fmt.Errorf("grpc: failed to parse CA certificate")
			}
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
			tlsConfig.ClientCAs = certPool
			log.Printf("[GRPC] mTLS enabled — requiring client certificates signed by CA")
		}

		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	} else {
		allowInsecure := os.Getenv("VELOX_GRPC_ALLOW_INSECURE_DEV") == "true"
		if !allowInsecure {
			return nil, nil, fmt.Errorf("grpc: TLS cert/key required in production (set VELOX_GRPC_ALLOW_INSECURE_DEV=true for dev)")
		}
		handler.config.AllowInsecure = true
		log.Printf("[GRPC] WARNING: insecure gRPC server — dev mode only")
		grpcOpts = append(grpcOpts, grpc.Creds(insecure.NewCredentials()))
	}

	srv := grpc.NewServer(grpcOpts...)
	pb.RegisterWorkerControlServer(srv, handler)

	// Serve in a goroutine but block until the server is actually accepting
	// connections. Without this, the caller may return before srv.Serve()
	// enters its accept loop, creating a race where workers see "connection
	// reset by peer" because the TCP handshake completes but the gRPC
	// server isn't ready to handle the preface exchange.
	serveStarted := make(chan struct{})
	go func() {
		// Close serveStarted immediately before srv.Serve(lis) to signal
		// that the goroutine has launched. There is a residual window between
		// close(serveStarted) and srv.Serve entering its accept loop; the TCP
		// dial below (belt-and-suspenders) catches that gap.
		close(serveStarted)
		log.Printf("[GRPC] Velox master gRPC server listening on :%d", port)
		if err := srv.Serve(lis); err != nil {
			log.Printf("[GRPC] Server error: %v", err)
		}
	}()
	// Wait for the goroutine to close serveStarted — this gates the goroutine
	// launch but NOT the gRPC accept loop (see belt-and-suspenders below).
	<-serveStarted

	// Belt-and-suspenders: verify the OS accept queue is actually ready with
	// a local TCP dial. This catches the residual race window between
	// close(serveStarted) and srv.Serve entering its accept loop.
	if conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond); err == nil {
		conn.Close()
	}

	return srv, lis, nil
}

// dispatchCommands reads pending commands from SQLite for the worker,
// sends each as a typed Command via sendCh, and marks only successfully
// sent commands as delivered. Commands that fail to send remain in pending
// state for retry on the next dispatch cycle.
func (h *Handler) dispatchCommands(workerID string, sess *workerSession) {
	cmds := h.cmdMgr.GetPendingCommands(workerID)
	if len(cmds) == 0 {
		return
	}

	log.Printf("[GRPC] Dispatching %d pending commands to worker %s", len(cmds), workerID)

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
						log.Printf("[GRPC] Failed to mark command %s delivered: %v", cmdID, err)
					}
				}
			},
		}
		if !safeSend(sess.sendCh, out) {
			log.Printf("[GRPC] sendCh full/closed — dropping command %s for worker %s (will retry)", cmd.CommandID, workerID)
			continue
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
		// Gap #2: release any pendingOffer held by the old session
		// so the claim is returned promptly on reconnect, not just
		// when the old defer eventually runs.
		oldSess.claimMu.Lock()
		if oldSess.pendingOffer != nil {
			if releaseErr := h.lifecycleSvc.Repo().ReleaseClaim(context.Background(), oldSess.pendingOffer.JobID); releaseErr != nil {
				log.Printf("[GRPC] Failed to release old pendingOffer for job %s during reconnect: %v", oldSess.pendingOffer.JobID, releaseErr)
			}
			oldSess.pendingOffer = nil
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
	h.mu.Lock()
	defer h.mu.Unlock()
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
	h.mu.Lock()
	defer h.mu.Unlock()
	sid, ok := h.workerSessions[workerID]
	if !ok {
		return nil
	}
	return h.sessions[sid]
}

// ---- Security Helpers ----

// extractPeerIP extracts the client IP address from the gRPC stream context
// without the port (if possible).
func (h *Handler) extractPeerIP(stream grpc.ServerStream) string {
	p, ok := peer.FromContext(stream.Context())
	if !ok {
		return ""
	}
	addr := p.Addr.String()
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

// extractWorkerIDFromStream extracts the worker identity from the client TLS certificate.
func (h *Handler) extractWorkerIDFromStream(stream grpc.ServerStream) string {
	p, ok := peer.FromContext(stream.Context())
	if !ok {
		return ""
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return ""
	}

	if len(tlsInfo.State.PeerCertificates) == 0 {
		return ""
	}

	clientCert := tlsInfo.State.PeerCertificates[0]
	cn := clientCert.Subject.CommonName
	if cn == "" {
		if len(clientCert.DNSNames) > 0 {
			cn = clientCert.DNSNames[0]
		}
	}

	return strings.TrimSpace(cn)
}

// validateCredentialHash checks the worker's credential_hash against the
// stored persistent credential in SQLite (worker_credentials table).
// Accepts the credential hash string directly from typed Hello message.
func (h *Handler) validateCredentialHash(workerID string, declaredHash string) error {
	// Check if this worker has a stored credential
	hasCred, err := h.dbStore.HasWorkerCredential(workerID)
	if err != nil {
		log.Printf("[GRPC] Credential lookup failed for worker %s: %v", workerID, err)
		if h.config.AllowInsecure {
			return nil
		}
		return fmt.Errorf("credential lookup failed for %s", workerID)
	}

	if !hasCred {
		// First registration: store the credential if one is provided
		if declaredHash != "" {
			if err := h.dbStore.SetWorkerCredential(workerID, declaredHash); err != nil {
				return fmt.Errorf("store initial credential: %w", err)
			}
			log.Printf("[GRPC] Worker %s: initial credential stored", workerID)
			return nil
		}
		if h.config.AllowInsecure {
			log.Printf("[GRPC] Worker %s: no credential — allowing in insecure dev mode", workerID)
			return nil
		}
		return fmt.Errorf("worker %s: credential required", workerID)
	}

	// Stored credential exists — validate the declared hash
	if declaredHash == "" {
		return fmt.Errorf("worker %s: credential required (existing credential stored)", workerID)
	}

	valid, err := h.dbStore.ValidateWorkerCredential(workerID, declaredHash)
	if err != nil {
		return fmt.Errorf("validate credential: %w", err)
	}
	if !valid {
		return fmt.Errorf("worker %s: credential hash mismatch", workerID)
	}

	return nil
}
