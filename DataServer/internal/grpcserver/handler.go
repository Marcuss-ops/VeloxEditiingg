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
	"velox-server/internal/ingest"
	"velox-server/internal/jobs"
	velmetrics "velox-server/internal/metrics"
	"velox-server/internal/placement"
	"velox-server/internal/registry"
	"velox-server/internal/store"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	workersreg "velox-server/internal/workers"
	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
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
//
// PR #4: taskRepo + taskAttemptRepo added for task-native dispatch.
type Handler struct {
	pb.UnimplementedWorkerControlServer

	registry        *workersreg.Registry
	cmdMgr          *workersreg.CommandManager
	jobsRepo        jobs.Repository
	taskRepo        taskgraph.Repository
	taskAttemptRepo taskattempts.Repository
	artifactSvc     *artifacts.Service
	// ingestionSvc is the canonical TaskReportIngestionService
	// (feat/task-report-ingestion). Wired post-construction via
	// SetIngestionSvc so the audit closure can run during bootstrap
	// without forcing every NewHandler caller to thread a new dependency.
	// Nil-checked defensively in handleTaskResult (rejects on misconfig).
	ingestionSvc *ingest.TaskReportIngestionService
	dbStore      *store.SQLiteStore
	config       *HandlerConfig
	authorizer   WorkerAuthorizer // P0: gates workers against VELOX_ALLOWED_WORKERS

	// Scorecard v1 / F2: forward typed WorkerResourceCounters from the
	// worker's periodic Heartbeat onto the Prometheus registry. Wired
	// post-construction via SetResourceSink so tests can inject a stub
	// sink without invoking the heavy Collector. Nil is safe: handleHeartbeat
	// falls through to the existing registry.Heartbeat() side and skips
	// the Prometheus projection (deployments without metrics still score).
	resourceSink velmetrics.WorkerResourceSink

	// Scorecard v1 / placement: forward placement rejection counters
	// from the placement pipeline (recordPlacementRejections) and
	// unsupported_executor handler onto the Prometheus registry.
	// Wired post-construction via SetPlacementRejectionSink so tests
	// can inject a stub. NIL-safe — handlers without a metrics
	// surface silently skip the projection (log-only mode).
	placementRejectionSink velmetrics.PlacementRejectionSink

	placementMatcher *placement.Matcher

	// capabilityRegistry gates ArtifactUploaded (the on-the-wire
	// "artifact.commit.v1" commit path) against the readiness state
	// of the master subsystems the commit depends on (coordinator,
	// spool, transport). Wired post-construction via
	// SetCapabilityRegistry. NIL-safe — a Handler constructed without
	// the registry skips the gate (legacy test paths + bootstrap
	// variants that don't wire it). The Stream() dispatch invokes
	// checkArtifactCommitGate() before handleArtifactUploaded; a
	// not-ready registry returns codes.PermissionDenied.
	capabilityRegistry *registry.CapabilityRegistry

	mu             sync.RWMutex
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

	// gRPC request context (carries trace context via otelgrpc).
	// Scorecard v2 / Step 15c: handlers use this instead of context.Background()
	// so spans have proper parent-child trace relationships.
	ctx    context.Context

	// Serialized output: all stream.Send() calls go through sendCh → sessionWriter.
	// No other goroutine may call stream.Send() directly.
	sendCh chan *outboundMessage

	// writerErr is a small (cap 1) channel used by sessionWriter to signal
	// a stream.Send() failure back to the Stream() main loop. Phase 4.2
	// requirement: a network-level send error MUST terminate the session,
	// otherwise pending offers can be left orphaned silently. The main loop
	// reads writerErr inside its select and triggers a teardown on receipt.
	writerErr chan error // Job offering synchronization (Issue 4 fix).
	// PR #4: replaced pendingOffer (job-based) with pendingTaskOffer (task-based).
	pendingTaskOffer *taskgraph.TaskWithSpec // TaskOffer sent, awaiting TaskAccepted/TaskRejected
	claimMu          sync.Mutex              // serializes the claim+send+set flow; also guards pendingTaskOffer r/w

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

	// Placement snapshot fields: typed executor map, capability map,
	// and their revision counter. Populated at Hello time and updated
	// on heartbeat-driven re-advertisement. The placement snapshot is
	// built from these fields under RLock so the snapshot is always
	// consistent without blocking the main message loop.
	executorsMu sync.RWMutex
	executors   map[placement.ExecutorKey]struct{}

	capabilitiesMu sync.RWMutex
	capabilities   map[string]bool

	capabilityRevision atomic.Uint64

	ready    atomic.Bool
	draining atomic.Bool

	lastHeartbeatUnix atomic.Int64

	// Version correlation (Step 4 / Velox Metrics Center): software
	// versions reported by the worker via heartbeat, stored on the
	// session so they can be stamped on task_attempts at report time.
	gitSHA        atomic.Value // string
	workerVersion atomic.Value // string
	engineVersion atomic.Value // string
	ffmpegVersion atomic.Value // string
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
	jobsRepo jobs.Repository,
	taskRepo taskgraph.Repository,
	taskAttemptRepo taskattempts.Repository,
	artifactSvc *artifacts.Service,
	dbStore *store.SQLiteStore,
	config *HandlerConfig,
) *Handler {
	if config == nil {
		config = &HandlerConfig{PushMode: true}
	}
	return &Handler{
		registry:         registry,
		cmdMgr:           cmdMgr,
		jobsRepo:         jobsRepo,
		taskRepo:         taskRepo,
		taskAttemptRepo:  taskAttemptRepo,
		artifactSvc:      artifactSvc,
		dbStore:          dbStore,
		config:           config,
		authorizer:       NewAllowlistAuthorizer(config.AllowedWorkers, config.AllowInsecure),
		placementMatcher: placement.NewMatcher(),
		sessions:         make(map[string]*workerSession),
		workerSessions:   make(map[string]string),
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

	// Add OpenTelemetry stats handler for gRPC trace context propagation.
	// otelgrpc.NewServerHandler() extracts W3C traceparent from inbound
	// gRPC metadata and injects it into the request context.
	// Scorecard v2 / Step 15c.
	grpcOpts = append(grpcOpts, grpc.StatsHandler(otelgrpc.NewServerHandler()))

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

// placementSnapshot builds an immutable WorkerSnapshot from the in-memory
// session state. The snapshot is consistent at a single instant (executors
// and capabilities read under their respective RLock). The caller must
// NOT hold any session mutex when calling this method.
func (s *workerSession) placementSnapshot(workerID string) placement.WorkerSnapshot {
	s.executorsMu.RLock()
	executors := make(map[placement.ExecutorKey]struct{}, len(s.executors))
	for key := range s.executors {
		executors[key] = struct{}{}
	}
	s.executorsMu.RUnlock()

	s.capabilitiesMu.RLock()
	caps := make(map[string]bool, len(s.capabilities))
	for key, enabled := range s.capabilities {
		caps[key] = enabled
	}
	s.capabilitiesMu.RUnlock()

	return placement.WorkerSnapshot{
		WorkerID:           workerID,
		SessionID:          s.sessionID,
		Ready:              s.ready.Load(),
		Draining:           s.draining.Load(),
		SessionAlive:       true,
		MaxParallelJobs:    int(s.maxParallelJobs.Load()),
		ActiveJobs:         int(s.activeJobsCount.Load()),
		Executors:          executors,
		Capabilities:       caps,
		CapabilityRevision: s.capabilityRevision.Load(),
		LastHeartbeat: time.Unix(
			s.lastHeartbeatUnix.Load(),
			0,
		).UTC(),
	}
}

// replaceCapabilities atomically replaces the session's executor and
// capability maps with the parsed values from the Hello handshake.
// It bumps the capability revision so any pending claim that was
// built from a stale snapshot can be detected by the fencing check.
func (s *workerSession) replaceCapabilities(
	executors map[placement.ExecutorKey]struct{},
	capabilities map[string]bool,
) {
	s.executorsMu.Lock()
	s.executors = executors
	s.executorsMu.Unlock()

	s.capabilitiesMu.Lock()
	s.capabilities = capabilities
	s.capabilitiesMu.Unlock()

	s.capabilityRevision.Add(1)
}

func maxParallelJobsFromCapabilities(capsMap map[string]interface{}) int {
	if capsMap == nil {
		return 0
	}
	if mpj, ok := capsMap["max_parallel_jobs"]; ok {
		switch v := mpj.(type) {
		case float64:
			return int(v)
		case int:
			return v
		case int32:
			return int(v)
		case int64:
			return int(v)
		}
	}
	if host, ok := capsMap["host"].(map[string]interface{}); ok {
		if mpj, ok := host["max_parallel_jobs"]; ok {
			switch v := mpj.(type) {
			case float64:
				return int(v)
			case int:
				return v
			case int32:
				return int(v)
			case int64:
				return int(v)
			}
		}
	}
	return 0
}

// invalidateExecutor removes a single executor key from the session's
// executor map and bumps the capability revision. Called when the
// worker rejects a task with reason="unsupported_executor" — the
// placement snapshot said the worker supports this executor, but the
// worker disagrees. Invalidating prevents further offers of the same
// incompatible executor until the next Hello re-advertises it.
func (s *workerSession) invalidateExecutor(key placement.ExecutorKey) {
	s.executorsMu.Lock()
	delete(s.executors, key)
	s.executorsMu.Unlock()

	s.capabilityRevision.Add(1)
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

// SetIngestionSvc installs the canonical TaskReportIngestionService so
// handleTaskResult can delegate to it. Bootstrap calls this immediately
// after NewHandler to wire the audit closure. Setting nil clears the
// reference (useful for tests that swap services mid-flight).
func (h *Handler) SetIngestionSvc(svc *ingest.TaskReportIngestionService) {
	h.ingestionSvc = svc
}

// SetResourceSink installs the WorkerResourceSink used by handleHeartbeat
// (Scorecard v1 / F2). Bootstrap wires metrics.NewCollector here; tests
// inject a recording stub. NIL-safe — handlers without a metrics surface
// still persist the typed heartbeat via registry.Heartbeat() but skip
// the Prometheus projection.
func (h *Handler) SetResourceSink(sink velmetrics.WorkerResourceSink) {
	h.resourceSink = sink
}

// SetPlacementRejectionSink installs the PlacementRejectionSink used by
// the placement pipeline (recordPlacementRejections + handleUnsupportedExecutorRejection).
// Bootstrap wires metrics.NewCollector here; tests inject a recording stub.
// NIL-safe — handlers without a metrics surface still log rejections but
// skip the Prometheus projection.
func (h *Handler) SetPlacementRejectionSink(sink velmetrics.PlacementRejectionSink) {
	h.placementRejectionSink = sink
}

// SetCapabilityRegistry installs the readiness registry that gates the
// on-the-wire "artifact.commit.v1" dispatch path. Bootstrap wires the
// canonical registry.NewCapabilityRegistry() (with coordinator + spool +
// transport probes) here; tests can inject a focused registry to verify
// the fail-closed semantic in handler_artifacts_test.go.
//
// NIL-safe — a Handler constructed without the registry (legacy test
// paths, partial-wiring bootstrap variants) skips the gate entirely.
func (h *Handler) SetCapabilityRegistry(r *registry.CapabilityRegistry) {
	h.capabilityRegistry = r
}

// ingestionService returns the wired TaskReportIngestionService, or nil
// if not configured. Exported as a typed accessor for tests that want
// to verify the wiring contract.
func (h *Handler) ingestionService() *ingest.TaskReportIngestionService {
	return h.ingestionSvc
}

// validateCredentialHash checks the worker's credential_hash against the
// stored persistent credential in SQLite (worker_credentials table).
// Accepts the credential hash string directly from typed Hello message.
//
// Nil dbStore is safe: returns nil (skip validation) — this lets protocol-
// level tests and boot-dry-run handlers operate without a live DB handle.
func (h *Handler) validateCredentialHash(workerID string, declaredHash string) error {
	if h.dbStore == nil {
		return nil
	}
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
