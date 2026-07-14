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
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
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
	pb "velox-shared/controltransport/pb"
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
	ctx context.Context

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
