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
	"sync"

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
