// Package worker — type definitions extracted from worker_init.go.
package worker

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"velox-shared/controltransport"
	"velox-worker-agent/internal/artifactgraph"
	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/prefetch"
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/spool"
	"velox-worker-agent/internal/taskrunner"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/internal/worker/concurrency"
	"velox-worker-agent/internal/worker/stageexec"
	"velox-worker-agent/internal/workercache"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/blob"
	"velox-worker-agent/pkg/cache"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/storage"
)

// Status represents the current status of a worker.
type Status string

const (
	StatusIdle    Status = "idle"
	StatusBusy    Status = "busy"
	StatusError   Status = "error"
	StatusStopped Status = "stopped"
)

// ConnectionState represents the worker's connection state to the master.
type ConnectionState string

const (
	ConnDisconnected   ConnectionState = "disconnected"
	ConnConnecting     ConnectionState = "connecting"
	ConnAuthenticating ConnectionState = "authenticating"
	ConnReady          ConnectionState = "ready"
	ConnDraining       ConnectionState = "draining"
)

// Registration backoff constants
const (
	registrationInitialBackoff = 5 * time.Second
	registrationMaxBackoff     = 5 * time.Minute
	registrationBackoffMult    = 2.0

	// connectionRetryBackoff is a short fixed delay used for connection-level
	// errors (reset, refused, transport unavailable). These typically happen
	// when the server is restarting and will recover in seconds. Exponential
	// backoff is reserved for application-level errors (credential mismatch,
	// protocol version, TLS).
	connectionRetryBackoff = 2 * time.Second

	// restartDrainTimeout is the maximum time the worker waits after
	// receiving a restart_worker / reboot_host command before auto-clearing
	// drain mode. If the process hasn't exited within this window (e.g.
	// Docker container restart failed), the worker self-heals by calling
	// autoUndrainAfter so it can resume accepting tasks. This prevents the
	// permanent-drain freeze documented in the gRPC drain-state disconnect
	// root-cause analysis.
	restartDrainTimeout = 120 * time.Second
)

// ActiveTaskExecution represents a single task execution in progress.
// Keyed by taskID (not jobID) so multi-task DAGs never collide.
type ActiveTaskExecution struct {
	TaskID        string
	AttemptID     string
	JobID         string
	Task          *PendingTaskExecution
	LeaseID       string
	StartedAt     time.Time
	Cancel        context.CancelFunc
	Progress      JobProgress
	AttemptEvents *telemetry.AttemptEventMachine
	// Fase E2: per-attempt intermediate-file telemetry (AttemptArtifactGraph).
	// Created in dispatchTaskRunner, threaded through the dispatch context
	// (artifactgraph.GraphFromContext), and profiled at attempt end. Executors
	// consume it via the context — never a global ledger.
	ArtifactGraph *artifactgraph.Graph
}

// PendingTaskExecution is the typed, strongly-validated representation of
// a task the worker has accepted via TaskOffer and is awaiting
// TaskLeaseGranted before dispatch. All identity fields are explicit
// typed members — no hidden keys in Parameters.
type PendingTaskExecution struct {
	TaskID          string
	JobID           string
	JobRevision     int
	AttemptID       string
	AttemptNumber   int
	LeaseID         string
	ExecutorID      string
	ExecutorVersion int
	Revision        int
	Spec            executor.TaskSpec
}

// ActiveTaskLease tracks a leased task-native entry for periodic
// MsgTaskLeaseRenewal dispatch (PR-2 / canonical-attempt-identity).
// Carries the full 6-field identity tuple so leaseRenewLoop can send
// it on every renewal tick.
type ActiveTaskLease struct {
	TaskID        string
	JobID         string
	AttemptID     string
	LeaseID       string
	AttemptNumber int
	Revision      int
}

// JobProgress is the in-memory projection of the canonical Attempt progress.
// It is published through active_jobs and is not a second progress tracker.
type JobProgress struct {
	Percent           int32
	Scene             int32
	TotalScenes       int32
	Segment           int32
	TotalSegments     int32
	SegmentCompleted  bool
	Phase             string
	Stage             string // legacy alias for phase in older worker consumers
	FramesEncoded     int64
	FramesDecoded     int64
	FramesComposited  int64
	FfmpegSpeedX      float64
	ElapsedMS         int64
	LastProgressAt    time.Time
	LastPublishedAt   time.Time // heartbeat publication throttle; not a second tracker
	CumulativeMetrics map[string]float64
}

// backoffConfig configures exponential backoff for retry operations.
type backoffConfig struct {
	initialInterval time.Duration
	maxInterval     time.Duration
	multiplier      float64
}

// ExitFunc is the function type for worker exit (used for testing).
type ExitFunc func(int)

// Worker represents a Velox worker agent.
type Worker struct {
	config           *config.WorkerConfig
	apiClient        *api.Client                              // Retained for data-plane operations (upload, asset download)
	transport        controltransport.ControlTransport        // Current session's transport (recreated per connect)
	transportFactory func() controltransport.ControlTransport // Factory for new transport instances
	logger           *logger.Logger
	// metrics is worker-instance state. Production workers never share job
	// counters through telemetry's process-global compatibility facade.
	// Nil is tolerated only for legacy test fixtures that construct Worker
	// literals instead of going through New.
	metrics *telemetry.WorkerMetrics

	// assetManager is the canonical asset download orchestrator (dedup,
	// transfer states, bounded pool, shared progress). Constructed lazily on
	// first use; closed in Stop(). The Transferer it runs is the master-bridge
	// pipeline (asset_downloader.go).
	assetManager   *downloader.Manager
	assetManagerMu sync.Mutex
	// cacheResolver is the canonical structured-resolution adapter over
	// assetManager (downloader/cache_resolution.go). It is the single
	// emission point for cache telemetry: the attempt-scoped tracker and
	// the worker-lifetime Prometheus view are both fed from one
	// RecordResolution call per resolved asset. Rebuilt after Stop() nils
	// the manager.
	cacheResolver       *downloader.CacheResolver
	cacheResolverMu     sync.Mutex
	transportMu         sync.RWMutex
	assetProgressQueue  chan assetProgressEnvelope
	assetProgressSendMu sync.Mutex
	assetProgressOnce   sync.Once
	// futureAssetController reconciles master snapshots only. It deliberately
	// does not own downloads; future prefetch will call the existing resolver
	// after reservation/budget gates are wired.
	prefetchController *prefetch.Controller
	prefetchScheduler  *prefetch.Scheduler

	// assetIntegrity remembers the self-verified digest+size of the most
	// recent successful download of each asset (computed while the file was
	// written). Payloads that reference velox-asset://<id> without sha256 /
	// size_bytes can then still reach the integrity-verified cache-hit path
	// on later accesses instead of re-downloading on every use. In-memory
	// only: after a restart the next access simply re-downloads once.
	assetIntegrity   map[string]assetIntegrityRecord
	assetIntegrityMu sync.Mutex

	// Status management — error state only; busy/idle derived from activeTasks
	status Status
	mu     sync.RWMutex
	// heartbeatWake publishes task start/finish edges immediately instead of
	// waiting for the previous idle ticker to expire.
	heartbeatWake chan struct{}
	// jobDone is signalled only after terminal TaskResult confirmation. It is
	// consumed by the remote shared-asset cleanup loop to run cleanup promptly
	// after a task, in addition to the periodic interval.
	jobDone chan struct{}

	// Active task executions: keyed by taskID for collision-free multi-task DAGs.
	// taskIDsByJob provides the reverse-lookup needed for CancelJob.
	activeTasks   map[string]*ActiveTaskExecution
	activeTasksMu sync.RWMutex
	taskIDsByJob  map[string][]string // jobID → []taskID

	// Connection state machine
	connState        ConnectionState
	connStateMu      sync.RWMutex
	connFailureCount int

	// Lifecycle management
	stopChan chan struct{}
	stopOnce sync.Once
	stopped  atomic.Bool
	wg       sync.WaitGroup

	// Backoff for heartbeat failures
	heartbeatBackoff *backoffConfig

	version string

	// releaseIdentity is the immutable per-process release certificate
	// (image-baked BUILD_INFO.json + engine SHA file + config + env).
	// Assembled once on first capabilitiesMap call (hello/heartbeat); see
	// loadReleaseIdentity in release_identity.go.
	releaseIdentity     controltransport.ReleaseIdentity
	releaseIdentityOnce sync.Once

	// telemetrySeq is the monotonically increasing sequence for the typed
	// WorkerTelemetrySnapshot emitted on every heartbeat. The master's
	// TelemetryGate rejects out-of-order / replayed snapshots against it
	// (shared/controltransport/telemetry_snapshot.go).

	// Command management
	drainMode    atomic.Bool
	commandMu    sync.Mutex
	seenCommands map[string]time.Time

	// Pending tasks: accepted via TaskOffer, waiting for TaskLeaseGranted
	// before executeTask dispatch (PR-2 canonical-attempt-identity). The
	// map is keyed by task_id (NOT job_id, NOT attempt_id) because
	// (task_id, worker_id, lease_id) is the canonical worker-bound
	// identity on the master's side and there is exactly one outstanding
	// offer per task per session.
	pendingTasks   map[string]*PendingTaskExecution
	pendingTasksMu sync.Mutex

	// Active task-native leases: keyed by task_id; the iteration source
	// for MsgTaskLeaseRenewal dispatch in leaseRenewLoop. Populated on
	// MsgTaskLeaseGranted (alongside pendingTasks → executeTask), drained
	// on Stop() / canonical terminal-state transition. Each entry carries
	// (task_id, attempt_id, lease_id) so the master's RenewLease CAS
	// predicate matches the canonical TaskAttempt row.
	activeTaskLeases   map[string]*ActiveTaskLease
	activeTaskLeasesMu sync.RWMutex

	// Artifact Commit Protocol v1 (Fase 3.4 / 3.6) — typed pending
	// reply dispatcher. The executeTask pipeline registers a
	// per-taskID channel before sending TaskOutputDeclared /
	// ArtifactUploadCompleted; the receive loop dispatches the
	// master's ArtifactUploadPlan / TaskCommitAck reply into the
	// channel and the pipeline unblocks. Channels are buffered (cap 1)
	// and removed on terminal exit or worker Stop.
	pendingArtifactAcks   map[string]chan controltransport.ControlMessage
	pendingArtifactAcksMu sync.RWMutex

	// reporter is the reporting subsystem (durable TaskResult outbox, ACK
	// waiter registry, replay loop, terminal-output cleanup) composed behind
	// a small interface. The Worker no longer owns the ACK maps/mutex or the
	// outbox methods directly; it delegates post-execution reporting to this
	// seam. Non-nil for every Worker returned by New(); nil only in
	// hand-built legacy test fixtures that never submit a result.
	reporter TaskResultReporter

	// Task completion stats for heartbeat reporting.
	// Wire keys (jobs_completed / jobs_failed) kept for master compatibility.
	tasksCompleted atomic.Int64
	tasksFailed    atomic.Int64

	// Telemetry snapshot sequence (monotonic per process; see telemetry_snapshot.go).
	telemetrySeq atomic.Uint64

	recentLogs *recentLogBuffer

	// Concurrency limiter (Phase 1: worker policy)
	concurrencyLimiter *concurrency.ConcurrencyLimiter // Stage executor (Step 2: stage/chunk execution with retry)
	stageExecutor      *stageexec.StageExecutor

	// Executor registry (PR-3.5): single source of truth for hello/heartbeat
	// capabilities and (eventually) for the taskrouter dispatch table.
	// Never nil after Worker construction — defaults to an empty registry
	// when no WithRegistry option is supplied to New().
	executorRegistry *executor.Registry

	// PR-3.7: persistent local cache + blob store + the TaskRunner
	// built from them. cache + blobs are non-nil only when the
	// corresponding With* option is supplied; taskRunner is always
	// non-nil (built from cache/blobs/registry in New).
	cache      *cache.PersistedLocalCache
	blobs      *blob.BlobArtifacts
	taskRunner *taskrunner.TaskRunner

	// publisherRegistry resolves the transport named by the master's
	// ArtifactUploadPlan. It is shared across attempts and is nil only
	// in legacy/headless test workers that do not use the typed sidecar
	// publication path.
	publisherRegistry *publisher.Registry
	outputSpool       *spool.Store

	// onWorkerIDCollision (RW-PROD-005 §3 anti-collision invariant) is
	// the optional observer invoked when the master rejects the worker's
	// Hello handshake with codes.AlreadyExists because another machine is
	// already registered with the same worker_id on a different
	// credential. Set via WithCollisionObserver() (typically by
	// cmd/velox-worker-agent/main.go to log the diagnostic + os.Exit(17)).
	// nil-safe: Start() guards w.onWorkerIDCollision != nil before invoking
	// so the worker package remains usable in unit tests that don't care
	// about the collision signal.
	onWorkerIDCollision func(err error)

	// Optional worker-side clip cache (workercache.Cache). When non-nil,
	// dispatchTaskRunner acquires a per-job lease relation on every cached
	// asset referenced by the job payload BEFORE invoking taskRunner.Run.
	// A defer at the same scope releases the relation on success, error, or
	// panic so workercache.Cleanup never deletes an asset inside an active
	// render.
	//
	// nil is acceptable for legacy/headless workers; legacy payloads skip
	// the lease step. CompiledRenderPlanV2 dispatch fails closed when this
	// cache is nil because every V2 input must be leased before execution.
	clipCache *workercache.Cache
	// canonicalAssetCache is the typed AssetRegistry → ContentAddressedCache
	// facade used by the download adapter. clipCache remains for legacy lease
	// call sites while new paths depend on this single typed boundary.
	canonicalAssetCache workercache.ContentAddressedCache

	// PR-3.6 / F4: worker-side resource sampler. Powers Heartbeat.resources
	// (cumulative typed counters → master F2 decodes + delta-converts) AND
	// api.HostInfo.{HasGPU,RAMBytes,DiskFreeBytes} (PR-3.6 future markers
	// at worker.go:177-183). Created in New(); goroutine launched in
	// runSession under sessionCtx so the loop terminates with the
	// session. nil-safe read paths in hostInfo / sendHeartbeat tolerate
	// a sampler that hasn't yet sampled.
	sampler *telemetry.Sampler

	// Fase E1 StorageResolver: the single placement abstraction over the
	// worker's storage classes (CACHE_PERSISTENT / ATTEMPT_TEMP /
	// ARTIFACT_FINAL) with the tmpfs threshold gate. Built in New() from
	// cfg; immutable after construction. nil only on construction failure
	// (which aborts New). Later phases (E2/F) consume it for cache, temp
	// and artifact placement.
	storageResolver *storage.Resolver

	// Exit function (for testing, defaults to os.Exit)
	exitFunc ExitFunc
}

// recentLogBuffer is a thread-safe ring buffer for recent log lines.
type recentLogBuffer struct {
	mu      sync.Mutex
	lines   []string
	errors  []string
	partial string
	max     int
}

func newRecentLogBuffer(max int) *recentLogBuffer {
	if max <= 0 {
		max = 500
	}
	return &recentLogBuffer{max: max}
}

func (b *recentLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	chunk := b.partial + string(p)
	parts := strings.Split(chunk, "\n")
	if len(parts) == 0 {
		b.partial = ""
		return len(p), nil
	}

	for i := 0; i < len(parts)-1; i++ {
		b.appendLineLocked(parts[i])
	}
	b.partial = parts[len(parts)-1]
	return len(p), nil
}

func (b *recentLogBuffer) appendLineLocked(line string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return
	}
	b.lines = append(b.lines, line)
	if len(b.lines) > b.max {
		b.lines = b.lines[len(b.lines)-b.max:]
	}

	ll := strings.ToLower(line)
	if strings.Contains(ll, "[error]") || strings.Contains(ll, " error ") || strings.HasSuffix(ll, " error") || strings.HasPrefix(ll, "error ") {
		b.errors = append(b.errors, line)
		if len(b.errors) > b.max {
			b.errors = b.errors[len(b.errors)-b.max:]
		}
	}
}

func (b *recentLogBuffer) Snapshot(maxLogs, maxErrors int) ([]string, []string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	outLogs := append([]string(nil), b.lines...)
	outErrs := append([]string(nil), b.errors...)

	if maxLogs > 0 && len(outLogs) > maxLogs {
		outLogs = outLogs[len(outLogs)-maxLogs:]
	}
	if maxErrors > 0 && len(outErrs) > maxErrors {
		outErrs = outErrs[len(outErrs)-maxErrors:]
	}
	return outLogs, outErrs
}
