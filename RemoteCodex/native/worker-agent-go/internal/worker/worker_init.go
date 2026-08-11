// Package worker — initialization and lifecycle management.
//
// File split:
//   - worker_init.go          : Option types + all With* setters + New().
//   - worker_status.go        : Status/setStatus/canTransitionTo/IsStopped/
//     IsDraining/cancelJob + SetExitFunc.
//   - worker_commands.go      : processCommand + autoUndrainAfter (command
//     dispatch, pre-existing).
//   - worker_command_dedup.go : commandKey/markCommandSeen/cleanupSeenCommands
//     (command dedup state).
package worker

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
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

// Option configures a Worker returned by New. Backward-compatible:
// existing callers passing only (cfg, version) keep working.
type Option func(*workerOptions)

type workerOptions struct {
	registry            *executor.Registry
	cache               *cache.PersistedLocalCache
	blobs               *blob.BlobArtifacts
	clipCache           *workercache.Cache
	onWorkerIDCollision func(err error)
}

// WithRegistry replaces the default (empty) executor registry. The
// caller owns the registry — Register calls after New() still take
// effect because the worker holds the same pointer.
// // This is the single supported way to surface hello/heartbeat
// capabilities.
//
// Passing nil panics loudly. The previous silent fallback to a fresh
// empty registry masked operator bugs (worker booted, advertised zero
// executors, every job routed to dead-letter). Loud startup is the
// correct safety posture.
func WithRegistry(reg *executor.Registry) Option {
	if reg == nil {
		panic("worker.WithRegistry: registry must not be nil — pass an explicit *executor.Registry or omit WithRegistry")
	}
	return func(o *workerOptions) {
		o.registry = reg
	}
}

// WithCache wires a persistent local cache into the worker.
// The same instance is exposed via Worker.cache and is threaded into
// the TaskRunner built by New() so cache hits/misses/evictions/
// corruptions appear in TaskExecutionReport.Metrics.
//
// Passing nil panics loudly; omit WithCache to fall back to noop
// defaults (useful only for unit tests that don't exercise the
// cache surface).
func WithCache(c *cache.PersistedLocalCache) Option {
	if c == nil {
		panic("worker.WithCache: cache must not be nil — pass an explicit *cache.PersistedLocalCache or omit WithCache")
	}
	return func(o *workerOptions) {
		o.cache = c
	}
} // WithBlobs wires a content-addressed blob store into the
// worker. Same instance is exposed via Worker.blobs and threaded
// into the TaskRunner built by New(); the upload queue is consumed
// by master-side transport.
//
// Passing nil panics loudly; omit WithBlobs to fall back to noop.
func WithBlobs(b *blob.BlobArtifacts) Option {
	if b == nil {
		panic("worker.WithBlobs: blobs must not be nil — pass an explicit *blob.BlobArtifacts or omit WithBlobs")
	}
	return func(o *workerOptions) {
		o.blobs = b
	}
}

// WithClipCache wires the worker-side workercache.Cache into the
// Worker. When set, dispatchTaskRunner acquires a per-job lease relation
// on every cached asset referenced by the job payload BEFORE invoking
// taskRunner.Run, and a defer at the same scope releases it on
// success/error/panic so the workercache.Cleanup loop never deletes an
// asset inside an active render.
//
// Passing nil panics loudly; omit WithClipCache to disable the
// lease surface entirely (legacy bootstrap profiles, headless
// tests, and workers without a clip cache SQLite).
func WithClipCache(c *workercache.Cache) Option {
	if c == nil {
		panic("worker.WithClipCache: cache must not be nil — pass an explicit *workercache.Cache or omit WithClipCache")
	}
	return func(o *workerOptions) {
		o.clipCache = c
	}
}

// WithCollisionObserver installs a callback invoked when the master
// rejects the worker's Hello handshake with codes.AlreadyExists because
// another machine is already registered with the same worker_id on a
// different credential (anti-collision invariant RW-PROD-005 §3).
//
// The callback is the SINGLE point of policy for "what should the
// worker do when this happens". The default production handler in
// cmd/velox-worker-agent/main.go logs the diagnostic to stderr and
// calls os.Exit(17) — a hard configuration error (two physical
// machines sharing a worker_id) is not safe to retry with backoff
// because doing so would mask the underlying operational fault and
// keep both machines in a flaky thrash.
//
// A non-nil callback REPLACES the default (no-op) behavior. Pass
// nil to opt out of the observer entirely (legacy / override mode
// where the operator accepts that two machines may register with
// the same worker_id and prefers the worker to keep trying with
// backoff instead of exiting). In production the
// VELOX_ALLOW_MULTI_HOST_WORKER_IDS env var (default false) gates
// whether the observer is wired at all.
//
// The callback receives the underlying ErrWorkerIDCollision-wrapped
// error for log context (peer IP, original gRPC status, etc.). It
// MUST be safe to call from the Start() goroutine context; the
// worker holds no locks during invocation.
func WithCollisionObserver(fn func(err error)) Option {
	return func(o *workerOptions) {
		o.onWorkerIDCollision = fn
	}
}

// New creates a new Worker instance.
// Returns an error if the initial transport setup fails (bad TLS config,
// missing control_grpc_url, insecure flag mismatch). Callers MUST check
// the error before calling Start().
// // Options: pass worker.WithRegistry(reg) to install a custom
// executor registry; otherwise an empty registry is used so hello is
// emitted immediately and dispatch upgrades are non-breaking.
func New(cfg *config.WorkerConfig, version string, opts ...Option) (*Worker, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid worker config: %w", err)
	}

	wo := &workerOptions{registry: executor.NewRegistry()}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(wo)
	}
	// wo.registry is never nil after the loop: WithRegistry(nil) panics,
	// the default above guarantees an empty registry. Defensive fallback
	// kept only for paranoid future callers adding their own Option that
	// deliberately clears the field; without it we'd crash on nil-deref.
	if wo.registry == nil {
		wo.registry = executor.NewRegistry()
	}

	logLevel := logger.ParseLevel(cfg.LogLevel)
	recentLogs := newRecentLogBuffer(600)
	logOut := io.MultiWriter(os.Stdout, recentLogs)
	log := logger.New(logLevel, logOut)
	log.SetPrefix(fmt.Sprintf("[%s]", cfg.WorkerID))

	// Use the configured MaxActiveJobs when explicitly set (>0).
	// Falls back to hardware-detected concurrency when the operator
	// has not set a value (MaxActiveJobs=0 in config). The >0 guard
	// replaces the previous >1 which silently ignored MaxActiveJobs=1.
	detectedConcurrency := detectMaxParallelJobs()
	if cfg.MaxActiveJobs > 0 {
		detectedConcurrency = cfg.MaxActiveJobs
	}
	log.Info("[CONCURRENCY] Detected %d CPUs, using %d max parallel jobs", runtime.NumCPU(), detectedConcurrency)

	apiClient := api.NewClient(cfg.MasterURL,
		api.WithWorkerID(cfg.WorkerID),
		api.WithTimeout(30*time.Second),
		api.WithRetry(3, 5*time.Second),
		api.WithCircuitBreaker(
			cfg.CircuitBreakerFailureThreshold,
			cfg.CircuitBreakerSuccessThreshold,
			time.Duration(cfg.CircuitBreakerTimeoutSecs)*time.Second,
		),
	)
	if token := strings.TrimSpace(os.Getenv("WORKER_TOKEN")); token != "" {
		apiClient.SetAuthToken(token)
		log.Info("[AUTH] Loaded worker token from WORKER_TOKEN")
	} else if token := strings.TrimSpace(os.Getenv("VELOX_WORKER_TOKEN")); token != "" {
		apiClient.SetAuthToken(token)
		log.Info("[AUTH] Loaded worker token from VELOX_WORKER_TOKEN")
	}
	if token := strings.TrimSpace(os.Getenv("VELOX_ADMIN_TOKEN")); token != "" {
		apiClient.SetAdminAuthToken(token)
		log.Info("[AUTH] Loaded separate admin token for artifact uploads")
	}

	// Initialize stage executor for GOD workflow
	stageExecCfg := &stageexec.StageExecutorConfig{
		MaxConcurrentChunks: cfg.MaxActiveJobs,
		ChunkTimeout:        5 * time.Minute,
		MaxChunkRetries:     2,
		ChunkRetryDelay:     2 * time.Second,
		StageTimeout:        15 * time.Minute,
	}
	stageExecutor := stageexec.NewStageExecutor(stageExecCfg)

	// Store a transport factory that creates fresh instances per session.
	// After Close(), transports are not reusable (channels + sync.Once),
	// so each reconnect loop iteration gets a brand-new transport.
	// Phase 2.1: the factory now returns (Transport, error); a non-nil error
	// surface here would mean config validation failed at startup time.
	transportFactory := func() controltransport.ControlTransport {
		t, err := newControlTransport(cfg, log)
		if err != nil {
			log.Error("[INIT] transport factory rejected config: %v", err)
			return nil
		}
		return t
	}

	initialTransport, err := newControlTransport(cfg, log)
	if err != nil {
		// Config problem on the very first attempt — fail the worker init
		// immediately so operators do not enter the reconnect loop with a
		// broken transport (previously this nil-panicked on first Connect).
		log.Error("[INIT] initial transport setup failed: %v", err)
		return nil, fmt.Errorf("transport factory: %w", err)
	}

	// Open the durable output spool before constructing the worker. Every
	// typed artifact publication registers its local file here before the
	// Master sees TaskOutputDeclared, so a worker restart can resume or
	// audit an output instead of losing the progress receipt.
	stateDir := cfg.StateDir
	if stateDir == "" {
		// Config loaded through LoadConfig receives the production
		// /var/lib default. Keep direct unit-test construction safe by
		// using a worker-scoped temp root when that defaulting step was
		// intentionally skipped.
		stateDir = filepath.Join(os.TempDir(), "velox-worker", cfg.WorkerID)
	}
	spoolDir := filepath.Join(stateDir, "executor_spool")
	if err := os.MkdirAll(spoolDir, 0o750); err != nil {
		_ = initialTransport.Close()
		return nil, fmt.Errorf("create output spool directory: %w", err)
	}
	outputSpool, err := spool.Open(filepath.Join(spoolDir, "worker_output_spool.sqlite3"))
	if err != nil {
		_ = initialTransport.Close()
		return nil, fmt.Errorf("open output spool: %w", err)
	}

	// Fase E1: build the StorageResolver from the resolved config. The
	// cache backing mirrors the doctor's canonical resolution
	// (VELOX_WORKER_CACHE_DIR || <state_dir>/cache); ATTEMPT_TEMP maps to
	// TempDir (with the opt-in tmpfs gate); ARTIFACT_FINAL maps to
	// OutputDir. Every NVMe backing falls back under the same stateDir
	// temp-root pattern used above, so minimal direct-construction configs
	// (unit tests, headless harnesses) never fail worker construction. The
	// threshold is repaired to the benchmarked default when a
	// direct-construction config skipped applyDefaults.
	storageCacheDir := strings.TrimSpace(os.Getenv("VELOX_WORKER_CACHE_DIR"))
	if storageCacheDir == "" {
		storageCacheDir = filepath.Join(stateDir, "cache")
	}
	storageTempDir := strings.TrimSpace(cfg.TempDir)
	if storageTempDir == "" {
		storageTempDir = filepath.Join(stateDir, "temp")
	}
	storageArtifactDir := strings.TrimSpace(cfg.OutputDir)
	if storageArtifactDir == "" {
		storageArtifactDir = filepath.Join(stateDir, "artifact")
	}
	storageThreshold := cfg.TmpfsThresholdBytes
	if storageThreshold <= 0 {
		storageThreshold = config.DefaultTmpfsThresholdBytes
	}
	storageResolver, err := storage.New(storage.Config{
		CacheDir:            storageCacheDir,
		TempDir:             storageTempDir,
		TmpfsDir:            cfg.TmpfsDir,
		ArtifactDir:         storageArtifactDir,
		TmpfsThresholdBytes: storageThreshold,
	})
	if err != nil {
		_ = initialTransport.Close()
		return nil, fmt.Errorf("build storage resolver: %w", err)
	}
	log.Info("[STORAGE] %s", storageResolver.String())

	// Build the TaskRunner from registry + cache + blobs. The
	// runner is shared by future executeTask routes and is also
	// where cache + blob counters get surfaced as report.Metrics entries.
	tr := taskrunner.NewTaskRunner(wo.registry, log)
	if wo.cache != nil {
		tr = tr.WithCache(wo.cache).WithCacheStats(wo.cache)
	}
	if wo.blobs != nil {
		tr = tr.WithArtifacts(wo.blobs).WithBlobStats(wo.blobs)
	}

	sampler := telemetry.NewResourceSampler("", "", cfg.WorkDir, 0, 0)
	sampler.SetTempDir(cfg.TempDir)
	w := &Worker{
		config:           cfg,
		apiClient:        apiClient,
		transportFactory: transportFactory,
		transport:        initialTransport,
		logger:           log,
		status:           StatusIdle,
		stopChan:         make(chan struct{}),
		heartbeatWake:    make(chan struct{}, 1),
		jobDone:          make(chan struct{}, 1),
		heartbeatBackoff: &backoffConfig{
			initialInterval: 5 * time.Second,
			maxInterval:     60 * time.Second,
			multiplier:      2.0,
		},
		version: version,

		seenCommands: make(map[string]time.Time),
		recentLogs:   recentLogs,
		activeTasks:  make(map[string]*ActiveTaskExecution),
		taskIDsByJob: make(map[string][]string),
		// Remembered self-verified digests for partial-metadata cache hits.
		assetIntegrity:     make(map[string]assetIntegrityRecord),
		prefetchController: prefetch.NewController(cfg.WorkerID),
		// PR-2: TaskOffer-accepted tasks awaiting TaskLeaseGranted before
		// executeTask dispatch. Keyed by task_id — one canonical entry per
		// outstanding offer per session.
		pendingTasks: make(map[string]*PendingTaskExecution),
		// PR-2 followup: per-task-native lease-state registry. Threaded
		// by MsgTaskLeaseGranted handler (separate PR) so leaseRenewLoop
		// can fire MsgTaskLeaseRenewal.
		activeTaskLeases:          make(map[string]*ActiveTaskLease),
		pendingTaskResultAcks:     make(map[string]chan *pb.TaskResultAck),
		pendingTaskResultAckCache: make(map[string]taskResultAckCacheEntry),
		connState:                 ConnDisconnected,
		concurrencyLimiter:        concurrency.NewConcurrencyLimiter(detectedConcurrency),
		stageExecutor:             stageExecutor,
		executorRegistry:          wo.registry,
		cache:                     wo.cache,
		blobs:                     wo.blobs,
		clipCache:                 wo.clipCache,
		canonicalAssetCache:       workercache.NewCanonicalAssetStore(wo.clipCache),
		// Anti-collision observer (RW-PROD-005 §3): wired from
		// workerOptions.onWorkerIDCollision (set by
		// WithCollisionObserver). nil-safe; Start() guards before
		// invoking. Production default (cmd/velox-worker-agent/main.go)
		// logs the diagnostic to stderr + os.Exit(17).
		onWorkerIDCollision: wo.onWorkerIDCollision,
		taskRunner:          tr,
		publisherRegistry:   publisher.NewRegistry(),
		outputSpool:         outputSpool,
		// Resource sampler. Empty procRoot/sysRoot
		// defaults to /proc + /sys. cfg.WorkDir may be empty on
		// minimal test setups; the sampler tolerates that path
		// (statvfs + resolveWorkDirDevice degrade to best-effort).
		// 5s tick + 3-tick emit cadence is the default from
		// NewResourceSampler.
		sampler:         sampler,
		storageResolver: storageResolver,
		exitFunc:        os.Exit,
	}

	// Load persisted state from previous run (command dedup, job recovery info).
	w.loadLocalState()
	// Reclaim stale partial downloads at process startup. Active transfers
	// are not yet running, so only old files are eligible; fresh partials
	// remain available for the first DownloadManager request to resume.
	if _, err := cleanupOrphanedAssetPartials(w.assetCacheDir(), 24*time.Hour); err != nil {
		log.Warn("[ASSET] startup partial cleanup failed: %v", err)
	}

	return w, nil
}

// APIClient returns the authenticated data-plane client used by the worker.
// The registration handshake populates its bearer token; long-lived remote
// services such as the protected-assets poller must therefore share this
// exact client rather than constructing an unauthenticated second client.
func (w *Worker) APIClient() *api.Client {
	if w == nil {
		return nil
	}
	return w.apiClient
}

// JobDone returns a notification channel which receives a best-effort signal
// after the worker receives terminal confirmation for a task result. The
// channel is intentionally buffered/coalescing: cleanup needs a prompt
// wake-up, not one event per DAG subtask.
func (w *Worker) JobDone() <-chan struct{} {
	if w == nil {
		return nil
	}
	return w.jobDone
}

// StorageResolver returns the canonical Fase E1 storage resolver (cache /
// temp / artifact placement with the tmpfs threshold gate). Non-nil for
// every Worker returned by New(). Later phases (E2 executor placement, F
// artifact staging) consume storage through this seam instead of
// scattering os.TempDir() calls.
func (w *Worker) StorageResolver() *storage.Resolver {
	if w == nil {
		return nil
	}
	return w.storageResolver
}

// AttachClipCache is used only by the composition root while the worker is
// still stopped. It keeps construction of the durable SQLite index next to
// the process lifecycle while preserving the existing Option API for tests.
func (w *Worker) AttachClipCache(c *workercache.Cache) {
	if w == nil || c == nil {
		panic("worker.AttachClipCache: cache is required")
	}
	w.clipCache = c
	w.canonicalAssetCache = workercache.NewCanonicalAssetStore(c)
}

// signalTaskTerminal wakes the cache cleanup loop only after the master has
// acknowledged a terminal TaskResult. Render completion, artifact upload,
// and local task teardown must not signal cleanup on their own.
func (w *Worker) signalTaskTerminal() {
	if w == nil || w.jobDone == nil {
		return
	}
	select {
	case w.jobDone <- struct{}{}:
	default:
	}
}
