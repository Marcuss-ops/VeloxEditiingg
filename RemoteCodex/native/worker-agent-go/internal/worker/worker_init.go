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
	"velox-shared/futureasset"
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
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/storage"
)

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
		ArtifactStaging: storage.ArtifactStagingConfig{
			Enabled:      cfg.ArtifactTmpfsEnabled,
			Dir:          cfg.ArtifactTmpfsDir,
			MaxPercent:   cfg.ArtifactTmpfsMaxPercent,
			ReserveBytes: cfg.ArtifactTmpfsReserveBytes,
		},
		// The storage resolver reports staging fallback reasons and the live
		// tmpfs reservation ledger through the worker's Prometheus registry.
		StagingMetrics: telemetry.GetPrometheusMetrics(),
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
	// Fase E2: thread the canonical StorageResolver into the TaskRunner so
	// executors resolve ARTIFACT_STAGING output placement through the
	// single central decision (tmpfs-with-reservation / NVMe fallback).
	tr = tr.WithStorageResolver(storageResolver)

	sampler := telemetry.NewResourceSampler("", "", cfg.WorkDir, 0, 0)
	sampler.SetTempDir(cfg.TempDir)
	var ramCache *prefetch.RAMCache
	if cfg.PrefetchRAMEnabled {
		ramCache = prefetch.NewRAMCache(cfg.TmpfsDir, cfg.PrefetchRAMBudgetBytes, cfg.PrefetchRAMMaxAssetBytes)
	}
	w := &Worker{
		config:           cfg,
		apiClient:        apiClient,
		transportFactory: transportFactory,
		transport:        initialTransport,
		logger:           log,
		metrics:          telemetry.NewWorkerMetrics(),
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
		prefetchScheduler: prefetch.NewScheduler(prefetch.Config{WorkerID: cfg.WorkerID, MaxConcurrent: cfg.PrefetchMaxConcurrent, ByteBudget: cfg.PrefetchByteBudget, MaxBandwidthBytesPerSecond: cfg.PrefetchMaxBandwidthBytesPerSecond, DiskRestrictedPercent: cfg.PrefetchDiskRestrictedPercent, DiskCriticalPercent: cfg.PrefetchDiskCriticalPercent, DiskRecoveryPercent: cfg.PrefetchDiskRecoveryPercent, RAM: ramCache, RAMMinFutureRefs: cfg.PrefetchRAMMinFutureRefs, RAMMaxNextUseDistance: cfg.PrefetchRAMMaxNextUseDistance, OnState: func(state string, _ futureasset.Job, asset futureasset.AssetManifest, _ error) {
			metrics := telemetry.GetPrometheusMetrics()
			switch state {
			case "requested":
				metrics.RecordPrefetchRequested()
			case "downloaded":
				metrics.RecordPrefetchDownloaded(asset.SizeBytes)
			case "useful":
				metrics.RecordPrefetchUseful()
			case "wasted":
				metrics.RecordPrefetchWastedBytes(asset.SizeBytes)
			}
		}, OnPrepared: func(job prefetch.PreparedJob) {
			log.Info("[PREFETCH] state=PREPARED job=%s task=%s assets=%d prepared_at=%s", job.JobID, job.TaskID, len(job.Assets), job.PreparedAt.UTC().Format(time.RFC3339Nano))
		}, OnEvent: recordPrefetchEvent}),
		// PR-2: TaskOffer-accepted tasks awaiting TaskLeaseGranted before
		// executeTask dispatch. Keyed by task_id — one canonical entry per
		// outstanding offer per session.
		pendingTasks: make(map[string]*PendingTaskExecution),
		// PR-2 followup: per-task-native lease-state registry. Threaded
		// by MsgTaskLeaseGranted handler (separate PR) so leaseRenewLoop
		// can fire MsgTaskLeaseRenewal.
		activeTaskLeases:    make(map[string]*ActiveTaskLease),
		connState:           ConnDisconnected,
		concurrencyLimiter:  concurrency.NewConcurrencyLimiter(detectedConcurrency),
		publisherPool:       NewPublisherPool(4),
		stageExecutor:       stageExecutor,
		executorRegistry:    wo.registry,
		cache:               wo.cache,
		blobs:               wo.blobs,
		clipCache:           wo.clipCache,
		canonicalAssetCache: workercache.NewCanonicalAssetStore(wo.clipCache),
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

	// Compose the reporting subsystem behind its small interface. The
	// reporter owns the durable TaskResult outbox, the ACK waiter registry,
	// and the replay loop; the Worker only holds the seam and delegates.
	// transport is a per-session value (recreated on every reconnect), so the
	// reporter reads it through an accessor that takes transportMu — never a
	// captured snapshot.
	w.reporter = newTaskResultReporter(taskResultReporterConfig{
		spool: outputSpool,
		transport: func() controltransport.ControlTransport {
			w.transportMu.RLock()
			defer w.transportMu.RUnlock()
			return w.transport
		}, workerID: cfg.WorkerID,
		protocol:  cfg.ProtocolVersion,
		outputDir: cfg.OutputDir,
		// The reporter owns the committed-output cleanup; give it the
		// resolver so it can free tmpfs RAM reservations and delete under
		// the ARTIFACT_STAGING root.
		storageResolver: storageResolver,
		logger:          log,
		onTerminal:      w.signalTaskTerminal,
		logArtifact: func(event string, pte *PendingTaskExecution, startedAt time.Time, commitID, artifactID, uploadID string, fields map[string]interface{}) {
			w.logArtifactProtocol(event, pte, startedAt, commitID, artifactID, uploadID, fields)
		},
		wg:       &w.wg,
		stopChan: w.stopChan,
	})

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
