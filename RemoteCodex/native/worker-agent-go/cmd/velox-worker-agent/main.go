// Command velox-worker-agent is the main entry point for the Velox Worker Agent.
//
// AGENT 2 - Standardized Logging:
// - Uses structured events for all startup/register/heartbeat/shutdown operations
// - All log output follows [EVENT_CODE] format for automatic parsing
//
// Composition-root layout:
//   - internal/bootstrap owns the initial-configuration resolution
//     (ResolveConfig), the default-config generator (GenerateDefaultConfig),
//     the env helpers (EnvOr/EnvBool), and the RW-PROD-003 bootstrap-gate
//     dispatch (Dispatch).
//   - This file keeps the process-level wiring: flag parsing, executor
//     registry construction, cache/blob wiring, and worker start/shutdown.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	boot "velox-worker-agent/internal/bootstrap"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/taskrunner/executors"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/internal/worker"
	"velox-worker-agent/internal/workercache"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/blob"
	"velox-worker-agent/pkg/bootstrap"
	"velox-worker-agent/pkg/cache"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/doctor"
	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/video"
	"velox-worker-agent/pkg/video/pipeline"
)

// diskWatcherStarted is the process-global sync.Once that ensures the
// disk-watch goroutine is started at most once for the lifetime of a
// process. Repeatedly resetting global telemetry state on hot reload
// paths was a previous bug pattern; this guard makes the boot sequence
// single-flight.
var diskWatcherStarted bool

// startDiskWatcher (RW-PROD-004 §3 A4) launches a 15s-tier goroutine
// that polls the engine-output directory's free bytes and forwards the
// change to telemetry.SetDiskState. Readiness is held until the first
// sample lands (DiskFreeBytes=0 = unknown) so dashboards do not see a
// misleading `disk_free_bytes=0` reading on a fresh boot.
//
// Returns immediately if the watcher has already been started
// (sync.Once-style). Safe to call before cache/blob/registry wiring;
// the watcher operates on cfg.MinDiskFreeMB and /tmp/velox/scene-composite.
//
// Cancel via ctx. The goroutine exits cleanly on ctx.Done().
func startDiskWatcher(ctx context.Context, cfg *config.WorkerConfig, watchDir string, log *logger.Logger) {
	if diskWatcherStarted {
		return
	}
	diskWatcherStarted = true
	thresholdBytes := int64(cfg.MinDiskFreeMB) * 1024 * 1024
	go func() {
		// Initial sample on startup so /health/ready has a real
		// disk_free_bytes value before any traffic. Without this
		// first read, the first 15 seconds of a fresh boot would
		// have DiskFreeBytes=0 → potentially `disk.critical` if the
		// threshold is positive, masking actual readiness.
		if free, err := telemetry.DiskFreeAt(watchDir); err == nil {
			telemetry.SetDiskState(free, thresholdBytes)
		}
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				free, err := telemetry.DiskFreeAt(watchDir)
				if err != nil {
					log.Warn("[DISK_WATCH] statfs %s failed: %v", watchDir, err)
					continue
				}
				telemetry.SetDiskState(free, thresholdBytes)
				if free < thresholdBytes {
					log.Warn("[DISK_WATCH] free=%d threshold=%d (below floor)", free, thresholdBytes)
				}
			}
		}
	}()
}

// Version is set at build time via -ldflags.
var Version = "dev"

func main() {
	// Parse command-line flags
	configPath := flag.String("config", boot.DefaultConfigPath, "path to config file")
	workDir := flag.String("work-dir", "", "working directory (overrides config)")
	masterURL := flag.String("master", "", "master server URL (overrides config)")
	workerID := flag.String("worker-id", "", "worker ID (overrides config, auto-generated if empty)")
	logLevel := flag.String("log-level", "", "log level: debug, info, warn, error (overrides config)")
	showVersion := flag.Bool("version", false, "show version and exit")
	generateConfig := flag.Bool("generate-config", false, "generate a default config file and exit")
	validateConfig := flag.Bool("validate-config", false, "validate production-readiness and exit (RW-PROD-002 / pkg/doctor)")
	// RW-PROD-004 §3 A9: --ready-endpoint overrides the /health/ready
	// mount path. Default is /health/ready (canonical). Operators set
	// this on Kubernetes podspecs where /readyz is the network-policy-
	// friendly path (kept separate from /health legacy). The flag is
	// read AFTER cfg.Validate() so a typed WorkerConfig.ReadyzEndpoint
	// may also be sourced from worker_config.json or VELOX_READYZ_ENDPOINT.
	readyzEndpointFlag := flag.String("ready-endpoint", "", "override the /health/ready mount path (RW-PROD-004 A9; default: /health/ready)")
	// Phase 1 (cap. 2 of 100% Velox certification plan): the
	// --bootstrap-report flag makes main.go run bootstrap.Run(), dump
	// the [BOOTSTRAP_REPORT] JSON to stderr, and exit with the verdict
	// code. It does NOT register with the master or bind any ports
	// beyond the existing health/metrics servers (when configured).
	// The real-bootstrap certifier (scripts/cert/real-bootstrap.sh)
	// uses this flag to verify a freshly-built image under production
	// deps, without needing a live mock master.
	bootstrapReportFlag := flag.Bool("bootstrap-report", false, "run bootstrap.Run() once + dump [BOOTSTRAP_REPORT] JSON to stderr + exit with verdict (Phase 1 of 100% Velox certification plan; cap. 2)")
	flag.Parse()

	// Show version and exit
	if *showVersion {
		fmt.Printf("velox-worker-agent version %s\n", Version)
		os.Exit(0)
	}

	// Generate a default config and exit
	if *generateConfig {
		cfg, genErr := boot.GenerateDefaultConfig(*configPath, *workDir)
		if genErr != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to save config: %v\n", genErr)
			os.Exit(1)
		}
		fmt.Printf("Generated config file: %s\n", *configPath)
		fmt.Printf("Worker ID: %s\n", cfg.WorkerID)
		os.Exit(0)
	}

	// Resolve the initial configuration: load/create worker_config.json,
	// apply CLI + env overrides, validate production-readiness, and
	// resolve version/bundle identity fields. Failures are fail-closed.
	cfg, resolvedVersion, cfgErr := boot.ResolveConfig(boot.ConfigOptions{
		ConfigPath:     *configPath,
		WorkDir:        *workDir,
		MasterURL:      *masterURL,
		WorkerID:       *workerID,
		LogLevel:       *logLevel,
		ReadyzEndpoint: *readyzEndpointFlag,
		Version:        Version,
	})
	if cfgErr != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", cfgErr)
		os.Exit(1)
	}

	// --validate-config: run the doctor validators and exit.
	// Replaces the transport-only config.Validate() with the full
	// RW-PROD-002 validator suite (pkg/doctor).
	if *validateConfig {
		fmt.Printf("velox-worker-agent version %s\n", Version)
		// Run without the executor registry — the doctor runs before
		// pipeline wiring, so the registry isn't built yet.
		validators := doctor.DefaultValidatorsForProfile(cfg.WorkerProfile)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		docErr := doctor.Run(ctx, cfg, validators, os.Stdout)
		// Also print a human-readable summary to stderr.
		if docErr != nil {
			fmt.Fprintf(os.Stderr, "Validation: %v\n", docErr)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "Validation: READY\n")
		os.Exit(0)
	}

	// PR-3.5/3.9: build the executor registry at the composition root
	// (this file) — NOT inside pkg/worker — so the worker package
	// stays free of pkg/video/pipeline. The composition root owns the
	// "which executors does this binary advertise" decision; ops
	// dashboards read it via worker.buildHello.
	//
	// PR-3.9 wires scene.composite.v1 against the canonical
	// pipeline.Runner that powers the worker-agent whenever the C++
	// video engine is installed + reachable. The pipeline + native
	// render-client wiring lives in video.NewPipelineRunner.
	logger.Info("[BOOT] Building executor registry at composition root (cmd/velox-worker-agent)")
	registry := executor.NewRegistry()

	// PR-3.9 fix: NewPipelineRunner panics on a nil log — supply a real
	// boot-time logger so renderer errors travel to stderr at start-up.
	// The composition-root logger is the canonical logger that the
	// run-loop later merges into worker.New (worker.New constructs its
	// own *logger.Logger from cfg.LogLevel); before that point this
	// bootLog is the only consumer of pipeline / native-client messages.
	bootLog := logger.New(logger.InfoLevel, os.Stderr)
	bootLog.SetPrefix("[BOOT]")

	// Worker profile gate: the "creator" profile does not perform video
	// rendering, so we skip the C++ pipeline runner entirely. This keeps
	// creator-only deployments from failing on a missing video engine.
	isCreator := cfg.IsCreatorProfile()
	var pipelineRunner *pipeline.Runner
	if !isCreator {
		var pipeErr error
		pipelineRunner, pipeErr = video.NewPipelineRunner(bootLog)
		if pipeErr != nil {
			// Fail closed: a missing C++ engine is a deploy-time problem.
			// Silently downgrading to an empty registry re-introduces the
			// dead-letter class of bug — every scene.composite job would
			// route to ErrExecutorNotFound. Ops must install the C++
			// engine or set VELOX_VIDEO_ENGINE_CPP_BIN.
			fmt.Fprintf(os.Stderr, "Error: failed to construct pipeline.runner for scene.composite.v1: %v\n", pipeErr)
			os.Exit(1)
		}
		logger.Info("[BOOT] pipeline.runner constructed for scene.composite.v1")
	} else {
		logger.Info("[BOOT] Worker profile is 'creator'; skipping C++ pipeline runner")
	}
	// RW-PROD-003 §3 A5: synchronous bootstrap-OK gate between the C++
	// engine construction (above) and the executor wiring (below). The
	// gate proves:
	//   A8:  bundle hash on disk == cfg.BundleHash
	//   A3:  ffmpeg + ffprobe are present and libx264 is enumerable
	//   A4:  OutputDir is mkdir-able + write-able + removable
	//   A1+A2: engine self-render of a 1×1 black frame matches the SHA-256
	//         baseline committed at <WorkDir>/tests/fixtures/engine_selftest_baseline.sha256
	//         within a hard 5s budget
	// Failure of any step is fail-closed: bootstrap dumps a JSON report
	// on stderr (RW-PROD-003 §6) and the worker calls os.Exit(1) BEFORE
	// any registration attempt. Master side selector therefore never sees
	// `registered=true` for a malformed worker.
	bootCtx, bootCancel := context.WithTimeout(context.Background(), 10*time.Second)
	report, err := boot.Dispatch(bootCtx, cfg, pipelineRunner, bootLog)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: bootstrap gate failed (RW-PROD-003): %v\n", err)
		os.Exit(1)
	}
	bootCancel()

	// --bootstrap-report certifier hook (Phase 1 of 100% certification
	// plan; cap. 2): the real-bootstrap operator wrapper invokes
	// `velox-worker-agent --bootstrap-report` to verify a freshly-built
	// image under production deps WITHOUT registering with a master.
	// Dispatch has already written [BOOTSTRAP_REPORT] to stderr; the
	// certifier reads it + asserts verdict=OK + 4 step PASS. Here we map
	// the verdict to the exit code (0=OK, 1=FAIL) so the wrapper's
	// `docker run ... ; echo $?` round-trip works.
	if *bootstrapReportFlag {
		if report != nil && report.Verdict == "OK" {
			os.Exit(0)
		}
		os.Exit(1)
	}

	// Render outputs must use the worker's configured, writable output root.
	// A hardcoded /tmp path can belong to another service account on a
	// multi-user host and would make remote execution fail at finalization.
	if !isCreator {
		sceneComposite := executors.NewSceneComposite(pipelineRunner, cfg.OutputDir)
		registry.MustRegister(sceneComposite)
		logger.Info("[BOOT] Registered executor: %s@%d", sceneComposite.Descriptor().ID, sceneComposite.Descriptor().Version)
	} else {
		logger.Info("[BOOT] Worker profile is 'creator'; scene.composite.v1 disabled")
	}
	// RenderPlan v1 is the sole timeline source for the deterministic
	// subtitle/audio/video stages. Register these adapters in the same
	// capability registry used by dispatch and worker hello.
	if err := executors.RegisterRenderPlanExecutors(registry, cfg.OutputDir); err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to register RenderPlan executors: %v\n", err)
		os.Exit(1)
	}
	logger.Info("[BOOT] Registered RenderPlan executors: %s@1, %s@1, %s@1, %s@1", executors.SubtitleAlignID, executors.AudioMixID, executors.ComposeID, executors.EncodeID)
	// RW-PROD-004 §3 A4: surface the live executor count on the read
	// snapshot so /health/ready has a non-zero Executors reason.
	// SetExecutorsCount accepts the entire roster size rather than +
	// 1/−1 arithmetic; the composition root is the single source of
	// truth for "what is currently advertised to the master".
	telemetry.SetExecutorsCount(len(registry.Descriptors()))

	// PR-3.7: persistent local cache + content-addressed blob store.
	// Step 6/8 roots are operator-overridable via env vars; the
	// defaults reflect the new canonical layout (subdirs of
	// cfg.StateDir = /var/lib/velox/worker). Both invalidate the
	// noop defaults in taskrunner/context.go (no silent-fallback
	// policy).
	cacheDir := boot.EnvOr("VELOX_WORKER_CACHE_DIR", filepath.Join(cfg.StateDir, "cache"))
	blobDir := boot.EnvOr("VELOX_WORKER_BLOB_DIR", filepath.Join(cfg.StateDir, "blobs"))
	localCache, cacheErr := cache.NewPersistedLocalCache(cache.CacheOptions{Root: cacheDir})
	if cacheErr != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to construct local cache at %s: %v\n", cacheDir, cacheErr)
		os.Exit(1)
	}
	// RW-PROD-004 §3 A4: MarkCacheReady(true) clears
	// `cache.not_initialized` from /health/ready. The bootstrap gate
	// above already proved the engine is reachable; this is a separate
	// quality gate on the per-host cache wiring (a missing cache root
	// without this flip would surface as `not_ready` in dashboards).
	telemetry.MarkCacheReady(true)
	blobs, blobErr := blob.NewBlobArtifacts(blob.BlobOptions{Root: blobDir})
	if blobErr != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to construct blob store at %s: %v\n", blobDir, blobErr)
		os.Exit(1)
	}
	// RW-PROD-004 §3 A4: MarkBlobReady(true) clears
	// `blob.not_initialized` from /health/ready. Symmetric to
	// MarkCacheReady just above.
	telemetry.MarkBlobReady(true)
	defer func() { _ = blobs.Close() }()
	logger.Info("[CACHE] PersistedLocalCache at %s (256 MiB default budget)", cacheDir)
	logger.Info("[BLOB] BlobArtifacts at %s (upload queue size 1024)", blobDir)

	w, workerErr := worker.New(cfg, resolvedVersion,
		worker.WithRegistry(registry),
		worker.WithCache(localCache),
		worker.WithBlobs(blobs),
		// Anti-collision observer (RW-PROD-005 §3): on master reject with
		// codes.AlreadyExists (another machine is already registered with
		// this worker_id on a different credential), exit 17 (anti-
		// collision sentinel). Gated by VELOX_ALLOW_MULTI_HOST_WORKER_IDS
		// default=false; legacy blue/green operators can opt out.
		worker.WithCollisionObserver(func(collisionErr error) {
			if boot.EnvBool("VELOX_ALLOW_MULTI_HOST_WORKER_IDS", false) {
				fmt.Fprintf(os.Stderr, "[WARN] VELOX_ALLOW_MULTI_HOST_WORKER_IDS=true \u2014 collision accepted, continuing with backoff. worker_id=%q master_url=%s err=%v\n", cfg.WorkerID, cfg.MasterURL, collisionErr)
				return
			}
			fmt.Fprintf(os.Stderr, "[ERROR] worker_id COLLISION detected \u2014 exit(17). another machine already registered with worker_id=%q on a different credential. master_url=%s err=%v\n"+
				"Hint: two physical machines cannot share a worker_id. Check VELOX_WORKER_ID on both hosts. "+
				"To force-accept collisions (NOT recommended), set VELOX_ALLOW_MULTI_HOST_WORKER_IDS=1.\n",
				cfg.WorkerID, cfg.MasterURL, collisionErr)
			fmt.Fprintf(os.Stdout, "[COLLISION] worker_id=%q master_url=%s err=%v\n", cfg.WorkerID, cfg.MasterURL, collisionErr)
			os.Exit(17)
		}),
	)
	if workerErr != nil {
		logger.LogRegisterFailed("(initial)", cfg.MasterURL, workerErr)
		os.Exit(1)
	}

	// Remote shared-asset cache: this SQLite index and its files live under
	// StateDir, never under a per-job workspace. The worker's existing
	// integrity-aware resolver owns byte downloads; this index owns durable
	// leases and eviction safety across jobs and process restarts.
	clipCacheDB := boot.EnvOr("VELOX_WORKER_CLIP_CACHE_DB", filepath.Join(cfg.StateDir, "clip-cache", "cache.db"))
	if err := os.MkdirAll(filepath.Dir(clipCacheDB), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create remote clip-cache directory: %v\n", err)
		os.Exit(1)
	}
	clipCache, clipCacheErr := workercache.Open(clipCacheDB)
	if clipCacheErr != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to open remote clip-cache index at %s: %v\n", clipCacheDB, clipCacheErr)
		os.Exit(1)
	}
	defer clipCache.Close()
	w.AttachClipCache(clipCache)
	// w was constructed before this index so its authenticated API client is
	// the one populated by the control-plane registration handshake.
	if w.APIClient() == nil {
		fmt.Fprintln(os.Stderr, "Error: worker API client unavailable for protected-assets polling")
		os.Exit(1)
	}
	pollInterval := 30 * time.Second
	if raw := boot.EnvOr("VELOX_CACHE_SNAPSHOT_INTERVAL", ""); raw != "" {
		if parsed, parseErr := time.ParseDuration(raw); parseErr == nil && parsed > 0 {
			pollInterval = parsed
		}
	}
	protectedPoller := worker.NewProtectedAssetsPoller(w.APIClient(), pollInterval)
	cleanupPolicy := workercache.LoadCleanupPolicy()
	protectedPoller.SnapshotMaxAge = cleanupPolicy.SnapshotMaxAge
	telemetry.MarkCacheProtectionReady(false)
	protectedPoller.OnSuccess = func(snap *api.ProtectedAssetSnapshot) {
		telemetry.MarkCacheProtectionReady(protectedPoller.IsReady())
		if snap != nil {
			if generated, err := time.Parse(time.RFC3339Nano, snap.GeneratedAt); err == nil {
				telemetry.SetProtectedSnapshotGeneratedAt(generated)
			}
		}
	}
	cleanupLoop := &workercache.CleanupLoop{
		Cache: clipCache, Policy: cleanupPolicy, Snapshot: protectedPoller,
		Barrier:  protectedPoller,
		Interval: cleanupPolicy.CleanupInterval, JobDone: w.JobDone(),
		OnTick: func(stats workercache.CleanupStats, err error) {
			metrics := telemetry.GetPrometheusMetrics()
			metrics.RecordCacheEvictions("ttl", stats.Removed)
			metrics.RecordCacheCleanup(time.Duration(stats.DurationMS) * time.Millisecond)
			metrics.RecordCacheCleanupSkips("protected", stats.SkippedProtected)
			metrics.RecordCacheCleanupSkips("leased", stats.SkippedLeased)
			metrics.RecordCacheCleanupSkips("grace", stats.SkippedGrace)
			metrics.RecordCacheCleanupSkips("in_flight", stats.SkippedInFlight)
			metrics.RecordCacheCleanupSkips("stale_snapshot", stats.SkippedSnapshotStale)
			metrics.RecordCacheCleanupSkips("no_snapshot", stats.SkippedSnapshotUnavailable)
			if entries, bytes, sizeErr := clipCache.Size(context.Background()); sizeErr == nil {
				metrics.SetCacheSize(entries, bytes)
			}
			if err != nil {
				reason := "error"
				if errors.Is(err, workercache.ErrSnapshotUnavailable) {
					reason = "no_snapshot"
				}
				if errors.Is(err, workercache.ErrSnapshotStale) {
					reason = "stale_snapshot"
				}
				if stats.SkippedSnapshotStale == 0 && stats.SkippedSnapshotUnavailable == 0 {
					metrics.RecordCacheCleanupSkip(reason)
				}
				logger.Warn("[CACHE_CLEANUP] inspected=%d removed=%d skipped_protected=%d skipped_leased=%d err=%v", stats.Inspected, stats.Removed, stats.SkippedProtected, stats.SkippedLeased, err)
				return
			}
			logger.Info("[CACHE_CLEANUP] inspected=%d removed=%d skipped_protected=%d skipped_leased=%d", stats.Inspected, stats.Removed, stats.SkippedProtected, stats.SkippedLeased)
		},
	}
	// RW-PROD-004 §3 A4: MarkBootstrapped(true) is set here because
	// Dispatch has already returned (with err==nil) and the
	// package-level bootstrap.Ok() gate is therefore true. We do NOT
	// force a hard-fail if bootstrap.Ok() returns false here — the
	// composition root already blocks Start() via bootstrap.HardGate
	// (worker.go), so a return path that produced a Worker but failed
	// bootstrap is unreachable in the current control flow. The
	// explicit flip is here for the explicit ordering invariant that
	// /health/ready reports Bootstrapped=true iff bootstrap.Ok()==true.
	if bootstrap.Ok() {
		telemetry.MarkBootstrapped(true)
	}

	// Set up context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := protectedPoller.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Warn("[CACHE_SNAPSHOT] poller stopped: %v", err)
		}
	}()
	go func() {
		if err := cleanupLoop.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Warn("[CACHE_CLEANUP] loop stopped: %v", err)
		}
	}()

	// Step 6/8: fail-fast self-check on cfg.StateDir writability.
	// Runs BEFORE any cache/blob wiring or disk-watcher startup so a
	// host where the canonical root is unwritable exits with a
	// precise diagnostic (UID + chown recipe) instead of mid-task
	// EACCES failures. The validator also emits a one-shot
	// DEPRECATION warning when the legacy assets_cache bind-mount
	// holds stranded data — informational, not blocking.
	stateCtx, stateCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := doctor.Run(stateCtx, cfg, []doctor.Validator{&doctor.StateDirValidator{}}, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "Error: state dir not writable (Step 6/8 fail-fast): %v\n", err)
		os.Exit(1)
	}
	stateCancel()

	// RW-PROD-004 §3 A4: start the disk watcher now that ctx is in scope.
	// Uses the worker's shutdown context so SIGTERM/SIGINT also exits the
	// watcher goroutine. A dedicated logger instance with [DISK_WATCH]
	// prefix keeps its output separable from main's composition-root log.
	dwatchLog := logger.New(logger.InfoLevel, os.Stderr)
	dwatchLog.SetPrefix("[DISK_WATCH]")
	startDiskWatcher(ctx, cfg, "/tmp/velox/scene-composite", dwatchLog)

	// Handle shutdown signals with structured logging
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		signalName := "UNKNOWN"
		switch sig {
		case syscall.SIGINT:
			signalName = "SIGINT"
		case syscall.SIGTERM:
			signalName = "SIGTERM"
		}
		logger.LogSignalReceived(cfg.WorkerID, signalName)
		w.Stop()
		cancel()
	}()

	// Log startup with structured event
	logger.LogStartup(cfg.WorkerID, Version, cfg.MasterURL)

	// Phase 1a: Start health server (for Docker HEALTHCHECK)
	healthPort := cfg.HealthPort
	if healthPort == 0 {
		logger.Info("[HEALTH] Health server disabled (health_port=0)")
	} else {
		telemetry.SetHealthWorkerID(cfg.WorkerID)
		// RW-PROD-004 §3 A9: the --ready-endpoint override is
		// funneled through StartHealthServerWithMux so a Kubernetes
		// podspec pointing at /readyz works without changing the
		// canonical /health/ready, /health/live, /health mount points.
		if err := telemetry.StartHealthServerWithMux(healthPort, cfg.ReadyzEndpoint); err != nil {
			logger.Warn("[HEALTH] Health server failed: %v", err)
		} else {
			logger.Info("[HEALTH] Health server starting on :%d (ready=%s)", healthPort, cfg.ReadyzEndpoint)
		}
	}

	// Phase 1b: Start Prometheus metrics server (only if port > 0)
	prometheusPort := cfg.PrometheusPort
	if prometheusPort == 0 {
		logger.Info("[TELEMETRY] Prometheus metrics disabled (port=0)")
	} else {
		// StartPrometheusServer binds synchronously so a port collision is
		// reported before the worker announces startup. The serving loop is
		// still asynchronous after the listener is acquired.
		if err := telemetry.StartPrometheusServer(prometheusPort); err != nil {
			logger.Error("[TELEMETRY] Prometheus metrics server failed on :%d: %v", prometheusPort, err)
			// Production workers must not claim readiness while the
			// configured observability endpoint is unavailable.
			os.Exit(1)
		}
		logger.Info("[TELEMETRY] Prometheus metrics server starting on :%d", prometheusPort)
	}

	// Start worker
	if err := w.Start(ctx); err != nil {
		logger.LogRegisterFailed(cfg.WorkerID, cfg.MasterURL, err)
		os.Exit(1)
	}
}
