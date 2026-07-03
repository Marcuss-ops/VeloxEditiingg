// Command velox-worker-agent is the main entry point for the Velox Worker Agent.
//
// AGENT 2 - Standardized Logging:
// - Uses structured events for all startup/register/heartbeat/shutdown operations
// - All log output follows [EVENT_CODE] format for automatic parsing
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/taskrunner/executors"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/internal/worker"
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

// Default paths
const (
	defaultConfigPath = "/opt/velox/worker_config.json"
	defaultWorkDir    = "/opt/velox"
)

// readVersionFile attempts to read version from VERSION.txt in the work directory
// as a fallback when the ldflags version is "dev".
func readVersionFile(workDir string) string {
	// Try several known locations for VERSION.txt
	candidates := []string{
		filepath.Join(workDir, "VERSION.txt"),
		filepath.Join(workDir, "..", "VERSION.txt"),
		filepath.Join(workDir, "..", "..", "VERSION.txt"),
		"/opt/velox/VERSION.txt",
		filepath.Join(workDir, "versions", "current", "VERSION.txt"),
	}
	seen := make(map[string]bool)
	for _, path := range candidates {
		abs, err := filepath.Abs(path)
		if err != nil {
			continue
		}
		if seen[abs] {
			continue
		}
		seen[abs] = true
		data, err := os.ReadFile(abs)
		if err == nil {
			v := strings.TrimSpace(string(data))
			if v != "" {
				return v
			}
		}
	}
	return ""
}

func readTextFileFirst(workDir, filename string) string {
	candidates := []string{
		filepath.Join(workDir, filename),
		filepath.Join(workDir, "versions", "current", filename),
		"/opt/velox/" + filename,
	}
	seen := make(map[string]bool)
	for _, path := range candidates {
		abs, err := filepath.Abs(path)
		if err != nil {
			continue
		}
		if seen[abs] {
			continue
		}
		seen[abs] = true
		data, err := os.ReadFile(abs)
		if err == nil {
			v := strings.TrimSpace(string(data))
			if v != "" {
				return v
			}
		}
	}
	return ""
}

// envOr returns the value of the named environment variable, trimmed
// of surrounding whitespace; if unset or empty, it returns fallback.
// Centralised so main.go does not sprinkle os.Getenv calls across the
// composition root.
func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func main() {
	// Parse command-line flags
	configPath := flag.String("config", defaultConfigPath, "path to config file")
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

	// Load or create config
	var cfg *config.WorkerConfig
	var err error

	if *generateConfig {
		// Generate default config
		dir := *workDir
		if dir == "" {
			dir = defaultWorkDir
		}
		cfg = config.DefaultConfig(dir)
		if err := config.SaveConfig(*configPath, cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to save config: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Generated config file: %s\n", *configPath)
		fmt.Printf("Worker ID: %s\n", cfg.WorkerID)
		os.Exit(0)
	}

	// Try to load existing config
	cfg, err = config.LoadConfig(*configPath)
	if err != nil {
		// Config file doesn't exist, create default
		if os.IsNotExist(err) {
			// Use structured event for config creation
			dir := *workDir
			if dir == "" {
				dir = defaultWorkDir
			}
			cfg = config.DefaultConfig(dir)
			logger.LogConfigCreated(*configPath, cfg.WorkerID)
		} else {
			logger.LogConfigError(err)
			os.Exit(1)
		}
	} else {
		// Log config loaded
		logger.LogConfigLoaded(*configPath, cfg.WorkerID)
	}

	// Apply command-line overrides
	if *workDir != "" {
		cfg.WorkDir = *workDir
	}
	if *masterURL != "" {
		cfg.MasterURL = *masterURL
	}
	if *workerID != "" {
		cfg.WorkerID = *workerID
	}
	if *logLevel != "" {
		cfg.LogLevel = *logLevel
	}
	if envWorkerID := os.Getenv("VELOX_WORKER_ID"); envWorkerID != "" {
		cfg.WorkerID = envWorkerID
	}
	if bundleVersion := os.Getenv("VELOX_BUNDLE_VERSION"); bundleVersion != "" {
		cfg.BundleVersion = bundleVersion
	}
	if cfg.BundleVersion == "" {
		cfg.BundleVersion = Version
	}

	// Validate config
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Normalize worker_id to prevent double host_ prefixes and dot-format IDs
	cfg.WorkerID = config.NormalizeWorkerID(cfg.WorkerID)
	logger.Info("[WORKER_ID] Normalized worker ID: %s", cfg.WorkerID)

	// Save config if it's new (ensures worker_id is persisted)
	if _, err := os.Stat(*configPath); os.IsNotExist(err) {
		if err := config.SaveConfig(*configPath, cfg); err != nil {
			logger.Warn("Failed to save config: %v", err)
		}
	}

	// Try to read VERSION.txt as a fallback for version reporting
	// The Version from ldflags takes precedence, but if it's "dev" we try VERSION.txt
	resolvedVersion := Version
	if resolvedVersion == "dev" {
		if v := readVersionFile(cfg.WorkDir); v != "" {
			resolvedVersion = v
			logger.Info("[VERSION] Loaded version from VERSION.txt: %s", resolvedVersion)
		}
	}
	// Also ensure BundleVersion is set from resolved version if not already set
	if cfg.BundleVersion == "" || cfg.BundleVersion == "dev" {
		cfg.BundleVersion = resolvedVersion
	}
	if bundleHash := os.Getenv("VELOX_BUNDLE_HASH"); bundleHash != "" {
		cfg.BundleHash = bundleHash
	}
	if assetCacheDir := os.Getenv("VELOX_ASSET_CACHE_DIR"); strings.TrimSpace(assetCacheDir) != "" {
		cfg.AssetCacheDir = strings.TrimSpace(assetCacheDir)
	}
	// RW-PROD-004 §3 A9: --ready-endpoint CLI flag beats the env var
	// and the JSON config (per the standard precedence: CLI > env > JSON).
	// Empty string from the flag is a no-op so the canonical
	// /health/ready stays in force when the operator does not opt in.
	if *readyzEndpointFlag != "" {
		cfg.ReadyzEndpoint = *readyzEndpointFlag
	}
	if cfg.ReadyzEndpoint == "" {
		cfg.ReadyzEndpoint = "/health/ready"
	}
	if cfg.BundleHash == "" {
		cfg.BundleHash = readTextFileFirst(cfg.WorkDir, "BUNDLE_HASH.txt")
	}
	if protocolVersion := os.Getenv("VELOX_WORKER_PROTOCOL_VERSION"); protocolVersion != "" {
		cfg.ProtocolVersion = protocolVersion
	}
	if cfg.ProtocolVersion == "" {
		cfg.ProtocolVersion = "v3"
	}
	if workerSecret := os.Getenv("VELOX_WORKER_SECRET"); workerSecret != "" {
		cfg.WorkerSecret = workerSecret
	}
	if engineVersion := os.Getenv("VELOX_ENGINE_VERSION"); engineVersion != "" {
		cfg.EngineVersion = engineVersion
	}
	if cfg.EngineVersion == "" {
		cfg.EngineVersion = resolvedVersion
	}
	if strings.TrimSpace(cfg.VideoEngineCppBin) != "" && strings.TrimSpace(os.Getenv("VELOX_VIDEO_ENGINE_CPP_BIN")) == "" {
		// Make the composition-root config authoritative for the native renderer.
		// The render client resolves the engine path from VELOX_VIDEO_ENGINE_CPP_BIN,
		// so mirror the validated config into the environment before pipeline wiring.
		if err := os.Setenv("VELOX_VIDEO_ENGINE_CPP_BIN", strings.TrimSpace(cfg.VideoEngineCppBin)); err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to export VELOX_VIDEO_ENGINE_CPP_BIN from config: %v\n", err)
			os.Exit(1)
		}
	}

	// Create worker
	// Option A (2026-06 fix): New() returns (*Worker, error) — a bad TLS or
	// insecure-flag misconfiguration is surfaced here instead of panicking
	// during Start().

	// --validate-config: run the doctor validators and exit.
	// Replaces the transport-only config.Validate() with the full
	// RW-PROD-002 validator suite (pkg/doctor).
	if *validateConfig {
		fmt.Printf("velox-worker-agent version %s\n", Version)
		// Run without the executor registry — the doctor runs before
		// pipeline wiring, so the registry isn't built yet.
		validators := doctor.DefaultValidators()
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
	pipelineRunner, pipeErr := video.NewPipelineRunner(bootLog)
	if pipeErr != nil {
		// Fail closed: a missing C++ engine is a deploy-time problem.
		// Silently downgrading to an empty registry re-introduces the
		// dead-letter class of bug — every scene.composite job would
		// route to ErrExecutorNotFound. Ops must install the C++
		// engine or set VELOX_VIDEO_ENGINE_CPP_BIN.
		fmt.Fprintf(os.Stderr, "Error: failed to construct pipeline.runner for scene.composite.v1: %v\n", pipeErr)
		os.Exit(1)
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
	report, err := dispatchBootstrap(bootCtx, cfg, pipelineRunner, bootLog)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: bootstrap gate failed (RW-PROD-003): %v\n", err)
		os.Exit(1)
	}
	bootCancel()

	// --bootstrap-report certifier hook (Phase 1 of 100% certification
	// plan; cap. 2): the real-bootstrap operator wrapper invokes
	// `velox-worker-agent --bootstrap-report` to verify a freshly-built
	// image under production deps WITHOUT registering with a master.
	// dispatchBootstrap has already written [BOOTSTRAP_REPORT] to
	// stderr; the certifier reads it + asserts verdict=OK + 4 step
	// PASS. Here we map the verdict to the exit code (0=OK, 1=FAIL)
	// so the wrapper's `docker run ... ; echo $?` round-trip works.
	if *bootstrapReportFlag {
		if report != nil && report.Verdict == "OK" {
			os.Exit(0)
		}
		os.Exit(1)
	}

	sceneComposite := executors.NewSceneComposite(pipelineRunner, "/tmp/velox/scene-composite")
	registry.MustRegister(sceneComposite)
	// RW-PROD-004 §3 A4: surface the live executor count on the read
	// snapshot so /health/ready has a non-zero Executors reason.
	// SetExecutorsCount accepts the entire roster size rather than +
	// 1/−1 arithmetic; the composition root is the single source of
	// truth for "what is currently advertised to the master".
	telemetry.SetExecutorsCount(len(registry.Descriptors()))
	logger.Info("[BOOT] Registered executor: %s@%d", sceneComposite.Descriptor().ID, sceneComposite.Descriptor().Version)

	// PR-3.7: persistent local cache + content-addressed blob store.
	// Roots are operator-overridable via env vars; the defaults reflect
	// the production /opt/velox layout. Both invalidate the noop
	// defaults in taskrunner/context.go (no silent-fallback policy).
	cacheDir := envOr("VELOX_WORKER_CACHE_DIR", "/opt/velox/cache")
	blobDir := envOr("VELOX_WORKER_BLOB_DIR", "/opt/velox/blobs")
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
	)
	if workerErr != nil {
		logger.LogRegisterFailed("(initial)", cfg.MasterURL, workerErr)
		os.Exit(1)
	}
	// RW-PROD-004 §3 A4: MarkBootstrapped(true) is set here because
	// dispatchBootstrap has already returned (with err==nil) and the
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
		go func() {
			if err := telemetry.StartPrometheusServer(prometheusPort); err != nil {
				logger.Warn("Prometheus server failed: %v", err)
			}
		}()
		logger.Info("[TELEMETRY] Prometheus metrics server starting on :%d", prometheusPort)
	}

	// Start worker
	if err := w.Start(ctx); err != nil {
		logger.LogRegisterFailed(cfg.WorkerID, cfg.MasterURL, err)
		os.Exit(1)
	}
}

// dispatchBootstrap (RW-PROD-003 A5) bridges the canonical
// *pipeline.Runner (built by video.NewPipelineRunner above) into the
// narrow bootstrap.RunnerView interface used by pkg/bootstrap. We use a
// tiny adapter because pkg/bootstrap keeps pkg/video at arm's length
// to keep its test surface free of CGO coupling.
//
// On success and failure alike we ALWAYS dump the JSON boot report to
// stderr so ops triage stays in one place — the per-step record travels
// with the short-form error caught by main(). The --bootstrap-report
// certifier reads the same [BOOTSTRAP_REPORT] block off stderr to
// assert verdict+steps without re-instrumenting the worker.
//
// Returns the *Report so --bootstrap-report can exit with the verdict
// code (0=OK, !0=FAIL) without re-deriving verdict from message text.
func dispatchBootstrap(
	ctx context.Context,
	cfg *config.WorkerConfig,
	runner *pipeline.Runner,
	log *logger.Logger,
) (*bootstrap.Report, error) {
	adapter := &pipelineRunnerAdapter{runner: runner}
	report, err := bootstrap.Run(ctx, cfg, adapter, bootstrap.Options{
		Logger: log,
	})
	if report != nil {
		_ = bootstrap.DumpReport(report)
	}
	return report, err
}

// pipelineRunnerAdapter is the one-method shim required by
// bootstrap.RunnerView. We do NOT export it (lowercase type) because
// the only caller is dispatchBootstrap in this file.
//
// Crucially: bootstrap.RenderClientIface and pipeline.RenderClient
// have IDENTICAL signatures (Render(ctx, *plan.RenderPlan) error) so the
// runner's render client can flow through to bootstrap without any
// struct wrapping — the adapter merely exposes the runner's accessor
// through a different interface name.
type pipelineRunnerAdapter struct {
	runner *pipeline.Runner
}

func (a *pipelineRunnerAdapter) RenderClient() bootstrap.RenderClientIface {
	if a == nil || a.runner == nil {
		return nil
	}
	return a.runner.RenderClient()
}
