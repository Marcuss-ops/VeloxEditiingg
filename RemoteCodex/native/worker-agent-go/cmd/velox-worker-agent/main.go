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

	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/taskrunner/executors"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/internal/worker"
	"velox-worker-agent/pkg/blob"
	"velox-worker-agent/pkg/cache"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
	"velox-worker-agent/pkg/video"
)

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
	validateConfig := flag.Bool("validate-config", false, "validate config JSON (transport check) and exit")
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

	// Create worker
	// Option A (2026-06 fix): New() returns (*Worker, error) — a bad TLS or
	// insecure-flag misconfiguration is surfaced here instead of panicking
	// during Start().

	// --validate-config: validate and exit before starting the worker loop.
	if *validateConfig {
		// Branding line for human operators running docker run --rm ... --validate-config
		fmt.Printf("velox-worker-agent version %s\n", Version)
		w, vErr := worker.New(cfg, resolvedVersion, worker.WithRegistry(executor.NewRegistry()))
		if vErr != nil {
			fmt.Fprintf(os.Stderr, "Validation FAILED: %v\n", vErr)
			os.Exit(1)
		}
		_ = w
		fmt.Printf("Configuration valid for worker %s\n", cfg.WorkerID)
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
	// render-client wiring lives in video.NewPipelineRunner, shared
	// with the legacy VideoGenerationWorkflow.
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
		// route to ErrExecutorNotFound. This is a deliberate PR-3.9
		// tightening vs the legacy "NewVideoGenerationWorkflow
		// warns-and-continue" behaviour; ops must install the C++
		// engine or set VELOX_VIDEO_ENGINE_CPP_BIN.
		fmt.Fprintf(os.Stderr, "Error: failed to construct pipeline.runner for scene.composite.v1: %v\n", pipeErr)
		os.Exit(1)
	}
	sceneComposite := executors.NewSceneComposite(pipelineRunner, "/tmp/velox/scene-composite")
	registry.MustRegister(sceneComposite)
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
	blobs, blobErr := blob.NewBlobArtifacts(blob.BlobOptions{Root: blobDir})
	if blobErr != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to construct blob store at %s: %v\n", blobDir, blobErr)
		os.Exit(1)
	}
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

	// Set up context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
		if err := telemetry.StartHealthServer(healthPort); err != nil {
			logger.Warn("[HEALTH] Health server failed: %v", err)
		} else {
			logger.Info("[HEALTH] Health server starting on :%d", healthPort)
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
