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
	"syscall"

	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/internal/worker"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

// Version is set at build time via -ldflags.
var Version = "dev"

// Default paths
const (
	defaultConfigPath = "/opt/velox/worker_config.json"
	defaultWorkDir    = "/opt/velox"
)

func main() {
	// Parse command-line flags
	configPath := flag.String("config", defaultConfigPath, "path to config file")
	workDir := flag.String("work-dir", "", "working directory (overrides config)")
	masterURL := flag.String("master", "", "master server URL (overrides config)")
	workerID := flag.String("worker-id", "", "worker ID (overrides config, auto-generated if empty)")
	logLevel := flag.String("log-level", "", "log level: debug, info, warn, error (overrides config)")
	showVersion := flag.Bool("version", false, "show version and exit")
	generateConfig := flag.Bool("generate-config", false, "generate a default config file and exit")
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

	// Validate config
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Save config if it's new (ensures worker_id is persisted)
	if _, err := os.Stat(*configPath); os.IsNotExist(err) {
		if err := config.SaveConfig(*configPath, cfg); err != nil {
			logger.Warn("Failed to save config: %v", err)
		}
	}

	// Create worker
	w := worker.New(cfg, Version)

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

	// Phase 1: Start Prometheus metrics server
	prometheusPort := 9090 // Default Prometheus port
	if cfg.PrometheusPort > 0 {
		prometheusPort = cfg.PrometheusPort
	}
	go func() {
		if err := telemetry.StartPrometheusServer(prometheusPort); err != nil {
			logger.Warn("Prometheus server failed: %v", err)
		}
	}()
	logger.Info("[TELEMETRY] Prometheus metrics server starting on :%d", prometheusPort)

	// Start worker
	if err := w.Start(ctx); err != nil {
		logger.LogRegisterFailed(cfg.WorkerID, cfg.MasterURL, err)
		os.Exit(1)
	}
}
