package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"velox-server/internal/config"
	"velox-server/internal/logging"
	"velox-server/internal/telemetry"
	"velox-shared/compatibility"
)

// Build-time metadata, injected via `go build -ldflags "-X main.Version=...
// -X main.BuildTime=..."`. Declared as `var` (not `const`) because
// ldflags -X only mutates writable symbols. Defaults are safe values
// for local dev; release pipelines (DataServer/Dockerfile and
// .github/workflows/master-image.yml) override them at build time.
var (
	Version   = "dev"
	BuildTime = "unknown"
)

const usageText = `Usage: velox-server [serve]

When invoked without arguments the binary behaves identically to 'velox-server serve'.

Subcommands:
  serve  Run the HTTP/gRPC master (default).

Examples:
  velox-server serve
`

func main() {
	// Emit the build identity early so operators see it in
	// `docker logs` / `journalctl` regardless of where the binary
	// actually listens. Cheap; no side effects.
	logServerf(context.Background(), logging.LevelInfo, logging.CodeServerLifecycle, "velox-server %s (built %s)", Version, BuildTime)

	// Capture the optional .env file and process environment exactly once.
	// The raw snapshot carries source provenance into the typed Config, while
	// shell-exported values retain precedence over file values.
	var raw config.RawConfig
	if envPath := config.EnvFilePath(); envPath != "" {
		var rawErr error
		raw, rawErr = config.RawConfigFromEnvFile(envPath)
		if rawErr != nil {
			logServerf(context.Background(), logging.LevelWarn, logging.CodeServerBootstrapWarn, "[BOOTSTRAP] WARNING: failed to load env file %s: %v", envPath, rawErr)
			raw = config.RawConfigFromEnv()
		}
	} else {
		raw = config.RawConfigFromEnv()
	}

	cfg, err := config.LoadFromRaw(raw)
	if err != nil {
		log.Fatalf("config load failed: %v", err)
	}
	if cfg.Compatibility.Mode == "strict" {
		compatibility.SetMode(compatibility.ModeStrict)
	} else {
		compatibility.SetMode(compatibility.ModeCompat)
	}
	logging.Configure(cfg.Runtime.Logging.Quiet, cfg.Runtime.Logging.JSONOutput, cfg.Runtime.Logging.Debug)
	if err := telemetry.Configure(cfg.Runtime.Telemetry); err != nil {
		log.Fatalf("telemetry configuration failed: %v", err)
	}
	if snapshot, snapshotErr := cfg.SnapshotJSON(); snapshotErr != nil {
		logServerf(context.Background(), logging.LevelWarn, logging.CodeServerBootstrapWarn, "[BOOTSTRAP] WARNING: config snapshot unavailable: %v", snapshotErr)
	} else {
		logServerf(context.Background(), logging.LevelInfo, logging.CodeServerBootstrap, "[BOOTSTRAP] config snapshot: %s", snapshot)
	}
	args := os.Args[1:]

	switch {
	case len(args) == 0:
		// Default: serve.
		if err := runServer(cfg); err != nil {
			log.Fatalf("server exit: %v", err)
		}
	case args[0] == "serve":
		if err := runServer(cfg); err != nil {
			log.Fatalf("server exit: %v", err)
		}
	case args[0] == "--help" || args[0] == "-h" || args[0] == "help":
		fmt.Print(usageText)
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n%s", args[0], usageText)
		os.Exit(2)
	}
}
