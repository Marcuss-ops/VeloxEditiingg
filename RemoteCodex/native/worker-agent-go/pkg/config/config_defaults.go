// Package config / config_defaults.go — safe-by-default value population.
//
// DefaultConfig builds a fresh WorkerConfig with sensible defaults;
// applyDefaults back-fills fields missing from older JSON configs. Both
// paths converge on the same defaulting code so there is a single source
// of truth for fallback values.
package config

import (
	"os"

	"velox-shared/identity"
)

// DefaultTmpfsThresholdBytes is the default size gate for StorageResolver
// tmpfs placement (Fase E1): ATTEMPT_TEMP files strictly smaller than this
// may land on TmpfsDir; at/above it they always land on TempDir (NVMe).
// 64 MiB is a benchmarked starting point for small manifests, concat lists,
// metadata, small audio fragments and control files — never large
// intermediates or the final artifact.
const DefaultTmpfsThresholdBytes int64 = 64 * 1024 * 1024

// GenerateWorkerID creates a unique worker ID in the format "worker-{8-char-hex}".
//
// Implementation lives in shared/identity so that the Velox master server and
// the worker agent share the exact same entropy source and format. This
// keeps ID-generation stable across the ecosystem and avoids drift.
func GenerateWorkerID() string {
	return identity.GenerateWorkerID()
}

// DefaultConfig creates a WorkerConfig with sensible default values.
// If workDir is empty, it defaults to "/opt/velox".
//
// DefaultConfig intentionally does NOT set `Environment` — that decision
// is owned by applyDefaults() so the JSON path (applyDefaults → applyEnvOverrides)
// and the DefaultConfig path (applyDefaults → applyEnvOverrides) converge on
// the same code. Setting it here would create a second source of truth.
func DefaultConfig(workDir string) *WorkerConfig {
	if workDir == "" {
		workDir = "/opt/velox"
	}

	return &WorkerConfig{
		MasterURL:       "http://localhost:8000",
		WorkerID:        GenerateWorkerID(),
		WorkerName:      "velox-worker",
		WorkDir:         workDir,
		LogLevel:        "info",
		BundleVersion:   "",
		ProtocolVersion: "v3",
		MaxActiveJobs:   1,    // 1 main job per VPS
		HealthPort:      8081, // Health HTTP endpoint for Docker HEALTHCHECK
		PrometheusPort:  9090, // Prometheus metrics endpoint
		WorkerSecret:    "",   // Set via EnvWorkerSecret or credential-file fallback
	}
}

// applyDefaults fills in backward-compatible defaults for fields that may be
// missing from older config files.
func (c *WorkerConfig) applyDefaults() {
	if c == nil {
		return
	}
	if c.HealthPort == 0 {
		c.HealthPort = 8081
	}
	if c.PrometheusPort == 0 {
		c.PrometheusPort = 9090
	}
	// RW-PROD-004: default disk-free floor in MiB. The disk watcher
	// (composition root) downsamples to bytes for ReadyState.SetDiskState.
	// 256 MiB matches the bootstrap output smoke-test envelope.
	if c.MinDiskFreeMB <= 0 {
		c.MinDiskFreeMB = 256
	}
	// Environment safe-by-default. The actual env-var overlay
	// (`VELOX_ENV`) runs after this in `applyEnvOverrides`; both layers
	// land on "production" if the operator never declares anything.
	if c.Environment == "" {
		c.Environment = "production"
	}
	// Step 6/8: canonical root for ALL mutable worker state. Wired
	// into StateDirValidator (fail-fast) and consumed by the
	// cache/blob constructors. Default "/var/lib/velox/worker"
	// matches canonical_worker_runtime.yml (UID 10001 mounts the
	// host /var/lib/velox/worker into the container at the same
	// path). Operators can pin to a different root via VELOX_STATE_DIR.
	if c.StateDir == "" {
		c.StateDir = "/var/lib/velox/worker"
	}
	// RW-PROD-002: output & scratch directories for the C++ engine.
	// Main.go injects VELOX_VIDEO_ENGINE_CPP_BIN as a CLI override
	// later; these defaults keep the doctor functional even when
	// the operator has not set the env var.
	if c.OutputDir == "" {
		c.OutputDir = "/tmp/velox/scene-composite"
	}
	if c.TempDir == "" {
		c.TempDir = os.TempDir() + "/velox-worker"
	}
	// Fase E1: tmpfs backing stays OPT-OUT (empty disables it) but the
	// size gate always has a benchmarked default so a configured
	// tmpfs_dir can never pair with a zero/negative threshold.
	if c.TmpfsThresholdBytes <= 0 {
		c.TmpfsThresholdBytes = DefaultTmpfsThresholdBytes
	}
	if c.VideoEngineCppBin == "" {
		c.VideoEngineCppBin = "velox-render-cpp"
	}
}
