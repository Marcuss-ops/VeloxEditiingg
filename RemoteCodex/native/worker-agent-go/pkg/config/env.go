// Package config — environmental-variable overlay.
//
// PR 1 (`codex/grpc-config-single-source`): the env-vars in this file
// are the SECOND precedence layer (above worker_config.json, below CLI
// flags). They are the API surface through which containerised / k8s
// deployments inject TLS material without baking it into worker_config.json.
//
// Every env var listed below is checked exactly here — never re-read
// from os.Getenv() in the transport factory, cmd/velox-worker-agent, or
// any other consumer.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Canonical mapping — keep this section aligned with applyEnvOverrides().
const (
	// EnvEnvironment tags the deployment lifecycle: dev / staging / production.
	EnvEnvironment = "VELOX_ENV"
	// EnvTLSCertFile is the worker's client leaf certificate (PEM).
	EnvTLSCertFile = "VELOX_GRPC_TLS_CERT_FILE"
	// EnvTLSKeyFile is the worker's private key (PEM).
	EnvTLSKeyFile = "VELOX_GRPC_TLS_KEY_FILE"
	// EnvTLSCAFile is the CA certificate that signed the master's cert (PEM).
	EnvTLSCAFile = "VELOX_GRPC_TLS_CA_FILE"
	// EnvAllowInsecureGRPCDev toggles plaintext gRPC for local dev only.
	EnvAllowInsecureGRPCDev = "VELOX_ALLOW_INSECURE_GRPC_DEV"
	// EnvWorkerSecret is the explicit raw worker secret and takes precedence
	// over the mounted file fallback.
	EnvWorkerSecret = "VELOX_WORKER_SECRET"
	// EnvWorkerCredentialFile points at the migration-mounted raw worker
	// secret. It is used only when EnvWorkerSecret is absent.
	EnvWorkerCredentialFile = "VELOX_WORKER_CREDENTIAL_FILE"
	// EnvMinDiskFreeMB overrides the readiness disk floor (RW-PROD-004 §3 A4).
	// Operators set this per-host to match the actual scratch-disk size;
	// the disk watcher in main.go downsamples MiB → bytes for ReadyState.
	EnvMinDiskFreeMB = "VELOX_MIN_DISK_FREE_MB"
	// EnvReadyzEndpoint overrides the /health/ready mount path (RW-PROD-004 §3 A9).
	// Default empty ⇒ /health/ready stays canonical. Operators set this on a
	// Kubernetes podspec that wants /readyz to keep the canonical mount out
	// of network policy scope.
	EnvReadyzEndpoint = "VELOX_READYZ_ENDPOINT"
	// EnvVideoEngineCppBin is the path to the C++ video render binary.
	// Defaults to "velox-render-cpp" (resolved via exec.LookPath in worker).
	EnvVideoEngineCppBin = "VELOX_VIDEO_ENGINE_CPP_BIN"
	// EnvWorkerClass is the fleet class (cpu-xlarge, gpu-a100, ...).
	EnvWorkerClass = "VELOX_WORKER_CLASS"
	// EnvRolloutGroup is the rollout cohort (v3.4, canary, holdout, ...).
	EnvRolloutGroup = "VELOX_ROLLOUT_GROUP"
	// EnvStateDir is the canonical root for ALL mutable worker state
	// (Step 6/8). Replaces the legacy bind-mount for the per-job
	// assets_cache and the per-subsystem /opt defaults (cache, blobs).
	// Default: "/var/lib/velox/worker" (applied by applyDefaults).
	EnvStateDir = "VELOX_STATE_DIR"
	// EnvWorkerProfile selects the runtime profile for this worker.
	// "creator" disables the C++ video pipeline and scene.composite.v1.
	EnvWorkerProfile = "VELOX_WORKER_PROFILE"
	// EnvTelemetryJSONDir opts into per-attempt telemetry JSON artifacts
	// (receipt, benchmark, diagnostic dump). Empty disables the JSON sinks.
	EnvTelemetryJSONDir = "VELOX_TELEMETRY_JSON_DIR"

	// EnvPrometheusPort controls the worker Prometheus scrape endpoint.
	// Set to 0 to disable the endpoint explicitly.
	EnvPrometheusPort = "VELOX_PROMETHEUS_PORT"
	// EnvAssetDownloadConcurrency caps the number of simultaneous asset byte
	// transfers the canonical download manager runs per worker. Default 4.
	EnvAssetDownloadConcurrency           = "VELOX_ASSET_DOWNLOAD_CONCURRENCY"
	EnvPublisherConcurrency               = "VELOX_PUBLISHER_CONCURRENCY"
	EnvProgressivePartConcurrency         = "VELOX_PROGRESSIVE_PART_CONCURRENCY"
	EnvAssetChunkedDownloadEnabled        = "VELOX_ASSET_CHUNKED_DOWNLOAD"
	EnvAssetChunkedDownloadThresholdBytes = "VELOX_ASSET_CHUNKED_DOWNLOAD_THRESHOLD_BYTES"
	EnvAssetChunkedDownloadConcurrency    = "VELOX_ASSET_CHUNKED_DOWNLOAD_CONCURRENCY"
	EnvPrefetchHorizonJobs                = "VELOX_PREFETCH_HORIZON_JOBS"
	EnvPrefetchProtectionLookaheadJobs    = "VELOX_PREFETCH_PROTECTION_LOOKAHEAD_JOBS"
	EnvPrefetchMaxConcurrent              = "VELOX_PREFETCH_MAX_CONCURRENT"
	EnvPrefetchByteBudget                 = "VELOX_PREFETCH_BYTE_BUDGET"
	EnvPrefetchMaxBandwidthBytesPerSecond = "VELOX_PREFETCH_MAX_BANDWIDTH_BPS"
	EnvPrefetchDiskRestrictedPercent      = "VELOX_PREFETCH_DISK_RESTRICTED_PERCENT"
	EnvPrefetchDiskCriticalPercent        = "VELOX_PREFETCH_DISK_CRITICAL_PERCENT"
	EnvPrefetchDiskRecoveryPercent        = "VELOX_PREFETCH_DISK_RECOVERY_PERCENT"
	EnvPrefetchRAMEnabled                 = "VELOX_PREFETCH_RAM_ENABLED"
	EnvPrefetchRAMBudgetBytes             = "VELOX_PREFETCH_RAM_BUDGET_BYTES"
	EnvPrefetchRAMMaxAssetBytes           = "VELOX_PREFETCH_RAM_MAX_ASSET_BYTES"

	// Network admission controller env vars.
	EnvNetworkIngressBudgetBytesPerSecond = "VELOX_NETWORK_INGRESS_BUDGET_BPS"
	EnvNetworkEgressBudgetBytesPerSecond  = "VELOX_NETWORK_EGRESS_BUDGET_BPS"
	EnvPrefetchRAMMinFutureRefs           = "VELOX_PREFETCH_RAM_MIN_FUTURE_REFS"
	EnvPrefetchRAMMaxNextUseDistance      = "VELOX_PREFETCH_RAM_MAX_NEXT_USE_DISTANCE"
	// EnvTmpfsDir selects the memory-backed scratch directory for small
	// ATTEMPT_TEMP files (Fase E1 StorageResolver). Empty disables tmpfs.
	EnvTmpfsDir = "VELOX_TMPFS_DIR"
	// EnvTmpfsThresholdBytes sets the tmpfs size gate in bytes (default
	// 64 MiB). Files at/above the threshold always land on TempDir (NVMe).
	EnvTmpfsThresholdBytes = "VELOX_TMPFS_THRESHOLD_BYTES"
	// EnvArtifactTmpfsEnabled opts into volatile RAM staging for the final
	// artifact (ARTIFACT_STAGING). Off by default.
	EnvArtifactTmpfsEnabled = "VELOX_ARTIFACT_TMPFS_ENABLED"
	// EnvArtifactTmpfsDir selects the tmpfs staging root (e.g.
	// /dev/shm/velox-artifacts). Required when enabled.
	EnvArtifactTmpfsDir = "VELOX_ARTIFACT_TMPFS_DIR"
	// EnvArtifactTmpfsMaxPercent caps the reserved fraction of the tmpfs
	// total (1-99, default 65).
	EnvArtifactTmpfsMaxPercent = "VELOX_ARTIFACT_TMPFS_MAX_PERCENT"
	// EnvArtifactTmpfsReserveBytes is the tmpfs headroom in bytes that must
	// always remain free (default 512 MiB).
	EnvArtifactTmpfsReserveBytes = "VELOX_ARTIFACT_TMPFS_RESERVE_BYTES"
	// EnvCacheHighWatermarkPercent sets the disk-usage percentage at/above
	// which the cache eviction loop starts deleting LRU blobs (default 80).
	EnvCacheHighWatermarkPercent = "VELOX_CACHE_HIGH_WATERMARK_PERCENT"
	// EnvCacheLowWatermarkPercent sets the stop target for pressure eviction
	// (default 72; must be < high).
	EnvCacheLowWatermarkPercent = "VELOX_CACHE_LOW_WATERMARK_PERCENT"
	// EnvCacheEvictionBatchSize caps the LRU blobs removed per pass
	// (default 128).
	EnvCacheEvictionBatchSize = "VELOX_CACHE_EVICTION_BATCH_SIZE"
	// EnvCacheEvictionIntervalSecs is the cleanup-loop tick cadence in
	// seconds (default 30).
	EnvCacheEvictionIntervalSecs = "VELOX_CACHE_EVICTION_INTERVAL_SECS"
	// EnvCacheScrubEnabled opts into the background integrity scrubber
	// (default off).
	EnvCacheScrubEnabled = "VELOX_CACHE_SCRUB_ENABLED"
	// EnvCacheScrubIntervalSecs is the scrub-loop tick cadence in seconds
	// (default 3600).
	EnvCacheScrubIntervalSecs = "VELOX_CACHE_SCRUB_INTERVAL_SECS"
	// EnvCacheScrubBytesPerPass is the soft byte budget per scrub pass
	// (default 256 MiB).
	EnvCacheScrubBytesPerPass = "VELOX_CACHE_SCRUB_BYTES_PER_PASS"
	// EnvCacheScrubMaxBlobsPerPass caps the blobs touched per pass
	// (default 8).
	EnvCacheScrubMaxBlobsPerPass = "VELOX_CACHE_SCRUB_MAX_BLOBS_PER_PASS"
)

// EnvBindings is the set of env-var names this package inspects.
// main.go may consult this slice (e.g. for debug dumps) but never to
// re-implement binding.
var EnvBindings = []string{
	EnvEnvironment,
	EnvTLSCertFile,
	EnvTLSKeyFile,
	EnvTLSCAFile,
	EnvAllowInsecureGRPCDev,
	EnvWorkerSecret,
	EnvWorkerCredentialFile,
	EnvMinDiskFreeMB,
	EnvReadyzEndpoint,
	EnvVideoEngineCppBin,
	EnvWorkerClass,
	EnvRolloutGroup,
	EnvStateDir,
	EnvWorkerProfile,
	EnvTelemetryJSONDir,
	EnvPrometheusPort,
	EnvAssetDownloadConcurrency,
	EnvPublisherConcurrency, EnvProgressivePartConcurrency,
	EnvAssetChunkedDownloadEnabled, EnvAssetChunkedDownloadThresholdBytes, EnvAssetChunkedDownloadConcurrency,
	EnvPrefetchHorizonJobs, EnvPrefetchProtectionLookaheadJobs,
	EnvPrefetchMaxConcurrent, EnvPrefetchByteBudget,
	EnvPrefetchMaxBandwidthBytesPerSecond, EnvPrefetchDiskRestrictedPercent,
	EnvPrefetchDiskCriticalPercent, EnvPrefetchDiskRecoveryPercent,
	EnvPrefetchRAMEnabled, EnvPrefetchRAMBudgetBytes,
	EnvPrefetchRAMMaxAssetBytes, EnvPrefetchRAMMinFutureRefs,
	EnvPrefetchRAMMaxNextUseDistance,
	EnvTmpfsDir,
	EnvTmpfsThresholdBytes,
	EnvArtifactTmpfsEnabled, EnvArtifactTmpfsDir,
	EnvArtifactTmpfsMaxPercent, EnvArtifactTmpfsReserveBytes,
	EnvCacheHighWatermarkPercent, EnvCacheLowWatermarkPercent,
	EnvCacheEvictionBatchSize, EnvCacheEvictionIntervalSecs,
	EnvCacheScrubEnabled, EnvCacheScrubIntervalSecs,
	EnvCacheScrubBytesPerPass, EnvCacheScrubMaxBlobsPerPass,
}

// envTruthy reports whether a string from os.Getenv should be interpreted
// as "true". Recognised truthy values: "1", "true", "TRUE", "yes", "on".
// Exposed at package level so tests and CLI docs can use the same set.
func envTruthy(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// applyEnvOverrides binds environment variables onto an already-parsed
// WorkerConfig. Called by LoadConfig() after applyDefaults().
//
// Mapping (PR 1 spec):
//
//	VELOX_ENV                          → cfg.Environment
//	VELOX_GRPC_TLS_CERT_FILE           → cfg.TLSCertFile
//	VELOX_GRPC_TLS_KEY_FILE            → cfg.TLSKeyFile
//	VELOX_GRPC_TLS_CA_FILE             → cfg.TLSCAFile
//	VELOX_ALLOW_INSECURE_GRPC_DEV      → cfg.AllowInsecureGRPC
//
// Order matters only w.r.t. each individual field — an env var always
// overrides whatever the JSON had for that field. Cross-field consistency
// is then enforced by Validate().
func applyEnvOverrides(cfg *WorkerConfig) error {
	if cfg == nil {
		return nil
	}
	if v := os.Getenv(EnvEnvironment); v != "" {
		cfg.Environment = v
	}
	if v := os.Getenv(EnvTLSCertFile); v != "" {
		cfg.TLSCertFile = v
	}
	if v := os.Getenv(EnvTLSKeyFile); v != "" {
		cfg.TLSKeyFile = v
	}
	if v := os.Getenv(EnvTLSCAFile); v != "" {
		cfg.TLSCAFile = v
	}
	if v := os.Getenv(EnvAllowInsecureGRPCDev); v != "" {
		cfg.AllowInsecureGRPC = envTruthy(v)
	}
	// VELOX_WORKER_SECRET is the canonical explicit source and wins over
	// the mounted file. This is intentionally resolved in the same env
	// layer as the file fallback so validation and registration observe the
	// same credential.
	if secret := strings.TrimSpace(os.Getenv(EnvWorkerSecret)); secret != "" {
		cfg.WorkerSecret = secret
	}
	// During the worker-runtime migration, the resolver mounts the raw
	// per-worker secret at this path. Use it only when the explicit env
	// secret is absent. A missing/unreadable file leaves WorkerSecret empty;
	// the existing registration gate then rejects the handshake rather than
	// inventing a credential or silently authenticating insecurely.
	if cfg.WorkerSecret == "" {
		if path := strings.TrimSpace(os.Getenv(EnvWorkerCredentialFile)); path != "" {
			data, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("read %s: %w", EnvWorkerCredentialFile, err)
			}
			secret := strings.TrimSpace(string(data))
			if secret == "" {
				return fmt.Errorf("%s points to an empty credential file", EnvWorkerCredentialFile)
			}
			cfg.WorkerSecret = secret
		}
	}
	// RW-PROD-004 §3 A4: MinDiskFreeMB takes the env-var lane (per-host
	// resource floor). The disk watcher applies cfg.MinDiskFreeMB to the
	// ready snapshot; we still want operators to ship a different floor
	// per cluster without re-baking worker_config.json on every node.
	if v := strings.TrimSpace(os.Getenv(EnvMinDiskFreeMB)); v != "" {
		if mb, perr := strconv.Atoi(v); perr == nil && mb > 0 {
			cfg.MinDiskFreeMB = mb
		}
	}
	// RW-PROD-004 §3 A9: VELOX_READYZ_ENDPOINT chooses the /health/ready
	// mount path. Empty string = canonical /health/ready; anything else
	// overrides and main.go wires the secondary mux accordingly.
	if v := strings.TrimSpace(os.Getenv(EnvReadyzEndpoint)); v != "" {
		cfg.ReadyzEndpoint = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvVideoEngineCppBin)); v != "" {
		cfg.VideoEngineCppBin = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvWorkerClass)); v != "" {
		cfg.WorkerClass = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvRolloutGroup)); v != "" {
		cfg.RolloutGroup = v
	}
	// Step 6/8: VELOX_STATE_DIR canonicalizes where the worker
	// materializes cache, blob, executor spool, and scratch assets.
	// Empty falls back to "/var/lib/velox/worker" via applyDefaults
	// so the doctor + sub-constructors can resolve without further
	// branching.
	if v := strings.TrimSpace(os.Getenv(EnvStateDir)); v != "" {
		cfg.StateDir = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvWorkerProfile)); v != "" {
		cfg.WorkerProfile = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvTelemetryJSONDir)); v != "" {
		cfg.TelemetryJSONDir = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvPrometheusPort)); v != "" {
		if port, perr := strconv.Atoi(v); perr == nil && port >= 0 && port <= 65535 {
			cfg.PrometheusPort = port
		}
	}
	// VELOX_ASSET_DOWNLOAD_CONCURRENCY caps the canonical download manager's
	// simultaneous byte transfers (default 4 in downloader.Config).
	if v := strings.TrimSpace(os.Getenv(EnvAssetDownloadConcurrency)); v != "" {
		if n, perr := strconv.Atoi(v); perr == nil && n > 0 {
			cfg.AssetDownloadConcurrency = n
		}
	}
	if v := strings.TrimSpace(os.Getenv(EnvPublisherConcurrency)); v != "" {
		if n, perr := strconv.Atoi(v); perr == nil && n > 0 {
			cfg.PublisherConcurrency = n
		}
	}
	if v := strings.TrimSpace(os.Getenv(EnvProgressivePartConcurrency)); v != "" {
		if n, perr := strconv.Atoi(v); perr == nil && n > 0 {
			cfg.ProgressivePartConcurrency = n
		}
	}
	bindPositiveInt := func(name string, dst *int) {
		if v := strings.TrimSpace(os.Getenv(name)); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				*dst = n
			}
		}
	}
	bindPositiveInt64 := func(name string, dst *int64) {
		if v := strings.TrimSpace(os.Getenv(name)); v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
				*dst = n
			}
		}
	}
	if v := strings.TrimSpace(os.Getenv(EnvAssetChunkedDownloadEnabled)); v != "" {
		cfg.AssetChunkedDownloadEnabled = envTruthy(v)
	}
	bindPositiveInt64(EnvAssetChunkedDownloadThresholdBytes, &cfg.AssetChunkedDownloadThresholdBytes)
	bindPositiveInt(EnvAssetChunkedDownloadConcurrency, &cfg.AssetChunkedDownloadConcurrency)
	bindPositiveInt(EnvPrefetchHorizonJobs, &cfg.PrefetchHorizonJobs)
	bindPositiveInt(EnvPrefetchProtectionLookaheadJobs, &cfg.PrefetchProtectionLookaheadJobs)
	bindPositiveInt(EnvPrefetchMaxConcurrent, &cfg.PrefetchMaxConcurrent)
	bindPositiveInt64(EnvPrefetchByteBudget, &cfg.PrefetchByteBudget)
	bindPositiveInt64(EnvPrefetchMaxBandwidthBytesPerSecond, &cfg.PrefetchMaxBandwidthBytesPerSecond)
	bindPositiveInt(EnvPrefetchDiskRestrictedPercent, &cfg.PrefetchDiskRestrictedPercent)
	bindPositiveInt(EnvPrefetchDiskCriticalPercent, &cfg.PrefetchDiskCriticalPercent)
	bindPositiveInt(EnvPrefetchDiskRecoveryPercent, &cfg.PrefetchDiskRecoveryPercent)

	// Network admission controller.
	bindPositiveInt64(EnvNetworkIngressBudgetBytesPerSecond, &cfg.NetworkIngressBudgetBytesPerSecond)
	bindPositiveInt64(EnvNetworkEgressBudgetBytesPerSecond, &cfg.NetworkEgressBudgetBytesPerSecond)
	if v := strings.TrimSpace(os.Getenv(EnvPrefetchRAMEnabled)); v != "" {
		cfg.PrefetchRAMEnabled = envTruthy(v)
	}
	bindPositiveInt64(EnvPrefetchRAMBudgetBytes, &cfg.PrefetchRAMBudgetBytes)
	bindPositiveInt64(EnvPrefetchRAMMaxAssetBytes, &cfg.PrefetchRAMMaxAssetBytes)
	bindPositiveInt(EnvPrefetchRAMMinFutureRefs, &cfg.PrefetchRAMMinFutureRefs)
	bindPositiveInt(EnvPrefetchRAMMaxNextUseDistance, &cfg.PrefetchRAMMaxNextUseDistance)
	// Fase E1 StorageResolver: VELOX_TMPFS_DIR opt-in memory-backed scratch
	// for small ATTEMPT_TEMP files; VELOX_TMPFS_THRESHOLD_BYTES tunes the
	// size gate (default 64 MiB via applyDefaults). Invalid numeric values
	// are ignored so a typo cannot zero the gate.
	if v := strings.TrimSpace(os.Getenv(EnvTmpfsDir)); v != "" {
		cfg.TmpfsDir = v
	}
	if v := strings.TrimSpace(os.Getenv(EnvTmpfsThresholdBytes)); v != "" {
		if n, perr := strconv.ParseInt(v, 10, 64); perr == nil && n > 0 {
			cfg.TmpfsThresholdBytes = n
		}
	}
	// ARTIFACT_STAGING: volatile RAM staging for the final artifact. Opt-in
	// via VELOX_ARTIFACT_TMPFS_ENABLED; the dir / max-percent / reserve are
	// the tuning surface. Invalid numerics are ignored (applyDefaults +
	// Validate then fail closed on the enabled combination).
	if v := strings.TrimSpace(os.Getenv(EnvArtifactTmpfsEnabled)); v != "" {
		cfg.ArtifactTmpfsEnabled = envTruthy(v)
	}
	if v := strings.TrimSpace(os.Getenv(EnvArtifactTmpfsDir)); v != "" {
		cfg.ArtifactTmpfsDir = v
	}
	bindPositiveInt(EnvArtifactTmpfsMaxPercent, &cfg.ArtifactTmpfsMaxPercent)
	bindPositiveInt64(EnvArtifactTmpfsReserveBytes, &cfg.ArtifactTmpfsReserveBytes)
	// Cache pressure-eviction tuning (RW-PROD cache pressure controller).
	// Invalid numerics are ignored (applyDefaults + Validate then fail closed
	// on the explicitly-set values).
	bindPositiveInt(EnvCacheHighWatermarkPercent, &cfg.CacheHighWatermarkPercent)
	bindPositiveInt(EnvCacheLowWatermarkPercent, &cfg.CacheLowWatermarkPercent)
	bindPositiveInt(EnvCacheEvictionBatchSize, &cfg.CacheEvictionBatchSize)
	bindPositiveInt(EnvCacheEvictionIntervalSecs, &cfg.CacheEvictionIntervalSecs)
	// Background integrity scrubber: opt-in flag + throttling knobs. Invalid
	// numerics are ignored (applyDefaults + Validate then fail closed on the
	// enabled combination).
	if v := strings.TrimSpace(os.Getenv(EnvCacheScrubEnabled)); v != "" {
		cfg.CacheScrubEnabled = envTruthy(v)
	}
	bindPositiveInt(EnvCacheScrubIntervalSecs, &cfg.CacheScrubIntervalSecs)
	bindPositiveInt64(EnvCacheScrubBytesPerPass, &cfg.CacheScrubBytesPerPass)
	bindPositiveInt(EnvCacheScrubMaxBlobsPerPass, &cfg.CacheScrubMaxBlobsPerPass)
	return nil
}
