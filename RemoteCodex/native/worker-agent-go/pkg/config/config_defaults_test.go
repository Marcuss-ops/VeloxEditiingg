package config

import (
	"testing"
)

// =====================================================================
// DefaultConfig / applyDefaults default-value tests
// =====================================================================
//
// Verifies the static DefaultConfig shape returned to bootstrap callers,
// the single-source-of-truth guarantee that applyDefaults() (not
// DefaultConfig) is the canonical place where Environment defaults
// to "production", and the empty-WorkDir fallback.

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig("/opt/velox")

	if cfg == nil {
		t.Fatal("Expected non-nil config")
	}

	if cfg.MasterURL != "http://localhost:8000" {
		t.Errorf("Expected default master_url http://localhost:8000, got %s", cfg.MasterURL)
	}

	if cfg.WorkerID == "" {
		t.Error("Expected non-empty worker_id")
	}

	if cfg.WorkerName != "velox-worker" {
		t.Errorf("Expected default worker_name velox-worker, got %s", cfg.WorkerName)
	}

	if cfg.WorkDir != "/opt/velox" {
		t.Errorf("Expected work_dir /opt/velox, got %s", cfg.WorkDir)
	}

	if cfg.LogLevel != "info" {
		t.Errorf("Expected default log_level info, got %s", cfg.LogLevel)
	}

	if cfg.HealthPort != 8081 {
		t.Errorf("Expected default health_port 8081, got %d", cfg.HealthPort)
	}

	// PR 1: DefaultConfig intentionally leaves Environment="" so the
	// single source for the default is applyDefaults(). Production callers
	// should observe cfg.Environment AFTER applyDefaults() — see
	// TestDefaultConfigAfterApplyDefaults below.
	if cfg.Environment != "" {
		t.Errorf("DefaultConfig should leave Environment unset (single-source: applyDefaults); got %q", cfg.Environment)
	}
}

// TestDefaultConfigAfterApplyDefaults confirms that applyDefaults()
// (the canonical single-source-of-truth setter) fills Environment to
// "production" when the operator hasn't supplied one.
func TestDefaultConfigAfterApplyDefaults(t *testing.T) {
	cfg := DefaultConfig("/opt/velox")
	cfg.applyDefaults()
	if cfg.Environment != "production" {
		t.Errorf("after applyDefaults, expected environment production, got %q", cfg.Environment)
	}
}

func TestDefaultConfigEmptyWorkDir(t *testing.T) {
	cfg := DefaultConfig("")

	if cfg.WorkDir != "/opt/velox" {
		t.Errorf("Expected default work_dir /opt/velox, got %s", cfg.WorkDir)
	}
}

// Fase E1 StorageResolver: tmpfs stays opt-out (empty disables it) while
// the size gate always carries the benchmarked default, so a configured
// tmpfs_dir can never pair with a zero/negative threshold.
func TestDefaultConfigStorageTmpfs(t *testing.T) {
	cfg := DefaultConfig("/opt/velox")
	cfg.applyDefaults()

	if cfg.TmpfsDir != "" {
		t.Errorf("tmpfs_dir should default to empty (opt-out), got %q", cfg.TmpfsDir)
	}
	if cfg.TmpfsThresholdBytes != DefaultTmpfsThresholdBytes {
		t.Errorf("tmpfs_threshold_bytes should default to %d, got %d", DefaultTmpfsThresholdBytes, cfg.TmpfsThresholdBytes)
	}

	// A zero/negative threshold is repaired by applyDefaults.
	cfg.TmpfsThresholdBytes = -5
	cfg.applyDefaults()
	if cfg.TmpfsThresholdBytes != DefaultTmpfsThresholdBytes {
		t.Errorf("negative threshold should be repaired to %d, got %d", DefaultTmpfsThresholdBytes, cfg.TmpfsThresholdBytes)
	}
	// An operator-supplied threshold survives applyDefaults untouched.
	cfg.TmpfsThresholdBytes = 128 * 1024 * 1024
	cfg.applyDefaults()
	if cfg.TmpfsThresholdBytes != 128*1024*1024 {
		t.Errorf("operator threshold should be preserved, got %d", cfg.TmpfsThresholdBytes)
	}
}

// ARTIFACT_STAGING: volatile RAM staging is opt-out, but its tuning knobs
// always carry safe defaults so toggling it on later cannot pair with a
// zero max-percent or a zero reserve.
func TestDefaultConfigArtifactTmpfsStaging(t *testing.T) {
	cfg := DefaultConfig("/opt/velox")
	cfg.applyDefaults()

	if cfg.ArtifactTmpfsEnabled {
		t.Error("artifact tmpfs staging should default to disabled")
	}
	if cfg.ArtifactTmpfsDir != "" {
		t.Errorf("artifact_tmpfs_dir should default to empty, got %q", cfg.ArtifactTmpfsDir)
	}
	if cfg.ArtifactTmpfsMaxPercent != DefaultArtifactTmpfsMaxPercent {
		t.Errorf("artifact_tmpfs_max_percent should default to %d, got %d",
			DefaultArtifactTmpfsMaxPercent, cfg.ArtifactTmpfsMaxPercent)
	}
	if cfg.ArtifactTmpfsReserveBytes != DefaultArtifactTmpfsReserveBytes {
		t.Errorf("artifact_tmpfs_reserve_bytes should default to %d, got %d",
			DefaultArtifactTmpfsReserveBytes, cfg.ArtifactTmpfsReserveBytes)
	}

	// Zero values are repaired by applyDefaults.
	cfg.ArtifactTmpfsMaxPercent = 0
	cfg.ArtifactTmpfsReserveBytes = 0
	cfg.applyDefaults()
	if cfg.ArtifactTmpfsMaxPercent != DefaultArtifactTmpfsMaxPercent {
		t.Errorf("zero max percent should be repaired to %d, got %d",
			DefaultArtifactTmpfsMaxPercent, cfg.ArtifactTmpfsMaxPercent)
	}
	if cfg.ArtifactTmpfsReserveBytes != DefaultArtifactTmpfsReserveBytes {
		t.Errorf("zero reserve should be repaired to %d, got %d",
			DefaultArtifactTmpfsReserveBytes, cfg.ArtifactTmpfsReserveBytes)
	}

	// An operator-supplied value survives applyDefaults untouched.
	cfg.ArtifactTmpfsMaxPercent = 70
	cfg.ArtifactTmpfsReserveBytes = 1 << 30
	cfg.applyDefaults()
	if cfg.ArtifactTmpfsMaxPercent != 70 {
		t.Errorf("operator max percent should be preserved, got %d", cfg.ArtifactTmpfsMaxPercent)
	}
	if cfg.ArtifactTmpfsReserveBytes != 1<<30 {
		t.Errorf("operator reserve should be preserved, got %d", cfg.ArtifactTmpfsReserveBytes)
	}
}

// Cache pressure-eviction tuning always carries safe defaults so the
// cleanup loop can never pair a zero watermark/batch with its LRU pass.
func TestDefaultConfigCachePressureEviction(t *testing.T) {
	cfg := DefaultConfig("/opt/velox")
	cfg.applyDefaults()

	if cfg.CacheHighWatermarkPercent != DefaultCacheHighWatermarkPercent {
		t.Errorf("cache_high_watermark_percent should default to %d, got %d",
			DefaultCacheHighWatermarkPercent, cfg.CacheHighWatermarkPercent)
	}
	if cfg.CacheLowWatermarkPercent != DefaultCacheLowWatermarkPercent {
		t.Errorf("cache_low_watermark_percent should default to %d, got %d",
			DefaultCacheLowWatermarkPercent, cfg.CacheLowWatermarkPercent)
	}
	if cfg.CacheEvictionBatchSize != DefaultCacheEvictionBatchSize {
		t.Errorf("cache_eviction_batch_size should default to %d, got %d",
			DefaultCacheEvictionBatchSize, cfg.CacheEvictionBatchSize)
	}
	if cfg.CacheEvictionIntervalSecs != DefaultCacheEvictionIntervalSecs {
		t.Errorf("cache_eviction_interval_secs should default to %d, got %d",
			DefaultCacheEvictionIntervalSecs, cfg.CacheEvictionIntervalSecs)
	}

	// Zero values are repaired by applyDefaults.
	cfg.CacheHighWatermarkPercent = 0
	cfg.CacheLowWatermarkPercent = 0
	cfg.CacheEvictionBatchSize = 0
	cfg.CacheEvictionIntervalSecs = 0
	cfg.applyDefaults()
	if cfg.CacheHighWatermarkPercent != DefaultCacheHighWatermarkPercent ||
		cfg.CacheLowWatermarkPercent != DefaultCacheLowWatermarkPercent ||
		cfg.CacheEvictionBatchSize != DefaultCacheEvictionBatchSize ||
		cfg.CacheEvictionIntervalSecs != DefaultCacheEvictionIntervalSecs {
		t.Errorf("zero cache-pressure fields should be repaired to defaults, got h=%d l=%d b=%d i=%d",
			cfg.CacheHighWatermarkPercent, cfg.CacheLowWatermarkPercent,
			cfg.CacheEvictionBatchSize, cfg.CacheEvictionIntervalSecs)
	}
}

// Background integrity scrubber is opt-in but its throttling knobs always
// carry safe defaults so toggling it on later cannot pair with a zero
// budget, interval, or blob count.
func TestDefaultConfigCacheScrub(t *testing.T) {
	cfg := DefaultConfig("/opt/velox")
	cfg.applyDefaults()

	if cfg.CacheScrubEnabled {
		t.Error("cache scrub should default to disabled")
	}
	if cfg.CacheScrubIntervalSecs != DefaultCacheScrubIntervalSecs {
		t.Errorf("cache_scrub_interval_secs should default to %d, got %d",
			DefaultCacheScrubIntervalSecs, cfg.CacheScrubIntervalSecs)
	}
	if cfg.CacheScrubBytesPerPass != DefaultCacheScrubBytesPerPass {
		t.Errorf("cache_scrub_bytes_per_pass should default to %d, got %d",
			DefaultCacheScrubBytesPerPass, cfg.CacheScrubBytesPerPass)
	}
	if cfg.CacheScrubMaxBlobsPerPass != DefaultCacheScrubMaxBlobsPerPass {
		t.Errorf("cache_scrub_max_blobs_per_pass should default to %d, got %d",
			DefaultCacheScrubMaxBlobsPerPass, cfg.CacheScrubMaxBlobsPerPass)
	}

	// Zero values are repaired by applyDefaults.
	cfg.CacheScrubIntervalSecs = 0
	cfg.CacheScrubBytesPerPass = 0
	cfg.CacheScrubMaxBlobsPerPass = 0
	cfg.applyDefaults()
	if cfg.CacheScrubIntervalSecs != DefaultCacheScrubIntervalSecs ||
		cfg.CacheScrubBytesPerPass != DefaultCacheScrubBytesPerPass ||
		cfg.CacheScrubMaxBlobsPerPass != DefaultCacheScrubMaxBlobsPerPass {
		t.Errorf("zero scrub fields should be repaired to defaults, got i=%d b=%d m=%d",
			cfg.CacheScrubIntervalSecs, cfg.CacheScrubBytesPerPass, cfg.CacheScrubMaxBlobsPerPass)
	}

	// An operator-supplied value survives applyDefaults untouched.
	cfg.CacheScrubIntervalSecs = 7200
	cfg.CacheScrubBytesPerPass = 1 << 30
	cfg.CacheScrubMaxBlobsPerPass = 16
	cfg.applyDefaults()
	if cfg.CacheScrubIntervalSecs != 7200 || cfg.CacheScrubBytesPerPass != 1<<30 || cfg.CacheScrubMaxBlobsPerPass != 16 {
		t.Errorf("operator scrub values should be preserved, got i=%d b=%d m=%d",
			cfg.CacheScrubIntervalSecs, cfg.CacheScrubBytesPerPass, cfg.CacheScrubMaxBlobsPerPass)
	}
}
