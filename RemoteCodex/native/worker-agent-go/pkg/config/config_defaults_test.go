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
