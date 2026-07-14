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
