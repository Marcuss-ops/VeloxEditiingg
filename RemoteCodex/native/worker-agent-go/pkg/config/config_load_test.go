package config

import (
	"os"
	"path/filepath"
	"testing"
)

// =====================================================================
// LoadConfig / SaveConfig / GenerateWorkerID round-trip tests
// =====================================================================
//
// Verifies the worker_config.json read/write round-trip, the missing-file
// and invalid-JSON failure paths, the SaveConfig invariant (must not
// overwrite an existing directory), nil-config rejection, and the
// GenerateWorkerID entropy contract. None of these tests touch TLS or
// env-var override logic — those live in config_tls_test.go and
// config_env_test.go respectively.

func TestLoadConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	configJSON := `{
		"master_url": "http://localhost:8080",
		"worker_id": "test-worker-001",
		"worker_name": "Test Worker",
		"work_dir": "/opt/velox",
		"log_level": "debug"
	}`

	if err := os.WriteFile(configPath, []byte(configJSON), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if cfg.MasterURL != "http://localhost:8080" {
		t.Errorf("Expected master_url http://localhost:8080, got %s", cfg.MasterURL)
	}

	if cfg.WorkerID != "test-worker-001" {
		t.Errorf("Expected worker_id test-worker-001, got %s", cfg.WorkerID)
	}

	if cfg.WorkerName != "Test Worker" {
		t.Errorf("Expected worker_name Test Worker, got %s", cfg.WorkerName)
	}

	if cfg.LogLevel != "debug" {
		t.Errorf("Expected log_level debug, got %s", cfg.LogLevel)
	}

	if cfg.HealthPort != 8081 {
		t.Errorf("Expected default health_port 8081 for legacy config, got %d", cfg.HealthPort)
	}

	// PR 1: missing `environment` JSON key + no VELOX_ENV env var → default "production".
	if cfg.Environment != "production" {
		t.Errorf("Expected default environment production, got %q", cfg.Environment)
	}
}

func TestLoadConfigNotFound(t *testing.T) {
	_, err := LoadConfig("/nonexistent/config.json")
	if err == nil {
		t.Error("Expected error for non-existent config file")
	}
}

func TestLoadConfigInvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	if err := os.WriteFile(configPath, []byte("invalid json"), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	_, err := LoadConfig(configPath)
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
}

func TestSaveConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	cfg := devValidBase()

	if err := SaveConfig(configPath, cfg); err != nil {
		t.Fatalf("Failed to save config: %v", err)
	}

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Error("Config file was not created")
	}

	loaded, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load saved config: %v", err)
	}

	if loaded.MasterURL != cfg.MasterURL {
		t.Errorf("Expected master_url %s, got %s", cfg.MasterURL, loaded.MasterURL)
	}

	if loaded.WorkerID != cfg.WorkerID {
		t.Errorf("Expected worker_id %s, got %s", cfg.WorkerID, loaded.WorkerID)
	}
}

func TestSaveConfigNil(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	err := SaveConfig(configPath, nil)
	if err == nil {
		t.Error("Expected error for nil config")
	}
}

func TestSaveConfigCreatesDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "subdir", "nested", "config.json")

	cfg := devValidBase()

	if err := SaveConfig(configPath, cfg); err != nil {
		t.Fatalf("Failed to save config: %v", err)
	}

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Error("Config file was not created in nested directory")
	}
}

func TestGenerateWorkerID(t *testing.T) {
	id1 := GenerateWorkerID()
	id2 := GenerateWorkerID()

	if len(id1) != 15 { // "worker-" (7) + 8 hex chars
		t.Errorf("Expected worker ID length 15, got %d", len(id1))
	}

	if id1[:7] != "worker-" {
		t.Errorf("Expected worker ID to start with 'worker-', got %s", id1[:7])
	}

	if id1 == id2 {
		t.Error("Expected different worker IDs to be different")
	}
}
