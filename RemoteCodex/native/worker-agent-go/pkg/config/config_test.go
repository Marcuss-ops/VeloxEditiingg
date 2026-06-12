// Package config provides configuration management for the Velox Worker Agent.
package config

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLoadConfig tests loading a valid config file.
func TestLoadConfig(t *testing.T) {
	// Create a temporary config file
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
}

// TestLoadConfigNotFound tests loading a non-existent config file.
func TestLoadConfigNotFound(t *testing.T) {
	_, err := LoadConfig("/nonexistent/config.json")
	if err == nil {
		t.Error("Expected error for non-existent config file")
	}
}

// TestLoadConfigInvalidJSON tests loading an invalid JSON file.
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

// TestSaveConfig tests saving a config file.
func TestSaveConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	cfg := &WorkerConfig{
		MasterURL:  "http://localhost:8080",
		WorkerID:   "test-worker-001",
		WorkerName: "Test Worker",
		WorkDir:    "/opt/velox",
		LogLevel:   "info",
	}

	if err := SaveConfig(configPath, cfg); err != nil {
		t.Fatalf("Failed to save config: %v", err)
	}

	// Verify the file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Error("Config file was not created")
	}

	// Load and verify
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

// TestSaveConfigNil tests saving a nil config.
func TestSaveConfigNil(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")

	err := SaveConfig(configPath, nil)
	if err == nil {
		t.Error("Expected error for nil config")
	}
}

// TestSaveConfigCreatesDirectory tests that SaveConfig creates parent directories.
func TestSaveConfigCreatesDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "subdir", "nested", "config.json")

	cfg := &WorkerConfig{
		MasterURL: "http://localhost:8080",
		WorkerID:  "test-worker-001",
		WorkDir:   "/opt/velox",
	}

	if err := SaveConfig(configPath, cfg); err != nil {
		t.Fatalf("Failed to save config: %v", err)
	}

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Error("Config file was not created in nested directory")
	}
}

// TestGenerateWorkerID tests worker ID generation.
func TestGenerateWorkerID(t *testing.T) {
	id1 := GenerateWorkerID()
	id2 := GenerateWorkerID()

	// Check format: worker-{8-hex-chars}
	if len(id1) != 15 { // "worker-" (7) + 8 hex chars
		t.Errorf("Expected worker ID length 15, got %d", len(id1))
	}

	if id1[:7] != "worker-" {
		t.Errorf("Expected worker ID to start with 'worker-', got %s", id1[:7])
	}

	// Two generated IDs should be different (with very high probability)
	if id1 == id2 {
		t.Error("Expected different worker IDs to be different")
	}
}

// TestDefaultConfig tests default config creation.
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

}

// TestDefaultConfigEmptyWorkDir tests default config with empty work dir.
func TestDefaultConfigEmptyWorkDir(t *testing.T) {
	cfg := DefaultConfig("")

	if cfg.WorkDir != "/opt/velox" {
		t.Errorf("Expected default work_dir /opt/velox, got %s", cfg.WorkDir)
	}
}

// TestValidateSuccess tests validation of a valid config.
func TestValidateSuccess(t *testing.T) {
	cfg := &WorkerConfig{
		MasterURL: "http://localhost:8080",
		WorkerID:  "test-worker-001",
		WorkDir:   "/opt/velox",
		LogLevel:  "info",
	}

	if err := cfg.Validate(); err != nil {
		t.Errorf("Expected validation to pass, got error: %v", err)
	}
}

// TestValidateNil tests validation of nil config.
func TestValidateNil(t *testing.T) {
	var cfg *WorkerConfig

	err := cfg.Validate()
	if err == nil {
		t.Error("Expected error for nil config")
	}
}

// TestValidateMissingFields tests validation with missing required fields.
func TestValidateMissingFields(t *testing.T) {
	tests := []struct {
		name   string
		config *WorkerConfig
	}{
		{
			name: "missing master_url",
			config: &WorkerConfig{
				WorkerID: "test-worker-001",
				WorkDir:  "/opt/velox",
			},
		},
		{
			name: "missing worker_id",
			config: &WorkerConfig{
				MasterURL: "http://localhost:8080",
				WorkDir:   "/opt/velox",
			},
		},
		{
			name: "missing work_dir",
			config: &WorkerConfig{
				MasterURL: "http://localhost:8080",
				WorkerID:  "test-worker-001",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if err == nil {
				t.Errorf("Expected validation error for %s", tt.name)
			}
		})
	}
}

// TestValidateInvalidLogLevel tests validation with invalid log level.
func TestValidateInvalidLogLevel(t *testing.T) {
	cfg := &WorkerConfig{
		MasterURL: "http://localhost:8080",
		WorkerID:  "test-worker-001",
		WorkDir:   "/opt/velox",
		LogLevel:  "invalid",
	}

	err := cfg.Validate()
	if err == nil {
		t.Error("Expected validation error for invalid log level")
	}
}

// TestValidateLogLevels tests all valid log levels.
func TestValidateLogLevels(t *testing.T) {
	validLevels := []string{"", "debug", "info", "warn", "error"}

	for _, level := range validLevels {
		t.Run("log_level_"+level, func(t *testing.T) {
			cfg := &WorkerConfig{
				MasterURL: "http://localhost:8080",
				WorkerID:  "test-worker-001",
				WorkDir:   "/opt/velox",
				LogLevel:  level,
			}

			if err := cfg.Validate(); err != nil {
				t.Errorf("Expected validation to pass for log_level %q, got error: %v", level, err)
			}
		})
	}
}

// TestString tests the String method.
func TestString(t *testing.T) {
	cfg := &WorkerConfig{
		MasterURL:  "http://localhost:8080",
		WorkerID:   "test-worker-001",
		WorkerName: "Test Worker",
		WorkDir:    "/opt/velox",
	}

	str := cfg.String()

	if str == "" {
		t.Error("Expected non-empty string representation")
	}

	// Check that key fields are in the string
	if !contains(str, "test-worker-001") {
		t.Error("Expected worker_id in string representation")
	}

	if !contains(str, "Test Worker") {
		t.Error("Expected worker_name in string representation")
	}
}

// TestStringNil tests the String method with nil config.
func TestStringNil(t *testing.T) {
	var cfg *WorkerConfig

	str := cfg.String()

	if str != "WorkerConfig{nil}" {
		t.Errorf("Expected 'WorkerConfig{nil}', got %q", str)
	}
}

// Helper function to check if a string contains a substring.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
