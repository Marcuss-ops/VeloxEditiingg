// Package config / config_load_save.go — JSON loading and persistence.
//
// LoadConfig reads + parses a worker_config.json and applies the
// default + env-var precedence layers; SaveConfig writes a config back
// to disk. CLI-flag overrides (highest precedence) are applied by the
// caller AFTER LoadConfig returns and BEFORE Validate() is called.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// LoadConfig reads and parses a WorkerConfig from a JSON file.
// Returns an error if the file cannot be read or parsed.
//
// Order of operations on the returned *WorkerConfig:
//  1. JSON unmarshal from `path`
//  2. applyDefaults() — safe-by-default fallback values
//  3. applyEnvOverrides() — VELOX_GRPC_TLS_* / VELOX_ALLOW_INSECURE_GRPC_DEV /
//     VELOX_ENV override the JSON values
//
// CLI-flag overrides (highest precedence) are applied by the caller
// (cmd/velox-worker-agent/main.go) AFTER this returns and BEFORE Validate().
func LoadConfig(path string) (*WorkerConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	var config WorkerConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", path, err)
	}

	config.applyDefaults()

	// PR 1: env vars override JSON-loaded values. CLI flags are still
	// applied by main.go AFTER this returns — they remain the highest
	// precedence layer.
	applyEnvOverrides(&config)

	return &config, nil
}

// SaveConfig writes a WorkerConfig to a JSON file with indentation.
// Creates parent directories if they don't exist.
func SaveConfig(path string, config *WorkerConfig) error {
	if config == nil {
		return errors.New("config cannot be nil")
	}

	// Ensure parent directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory %s: %w", dir, err)
	}

	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file %s: %w", path, err)
	}

	return nil
}
