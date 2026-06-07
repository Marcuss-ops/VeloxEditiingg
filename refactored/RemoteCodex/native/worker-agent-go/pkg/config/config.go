// Package config provides configuration management for the Velox Worker Agent.
package config

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// APIMode determines which API endpoints to use for master communication.
type APIMode string

const (
	// APIModeLegacyV1 uses Python master v1 endpoints (/api/v1/workers/heartbeat, /api/v1/queue/job)
	APIModeLegacyV1 APIMode = "legacy_v1"
	// APIModeNewAPI uses new Go master endpoints (/api/workers/*, /api/jobs/get)
	APIModeNewAPI APIMode = "new_api"
)

// WorkerConfig holds the worker configuration loaded from JSON.
// Example config file: /opt/velox/worker_config.json
type WorkerConfig struct {
	MasterURL  string  `json:"master_url"`  // URL of the master server (e.g., http://master.example.com:8000)
	WorkerID   string  `json:"worker_id"`   // Unique worker identifier (e.g., worker-001 or auto-generated)
	WorkerName string  `json:"worker_name"` // Human-readable worker name (e.g., video-worker-1)
	WorkDir    string  `json:"work_dir"`    // Base directory for velox installations (e.g., /opt/velox)
	VenvPath   string  `json:"venv_path"`   // Path to Python virtualenv (e.g., /opt/velox/.venv)
	LogLevel   string  `json:"log_level"`   // Log level: debug, info, warn, error
	APIMode    APIMode `json:"api_mode"`    // API mode: legacy_v1 or new_api (default: new_api)

	// Phase 1: GOD Workflow feature flags
	GodCPUWorkflowEnabled bool `json:"god_cpu_workflow_enabled"` // Enable GOD CPU workflow path (Phase 1)

	// Phase 1: Worker policy
	MaxActiveJobs  int `json:"max_active_jobs"` // Maximum concurrent active jobs (default: 1)
	CPUWorkerPool  int `json:"cpu_worker_pool"` // CPU worker pool size (default: 8)
	PrometheusPort int `json:"prometheus_port"` // Prometheus metrics port (default: 9090)

	// Phase 2: Render plan validation
	// DEPRECATED: This flag will be removed in a future release.
	// All jobs MUST include render_plan_version in the payload.
	// Set to false to enforce strict validation.
	AllowLegacyRenderPlanVersionFallback bool `json:"allow_legacy_render_plan_version_fallback"` // Allow fallback from version field if render_plan_version is missing (default: false - legacy fallback disabled)

	// Phase 3: Command polling
	EnableCommandPolling bool `json:"enable_command_polling"` // Poll master for ad-hoc commands (default: false)
}

// ErrInvalidConfig is returned when configuration validation fails.
var ErrInvalidConfig = errors.New("invalid configuration")

// LoadConfig reads and parses a WorkerConfig from a JSON file.
// Returns an error if the file cannot be read or parsed.
func LoadConfig(path string) (*WorkerConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	var config WorkerConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", path, err)
	}

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

// GenerateWorkerID creates a unique worker ID in the format "worker-{8-char-hex}".
// This matches the Python implementation: f"worker-{uuid.uuid4().hex[:8]}"
func GenerateWorkerID() string {
	bytes := make([]byte, 4) // 4 bytes = 8 hex chars
	if _, err := rand.Read(bytes); err != nil {
		// Fallback to a deterministic ID if random fails (should never happen)
		return "worker-00000000"
	}
	return "worker-" + hex.EncodeToString(bytes)
}

// DefaultConfig creates a WorkerConfig with sensible default values.
// If workDir is empty, it defaults to "/opt/velox".
func DefaultConfig(workDir string) *WorkerConfig {
	if workDir == "" {
		workDir = "/opt/velox"
	}

	return &WorkerConfig{
		MasterURL:  "http://localhost:8000",
		WorkerID:   GenerateWorkerID(),
		WorkerName: "velox-worker",
		WorkDir:    workDir,
		VenvPath:   filepath.Join(workDir, ".venv"),
		LogLevel:   "info",
		APIMode:    APIModeNewAPI, // Default to new API

		// Phase 1: GOD Workflow defaults
		GodCPUWorkflowEnabled: false, // Disabled by default, enable via config
		MaxActiveJobs:         1,     // 1 main job per VPS
		CPUWorkerPool:         8,     // 8-core concurrency
		EnableCommandPolling:  false, // Disabled by default to avoid legacy command polling noise
	}
}

// Validate checks that all required configuration fields are set.
// Returns an error with details if validation fails.
func (c *WorkerConfig) Validate() error {
	if c == nil {
		return fmt.Errorf("%w: config is nil", ErrInvalidConfig)
	}

	var errs []string

	if c.MasterURL == "" {
		errs = append(errs, "master_url is required")
	}

	if c.WorkerID == "" {
		errs = append(errs, "worker_id is required")
	}

	if c.WorkDir == "" {
		errs = append(errs, "work_dir is required")
	}

	// Validate log level if set
	validLogLevels := map[string]bool{
		"":      true, // empty is ok, will use default
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
	}
	if !validLogLevels[c.LogLevel] {
		errs = append(errs, fmt.Sprintf("invalid log_level: %s (valid: debug, info, warn, error)", c.LogLevel))
	}

	if len(errs) > 0 {
		// Build error message from all validation errors
		errMsg := "validation errors: "
		for i, e := range errs {
			if i > 0 {
				errMsg += "; "
			}
			errMsg += e
		}
		return fmt.Errorf("%w: %s", ErrInvalidConfig, errMsg)
	}

	return nil
}

// String returns a formatted string representation of the config (for logging).
func (c *WorkerConfig) String() string {
	if c == nil {
		return "WorkerConfig{nil}"
	}
	return fmt.Sprintf("WorkerConfig{WorkerID: %s, WorkerName: %s, MasterURL: %s, WorkDir: %s}",
		c.WorkerID, c.WorkerName, c.MasterURL, c.WorkDir)
}
