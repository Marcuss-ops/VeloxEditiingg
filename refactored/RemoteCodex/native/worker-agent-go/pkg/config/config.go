// Package config provides configuration management for the Velox Worker Agent.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"velox-shared/identity"
)

// WorkerConfig holds the worker configuration loaded from JSON.
// Example config file: /opt/velox/worker_config.json
type WorkerConfig struct {
	MasterURL       string `json:"master_url"`  // URL of the master server (e.g., http://master.example.com:8000)
	WorkerID        string `json:"worker_id"`   // Unique worker identifier (e.g., worker-001 or auto-generated)
	WorkerName      string `json:"worker_name"` // Human-readable worker name (e.g., video-worker-1)
	WorkDir         string `json:"work_dir"`    // Base directory for velox installations (e.g., /opt/velox)
	LogLevel        string `json:"log_level"`   // Log level: debug, info, warn, error
	BundleVersion   string `json:"bundle_version,omitempty"`
	BundleHash      string `json:"bundle_hash,omitempty"`
	ProtocolVersion string `json:"protocol_version,omitempty"`
	EngineVersion   string `json:"engine_version,omitempty"`

	// Worker policy
	MaxActiveJobs  int `json:"max_active_jobs"`  // Maximum concurrent active jobs (default: 1)
	PrometheusPort int `json:"prometheus_port"`  // Prometheus metrics port (default: 9090, 0=disabled)
	HealthPort     int `json:"health_port"`      // Health HTTP port (default: 8081, 0=disabled)

	// ControlGRPCURL is the gRPC endpoint for the worker control stream.
	// Example: "master.example.com:8443"
	ControlGRPCURL string `json:"control_grpc_url,omitempty"`

	// mTLS configuration for gRPC transport (Phase 7).
	// TLSCertFile is the path to the worker's client certificate (PEM).
	// If empty, insecure transport is used.
	TLSCertFile string `json:"tls_cert_file,omitempty"`

	// TLSKeyFile is the path to the worker's private key (PEM).
	TLSKeyFile string `json:"tls_key_file,omitempty"`

	// TLSCAFile is the path to the CA certificate that signed the server's cert.
	// Required to verify the server's identity.
	TLSCAFile string `json:"tls_ca_file,omitempty"`

	// WorkerSecret is the pre-shared secret used to derive the persistent credential hash.
	// Set via VELOX_WORKER_SECRET env var. Combined with WorkerID to produce SHA-256 credential.
	WorkerSecret string `json:"-"`

	// AllowInsecureGRPC enables unencrypted gRPC transport. Only valid in
	// dev; transport_factory.go refuses to start without VELOX_ALLOW_INSECURE_GRPC_DEV=true.
	AllowInsecureGRPC bool `json:"allow_insecure_grpc_dev,omitempty"`

	// RequiresWorkerSecret flips on the server-side credential_hash authentication.
	// The transport factory refuses to start when this is true and WorkerSecret is empty.
	RequiresWorkerSecret bool `json:"requires_worker_secret,omitempty"`

	// Asset cache: shared directory for caching downloaded scene images, clips, and audio.
	// Default: "" (disabled — each job downloads its own assets)
	AssetCacheDir string `json:"asset_cache_dir,omitempty"`

	// Circuit breaker configuration
	CircuitBreakerFailureThreshold int `json:"circuit_breaker_failure_threshold,omitempty"` // Failures to open circuit (default: 5)
	CircuitBreakerSuccessThreshold int `json:"circuit_breaker_success_threshold,omitempty"` // Successes to close circuit (default: 3)
	CircuitBreakerTimeoutSecs      int `json:"circuit_breaker_timeout_secs,omitempty"`      // Seconds before half-open (default: 60)
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

	config.applyDefaults()

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
//
// Implementation lives in shared/identity so that the Velox master server and
// the worker agent share the exact same entropy source and format. This
// keeps ID-generation stable across the ecosystem and avoids drift.
func GenerateWorkerID() string {
	return identity.GenerateWorkerID()
}

// DefaultConfig creates a WorkerConfig with sensible default values.
// If workDir is empty, it defaults to "/opt/velox".
func DefaultConfig(workDir string) *WorkerConfig {
	if workDir == "" {
		workDir = "/opt/velox"
	}

	return &WorkerConfig{
		MasterURL:               "http://localhost:8000",
		WorkerID:                GenerateWorkerID(),
		WorkerName:              "velox-worker",
		WorkDir:                 workDir,
		LogLevel:                "info",
		BundleVersion:           "",
		ProtocolVersion:         "2026-06-worker-v1",
		MaxActiveJobs:           1,    // 1 main job per VPS
		HealthPort:              8081, // Health HTTP endpoint for Docker HEALTHCHECK
		WorkerSecret:            "",   // Set via VELOX_WORKER_SECRET env var
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

	// Validate transport settings
	if c.ControlGRPCURL == "" {
		errs = append(errs, "control_grpc_url is required")
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

// NormalizeWorkerID normalizes IP-derived worker IDs by stripping all leading
// "host_" prefixes and replacing dots with underscores.
//
// Implementation lives in shared/identity so the canonical rules are shared
// with the Velox master server. Test cases live in shared/identity_test.go.
func NormalizeWorkerID(id string) string {
	return identity.NormalizeWorkerID(id)
}

// String returns a formatted string representation of the config (for logging).
func (c *WorkerConfig) String() string {
	if c == nil {
		return "WorkerConfig{nil}"
	}
	return fmt.Sprintf("WorkerConfig{WorkerID: %s, WorkerName: %s, MasterURL: %s, WorkDir: %s}",
		c.WorkerID, c.WorkerName, c.MasterURL, c.WorkDir)
}
