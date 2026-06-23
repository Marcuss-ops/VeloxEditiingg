// Package config provides configuration management for the Velox Worker Agent.
//
// PR 1 (`codex/grpc-config-single-source`): every TLS-related field is
// resolved in ONE place — this package — through LoadConfig() and
// Validate(). The transport factory receives an already-validated
// `GRPCTLSConfig` struct (see `WorkerConfig.GRPCTLS()`) and is no longer
// allowed to make its own env-var reads or combinatorial decisions.
//
// Precedence (highest wins): CLI flags > environment variables >
// worker_config.json > built-in defaults. This package handles
// everything below "CLI flags" — main.go applies the CLI flag overrides
// BEFORE Validate() is called.
package config

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"velox-shared/identity"
)

// GRPCTLSConfig is the canonical, fully-resolved TLS configuration for the
// worker's gRPC control plane. It must be sourced from WorkerConfig.GRPCTLS()
// — never reconstructed by callers from raw fields. Validation invariants
// for this struct live in WorkerConfig.Validate(); transport code only
// reads the struct, never recomputes it.
type GRPCTLSConfig struct {
	// CertFile is the path to the worker's client leaf certificate (PEM).
	// If empty AND AllowInsecureDev is false, the worker cannot start.
	CertFile string
	// KeyFile is the path to the worker's private key (PEM). Must pair
	// with CertFile.
	KeyFile string
	// CAFile is the path to the CA that signed the master's certificate.
	// Required to verify the server's identity (otherwise the worker is
	// trusting any cert the master presents).
	CAFile string
	// AllowInsecureDev disables encryption on the gRPC control plane.
	// Only valid when Environment != "production" — production
	// rejects this combination.
	AllowInsecureDev bool
}

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

	// Environment tags the deployment lifecycle: "dev" / "staging" / "production".
	// Binds from env var VELOX_ENV. Used by Validate() to gate dev-only
	// features (e.g. AllowInsecureGRPC). Defaults to "production" when empty
	// so the absence of an explicit declaration is safe-by-default.
	Environment string `json:"environment,omitempty"`

	// Worker policy
	MaxActiveJobs  int `json:"max_active_jobs"` // Maximum concurrent active jobs (default: 1)
	PrometheusPort int `json:"prometheus_port"` // Prometheus metrics port (default: 9090, 0=disabled)
	HealthPort     int `json:"health_port"`     // Health HTTP port (default: 8081, 0=disabled)

	// ControlGRPCURL is the gRPC endpoint for the persistent worker control stream.
	// Velox exclusively uses a gRPC-push architecture; this field is mandatory.
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
		WorkerSecret:    "",   // Set via VELOX_WORKER_SECRET env var
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
	// Environment safe-by-default. The actual env-var overlay
	// (`VELOX_ENV`) runs after this in `applyEnvOverrides`; both layers
	// land on "production" if the operator never declares anything.
	if c.Environment == "" {
		c.Environment = "production"
	}
}

// GRPCTLS bundles the four TLS-related WorkerConfig fields into a
// transport-friendly struct. Callers MUST consume TLS configuration
// through this accessor — never reconstruct it from individual fields,
// or combinatorial invariants get re-implemented in the wrong place.
func (c *WorkerConfig) GRPCTLS() GRPCTLSConfig {
	if c == nil {
		return GRPCTLSConfig{}
	}
	return GRPCTLSConfig{
		CertFile:         c.TLSCertFile,
		KeyFile:          c.TLSKeyFile,
		CAFile:           c.TLSCAFile,
		AllowInsecureDev: c.AllowInsecureGRPC,
	}
}

// Validate checks that all required configuration fields are set.
// Returns an error with details if validation fails.
//
// PR 1 invariants added for GRPCTLS:
//   - Exactly one of {cert+key+ca full triple WITH matching key, allow_insecure_grpc_dev=true in non-prod}
//     must be configured. Anything in between (only cert, cert+key no ca, mismatched key, etc.)
//     is REJECTED with a precise error.
//   - Setting both TLS AND allow_insecure_grpc_dev is REJECTED.
//   - allow_insecure_grpc_dev=true in `production` environment is REJECTED.
//   - The tls_cert_file path must exist on disk AND tls_key_file must pair against it.
//   - environment field, if set, must be one of dev|staging|production.
//
// SPEC NOTE — migration breaking change:
//
//	Previously the transport factory accepted a plain (no-TLS / no-insecure) WorkerConfig
//	when VELOX_ALLOW_INSECURE_GRPC_DEV=true was set in the environment, even in
//	production-like deployments. PR 1 tightens this: workers MUST either (a) provide
//	the full TLS triple, or (b) opt into dev-only plaintext explicitly by setting
//	VELOX_ENV=dev (or `environment: "dev"` / `"staging"` in JSON). Existing
//	deployments that relied on the undocumented env-only bypass need to add
//	VELOX_ENV=dev or `environment: "dev"` to keep working. See
//	docs/operations/PR-1-migration.md for the operator recipe.
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

	// Velox uses a gRPC-push-only architecture; control_grpc_url is mandatory.
	if c.ControlGRPCURL == "" {
		errs = append(errs, "control_grpc_url is required")
	}

	// ---- PR 1: TLS combinatorial checks ----
	// Local var named `tlsCfg` to avoid shadowing the imported
	// `crypto/tls` package — bare `tls.` later refers to that package.
	tlsCfg := c.GRPCTLS()

	// Environment gate. Default to "production" so missing declaration is
	// safe-by-default. The user's spec said "ambiente != production" for
	// the insecure dev-flag allow-list — we honour that literally here.
	env := c.Environment
	if env == "" {
		env = "production"
	}
	if env != "dev" && env != "staging" && env != "production" {
		errs = append(errs, fmt.Sprintf(
			"invalid environment: %q (valid: dev, staging, production)", c.Environment))
	}

	hasCert := tlsCfg.CertFile != ""
	hasKey := tlsCfg.KeyFile != ""
	hasCA := tlsCfg.CAFile != ""
	hasFullTLS := hasCert && hasKey && hasCA

	// Rule: TLS AND insecure cannot both be active.
	if hasFullTLS && tlsCfg.AllowInsecureDev {
		errs = append(errs, "tls_cert_file/tls_key_file/tls_ca_file and "+
			"allow_insecure_grpc_dev cannot be active simultaneously")
	}
	// Rule: insecure is forbidden in `production` only. Spec: "ambiente != production".
	if tlsCfg.AllowInsecureDev && env == "production" {
		errs = append(errs, fmt.Sprintf(
			"allow_insecure_grpc_dev=true is only valid in non-production environments (got %q); "+
				"never use insecure gRPC in production", env))
	}
	// Rule: partial TLS is rejected (cert only, cert+key no ca, ca only, etc.).
	if (hasCert || hasKey || hasCA) && !hasFullTLS {
		missing := []string{}
		if !hasCert {
			missing = append(missing, "tls_cert_file")
		}
		if !hasKey {
			missing = append(missing, "tls_key_file")
		}
		if !hasCA {
			missing = append(missing, "tls_ca_file")
		}
		errs = append(errs, fmt.Sprintf(
			"partial TLS configuration: provide all three of tls_cert_file/tls_key_file/tls_ca_file. "+
				"Missing: %s", strings.Join(missing, ", ")))
	}
	// Rule: with no TLS at all, must opt into insecure dev mode.
	if !hasFullTLS && !tlsCfg.AllowInsecureDev {
		errs = append(errs, "no TLS configured and insecure dev flag not enabled. "+
			"Either set tls_cert_file+tls_key_file+tls_ca_file, "+
			"or set allow_insecure_grpc_dev=true with environment=dev|staging")
	}
	// Rule: full TLS means cert + key must be present on disk AND the key
	// must actually pair against the cert (the user's spec listed
	// "chiave non compatibile col certificato" as a test case).
	if hasFullTLS {
		if info, err := os.Stat(tlsCfg.CertFile); err != nil {
			if os.IsNotExist(err) {
				errs = append(errs, fmt.Sprintf(
					"tls_cert_file not found at %q", tlsCfg.CertFile))
			} else {
				errs = append(errs, fmt.Sprintf(
					"tls_cert_file inaccessible at %q: %v", tlsCfg.CertFile, err))
			}
		} else if info.IsDir() {
			errs = append(errs, fmt.Sprintf(
				"tls_cert_file %q is a directory, not a PEM file", tlsCfg.CertFile))
		} else if _, err := tls.LoadX509KeyPair(tlsCfg.CertFile, tlsCfg.KeyFile); err != nil {
			errs = append(errs, fmt.Sprintf(
				"tls_cert_file / tls_key_file pair rejected by crypto/tls: %v", err))
		}
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
	return fmt.Sprintf("WorkerConfig{WorkerID: %s, WorkerName: %s, MasterURL: %s, WorkDir: %s, Environment: %s}",
		c.WorkerID, c.WorkerName, c.MasterURL, c.WorkDir, c.Environment)
}
