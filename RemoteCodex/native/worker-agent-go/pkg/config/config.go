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
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

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

	// MinDiskFreeMB is the disk-free floor the worker reports to
	// /health/ready as `disk.critical` (RW-PROD-004 §3 reason
	// taxonomy). The disk watcher (composition root) translates this
	// to bytes and updates ReadyState.SetDiskState every 15s. Default
	// 256 MiB matches the production scratch-pad envelope for the
	// scene.composite.v1 pipeline (the bootstrap output dir typically
	// sees a few hundred MiB of working-set at peak). Operators can
	// raise it for richer masters / lower for tighter scratch disks.
	MinDiskFreeMB int `json:"min_disk_free_mb,omitempty"` // Floor in MiB (default: 256)

	// OutputDir is the directory where the C++ engine writes rendered frames.
	// Defaults to /tmp/velox/scene-composite (the composition root default).
	// RW-PROD-002 §3 A4: validated by pkg/doctor for mkdir+write+remove.
	OutputDir string `json:"output_dir,omitempty"`

	// TempDir is the scratch directory for intermediate artifacts during
	// video pipeline execution. Defaults to os.TempDir()/velox-worker.
	// RW-PROD-002 §3 A4: validated by pkg/doctor for mkdir+write+remove.
	TempDir string `json:"temp_dir,omitempty"`

	// WorkerClass is the operator-assigned fleet class (cpu-xlarge, gpu-a100,
	// mixed, io, ...). Binds from VELOX_WORKER_CLASS env. Surfaces in Hello
	// metadata → master WorkerInfo.Class → GET /api/v1/workers?class= filter.
	// RW-PROD-005 §3 A9.
	WorkerClass string `json:"worker_class,omitempty"`

	// RolloutGroup is the operator-assigned rollout cohort (v3.4, canary,
	// holdout, ...). Binds from VELOX_ROLLOUT_GROUP env. Surfaces in Hello
	// metadata → master WorkerInfo.RolloutGroup → GET /api/v1/workers?rollout_group= filter.
	// RW-PROD-005 §3 A9.
	RolloutGroup string `json:"rollout_group,omitempty"`

	// VideoEngineCppBin is the path to the native C++ video-render binary.
	// Defaults to "velox-render-cpp" (resolved via exec.LookPath).
	// Operators override via VELOX_VIDEO_ENGINE_CPP_BIN env in main.go.
	// RW-PROD-002 §3 A5: validated by pkg/doctor for existence + X_OK.
	VideoEngineCppBin string `json:"video_engine_cpp_bin,omitempty"`

	// ReadyzEndpoint overrides the /health/ready mount path
	// (default: /health/ready). Read by cmd/velox-worker-agent/main.go
	// to wire the systemd-side reference (RW-PROD-004 §3 A9) — a
	// Kubernetes podspec that wants /readyz works without changing
	// the canonical mount. NEVER set this from main.go to anything
	// that would conflict with the legacy /health endpoint family.
	ReadyzEndpoint string `json:"readyz_endpoint,omitempty"`

	// Warnings is populated by Validate() with non-fatal findings that should
	// be surfaced to operators but do NOT block startup. The primary use is
	// "weak_permissions_warn" on the TLS private key in non-production
	// environments (RW-PROD-001 A2). Keep this field internal — it never
	// participates in JSON serialization (tag: "-") so operators do not
	// accidentally bake warnings into committed configs.
	Warnings []string `json:"-"`
}

// ErrInvalidConfig is returned when configuration validation fails.
var ErrInvalidConfig = errors.New("invalid configuration")

// minCertValidity is the floor for ResidualValidity on a TLS leaf cert
// (RW-PROD-001 A1). Anything under this triggers a hard reject so a worker
// cannot connect with a cert that will expire during a typical task.
//
// Spec reference: docs/rw-prod/RW-PROD-001.md §2 (A1) — production pause
// window: 14 days. Bumping this up to e.g. 30 days is allowed for
// stakeholders who prefer a wider safety margin, but the runbook says
// 14 days matches the production PKI rotation cadence
// (cert TTL = 14 days, see scripts/gen-production-pki.sh `worker` cmd).
const minCertValidity = 14 * 24 * time.Hour

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
	// RW-PROD-004: default disk-free floor in MiB. The disk watcher
	// (composition root) downsamples to bytes for ReadyState.SetDiskState.
	// 256 MiB matches the bootstrap output smoke-test envelope.
	if c.MinDiskFreeMB <= 0 {
		c.MinDiskFreeMB = 256
	}
	// Environment safe-by-default. The actual env-var overlay
	// (`VELOX_ENV`) runs after this in `applyEnvOverrides`; both layers
	// land on "production" if the operator never declares anything.
	if c.Environment == "" {
		c.Environment = "production"
	}
	// RW-PROD-002: output & scratch directories for the C++ engine.
	// Main.go injects VELOX_VIDEO_ENGINE_CPP_BIN as a CLI override
	// later; these defaults keep the doctor functional even when
	// the operator has not set the env var.
	if c.OutputDir == "" {
		c.OutputDir = "/tmp/velox/scene-composite"
	}
	if c.TempDir == "" {
		c.TempDir = os.TempDir() + "/velox-worker"
	}
	if c.VideoEngineCppBin == "" {
		c.VideoEngineCppBin = "velox-render-cpp"
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

	// Reset any previous validation warnings (re-Validate() should not
	// accumulate duplicate findings).
	c.Warnings = nil

	if c.MasterURL == "" {
		errs = append(errs, "master_url is required")
	}

	// RW-PROD-001 A4: canonicalize the worker_id and enforce strict shape.
	// Run AFTER the empty-check so a missing worker_id produces the more
	// helpful "worker_id is required" error rather than a cryptic regex miss.
	c.WorkerID = NormalizeWorkerID(c.WorkerID)
	if c.WorkerID == "" {
		errs = append(errs, "worker_id is required")
	} else if !identity.IsValidWorkerID(c.WorkerID) {
		// Caller (cmd/velox-worker-agent/main.go) logs via
		// logger.LogCertRejected once validation completes; here we record
		// both as an error (production-stop) AND carry the structured
		// reason for downstream emission.
		errs = append(errs, fmt.Sprintf("invalid worker_id shape: %q (RW-PROD-001 A4 enforces ^[a-z][a-z0-9-]{2,62}$)", c.WorkerID))
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
	//
	// RW-PROD-001 additions layered on top of the existing pair-incompat
	// guard, in this exact order:
	//  1. stat the key for permissions (A2) — production hard-fails,
	//     dev/staging records a non-fatal Warning.
	//  2. stat the cert (existence + is-not-directory).
	//  3. LoadX509KeyPair for cert/key compatibility.
	//  4. parse the leaf certificate and reject if (a) expired OR
	//     (b) expires in less than minCertValidity (14d).
	if hasFullTLS {
		// -------- A2: key file permissions enforcement --------
		if runtime.GOOS != "windows" {
			if keyInfo, err := os.Stat(tlsCfg.KeyFile); err == nil {
				// POSIX mode bitmask & 0o077 isolates "group" + "other"
				// rwx bits — anything non-zero is world-or-group readable,
				// which is unsafe for a private key.
				perm := keyInfo.Mode().Perm()
				if perm&0o077 != 0 {
					if env == "production" {
						errs = append(errs, fmt.Sprintf(
							"tls_key_file %q has insecure permissions %04o (must be 0600); "+
								"RW-PROD-001 A2 fail-closed in production",
							tlsCfg.KeyFile, perm))
					} else {
						// Non-production: record a non-fatal Warning so the
						// caller can log via logger.LogCertRejected once
						// validation completes. DO NOT add to errs — we want
						// the worker to start so local development isn't
						// blocked by chmod.
						c.Warnings = append(c.Warnings, fmt.Sprintf(
							"weak_permissions_warn: tls_key_file %q has %04o, must be 0600 (RW-PROD-001 A2)",
							tlsCfg.KeyFile, perm))
					}
				}
			}
			// Stat failures on the key (NotExist, etc.) are reported later
			// by the LoadX509KeyPair guard below — a duplicate check here
			// would mask the more descriptive error.
		}

		// -------- existence + directory check on cert file --------
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
		} else if cert, err := tls.LoadX509KeyPair(tlsCfg.CertFile, tlsCfg.KeyFile); err != nil {
			// -------- A1/cert-key compat (existing guard) --------
			errs = append(errs, fmt.Sprintf(
				"tls_cert_file / tls_key_file pair rejected by crypto/tls: %v", err))
		} else if leaf, perr := x509.ParseCertificate(cert.Certificate[0]); perr != nil {
			// -------- A1: parse leaf cert --------
			errs = append(errs, fmt.Sprintf(
				"tls_cert_file could not be parsed as x509 leaf: %v", perr))
		} else {
			// -------- A1: expiry window check (14-day floor) --------
			now := time.Now().UTC()
			switch {
			case now.After(leaf.NotAfter):
				errs = append(errs, fmt.Sprintf(
					"certificate has expired: not_after=%s now=%s (RW-PROD-001 A1)",
					leaf.NotAfter.UTC().Format(time.RFC3339), now.Format(time.RFC3339)))
			case leaf.NotAfter.Sub(now) < minCertValidity:
				errs = append(errs, fmt.Sprintf(
					"certificate expires too soon: not_after=%s remaining=%s floor=%s (RW-PROD-001 A1)",
					leaf.NotAfter.UTC().Format(time.RFC3339),
					leaf.NotAfter.Sub(now).Round(time.Second),
					minCertValidity))
			}
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
