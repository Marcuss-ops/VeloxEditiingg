// Package config / config_types.go — shared configuration types, the
// validation sentinel error, and package constants.
//
// Kept separate from load/save, defaults, and validation so each concern
// lives in one file. See config.go for the package overview.
package config

import (
	"errors"
	"time"
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

	// StateDir is the canonical root for ALL mutable worker state:
	// assets_cache, blobs, executor_spool, scratch tmp. Step 6/8
	// replaces the legacy assets_cache bind-mount and the per-subsystem
	// /opt defaults (cache, blobs). When empty at applyDefaults time,
	// falls back to "/var/lib/velox/worker" so systemd-toggled
	// deployments do not silently regress to the legacy /opt layout.
	// Operators override via VELOX_STATE_DIR.
	StateDir string `json:"state_dir,omitempty"`

	// Worker policy
	MaxActiveJobs  int `json:"max_active_jobs"`           // Maximum concurrent active jobs (default: 1)
	PrometheusPort int `json:"prometheus_port,omitempty"` // Prometheus metrics port (default: 9090; set via VELOX_PROMETHEUS_PORT=0 to disable)
	HealthPort     int `json:"health_port"`               // Health HTTP port (default: 8081, 0=disabled)

	// AssetDownloadConcurrency caps the number of simultaneous asset byte
	// transfers the canonical download manager runs per worker. Binds from
	// VELOX_ASSET_DOWNLOAD_CONCURRENCY; default 4.
	AssetDownloadConcurrency int `json:"asset_download_concurrency,omitempty"`

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

	// WorkerProfile selects the runtime profile for this worker.
	// "creator" disables the C++ video pipeline and scene.composite.v1
	// executor; any other value (or empty) keeps the historical video-worker
	// behaviour. Binds from VELOX_WORKER_PROFILE env.
	WorkerProfile string `json:"worker_profile,omitempty"`

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

// CreatorProfile is the canonical value for WorkerProfile that disables
// the C++ video pipeline and enables creative capabilities. It is the
// single source of truth for the creator profile name.
const CreatorProfile = "creator"

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
