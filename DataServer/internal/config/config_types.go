package config

import "time"

// ServerConfig holds HTTP and gRPC server settings.
type ServerConfig struct {
	Port            int
	StudioPort      int
	GRPCPort        int  // gRPC port for worker control stream (0 = disabled)
	GRPCPushMode    bool // Phase 5+: send JobOffer directly, workers respond JobAccepted (requires GRPCPort > 0)
	TLSCertFile     string
	TLSKeyFile      string
	GRPCTLSCertFile string // gRPC server certificate (PEM). Required when GRPCPort > 0 with mTLS.
	GRPCTLSKeyFile  string // gRPC server private key (PEM)
	GRPCTLSCAFile   string // CA cert for verifying client certificates (mTLS). Empty = no client auth.
	AllowLocalhost  bool

	// GinMode mirrors GIN_MODE ("debug" | "release" | "test").
	// Used by newRouter to set gin.SetMode() and by Validate() for
	// the production safety gates (NopBlobStore ban).
	GinMode string
}

// RuntimeConfig holds filesystem and data directory settings.
type RuntimeConfig struct {
	DataDir      string
	RuntimeDir   string
	VideosDir    string
	StaticDir    string
	JobQueueFile string
	SecretsDir   string
	StagingDir   string // Staging directory for artifact uploads (before verification)
	StorageDir   string // Final storage directory for verified artifacts

	// MaxVoiceoverBytes caps the total voiceover asset store size.
	// Default 256 MiB; configured via VELOX_MAX_VOICEOVER_BYTES.
	MaxVoiceoverBytes int64

	// Environment mirrors VELOX_ENVIRONMENT ("development" | "staging" |
	// "production" | "prod" | ""). Used by Validate() for production
	// safety gates (NopBlobStore ban).
	Environment string

	// AllowNopBlobStoreDev enables the no-op blob store for local
	// development ONLY.  The Validate() method bans this flag when
	// GIN_MODE=release or Environment is production/prod.
	// Configured via VELOX_ALLOW_NOP_BLOB_STORE_DEV=true.
	AllowNopBlobStoreDev bool

	// AllowLoopbackAdminAuthDev permits loopback requests to bypass the
	// admin bearer token in local development only. It is rejected for
	// production/staging deployments by Validate().
	AllowLoopbackAdminAuthDev bool

	// GRPCAllowInsecureDev enables insecure gRPC connections (no TLS)
	// for local development ONLY. Configured via
	// VELOX_GRPC_ALLOW_INSECURE_DEV=true.
	GRPCAllowInsecureDev bool


	// ReleaseChannel mirrors VELOX_RELEASE_CHANNEL
	// ("dev" | "staging" | "production"). PR-5 P0: when != "dev",
	// GRPCAllowInsecureDev=true is treated as a fatal misconfiguration
	// by bootstrap.go (Cmd/server/bootstrap.go: runServer fail-fast).
	// Default: "dev" for backward compatibility with installs that
	// pre-date PR-5.
	ReleaseChannel string

	// CommitHMACKey mirrors VELOX_COMMIT_HMAC_KEY (P0 #6, Verdetto
	// Blocco 2). Hex-encoded bytes used as the HMAC-SHA256 key for the
	// deterministic commit-token derivation in completion.Coordinator.
	// Must be at least 32 raw bytes (64 hex chars) for HMAC-SHA256 to
	// operate with its nominal entropy; production deployments MUST
	// set this to a random secret. Validate() fail-closes on
	// Environment == production with a missing or short key.
	CommitHMACKey string
}

// DatabaseConfig holds database connection settings for the
// platform/database abstraction:
//   - DBPath is the absolute path to the SQLite database file.
//     Required when Driver == "sqlite" (or empty, which defaults to sqlite).
//   - Driver selects the SQL backend. "sqlite" or "postgres" are the
//     only valid values; empty falls back to "sqlite" for backward compat.
//   - URL is the Postgres DSN. Required when Driver == "postgres".
//   - MaxOpenConns / MaxIdleConns / ConnMaxLifetime are pool knobs.
//     Zero means "use platform/database.Open() default" — see
//     internal/platform/database/database.go for the per-driver values.
//   - MigrateOnStart controls whether the bootstrap path runs the
//     embedded schema migrations at boot. Defaults to true (legacy
//     behaviour) so existing deployments boot with the master-owned
//     schema bootstrap they always had; operators running an external
//     migration tool (Atlas / goose / sql-migrate / Ansible-deployed
//     schema) opt out by setting VELOX_DB_MIGRATE_ON_START=false (or
//     "0" / "off" / "no") so the master skips both the migrations
//     runner AND the post-migration ensure-column adjustments. The
//     opt-out path is orthogonal to the driver dispatch in
//     cmd/server/bootstrap.go so a single forward-only deployment
//     works the same way regardless of which SQL backend is selected.
type DatabaseConfig struct {
	DBPath          string        // SQLite file path (required when Driver=sqlite)
	Driver          string        // "sqlite" | "postgres" | "" (defaults to sqlite)
	URL             string        // Postgres DSN (required when Driver=postgres)
	MaxOpenConns    int           // 0 → driver default
	MaxIdleConns    int           // 0 → driver default
	ConnMaxLifetime time.Duration // 0 → driver default
	MigrateOnStart  bool          // defaults true; false = forward-only tool mode
}

// WorkersConfig holds worker management settings.
type WorkersConfig struct {
	// AllowedWorkers is the raw VELOX_ALLOWED_WORKERS CSV string,
	// kept for compatibility with the legacy AllowlistAuthorizer.
	AllowedWorkers string
	// AllowedWorkerIDs is the parsed, deduped-against-empty slice
	// of worker IDs the master admits. This is the canonical input
	// to ValidateProductionWorkers — the raw CSV is only kept so we
	// can echo it back in the gRPC HandlerConfig unchanged.
	AllowedWorkerIDs []string

	MaxJobAttempts   int
	BundleDir        string
	HeartbeatTimeout int
	CodeVersion      string
	VersionNumber    string
	ScriptDir        string
	// MasterURL is the publicly-advertised master URL (workers download bundles through it).
	// Populated from the MASTER_PUBLIC_URL > VELOX_MASTER_URL > MASTER_URL chain.
	MasterURL string
	// MasterServerURL is the server-facing master URL used for upstream proxying
	// (e.g. draft forwarding to a sibling master). Populated from
	// VELOX_MASTER_SERVER_URL > VELOX_REMOTE_WORKER_URL. Previously lived at the root
	// of Config as `MasterServerURL` (formerly exposed as the deprecated
	// deprecation shim.
	MasterServerURL string
	AllowedIPs      []string

	// PlacementPinWorkerID mirrors VELOX_PLACEMENT_PIN_WORKER_ID. When
	// non-empty, the placement matcher emits RejectPlacementPinExcluded
	// for every worker_id != pin so only the pinned worker is eligible.
	// Operator-driven deterministic-pick used by the per-worker cert
	// smoke harness (tests/worker-cert/smoke_one.sh); empty string
	// disables the pin (the matcher then behaves as the pre-pin
	// stateless engine). Bootstrap forwards the value to the Handler
	// via grpcserver.Handler.SetPlacementPin.
	PlacementPinWorkerID string

	// StaleThresholdSeconds is the heartbeat age (in seconds) after
	// which a worker with an active session is considered STALE.
	// PersistWorkerHeartbeat emits WORKER_STALE_DETECTED on the
	// transition into this state. Default 150s (matches the canonical
	// workers.ConnectionStaleThreshold used at the read-side so the
	// persistent mirror and the read-time derivation stay aligned).
	StaleThresholdSeconds int

	// PartitionThresholdSeconds is the heartbeat age (in seconds)
	// after which a worker is considered partitioned (no longer
	// reachable through any heartbeat path). PersistWorkerHeartbeat
	// emits WORKER_PARTITION_DETECTED on the transition into this
	// state and WORKER_PARTITION_RESOLVED on the way back to
	// CONNECTED. The reconciler (ReconcileWorkerPartitions, callable
	// from the master cron loop) also flips workers into PARTITIONED
	// when their heartbeat stream has stopped entirely.
	// Default 300s.
	PartitionThresholdSeconds int
}

// RetentionConfig groups the configurable retention windows for the
// auxiliary tables the heartbeat path writes to.
//
//   - WorkerMetricsDays: how long worker_metric_samples rows are kept
//     before the prune pass deletes them. Default 7d.
//   - WorkerEventsDays: how long worker_events rows are kept. Default
//     30d. The TASKS_RUNTIME_DISAPPEARED / WORKER_STATE_CHANGED /
//     WORKER_STALE_DETECTED / WORKER_PARTITION_DETECTED /
//     WORKER_PARTITION_RESOLVED audit trail is bounded by this window.
//
// Setting either field to <= 0 disables the corresponding prune pass
// (interpreted as "retention forever"), which is the canonical opt-out
// for the rare audit-only deployment that prefers unbounded retention.
// The default loads via intFromEnv(... , 0, 1), so the operator can
// opt out with VELOX_RETENTION_WORKER_EVENTS_DAYS=0 (or any non-positive
// integer) without writing Go code.
type RetentionConfig struct {
	WorkerMetricsDays int
	WorkerEventsDays  int

	// WorkerResourceRawDays controls raw worker_resource_samples retention.
	// Default 90 days; <= 0 disables pruning.
	WorkerResourceRawDays int
	// WorkerResourceRollupDays controls hourly rollup retention.
	// Default 365 days; <= 0 disables pruning.
	WorkerResourceRollupDays int
}

// PipelineConfig groups configuration that controls the production-pipeline
// integration (Drive proxy target, job-to-master routing, etc.).
type PipelineConfig struct {
	// JobMasterURL is the destination for proxying /api/drive/* and other job-routed
	// requests. Populated from VELOX_JOB_MASTER_URL. Previously lived at the root
	// of Config as `JobMasterURL`.
	JobMasterURL string
	// OllamaURL and OllamaModel configure the native per-scene translation stage.
	OllamaURL   string
	OllamaModel string
}

// AuthConfig holds authentication settings.
type AuthConfig struct {
	AdminToken string

	// InstaeditControlJWTSecret is the HS256 shared secret used to
	// verify the short-lived JWT issued by the InstaEdit BFF when
	// proxying user-facing requests to the Velox master. Distinct
	// from SOCIAL_API_TOKEN (which authenticates the reverse
	// Velox→InstaEdit direction). MUST be at least 32 bytes; the
	// instaeditauth.New constructor enforces this at boot. Loaded
	// from INSTAEDIT_CONTROL_JWT_SECRET.
	InstaeditControlJWTSecret string
	VeloxWebhookSecret        string
}

// M2MConfig holds the runtime knobs controlling the
// machine-to-machine auth surface on POST /api/v1/jobs. The
// per-client overrides live on `m2m_api_keys.rate_limit_rps` etc. —
// values here apply when the per-client override is 0. Keeping the
// two surfaces separated means operators can adjust defaults without
// touching the DB, and per-client tiering (paid tier / enterprise)
// can override the defaults for specific clients without changing
// every other client.
type M2MConfig struct {
	// DefaultRPS is the sustained token-bucket refill rate for
	// per-client M2M requests. The bucket's CAPACITY is
	// DefaultBurst, which determines how many requests a client
	// may send in a single burst before throttling kicks in.
	DefaultRPS int

	// DefaultBurst is the token-bucket capacity. Setting it to
	// rps*2 gives ~1s of headroom for legitimate scripted bursts;
	// setting it too high lets a single client drain the DB.
	DefaultBurst int

	// MaxScenesPerRequest caps the total scene count per accepted
	// POST. Independent of the existing MaxScenes cap on a single
	// SubmitJobRequest; the per-request quota is the additional
	// ceiling enforced at the M2M layer (away from the validator
	// surface so a single request body of 10k scenes doesn't sneak
	// past the per-request quota).
	MaxScenesPerRequest int

	// MaxTotalDurationSecondsPerRequest caps the summed
	// SubmitScene.DurationSeconds at request time. The rationale
	// mirrors MaxScenesPerRequest: a legitimate video rarely
	// exceeds an hour; anything longer is almost certainly
	// misconfigured / abusive.
	MaxTotalDurationSecondsPerRequest float64
}

// StorageConfig holds S3/MinIO/R2 settings.
type StorageConfig struct {
	Endpoint    string
	Region      string
	Bucket      string
	AccessKeyID string
	SecretKey   string
	UseSSL      bool
}

// DriveConfig holds Google Drive integration settings.
type DriveConfig struct {
	ClientID       string
	ClientSecret   string
	RedirectURI    string
	TokensDir      string
	CredentialsDir string
}

// AnsibleConfig holds Ansible deployment settings.
type AnsibleConfig struct {
	PlaybookDir string
}

// RenderConfig holds remote rendering engine settings.
type RenderConfig struct {
	RemoteEngineURL          string
	RemoteEngineToken        string
	RemoteEngineTimeoutMS    int
	RemoteEngineRetries      int
	RemoteEnginePollInterval int
}

// Config is the top-level configuration.
type Config struct {
	// Sub-configs (single source of truth for all settings)
	Server    ServerConfig
	Runtime   RuntimeConfig
	Database  DatabaseConfig
	Workers   WorkersConfig
	Retention RetentionConfig
	Auth      AuthConfig
	M2M       M2MConfig
	Storage   StorageConfig
	Drive     DriveConfig
	Ansible   AnsibleConfig
	Render    RenderConfig
	Pipeline  PipelineConfig

	// AllowedExternalDomains is the explicit allowlist applied to
	// outgoing URLs submitted via POST /api/v1/jobs
	// (voiceover_paths, scenes.clip_link, scenes.image_link). When
	// empty, only the SSRF blocklist is enforced (private /
	// loopback / metadata IPs are denied; everything else is
	// allowed). When non-empty, an entry is the precise hostname
	// suffix or a `*.foo.com` wildcard. See
	// internal/handlers/server/pipeline/ssrf_url.go for the full
	// policy.
	AllowedExternalDomains []string
}
