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
	// Configured via VELOX_ALLOW_NOP_BLOBSTORE_DEV=true.
	AllowNopBlobStoreDev bool

	// GRPCAllowInsecureDev enables insecure gRPC connections (no TLS)
	// for local development ONLY. Configured via
	// VELOX_GRPC_ALLOW_INSECURE_DEV=true.
	GRPCAllowInsecureDev bool
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
}

// PipelineConfig groups configuration that controls the production-pipeline
// integration (Drive proxy target, job-to-master routing, etc.).
type PipelineConfig struct {
	// JobMasterURL is the destination for proxying /api/drive/* and other job-routed
	// requests. Populated from VELOX_JOB_MASTER_URL. Previously lived at the root
	// of Config as `JobMasterURL`.
	JobMasterURL string
}

// AuthConfig holds authentication settings.
type AuthConfig struct {
	AdminToken string
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

// YouTubeConfig holds YouTube integration settings.
type YouTubeConfig struct {
	APIKey         string
	TokensDir      string
	PostingPath    string
	CredentialsDir string
}

// AnsibleConfig holds Ansible deployment settings.
type AnsibleConfig struct {
	PlaybookDir string
}

// FrontendConfig holds SPA and frontend settings.
type FrontendConfig struct {
	SPADir          string
	GradioAppURL    string
	DarkEditorDir   string
	DarkEditorProxy string
}

// RenderConfig holds remote rendering engine settings.
type RenderConfig struct {
	RemoteEngineURL          string
	RemoteEngineToken        string
	RemoteEngineTimeoutMS    int
	RemoteEngineRetries      int
	RemoteEnginePollInterval int
}

// NVIDIAConfig holds NVIDIA AI settings.
type NVIDIAConfig struct {
	APIKey  string
	TextURL string
}

// Config is the top-level configuration.
type Config struct {
	// Sub-configs (single source of truth for all settings)
	Server   ServerConfig
	Runtime  RuntimeConfig
	Database DatabaseConfig
	Workers  WorkersConfig
	Auth     AuthConfig
	Storage  StorageConfig
	Drive    DriveConfig
	YouTube  YouTubeConfig
	Ansible  AnsibleConfig
	Frontend FrontendConfig
	Render   RenderConfig
	NVIDIA   NVIDIAConfig
	Pipeline PipelineConfig
}
