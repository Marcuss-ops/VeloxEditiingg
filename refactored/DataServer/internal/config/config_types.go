package config

// ServerConfig holds HTTP and gRPC server settings.
type ServerConfig struct {
	Port           int
	StudioPort     int
	GRPCPort        int    // gRPC port for worker control stream (0 = disabled)
	GRPCShadowMode  bool   // Phase 4: notify workers about available jobs, still claim via HTTP
	GRPCPushMode    bool   // Phase 5+: send JobOffer directly, workers respond JobAccepted
	TLSCertFile    string
	TLSKeyFile     string
	GRPCTLSCertFile string // gRPC server certificate (PEM). Required when GRPCPort > 0 with mTLS.
	GRPCTLSKeyFile  string // gRPC server private key (PEM)
	GRPCTLSCAFile   string // CA cert for verifying client certificates (mTLS). Empty = no client auth.
	AllowLocalhost bool
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
}

// DatabaseConfig holds database connection settings.
type DatabaseConfig struct {
	DBPath string // Absolute path to SQLite database file (required)
}

// WorkersConfig holds worker management settings.
type WorkersConfig struct {
	AllowedWorkers      string
	ForceSingleWorker   string
	AllowlistRegistered bool
	MaxJobAttempts      int
	BundleDir           string
	HeartbeatTimeout    int
	CodeVersion         string
	VersionNumber       string
	ScriptDir           string
	// MasterURL is the publicly-advertised master URL (workers download bundles through it).
	// Populated from the MASTER_PUBLIC_URL > VELOX_MASTER_URL > MASTER_URL chain.
	MasterURL string
	// MasterServerURL is the server-facing master URL used for upstream proxying
	// (e.g. draft forwarding to a sibling master). Populated from
	// VELOX_MASTER_SERVER_URL > VELOX_REMOTE_WORKER_URL. Previously lived at the root
	// of Config as `MasterServerURL`; see Config.LegacyMasterServerURL for the
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
	RemoteFallback string
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
	// Sub-configs only — no flat field aliases.
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

	// MasterServerURL and JobMasterURL have been moved into sub-configs (Workers and
	// Pipeline respectively, per spec §8). Use the accessors below until all
	// callers have been migrated.
}
