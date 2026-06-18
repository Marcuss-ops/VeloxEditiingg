package config

import (
	"fmt"
	"os"
	"path/filepath"
)

// FromEnv loads configuration from environment variables.
// Only sub-configs are populated — no flat field aliases.
func FromEnv() *Config {
	// First pass: determine data directory for dependent configs
	dataDir := GetDataDir()
	runtimeDir := os.Getenv("VELOX_RUNTIME_DIR")
	if runtimeDir == "" {
		if dataDir != "" {
			runtimeDir = filepath.Dir(dataDir)
		} else {
			runtimeDir = ".velox"
		}
	}
	if dataDir == "" {
		dataDir = filepath.Join(runtimeDir, "data")
	}
	secretsDir := os.Getenv("VELOX_SECRETS_DIR")
	if secretsDir == "" {
		secretsDir = filepath.Join(runtimeDir, "secrets")
	}

	// Load sub-configs
	server := loadServerConfig()
	runtime := loadRuntimeConfig(dataDir)
	database := loadDatabaseConfig()
	workers := loadWorkersConfig()
	auth := loadAuthConfig()
	storage := loadStorageConfig()
	drive := loadDriveConfig(secretsDir, dataDir)
	youtube := loadYouTubeConfig(secretsDir, dataDir)
	ansible := loadAnsibleConfig(runtime.DataDir)
	frontend := loadFrontendConfig()
	render := loadRenderConfig()
	nvidia := loadNVIDIAConfig()

	// Note: legacy `masterServerURL := GetMasterServerURL()` removed because
	// the value flows through the Workers sub-config (Workers.MasterServerURL
	// → config_workers.go) and via the flat alias below. Declaring it locally
	// here triggered a Go "declared and not used" error.

	// Build flat Config for backward compatibility. The legacy flat DB
	// (DBDriver/DBDSN/DBMax*) aliases are intentionally dropped from this
	// initializer because DatabaseConfig only exposes DBPath after the
	// SQLite-only S6 cleanup; callers needing legacy DB pool tuning should
	// migrate to the Database sub-config (c.Database).
	c := &Config{
		MasterPort:           server.Port,
		StudioPort:           server.StudioPort,
		TLSCertFile:          server.TLSCertFile,
		TLSKeyFile:           server.TLSKeyFile,
		AllowLocalhostMaster: server.AllowLocalhost,

		DataDir:      runtime.DataDir,
		RuntimeDir:   runtime.RuntimeDir,
		VideosDir:    runtime.VideosDir,
		StaticDir:    runtime.StaticDir,
		JobQueueFile: runtime.JobQueueFile,
		SecretsDir:   runtime.SecretsDir,

		// Legacy flat DB pool fields dropped (see comment above). The Database
		// sub-config (c.Database) carries DBPath; pool tuning lives in env
		// loader added by future PR if it becomes necessary again.

		AllowedWorkers:           workers.AllowedWorkers,
		ForceSingleWorker:        workers.ForceSingleWorker,
		AllowlistAllowRegistered: workers.AllowlistRegistered,
		MaxJobAttempts:           workers.MaxJobAttempts,
		WorkerBundleDir:          workers.BundleDir,
		WorkerHeartbeatTimeout:   workers.HeartbeatTimeout,
		CodeVersion:              workers.CodeVersion,
		VersionNumber:            workers.VersionNumber,
		ScriptDir:                workers.ScriptDir,
		MasterURL:                workers.MasterURL,
		AllowedWorkerIPs:         workers.AllowedIPs,

		AdminToken: auth.AdminToken,

		S3Endpoint:        storage.Endpoint,
		S3Region:          storage.Region,
		S3Bucket:          storage.Bucket,
		S3AccessKeyID:     storage.AccessKeyID,
		S3SecretAccessKey: storage.SecretKey,
		S3UseSSL:          storage.UseSSL,

		DriveClientID:       drive.ClientID,
		DriveClientSecret:   drive.ClientSecret,
		DriveRedirectURI:    drive.RedirectURI,
		DriveTokensDir:      drive.TokensDir,
		DriveCredentialsDir: drive.CredentialsDir,

		YouTubeAPIKey:         youtube.APIKey,
		YouTubeTokensDir:      youtube.TokensDir,
		YouTubePostingPath:    youtube.PostingPath,
		YouTubeCredentialsDir: youtube.CredentialsDir,

		PlaybookDir: ansible.PlaybookDir,

		GradioAppURL:       frontend.GradioAppURL,
		SPADir:             frontend.SPADir,
		DarkEditorDir:      frontend.DarkEditorDir,
		DarkEditorProxyURL: frontend.DarkEditorProxy,

		RemoteEngineURL:          render.RemoteEngineURL,
		RemoteEngineToken:        render.RemoteEngineToken,
		RemoteEngineTimeoutMS:    render.RemoteEngineTimeoutMS,
		RemoteEngineRetries:      render.RemoteEngineRetries,
		RemoteEnginePollInterval: render.RemoteEnginePollInterval,		NVIDIAAPIKey:  nvidia.APIKey,
		NVIDIATextURL: nvidia.TextURL,
	}
	pipeline := loadPipelineConfig()
	c.Server = server
	c.Runtime = runtime
	c.Database = database
	c.Workers = workers
	c.Auth = auth
	c.Storage = storage
	c.Drive = drive
	c.YouTube = youtube
	c.Ansible = ansible
	c.Frontend = frontend
	c.Render = render
	c.NVIDIA = nvidia
	c.Pipeline = pipeline
	return c
}

// Validate checks that required fields are set.
// Returns nil if the configuration is valid, or an error describing the first missing field.
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("config: nil Config")
	}
	if c.Database.DBPath == "" {
		return fmt.Errorf("config: VELOX_DB_PATH is required (absolute path to SQLite database)")
	}
	if !filepath.IsAbs(c.Database.DBPath) {
		return fmt.Errorf("config: VELOX_DB_PATH must be an absolute path, got: %s", c.Database.DBPath)
	}
	// GRPC control-plane fail-fast: if push mode is the primary delivery
	// channel then gRPC must be enabled, otherwise the master accepts HTTP
	// API calls but workers have no way to receive JobOffer/JobLeaseGranted
	// and silently degrade to "no jobs ever picked up".
	if c.Server.GRPCPushMode && c.Server.GRPCPort <= 0 {
		return fmt.Errorf(
			"config: GRPCPushMode=true requires VELOX_GRPC_PORT>0 (got %d). " +
				"Either set VELOX_GRPC_PORT or disable VELOX_GRPC_PUSH_MODE.",
			c.Server.GRPCPort)
	}
	return nil
}
