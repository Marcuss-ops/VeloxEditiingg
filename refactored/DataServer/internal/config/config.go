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

	// Proxy draft create-master to remote
	masterServerURL := GetMasterServerURL()

	// Build flat Config for backward compatibility
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

		DBDriver:          database.Driver,
		DBDSN:             database.DSN,
		DBMaxOpenConns:    database.MaxOpenConns,
		DBMaxIdleConns:    database.MaxIdleConns,
		DBConnMaxLifetime: database.ConnMaxLifetime,
		DBConnMaxIdleTime: database.ConnMaxIdleTime,

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
		RemoteEnginePollInterval: render.RemoteEnginePollInterval,

		NVIDIAAPIKey:  nvidia.APIKey,
		NVIDIATextURL: nvidia.TextURL,

	return &Config{
		Server:          server,
		Runtime:         runtime,
		Database:        database,
		Workers:         workers,
		Auth:            auth,
		Storage:         storage,
		Drive:           drive,
		YouTube:         youtube,
		Ansible:         ansible,
		Frontend:        frontend,
		Render:          render,
		NVIDIA:          nvidia,
		MasterServerURL: masterServerURL,
		JobMasterURL:    os.Getenv("VELOX_JOB_MASTER_URL"),
	}
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
	return nil
}
