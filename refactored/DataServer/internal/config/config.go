package config

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// FromEnv loads configuration from environment variables.
// Populates both flat fields (for backward compatibility) and sub-configs.
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
	database := loadDatabaseConfig(runtime.DataDir, runtime.RuntimeDir)
	workers := loadWorkersConfig()
	auth := loadAuthConfig()
	storage := loadStorageConfig()
	drive := loadDriveConfig(secretsDir, runtime.DataDir)
	youtube := loadYouTubeConfig(secretsDir, runtime.DataDir)
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

		MasterServerURL: masterServerURL,
		JobMasterURL:    os.Getenv("VELOX_JOB_MASTER_URL"),

		// Sub-configs
		Server:   server,
		Runtime:  runtime,
		Database: database,
		Workers:  workers,
		Auth:     auth,
		Storage:  storage,
		Drive:    drive,
		YouTube:  youtube,
		Ansible:  ansible,
		Frontend: frontend,
		Render:   render,
	}

	// Load Drive OAuth from credentials.json if not explicitly set
	populateDriveOAuthFromCredentials(c)

	return c
}

func populateDriveOAuthFromCredentials(c *Config) {
	if c == nil || (c.DriveClientID != "" && c.DriveClientSecret != "") {
		return
	}
	if c.DriveCredentialsDir == "" {
		return
	}
	credPath := c.DriveCredentialsDir
	if filepath.Base(credPath) != "credentials.json" {
		credPath = filepath.Join(credPath, "credentials.json")
	}
	data, err := os.ReadFile(credPath)
	if err != nil {
		return
	}
	var payload struct {
		Installed struct {
			ClientID     string   `json:"client_id"`
			ClientSecret string   `json:"client_secret"`
			RedirectURIs []string `json:"redirect_uris"`
		} `json:"installed"`
		Web struct {
			ClientID     string   `json:"client_id"`
			ClientSecret string   `json:"client_secret"`
			RedirectURIs []string `json:"redirect_uris"`
		} `json:"web"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return
	}
	src := payload.Installed
	if src.ClientID == "" && src.ClientSecret == "" {
		src = payload.Web
	}
	if c.DriveClientID == "" {
		c.DriveClientID = src.ClientID
	}
	if c.DriveClientSecret == "" {
		c.DriveClientSecret = src.ClientSecret
	}
	if c.DriveRedirectURI == "" && len(src.RedirectURIs) > 0 {
		c.DriveRedirectURI = src.RedirectURIs[0]
	}
}
