package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Sub-config loader functions

func loadServerConfig() ServerConfig {
	c := ServerConfig{
		Port:       8000,
		StudioPort: 5000,
	}
	if p := os.Getenv("VELOX_MASTER_PORT"); p != "" {
		if v, _ := strconv.Atoi(p); v > 0 {
			c.Port = v
		}
	}
	if p := os.Getenv("VELOX_STUDIO_PORT"); p != "" {
		if v, _ := strconv.Atoi(p); v >= 0 {
			c.StudioPort = v
		}
	}
	c.TLSCertFile = os.Getenv("VELOX_TLS_CERT_FILE")
	c.TLSKeyFile = os.Getenv("VELOX_TLS_KEY_FILE")
	c.AllowLocalhost = os.Getenv("VELOX_ALLOW_LOCALHOST_MASTER") == "true" ||
		os.Getenv("VELOX_ALLOW_LOCALHOST_MASTER") == "1" ||
		os.Getenv("VELOX_DEV_MODE") == "true"
	return c
}

func loadRuntimeConfig(dataDir string) RuntimeConfig {
	c := RuntimeConfig{
		VideosDir: os.Getenv("VELOX_VIDEOS_DIR"),
		StaticDir: os.Getenv("VELOX_STATIC_DIR"),
	}
	c.RuntimeDir = os.Getenv("VELOX_RUNTIME_DIR")
	c.DataDir = dataDir
	if c.RuntimeDir == "" {
		if c.DataDir != "" {
			c.RuntimeDir = filepath.Dir(c.DataDir)
		} else {
			c.RuntimeDir = ".velox"
		}
	}
	if c.DataDir == "" {
		c.DataDir = filepath.Join(c.RuntimeDir, "data")
	}
	c.JobQueueFile = os.Getenv("VELOX_JOB_QUEUE_FILE")
	c.SecretsDir = os.Getenv("VELOX_SECRETS_DIR")
	if c.SecretsDir == "" {
		c.SecretsDir = filepath.Join(c.RuntimeDir, "secrets")
	}
	return c
}

func loadDatabaseConfig(dataDir, runtimeDir string) DatabaseConfig {
	c := DatabaseConfig{
		Driver:          os.Getenv("VELOX_DB_DRIVER"),
		MaxOpenConns:    50,
		MaxIdleConns:    10,
		ConnMaxLifetime: 1800,
		ConnMaxIdleTime: 300,
	}
	if c.Driver == "" {
		c.Driver = "sqlite3"
	}
	c.DSN = os.Getenv("VELOX_DB_DSN")
	if c.DSN == "" && dataDir != "" {
		c.DSN = dataDir + "/velox.db"
	} else if c.DSN == "" {
		c.DSN = filepath.Join(runtimeDir, "data", "velox.db")
	}
	if n, _ := strconv.Atoi(os.Getenv("VELOX_DB_MAX_OPEN_CONNS")); n > 0 {
		c.MaxOpenConns = n
	}
	if n, _ := strconv.Atoi(os.Getenv("VELOX_DB_MAX_IDLE_CONNS")); n > 0 {
		c.MaxIdleConns = n
	}
	if n, _ := strconv.Atoi(os.Getenv("VELOX_DB_CONN_MAX_LIFETIME")); n > 0 {
		c.ConnMaxLifetime = n
	}
	if n, _ := strconv.Atoi(os.Getenv("VELOX_DB_CONN_MAX_IDLE_TIME")); n > 0 {
		c.ConnMaxIdleTime = n
	}
	return c
}

func loadWorkersConfig() WorkersConfig {
	c := WorkersConfig{
		MaxJobAttempts:   3,
		HeartbeatTimeout: 900,
		VersionNumber:    "v1.0.6",
	}
	c.AllowedWorkers = os.Getenv("VELOX_ALLOWED_WORKERS")
	c.ForceSingleWorker = os.Getenv("VELOX_FORCE_SINGLE_WORKER")
	if n, _ := strconv.Atoi(os.Getenv("VELOX_MAX_JOB_ATTEMPTS")); n > 0 {
		c.MaxJobAttempts = n
	}
	allowReg := os.Getenv("VELOX_ALLOWLIST_ALLOW_REGISTERED")
	c.AllowlistRegistered = allowReg == "1" || allowReg == "true" || allowReg == "yes"
	c.BundleDir = os.Getenv("VELOX_WORKER_BUNDLE_DIR")
	c.CodeVersion = os.Getenv("VELOX_CODE_VERSION")
	c.VersionNumber = os.Getenv("VELOX_VERSION_NUMBER")
	if c.VersionNumber == "" {
		if v, err := os.ReadFile("../VERSION.txt"); err == nil {
			c.VersionNumber = strings.TrimSpace(string(v))
		}
	}
	if c.VersionNumber == "" {
		c.VersionNumber = "v1.0.6"
	}
	if c.CodeVersion == "" {
		c.CodeVersion = c.VersionNumber
	}
	if n, _ := strconv.Atoi(os.Getenv("VELOX_WORKER_HEARTBEAT_TIMEOUT")); n > 0 {
		c.HeartbeatTimeout = n
	}
	c.ScriptDir = os.Getenv("VELOX_SCRIPT_DIR")
	c.MasterURL = os.Getenv("MASTER_PUBLIC_URL")
	if c.MasterURL == "" {
		c.MasterURL = os.Getenv("VELOX_MASTER_URL")
	}
	if c.MasterURL == "" {
		c.MasterURL = os.Getenv("MASTER_URL")
	}
	if ips := os.Getenv("VELOX_ALLOWED_WORKER_IPS"); ips != "" {
		c.AllowedIPs = parseCommaList(ips)
	}
	return c
}

func loadAuthConfig() AuthConfig {
	c := AuthConfig{
		AdminToken: os.Getenv("VELOX_ADMIN_TOKEN"),
	}
	if c.AdminToken == "" {
		c.AdminToken = os.Getenv("MASTER_ADMIN_TOKEN")
	}
	return c
}

func loadStorageConfig() StorageConfig {
	c := StorageConfig{
		Region: "us-east-1",
	}
	c.Endpoint = os.Getenv("VELOX_S3_ENDPOINT")
	if r := os.Getenv("VELOX_S3_REGION"); r != "" {
		c.Region = r
	}
	c.Bucket = os.Getenv("VELOX_S3_BUCKET")
	c.AccessKeyID = os.Getenv("VELOX_S3_ACCESS_KEY_ID")
	c.SecretKey = os.Getenv("VELOX_S3_SECRET_ACCESS_KEY")
	c.UseSSL = os.Getenv("VELOX_S3_USE_SSL") == "true" || os.Getenv("VELOX_S3_USE_SSL") == "1"
	return c
}

func loadDriveConfig(secretsDir, dataDir string) DriveConfig {
	c := DriveConfig{
		ClientID:     os.Getenv("VELOX_DRIVE_CLIENT_ID"),
		ClientSecret: os.Getenv("VELOX_DRIVE_CLIENT_SECRET"),
		RedirectURI:  os.Getenv("VELOX_DRIVE_REDIRECT_URI"),
		TokensDir:    os.Getenv("VELOX_DRIVE_TOKENS_DIR"),
	}
	c.CredentialsDir = os.Getenv("VELOX_DRIVE_CREDENTIALS_DIR")
	if c.TokensDir == "" {
		c.TokensDir = firstExistingDir([]string{
			filepath.Join(secretsDir, "drive", "tokens"),
			filepath.Join(dataDir, "drive", "tokens"),
		})
		if c.TokensDir == "" {
			c.TokensDir = filepath.Join(secretsDir, "drive", "tokens")
		}
	}
	if c.CredentialsDir == "" {
		c.CredentialsDir = firstExistingDir([]string{
			filepath.Join(secretsDir, "drive", "credentials"),
			filepath.Join(dataDir, "drive", "credentials"),
		})
		if c.CredentialsDir == "" {
			c.CredentialsDir = filepath.Join(secretsDir, "drive", "credentials")
		}
	}
	return c
}

func loadYouTubeConfig(secretsDir, dataDir string) YouTubeConfig {
	c := YouTubeConfig{
		APIKey:         os.Getenv("VELOX_YOUTUBE_API_KEY"),
		TokensDir:      os.Getenv("VELOX_YOUTUBE_TOKENS_DIR"),
		PostingPath:    os.Getenv("VELOX_YOUTUBE_POSTING_PATH"),
		RemoteFallback: os.Getenv("VELOX_REMOTE_FALLBACK_URL"),
	}
	c.CredentialsDir = os.Getenv("VELOX_YOUTUBE_CREDENTIALS_DIR")
	if c.TokensDir == "" {
		c.TokensDir = firstExistingDir([]string{
			filepath.Join(secretsDir, "youtube", "tokens"),
			filepath.Join(dataDir, "youtube", "tokens"),
		})
		if c.TokensDir == "" {
			c.TokensDir = filepath.Join(secretsDir, "youtube", "tokens")
		}
	}
	if c.CredentialsDir == "" {
		c.CredentialsDir = firstExistingDir([]string{
			filepath.Join(secretsDir, "youtube", "credentials"),
			filepath.Join(dataDir, "youtube", "credentials"),
		})
		if c.CredentialsDir == "" {
			c.CredentialsDir = filepath.Join(secretsDir, "youtube", "credentials")
		}
	}
	return c
}

func loadAnsibleConfig(dataDir string) AnsibleConfig {
	c := AnsibleConfig{
		PlaybookDir: os.Getenv("VELOX_ANSIBLE_PLAYBOOK_DIR"),
	}
	if c.PlaybookDir == "" {
		c.PlaybookDir = filepath.Join(dataDir, "ansible", "playbooks")
	}
	return c
}

func loadFrontendConfig() FrontendConfig {
	c := FrontendConfig{
		SPADir:          os.Getenv("VELOX_SPA_DIR"),
		GradioAppURL:    os.Getenv("VELOX_GRADIO_APP_URL"),
		DarkEditorDir:   os.Getenv("VELOX_DARK_EDITOR_DIR"),
		DarkEditorProxy: os.Getenv("VELOX_DARK_EDITOR_PROXY_URL"),
	}
	if c.GradioAppURL == "" {
		c.GradioAppURL = "http://127.0.0.1:7860"
	}
	return c
}

func loadRenderConfig() RenderConfig {
	c := RenderConfig{
		RemoteEngineURL:   os.Getenv("VELOX_REMOTE_ENGINE_URL"),
		RemoteEngineToken: os.Getenv("VELOX_REMOTE_ENGINE_TOKEN"),
	}
	c.RemoteEngineTimeoutMS = 60000
	if n, _ := strconv.Atoi(os.Getenv("VELOX_REMOTE_ENGINE_TIMEOUT_MS")); n > 0 {
		c.RemoteEngineTimeoutMS = n
	}
	c.RemoteEngineRetries = 3
	if n, _ := strconv.Atoi(os.Getenv("VELOX_REMOTE_ENGINE_RETRIES")); n > 0 {
		c.RemoteEngineRetries = n
	}
	c.RemoteEnginePollInterval = 30
	if n, _ := strconv.Atoi(os.Getenv("VELOX_REMOTE_ENGINE_POLL_INTERVAL")); n >= 5 {
		c.RemoteEnginePollInterval = n
	}
	return c
}

func loadNVIDIAConfig() NVIDIAConfig {
	return NVIDIAConfig{
		APIKey:  os.Getenv("VELOX_NVIDIA_API_KEY"),
		TextURL: os.Getenv("VELOX_NVIDIA_TEXT_URL"),
	}
}

// FromEnv loads configuration from environment variables.
// Populates both flat fields (for backward compatibility) and sub-configs.
func FromEnv() *Config {
	// First pass: determine data directory for dependent configs
	dataDir := os.Getenv("VELOX_DATA_DIR")
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
	masterServerURL := os.Getenv("VELOX_MASTER_SERVER_URL")
	if masterServerURL == "" {
		masterServerURL = os.Getenv("VELOX_REMOTE_WORKER_URL")
	}

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
		RemoteFallbackURL:     youtube.RemoteFallback,

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

func parseCommaList(s string) []string {
	if s == "" {
		return nil
	}
	parts := make([]string, 0)
	for _, p := range splitByComma(s) {
		if p != "" {
			parts = append(parts, p)
		}
	}
	return parts
}

func firstExistingDir(candidates []string) string {
	for _, path := range candidates {
		if path == "" {
			continue
		}
		info, err := os.Stat(path)
		if err == nil && info.IsDir() {
			return path
		}
	}
	return ""
}

func splitByComma(s string) []string {
	var result []string
	for _, part := range strings.Split(s, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
