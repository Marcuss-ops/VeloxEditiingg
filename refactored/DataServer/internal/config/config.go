package config

import (
	"fmt"
	"log"
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

func loadDatabaseConfig() DatabaseConfig {
	raw := os.Getenv("VELOX_DB_PATH")
	if raw == "" {
		return DatabaseConfig{}
	}
	resolved := raw
	if filepath.IsAbs(raw) {
		if r, err := filepath.EvalSymlinks(raw); err == nil {
			log.Printf("config: database path resolved: %s -> %s", raw, r)
			resolved = r
		} else {
			log.Printf("config: cannot resolve symlinks for %s: %v (using original path)", raw, err)
		}
	}
	return DatabaseConfig{DBPath: resolved}
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

func loadDriveConfig(secretsDir string) DriveConfig {
	c := DriveConfig{
		ClientID:     os.Getenv("VELOX_DRIVE_CLIENT_ID"),
		ClientSecret: os.Getenv("VELOX_DRIVE_CLIENT_SECRET"),
		RedirectURI:  os.Getenv("VELOX_DRIVE_REDIRECT_URI"),
		TokensDir:    os.Getenv("VELOX_DRIVE_TOKENS_DIR"),
	}
	c.CredentialsDir = os.Getenv("VELOX_DRIVE_CREDENTIALS_DIR")
	if c.TokensDir == "" {
		c.TokensDir = filepath.Join(secretsDir, "drive", "tokens")
	}
	if c.CredentialsDir == "" {
		c.CredentialsDir = filepath.Join(secretsDir, "drive", "credentials")
	}
	return c
}

func loadYouTubeConfig(secretsDir string) YouTubeConfig {
	c := YouTubeConfig{
		APIKey:         os.Getenv("VELOX_YOUTUBE_API_KEY"),
		TokensDir:      os.Getenv("VELOX_YOUTUBE_TOKENS_DIR"),
		PostingPath:    os.Getenv("VELOX_YOUTUBE_POSTING_PATH"),
		RemoteFallback: os.Getenv("VELOX_REMOTE_FALLBACK_URL"),
	}
	c.CredentialsDir = os.Getenv("VELOX_YOUTUBE_CREDENTIALS_DIR")
	if c.TokensDir == "" {
		c.TokensDir = filepath.Join(secretsDir, "youtube", "tokens")
	}
	if c.CredentialsDir == "" {
		c.CredentialsDir = filepath.Join(secretsDir, "youtube", "credentials")
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
	drive := loadDriveConfig(secretsDir)
	youtube := loadYouTubeConfig(secretsDir)
	ansible := loadAnsibleConfig(runtime.DataDir)
	frontend := loadFrontendConfig()
	render := loadRenderConfig()
	nvidia := loadNVIDIAConfig()

	// Derived fields
	masterServerURL := os.Getenv("VELOX_MASTER_SERVER_URL")
	if masterServerURL == "" {
		masterServerURL = os.Getenv("VELOX_REMOTE_WORKER_URL")
	}

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
