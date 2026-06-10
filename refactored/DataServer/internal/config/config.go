package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Config struct {
	MasterPort    int
	StudioPort    int
	StaticDir     string
	VideosDir     string
	RedisHost     string
	RedisPort     string
	RedisDB       int
	RedisPassword string
	QueuePrefix   string
	// Data directory for JSON files (jobs, workers, etc.)
	DataDir string
	// Runtime root for local development and on-disk state.
	RuntimeDir string
	// Job queue file path (defaults to DataDir/jobs/job_queue.json)
	JobQueueFile string
	// Allowlist: VELOX_ALLOWED_WORKERS comma-separated IP/worker_id/name, or "*"/"ALL" to allow all
	AllowedWorkers string
	// If set, only this worker_id or IP can get jobs (single-worker mode)
	ForceSingleWorker string
	// If true, worker already in registry is allowed even when not in allowlist
	AllowlistAllowRegistered bool
	// Max job attempts before moving to dead queue
	MaxJobAttempts int
	// Optional: forward draft (no script/voiceover) requests to this URL (e.g. remote generator)
	MasterServerURL string
	// Job Master (FastAPI): API dati analytics, finance, ansible, youtube. Se impostato, /api/* viene inoltrato qui.
	JobMasterURL string
	// Gradio standalone UI (link from SPA; not proxied as default home)
	GradioAppURL string
	// SPA root: directory containing index.html (e.g. frontend_standalone/web/dist). GET / serves SPA with history fallback.
	SPADir string
	// Dark Editor static files directory
	DarkEditorDir string
	// Dark Editor proxy URL (for Next.js on separate port)
	DarkEditorProxyURL string
	// Worker bundle directory for worker downloads
	WorkerBundleDir string
	// Code version for worker updates
	CodeVersion string
	// Version number for display
	VersionNumber string
	// Heartbeat timeout for workers (seconds)
	WorkerHeartbeatTimeout int

	// Enterprise database configuration
	DBDriver string // "postgres" or "sqlite3"
	DBDSN    string // Data Source Name
	// Database connection pooling
	DBMaxOpenConns    int
	DBMaxIdleConns    int
	DBConnMaxLifetime int // seconds
	DBConnMaxIdleTime int // seconds

	// S3/MinIO/R2 storage configuration
	S3Endpoint        string
	S3Region          string
	S3Bucket          string
	S3AccessKeyID     string
	S3SecretAccessKey string
	S3UseSSL          bool

	// Remote Script Engine
	RemoteEngineURL       string
	RemoteEngineToken     string
	RemoteEngineTimeoutMS int
	RemoteEngineRetries   int

	// Drive API configuration
	DriveClientID     string
	DriveClientSecret string
	DriveRedirectURI  string
	DriveTokensDir    string

	// NVIDIA API configuration for AI image generation
	NVIDIAAPIKey  string // NVIDIA API key for FLUX image generation
	NVIDIATextURL string // Optional NVIDIA/OpenAI-compatible chat endpoint for text generation

	// YouTube API configuration
	YouTubeAPIKey      string // Google API key for YouTube Data API v3
	YouTubeTokensDir   string // OAuth2 tokens directory
	YouTubePostingPath string // Root of YoutubePosting project
	RemoteFallbackURL  string // Remote scraper fallback URL (default: http://77.93.152.122:5000)

	// Secrets directory (unified location for all credentials/tokens)
	// Defaults to RuntimeDir/secrets if not set
	SecretsDir            string
	DriveCredentialsDir   string // Drive OAuth client secrets
	YouTubeCredentialsDir string // YouTube OAuth client secrets

	// Install handler configuration (migrated from Python FastAPI)
	ScriptDir        string   // Directory containing install scripts (RemoteCodex)
	MasterURL        string   // Master URL for generated install scripts
	AllowedWorkerIPs []string // IP allowlist for install endpoints
	// Admin bearer token for sensitive operational routes.
	// If empty, those routes are available only from localhost.
	AdminToken string

	// Dev mode: allow localhost master URL for remote workers
	// Set VELOX_ALLOW_LOCALHOST_MASTER=true for local development
	AllowLocalhostMaster bool
}

func FromEnv() *Config {
	c := &Config{
		MasterPort:  8000,
		StudioPort:  5000,
		RedisHost:   "localhost",
		RedisPort:   "6379",
		RedisDB:     0,
		QueuePrefix: "velox",
	}
	if p := os.Getenv("VELOX_MASTER_PORT"); p != "" {
		if v, _ := strconv.Atoi(p); v > 0 {
			c.MasterPort = v
		}
	}
	if p := os.Getenv("VELOX_STUDIO_PORT"); p != "" {
		if v, _ := strconv.Atoi(p); v >= 0 {
			c.StudioPort = v
		}
	}
	if d := os.Getenv("VELOX_STATIC_DIR"); d != "" {
		c.StaticDir = d
	}
	if h := os.Getenv("VELOX_REDIS_HOST"); h != "" {
		c.RedisHost = h
	}
	if p := os.Getenv("VELOX_REDIS_PORT"); p != "" {
		c.RedisPort = p
	}
	if db := os.Getenv("VELOX_REDIS_DB"); db != "" {
		if v, _ := strconv.Atoi(db); v >= 0 {
			c.RedisDB = v
		}
	}
	c.RedisPassword = os.Getenv("VELOX_REDIS_PASSWORD")
	c.AllowedWorkers = os.Getenv("VELOX_ALLOWED_WORKERS")
	c.ForceSingleWorker = os.Getenv("VELOX_FORCE_SINGLE_WORKER")
	c.MaxJobAttempts = 3
	if n, _ := strconv.Atoi(os.Getenv("VELOX_MAX_JOB_ATTEMPTS")); n > 0 {
		c.MaxJobAttempts = n
	}
	allowReg := os.Getenv("VELOX_ALLOWLIST_ALLOW_REGISTERED")
	c.AllowlistAllowRegistered = allowReg == "1" || allowReg == "true" || allowReg == "yes"
	// Proxy draft create-master to remote (same env names as Python)
	for _, key := range []string{"VELOX_MASTER_SERVER_URL", "VELOX_REMOTE_WORKER_URL", "VELOX_REMOTE_SCRIPT_BACKEND", "VELOX_SCRIPT_BACKEND"} {
		if u := os.Getenv(key); u != "" {
			c.MasterServerURL = u
			break
		}
	}
	// NOTE: PythonBackendURL removed - Python backend no longer exists
	c.JobMasterURL = os.Getenv("VELOX_JOB_MASTER_URL") // e.g. http://127.0.0.1:8001
	c.GradioAppURL = os.Getenv("VELOX_GRADIO_APP_URL")
	if c.GradioAppURL == "" {
		c.GradioAppURL = "http://127.0.0.1:7860" // Studio avanzato (link dalla SPA)
	}
	c.SPADir = os.Getenv("VELOX_SPA_DIR")                           // e.g. refactored/frontend_standalone/web/dist
	c.DarkEditorDir = os.Getenv("VELOX_DARK_EDITOR_DIR")            // dark_editor static files
	c.DarkEditorProxyURL = os.Getenv("VELOX_DARK_EDITOR_PROXY_URL") // proxy to Next.js
	c.VideosDir = os.Getenv("VELOX_VIDEOS_DIR")                     // directory for completed videos
	c.RuntimeDir = os.Getenv("VELOX_RUNTIME_DIR")
	// Data directory for JSON files (jobs, workers, ansible, etc.)
	c.DataDir = os.Getenv("VELOX_DATA_DIR")
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
	// Worker bundle directory
	c.WorkerBundleDir = os.Getenv("VELOX_WORKER_BUNDLE_DIR")
	// If empty, NewWorkerUpdateHandler will use DataDir/worker_downloads
	// Code version (computed from git hash or set explicitly)
	c.CodeVersion = os.Getenv("VELOX_CODE_VERSION")
	c.VersionNumber = os.Getenv("VELOX_VERSION_NUMBER")
	if c.VersionNumber == "" {
		c.VersionNumber = "1.0.0"
	}
	// Worker heartbeat timeout (default 15 minutes)
	c.WorkerHeartbeatTimeout = 900
	if n, _ := strconv.Atoi(os.Getenv("VELOX_WORKER_HEARTBEAT_TIMEOUT")); n > 0 {
		c.WorkerHeartbeatTimeout = n
	}

	// Enterprise database configuration
	c.DBDriver = os.Getenv("VELOX_DB_DRIVER")
	if c.DBDriver == "" {
		c.DBDriver = "sqlite3" // default to SQLite
	}
	c.DBDSN = os.Getenv("VELOX_DB_DSN")
	if c.DBDSN == "" && c.DataDir != "" {
		c.DBDSN = c.DataDir + "/velox.db"
	} else if c.DBDSN == "" {
		c.DBDSN = filepath.Join(c.RuntimeDir, "data", "velox.db")
	}
	c.DBMaxOpenConns = 50
	if n, _ := strconv.Atoi(os.Getenv("VELOX_DB_MAX_OPEN_CONNS")); n > 0 {
		c.DBMaxOpenConns = n
	}
	c.DBMaxIdleConns = 10
	if n, _ := strconv.Atoi(os.Getenv("VELOX_DB_MAX_IDLE_CONNS")); n > 0 {
		c.DBMaxIdleConns = n
	}
	c.DBConnMaxLifetime = 1800 // 30 minutes
	if n, _ := strconv.Atoi(os.Getenv("VELOX_DB_CONN_MAX_LIFETIME")); n > 0 {
		c.DBConnMaxLifetime = n
	}
	c.DBConnMaxIdleTime = 300 // 5 minutes
	if n, _ := strconv.Atoi(os.Getenv("VELOX_DB_CONN_MAX_IDLE_TIME")); n > 0 {
		c.DBConnMaxIdleTime = n
	}

	// S3/MinIO/R2 storage configuration
	c.S3Endpoint = os.Getenv("VELOX_S3_ENDPOINT")
	c.S3Region = os.Getenv("VELOX_S3_REGION")
	if c.S3Region == "" {
		c.S3Region = "us-east-1"
	}
	c.S3Bucket = os.Getenv("VELOX_S3_BUCKET")
	c.S3AccessKeyID = os.Getenv("VELOX_S3_ACCESS_KEY_ID")
	c.S3SecretAccessKey = os.Getenv("VELOX_S3_SECRET_ACCESS_KEY")
	c.S3UseSSL = os.Getenv("VELOX_S3_USE_SSL") == "true" || os.Getenv("VELOX_S3_USE_SSL") == "1"

	// Remote Script Engine configuration
	c.RemoteEngineURL = os.Getenv("VELOX_REMOTE_ENGINE_URL")
	c.RemoteEngineToken = os.Getenv("VELOX_REMOTE_ENGINE_TOKEN")
	c.RemoteEngineTimeoutMS = 60000 // default 60s
	if n, _ := strconv.Atoi(os.Getenv("VELOX_REMOTE_ENGINE_TIMEOUT_MS")); n > 0 {
		c.RemoteEngineTimeoutMS = n
	}
	c.RemoteEngineRetries = 3
	if n, _ := strconv.Atoi(os.Getenv("VELOX_REMOTE_ENGINE_RETRIES")); n > 0 {
		c.RemoteEngineRetries = n
	}

	// NOTE: GoOnlyMode and GoOnlyWhitelist removed - Go-only mode is now permanent

	// Drive API configuration
	c.DriveClientID = os.Getenv("VELOX_DRIVE_CLIENT_ID")
	c.DriveClientSecret = os.Getenv("VELOX_DRIVE_CLIENT_SECRET")
	c.DriveRedirectURI = os.Getenv("VELOX_DRIVE_REDIRECT_URI")
	c.DriveTokensDir = os.Getenv("VELOX_DRIVE_TOKENS_DIR")

	// NVIDIA API configuration
	c.NVIDIAAPIKey = os.Getenv("VELOX_NVIDIA_API_KEY")
	c.NVIDIATextURL = os.Getenv("VELOX_NVIDIA_TEXT_URL")

	// YouTube API configuration
	c.YouTubeAPIKey = os.Getenv("VELOX_YOUTUBE_API_KEY")
	c.YouTubeTokensDir = os.Getenv("VELOX_YOUTUBE_TOKENS_DIR")
	c.YouTubePostingPath = os.Getenv("VELOX_YOUTUBE_POSTING_PATH")
	c.RemoteFallbackURL = os.Getenv("VELOX_REMOTE_FALLBACK_URL")
	if c.RemoteFallbackURL == "" {
		c.RemoteFallbackURL = "http://77.93.152.122:5000"
	}

	// Secrets directory configuration
	c.SecretsDir = os.Getenv("VELOX_SECRETS_DIR")
	c.DriveCredentialsDir = os.Getenv("VELOX_DRIVE_CREDENTIALS_DIR")
	c.YouTubeCredentialsDir = os.Getenv("VELOX_YOUTUBE_CREDENTIALS_DIR")
	if c.SecretsDir == "" {
		c.SecretsDir = filepath.Join(c.RuntimeDir, "secrets")
	}

	// Drive tokens: prefer explicit, then existing secrets/data dir, then deterministic fallback
	if c.DriveTokensDir == "" {
		driveTokenCandidates := []string{
			filepath.Join(c.SecretsDir, "drive", "tokens"),
			filepath.Join(c.DataDir, "secrets", "drive", "tokens"),
			filepath.Join(c.DataDir, "drive", "tokens"),
			filepath.Join("DataServer", "data", "secrets", "drive", "tokens"),
			filepath.Join("DataServer", "data", "drive", "tokens"),
		}
		c.DriveTokensDir = firstExistingDir(driveTokenCandidates)
		if c.DriveTokensDir == "" {
			c.DriveTokensDir = filepath.Join(c.SecretsDir, "drive", "tokens")
		}
	}

	// Drive credentials: prefer explicit, then existing secrets/data dir, then deterministic fallback
	if c.DriveCredentialsDir == "" {
		driveCredCandidates := []string{
			filepath.Join(c.SecretsDir, "drive", "credentials"),
			filepath.Join(c.DataDir, "secrets", "drive", "credentials"),
			filepath.Join(c.DataDir, "drive", "credentials"),
			filepath.Join("DataServer", "data", "secrets", "drive", "credentials"),
			filepath.Join("DataServer", "data", "drive", "credentials"),
		}
		c.DriveCredentialsDir = firstExistingDir(driveCredCandidates)
		if c.DriveCredentialsDir == "" {
			c.DriveCredentialsDir = filepath.Join(c.SecretsDir, "drive", "credentials")
		}
	}
	populateDriveOAuthFromCredentials(c)

	// YouTube tokens: prefer explicit, then secrets dir, then legacy path
	if c.YouTubeTokensDir == "" {
		youtubeTokenCandidates := []string{
			filepath.Join(c.SecretsDir, "youtube", "tokens"),
			filepath.Join(c.DataDir, "secrets", "youtube", "tokens"),
			filepath.Join(c.DataDir, "youtube", "tokens"),
			filepath.Join("DataServer", "data", "secrets", "youtube", "tokens"),
			filepath.Join("DataServer", "data", "youtube", "tokens"),
		}
		c.YouTubeTokensDir = firstExistingDir(youtubeTokenCandidates)
		if c.YouTubeTokensDir == "" {
			c.YouTubeTokensDir = filepath.Join(c.SecretsDir, "youtube", "tokens")
		}
	}

	// YouTube credentials: prefer explicit, then secrets dir
	if c.YouTubeCredentialsDir == "" {
		youtubeCredCandidates := []string{
			filepath.Join(c.SecretsDir, "youtube", "credentials"),
			filepath.Join(c.DataDir, "secrets", "youtube", "credentials"),
			filepath.Join("DataServer", "data", "secrets", "youtube", "credentials"),
		}
		c.YouTubeCredentialsDir = firstExistingDir(youtubeCredCandidates)
		if c.YouTubeCredentialsDir == "" {
			c.YouTubeCredentialsDir = filepath.Join(c.SecretsDir, "youtube", "credentials")
		}
	}

	// Install handler configuration (migrated from Python FastAPI)
	c.ScriptDir = os.Getenv("VELOX_SCRIPT_DIR")
	// MasterURL: prefer MASTER_PUBLIC_URL (for remote workers), then VELOX_MASTER_URL, then legacy MASTER_URL
	// MASTER_PUBLIC_URL is the recommended var for production deployments with remote workers
	c.MasterURL = os.Getenv("MASTER_PUBLIC_URL")
	if c.MasterURL == "" {
		c.MasterURL = os.Getenv("VELOX_MASTER_URL")
	}
	if c.MasterURL == "" {
		c.MasterURL = os.Getenv("MASTER_URL") // Legacy alias
	}
	// Parse allowed worker IPs from comma-separated list
	if ips := os.Getenv("VELOX_ALLOWED_WORKER_IPS"); ips != "" {
		c.AllowedWorkerIPs = parseCommaList(ips)
	}
	c.AdminToken = os.Getenv("VELOX_ADMIN_TOKEN")
	if c.AdminToken == "" {
		c.AdminToken = os.Getenv("MASTER_ADMIN_TOKEN")
	}

	// Dev mode: allow localhost master URL for remote workers
	// WARNING: Only use in development environments
	c.AllowLocalhostMaster = os.Getenv("VELOX_ALLOW_LOCALHOST_MASTER") == "true" ||
		os.Getenv("VELOX_ALLOW_LOCALHOST_MASTER") == "1" ||
		os.Getenv("VELOX_DEV_MODE") == "true"

	return c
}

// parseCommaList parses a comma-separated string into a slice
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

// populateDriveOAuthFromCredentials loads OAuth client fields from credentials.json
// when VELOX_DRIVE_CLIENT_ID/SECRET are not explicitly set.
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

// splitByComma splits a string by comma and trims whitespace
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
