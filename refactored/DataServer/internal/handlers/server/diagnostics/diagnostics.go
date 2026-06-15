package diagnostics

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/audit"
	"velox-server/internal/store"
)

// DataLayoutHandler provides diagnostics about the data layer.
type DataLayoutHandler struct {
	dataDir    string
	secretsDir string
	dbPath     string
	store      *store.SQLiteStore
}

// NewDataLayoutHandler creates a new data layout diagnostics handler.
func NewDataLayoutHandler(dataDir, secretsDir, dbPath string, dbStore *store.SQLiteStore) *DataLayoutHandler {
	return &DataLayoutHandler{
		dataDir:    dataDir,
		secretsDir: secretsDir,
		dbPath:     dbPath,
		store:      dbStore,
	}
}

// DataLayoutResponse represents the response from /diagnostics/data-layout.
type DataLayoutResponse struct {
	OK         bool                        `json:"ok"`
	Timestamp  string                      `json:"timestamp"`
	DataDir    string                      `json:"data_dir"`
	SecretsDir string                      `json:"secrets_dir"`
	Database   DatabaseInfo                `json:"database"`
	Domains    map[string]DomainInfo       `json:"domains"`
	Audit      *audit.DataLayerAuditResult `json:"audit,omitempty"`
	Counts     CountsInfo                  `json:"counts"`
}

// DatabaseInfo contains database diagnostics.
type DatabaseInfo struct {
	Path          string `json:"path"`
	Exists        bool   `json:"exists"`
	SizeBytes     int64  `json:"size_bytes"`
	SizeFormatted string `json:"size_formatted"`
}

// DomainInfo contains diagnostics for a specific domain.
type DomainInfo struct {
	Primary       string `json:"primary"`
	PrimaryExists bool   `json:"primary_exists"`
	Cache         string `json:"cache,omitempty"`
	CacheExists   bool   `json:"cache_exists,omitempty"`
	Legacy        string `json:"legacy,omitempty"`
	LegacyExists  bool   `json:"legacy_exists,omitempty"`
	Status        string `json:"status"`
}

// CountsInfo contains file counts.
type CountsInfo struct {
	YouTubeChannels int `json:"youtube_channels"`
	YouTubeGroups   int `json:"youtube_groups"`
	YouTubeTokens   int `json:"youtube_tokens"`
	Workers         int `json:"workers"`
	Jobs            int `json:"jobs"`
}

// GetDataLayoutHandler returns the /diagnostics/data-layout endpoint.
func (h *DataLayoutHandler) GetDataLayoutHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		response := DataLayoutResponse{
			OK:         true,
			Timestamp:  time.Now().UTC().Format(time.RFC3339),
			DataDir:    h.dataDir,
			SecretsDir: h.secretsDir,
			Domains:    make(map[string]DomainInfo),
		}

		// Database info
		response.Database = DatabaseInfo{
			Path:   h.dbPath,
			Exists: fileExists(h.dbPath),
		}
		if response.Database.Exists {
			if info, err := os.Stat(h.dbPath); err == nil {
				response.Database.SizeBytes = info.Size()
				response.Database.SizeFormatted = formatSize(info.Size())
			}
		}

		// YouTube Channels (canonical SQLite)
		response.Domains["youtube_channels"] = DomainInfo{
			Primary:       "SQLite: youtube_channels table (canonical)",
			PrimaryExists: true,
			Status:        "healthy",
		}

		// YouTube Groups (canonical SQLite)
		response.Domains["youtube_groups"] = DomainInfo{
			Primary:       "SQLite: youtube_groups_v2 table (canonical)",
			PrimaryExists: true,
			Status:        "healthy",
		}

		// YouTube Tokens
		tokensPath := filepath.Join(h.secretsDir, "youtube", "tokens")
		tokenCount := countTokenFiles(tokensPath)
		response.Domains["youtube_tokens"] = DomainInfo{
			Primary:       tokensPath,
			PrimaryExists: dirExists(tokensPath),
			Status:        "healthy",
		}
		response.Counts.YouTubeTokens = tokenCount

		// Workers (now in SQLite)
		response.Domains["workers"] = DomainInfo{
			Primary:       "SQLite: workers table",
			PrimaryExists: true,
			Status:        "healthy",
		}

		// Drive Credentials
		credsPath := filepath.Join(h.dataDir, "drive", "credentials")
		response.Domains["drive_credentials"] = DomainInfo{
			Primary:       credsPath,
			PrimaryExists: dirExists(credsPath),
			Status:        "healthy",
		}

		// Ansible (SQLite tables)
		response.Domains["ansible_hosts"] = DomainInfo{
			Primary:       "SQLite: ansible_hosts table",
			PrimaryExists: true,
			Status:        "healthy",
		}
		response.Domains["ansible_runs"] = DomainInfo{
			Primary:       "SQLite: ansible_runs + ansible_run_hosts tables",
			PrimaryExists: true,
			Status:        "healthy",
		}

		// Bundle Manifest
		manifestPath := filepath.Join(h.dataDir, "bundle", "manifest_v2.json")
		response.Domains["bundle_manifest"] = DomainInfo{
			Primary:       manifestPath,
			PrimaryExists: fileExists(manifestPath),
			Status:        "healthy",
		}

		// Run audit
		auditor := audit.NewDataLayerAuditor(h.dataDir, h.secretsDir)
		response.Audit = auditor.Audit()

		// Overall status
		response.OK = response.Audit.Passed

		c.JSON(http.StatusOK, response)
	}
}

// GetChannelCountsHandler returns channel/group counts from SQLite.
func (h *DataLayoutHandler) GetChannelCountsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		counts := CountsInfo{}

		if h.store != nil {
			if channels, err := h.store.ListYouTubeChannels(); err == nil {
				counts.YouTubeChannels = len(channels)
			}
			if groups, err := h.store.ListYouTubeGroupsV2(); err == nil {
				counts.YouTubeGroups = len(groups)
			}
		}

		c.JSON(http.StatusOK, counts)
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}

func countTokenFiles(dir string) int {
	if !dirExists(dir) {
		return 0
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}

	count := 0
	for _, e := range entries {
		if !e.IsDir() &&
			len(e.Name()) > 12 &&
			e.Name()[:8] == "account_" &&
			e.Name()[len(e.Name())-5:] == ".json" {
			count++
		}
	}
	return count
}

func formatSize(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.1f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.1f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.1f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
