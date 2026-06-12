package diagnostics

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/audit"
)

// DataLayoutHandler provides diagnostics about the data layer.
type DataLayoutHandler struct {
	dataDir    string
	secretsDir string
	dbPath     string
}

// NewDataLayoutHandler creates a new data layout diagnostics handler.
func NewDataLayoutHandler(dataDir, secretsDir, dbPath string) *DataLayoutHandler {
	return &DataLayoutHandler{
		dataDir:    dataDir,
		secretsDir: secretsDir,
		dbPath:     dbPath,
	}
}

// DataLayoutResponse represents the response from /diagnostics/data-layout.
type DataLayoutResponse struct {
	OK           bool                   `json:"ok"`
	Timestamp    string                 `json:"timestamp"`
	DataDir      string                 `json:"data_dir"`
	SecretsDir   string                 `json:"secrets_dir"`
	Database     DatabaseInfo           `json:"database"`
	Domains      map[string]DomainInfo  `json:"domains"`
	Audit        *audit.DataLayerAuditResult `json:"audit,omitempty"`
	Counts       CountsInfo             `json:"counts"`
}

// DatabaseInfo contains database diagnostics.
type DatabaseInfo struct {
	Path       string `json:"path"`
	Exists     bool   `json:"exists"`
	SizeBytes  int64  `json:"size_bytes"`
	SizeFormatted string `json:"size_formatted"`
}

// DomainInfo contains diagnostics for a specific domain.
type DomainInfo struct {
	Primary      string `json:"primary"`
	PrimaryExists bool  `json:"primary_exists"`
	Cache        string `json:"cache,omitempty"`
	CacheExists  bool   `json:"cache_exists,omitempty"`
	Legacy       string `json:"legacy,omitempty"`
	LegacyExists bool   `json:"legacy_exists,omitempty"`
	Status       string `json:"status"`
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
			OK:        true,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			DataDir:   h.dataDir,
			SecretsDir: h.secretsDir,
			Domains:   make(map[string]DomainInfo),
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

		// YouTube Channels
		channelsPath := filepath.Join(h.dataDir, "youtube", "channels", "channels.json")
		response.Domains["youtube_channels"] = DomainInfo{
			Primary:       channelsPath,
			PrimaryExists: fileExists(channelsPath),
			Cache:         filepath.Join(h.dataDir, "youtube", "youtube_api_cache.json"),
			CacheExists:   fileExists(filepath.Join(h.dataDir, "youtube", "youtube_api_cache.json")),
			Legacy:        "GroupYoutubeManager/ChannelsSaved.json",
			LegacyExists:  false, // Should not exist
			Status:        getStatus(fileExists(channelsPath), false),
		}

		// YouTube Groups
		groupsPath := filepath.Join(h.dataDir, "youtube", "groups.json")
		youtubeManagerPath := filepath.Join(h.dataDir, "youtube", "youtube_manager.json")
		response.Domains["youtube_groups"] = DomainInfo{
			Primary:       groupsPath,
			PrimaryExists: fileExists(groupsPath),
			Legacy:        "youtube_manager.json",
			LegacyExists:  fileExists(youtubeManagerPath),
			Status:        getStatus(fileExists(groupsPath), fileExists(youtubeManagerPath)),
		}

		// YouTube Tokens
		tokensPath := filepath.Join(h.secretsDir, "youtube", "tokens")
		tokenCount := countTokenFiles(tokensPath)
		legacyGroupPath := filepath.Join(h.dataDir, "youtube", "group")
		response.Domains["youtube_tokens"] = DomainInfo{
			Primary:       tokensPath,
			PrimaryExists: dirExists(tokensPath),
			Legacy:        "youtube/group/*/account_*.json",
			LegacyExists:  dirExists(legacyGroupPath),
			Status:        getStatus(dirExists(tokensPath), dirExists(legacyGroupPath)),
		}
		response.Counts.YouTubeTokens = tokenCount

		// Workers
		workersPath := filepath.Join(h.dataDir, "workers.json")
		workersLegacyPath := filepath.Join(h.dataDir, "workers", "workers.json")
		response.Domains["workers"] = DomainInfo{
			Primary:       workersPath,
			PrimaryExists: fileExists(workersPath),
			Legacy:        "workers/workers.json",
			LegacyExists:  fileExists(workersLegacyPath),
			Status:        getStatus(fileExists(workersPath), fileExists(workersLegacyPath)),
		}

		// Drive Credentials
		credsPath := filepath.Join(h.dataDir, "drive", "credentials")
		credsLegacyPath := filepath.Join(h.dataDir, "drive", "Credentials")
		response.Domains["drive_credentials"] = DomainInfo{
			Primary:       credsPath,
			PrimaryExists: dirExists(credsPath),
			Legacy:        "Credentials/",
			LegacyExists:  dirExists(credsLegacyPath),
			Status:        getStatus(dirExists(credsPath), dirExists(credsLegacyPath)),
		}

		// Ansible Runs
		ansiblePath := filepath.Join(h.dataDir, "ansible_runs.json")
		ansibleLegacyPath := filepath.Join(h.dataDir, "ansible", "ansible_runs.json")
		response.Domains["ansible_runs"] = DomainInfo{
			Primary:       ansiblePath,
			PrimaryExists: fileExists(ansiblePath),
			Legacy:        "ansible/ansible_runs.json",
			LegacyExists:  fileExists(ansibleLegacyPath),
			Status:        getStatus(fileExists(ansiblePath), fileExists(ansibleLegacyPath)),
		}

		// Bundle Manifest
		manifestPath := filepath.Join(h.dataDir, "bundle", "manifest_v2.json")
		manifestLegacyPath := filepath.Join(h.dataDir, "worker_downloads", "bundle_manifest.json")
		response.Domains["bundle_manifest"] = DomainInfo{
			Primary:       manifestPath,
			PrimaryExists: fileExists(manifestPath),
			Legacy:        "worker_downloads/bundle_manifest.json",
			LegacyExists:  fileExists(manifestLegacyPath),
			Status:        getStatus(fileExists(manifestPath), fileExists(manifestLegacyPath)),
		}

		// Run audit
		auditor := audit.NewDataLayerAuditor(h.dataDir, h.secretsDir)
		response.Audit = auditor.Audit()
		
		// Overall status
		response.OK = response.Audit.Passed

		c.JSON(http.StatusOK, response)
	}
}

// GetChannelCountsHandler returns channel/group counts.
func (h *DataLayoutHandler) GetChannelCountsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		counts := CountsInfo{}

		// YouTube channels
		channelsPath := filepath.Join(h.dataDir, "youtube", "channels", "channels.json")
		if data, err := os.ReadFile(channelsPath); err == nil {
			// Simple count by counting opening brackets
			for i := 0; i < len(data); i++ {
				if data[i] == '{' && i > 0 && data[i-1] == '[' {
					counts.YouTubeChannels++
				} else if data[i] == '{' && i > 0 && (data[i-1] == ',' || data[i-1] == '[') {
					// This is a rough count
				}
			}
			// Better: count "id" occurrences
			counts.YouTubeChannels = countSubstring(data, `"id"`)
		}

		// YouTube groups
		groupsPath := filepath.Join(h.dataDir, "youtube", "groups.json")
		if data, err := os.ReadFile(groupsPath); err == nil {
			counts.YouTubeGroups = countSubstring(data, `"name"`)
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

func getStatus(primaryExists, legacyExists bool) string {
	if !primaryExists {
		return "missing_primary"
	}
	if legacyExists {
		return "has_legacy"
	}
	return "healthy"
}

func countSubstring(data []byte, substr string) int {
	count := 0
	for i := 0; i <= len(data)-len(substr); i++ {
		if string(data[i:i+len(substr)]) == substr {
			count++
		}
	}
	return count
}
