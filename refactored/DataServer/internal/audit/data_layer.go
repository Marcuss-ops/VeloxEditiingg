// Package audit provides automatic data layer validation.
// Fails CI or startup if legacy files, duplicate paths, or multiple sources of truth reappear.
package audit

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// DataLayerAuditResult contains the result of a data layer audit.
type DataLayerAuditResult struct {
	Passed    bool
	Errors    []string
	Warnings  []string
	Info      []string
	DataDir   string
	CheckedAt string
}

// DataLayerAuditor validates data layer structure and detects legacy files.
type DataLayerAuditor struct {
	dataDir     string
	secretsDir  string
	allowedLegacy map[string]bool // Explicitly allowed legacy files
}

// NewDataLayerAuditor creates a new data layer auditor.
func NewDataLayerAuditor(dataDir, secretsDir string) *DataLayerAuditor {
	return &DataLayerAuditor{
		dataDir:    dataDir,
		secretsDir: secretsDir,
		allowedLegacy: make(map[string]bool),
	}
}

// Audit performs a complete data layer audit.
func (a *DataLayerAuditor) Audit() *DataLayerAuditResult {
	result := &DataLayerAuditResult{
		Passed: true,
		Info:   make([]string, 0),
		Errors: make([]string, 0),
		Warnings: make([]string, 0),
		DataDir: a.dataDir,
	}

	// Check for legacy files that should not exist
	a.checkLegacyFiles(result)
	
	// Check for duplicate source of truth (directory naming inconsistencies)
	a.checkDuplicateSources(result)
	
	// Check for inconsistent naming
	a.checkNamingConsistency(result)
	
	// Check primary files exist
	a.checkPrimaryFiles(result)
	
	// Check database integrity
	a.checkDatabase(result)

	result.Passed = len(result.Errors) == 0
	return result
}

// checkLegacyFiles verifies that known legacy files are not present.
func (a *DataLayerAuditor) checkLegacyFiles(result *DataLayerAuditResult) {
	legacyFiles := []string{
		"youtube/youtube_manager.json",
		"youtube/groups.json",
		"youtube/channels/channels.json",
		"youtube/GroupYoutubeManager/ChannelsSaved.json",
		"youtube/group",
		"drive/Credentials",
		"ansible/ansible_runs.json",
		"job_queue.json",
		"job_queue_recovered.json",
		"jobs_queue.json",
		"video_uploads.db",
		"worker_downloads/bundle_manifest.json",
		"analytics/feed_cache.json",
		"youtube/history/upload_history.json",
		"drive/drive_links.json",
		"drive/drive_links.yaml",
		"drive/drive_links.yml",
	}

	for _, legacy := range legacyFiles {
		path := filepath.Join(a.dataDir, legacy)
		if a.fileExists(path) && !a.allowedLegacy[legacy] {
			result.Errors = append(result.Errors, fmt.Sprintf("Legacy file detected: %s", legacy))
		}
	}
}

// checkDuplicateSources detects multiple sources of truth for the same domain.
func (a *DataLayerAuditor) checkDuplicateSources(result *DataLayerAuditResult) {
	// Drive Credentials: should only have lowercase credentials/
	credsLower := filepath.Join(a.dataDir, "drive", "credentials")
	credsUpper := filepath.Join(a.dataDir, "drive", "Credentials")

	if a.dirExists(credsLower) && a.dirExists(credsUpper) {
		result.Errors = append(result.Errors, "Drive has duplicate credentials: credentials/ AND Credentials/")
	}
}

// checkNamingConsistency verifies consistent naming conventions.
func (a *DataLayerAuditor) checkNamingConsistency(result *DataLayerAuditResult) {
	// Check for mixed case directories
	driveDir := filepath.Join(a.dataDir, "drive")
	if a.dirExists(driveDir) {
		entries, err := os.ReadDir(driveDir)
		if err == nil {
			hasLower := false
			hasUpper := false
			for _, e := range entries {
				if e.IsDir() {
					if strings.ToLower(e.Name()) == "credentials" {
						if e.Name() == "credentials" {
							hasLower = true
						}
						if e.Name() == "Credentials" {
							hasUpper = true
						}
					}
				}
			}
			if hasLower && hasUpper {
				result.Errors = append(result.Errors, "Inconsistent directory naming: both 'credentials' and 'Credentials' exist")
			}
		}
	}
}

// checkPrimaryFiles verifies that primary source of truth files exist.
func (a *DataLayerAuditor) checkPrimaryFiles(result *DataLayerAuditResult) {
	primaryFiles := []struct {
		path     string
		required bool
	}{
		{"velox.db", true},           // SQLite is now the primary store
		// ansible_runs.json is no longer a primary source — SQLite only
		{"bundle/manifest_v2.json", false}, // Optional (generated)
	}

	for _, pf := range primaryFiles {
		path := filepath.Join(a.dataDir, pf.path)
		if !a.fileExists(path) {
			if pf.required {
				result.Errors = append(result.Errors, fmt.Sprintf("Missing primary file: %s", pf.path))
			} else {
				result.Warnings = append(result.Warnings, fmt.Sprintf("Missing optional file: %s", pf.path))
			}
		} else {
			result.Info = append(result.Info, fmt.Sprintf("Primary file OK: %s", pf.path))
		}
	}

	// Check YouTube tokens
	tokensDir := filepath.Join(a.secretsDir, "youtube", "tokens")
	if !a.dirExists(tokensDir) {
		result.Warnings = append(result.Warnings, "YouTube tokens directory missing: "+tokensDir)
	} else {
		tokenFiles, _ := os.ReadDir(tokensDir)
		count := 0
		for _, f := range tokenFiles {
			if strings.HasPrefix(f.Name(), "account_") && strings.HasSuffix(f.Name(), ".json") {
				count++
			}
		}
		result.Info = append(result.Info, fmt.Sprintf("YouTube tokens: %d found", count))
	}
}

// checkDatabase verifies SQLite database integrity.
func (a *DataLayerAuditor) checkDatabase(result *DataLayerAuditResult) {
	dbPath := filepath.Join(a.dataDir, "velox.db")
	if !a.fileExists(dbPath) {
		result.Warnings = append(result.Warnings, "SQLite database not found: velox.db")
		return
	}

	// Check for duplicate velox.db files
	paths := []string{
		filepath.Join(a.dataDir, "worker_runtime", "velox.db"),
		filepath.Join(a.dataDir, "..", "data", "velox.db"),
	}
	
	for _, p := range paths {
		if a.fileExists(p) {
			result.Warnings = append(result.Warnings, fmt.Sprintf("Duplicate velox.db found: %s", p))
		}
	}
}

// fileExists checks if a file exists.
func (a *DataLayerAuditor) fileExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

// dirExists checks if a directory exists.
func (a *DataLayerAuditor) dirExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}

// AllowLegacy explicitly allows a legacy file (for migration periods).
func (a *DataLayerAuditor) AllowLegacy(path string) {
	a.allowedLegacy[path] = true
}

// FailOnError returns an error if audit fails, nil otherwise.
func (r *DataLayerAuditResult) FailOnError() error {
	if !r.Passed {
		var sb strings.Builder
		sb.WriteString("DATA LAYER AUDIT FAILED\n")
		for _, e := range r.Errors {
			sb.WriteString(fmt.Sprintf("  ERROR: %s\n", e))
		}
		for _, w := range r.Warnings {
			sb.WriteString(fmt.Sprintf("  WARNING: %s\n", w))
		}
		return fmt.Errorf("%s", sb.String())
	}
	return nil
}

// String returns a human-readable audit report.
func (r *DataLayerAuditResult) String() string {
	var sb strings.Builder
	if r.Passed {
		sb.WriteString("[OK] Data Layer Audit PASSED\n")
	} else {
		sb.WriteString("[ERROR] Data Layer Audit FAILED\n")
	}
	
	sb.WriteString(fmt.Sprintf("DataDir: %s\n", r.DataDir))
	
	if len(r.Errors) > 0 {
		sb.WriteString(fmt.Sprintf("Errors: %d\n", len(r.Errors)))
		for _, e := range r.Errors {
			sb.WriteString(fmt.Sprintf("  - %s\n", e))
		}
	}
	
	if len(r.Warnings) > 0 {
		sb.WriteString(fmt.Sprintf("Warnings: %d\n", len(r.Warnings)))
		for _, w := range r.Warnings {
			sb.WriteString(fmt.Sprintf("  - %s\n", w))
		}
	}
	
	if len(r.Info) > 0 {
		sb.WriteString(fmt.Sprintf("Info: %d items\n", len(r.Info)))
	}
	
	return sb.String()
}
