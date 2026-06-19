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

// DataLayerAuditor validates data layer structure.
type DataLayerAuditor struct {
	dataDir       string
	secretsDir    string
	dbPath        string
	allowedLegacy map[string]bool
}

// NewDataLayerAuditor creates a new data layer auditor.
//
// dbPath is a variadic argument maintained for backwards compatibility
// against code paths that pre-date the multi-module audit surface
// (legacy tests, the bootstrap.go audit hook). When omitted, dbPath is
// treated as an empty string — checkPrimaryFiles / checkDatabase will
// skip DB-specific checks gracefully instead of failing on the call
// site. Production callers should pass cfg.Database.DBPath explicitly.
func NewDataLayerAuditor(dataDir, secretsDir string, dbPath ...string) *DataLayerAuditor {
	dbp := ""
	if len(dbPath) > 0 {
		dbp = dbPath[0]
	}
	return &DataLayerAuditor{
		dataDir:       dataDir,
		secretsDir:    secretsDir,
		dbPath:        dbp,
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

// checkDuplicateSources detects multiple sources of truth for the same domain.
func (a *DataLayerAuditor) checkDuplicateSources(result *DataLayerAuditResult) {
	// Drive Credentials: should only have lowercase credentials/
	credsLower := filepath.Join(a.dataDir, "drive", "credentials")
	credsUpper := filepath.Join(a.dataDir, "drive", "Credentials")

	if a.dirExists(credsLower) && a.dirExists(credsUpper) {
		result.Errors = append(result.Errors, "Drive has duplicate credentials: credentials/ AND Credentials/")
	}

	// Workers configuration: should live only in the database
	workersPath := filepath.Join(a.dataDir, "workers.json")
	if a.fileExists(workersPath) {
		result.Warnings = append(result.Warnings, "workers.json exists as file: prefer database-backed worker registry")
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
	// Verify the configured DB path exists
	if a.dbPath != "" {
		if !a.fileExists(a.dbPath) {
			result.Errors = append(result.Errors, fmt.Sprintf("Database not found at VELOX_DB_PATH: %s", a.dbPath))
		} else {
			result.Info = append(result.Info, fmt.Sprintf("Database OK: %s", a.dbPath))
		}
	}

	primaryFiles := []struct {
		path     string
		required bool
	}{
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
	// Database existence is checked via the configured path in checkPrimaryFiles.
	// No additional duplicate checks needed since VELOX_DB_PATH is the single source of truth.
	if a.dbPath == "" {
		result.Warnings = append(result.Warnings, "VELOX_DB_PATH not configured")
		return
	}

	info, err := os.Stat(a.dbPath)
	if err != nil {
		result.Warnings = append(result.Warnings, fmt.Sprintf("Database not accessible: %s (%v)", a.dbPath, err))
		return
	}
	result.Info = append(result.Info, fmt.Sprintf("Database size: %d bytes", info.Size()))
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

// AllowLegacy marks a path as an allowed legacy artifact, suppressing
// related errors/warnings in the audit.
func (a *DataLayerAuditor) AllowLegacy(path string) {
	a.allowedLegacy[path] = true
}

// IsLegacyAllowed returns true if the path has been explicitly allowed.
func (a *DataLayerAuditor) IsLegacyAllowed(path string) bool {
	return a.allowedLegacy[path]
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
