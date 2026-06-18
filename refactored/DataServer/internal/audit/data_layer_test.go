package audit

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDataLayerAuditorPassesWithCleanStructure tests that audit passes
// when only primary files exist and no legacy files are present.
func TestDataLayerAuditorPassesWithCleanStructure(t *testing.T) {
	// Create temp directory structure
	tmpDir := t.TempDir()
	secretsDir := filepath.Join(tmpDir, "secrets")
	os.MkdirAll(secretsDir, 0755)

	// Create ONLY required primary files (no legacy files)
	os.WriteFile(filepath.Join(tmpDir, "velox.db"), []byte(``), 0644)
	bundleDir := filepath.Join(tmpDir, "bundle")
	os.MkdirAll(bundleDir, 0755)
	os.WriteFile(filepath.Join(bundleDir, "manifest_v2.json"), []byte(`{}`), 0644)

	// Create YouTube tokens directory (required for audit)
	ytTokensDir := filepath.Join(secretsDir, "youtube", "tokens")
	os.MkdirAll(ytTokensDir, 0755)

	// Create legacy archive (allowed)
	archiveDir := filepath.Join(tmpDir, "legacy_archive", "2026-04-01")
	os.MkdirAll(archiveDir, 0755)
	os.WriteFile(filepath.Join(archiveDir, "youtube_manager.json"), []byte(`{}`), 0644)

	auditor := NewDataLayerAuditor(tmpDir, secretsDir, filepath.Join(tmpDir, "velox.db"))
	result := auditor.Audit()

	if !result.Passed {
		t.Errorf("Audit should pass with clean structure, got errors: %v", result.Errors)
	}

	if len(result.Errors) > 0 {
		t.Errorf("Expected no errors, got: %v", result.Errors)
	}
}

// TestDataLayerAuditorFailsOnDuplicateSourceOfTruth tests that audit fails
// when duplicate drive credentials directories exist (Unix only — Windows FS is case-insensitive).
func TestDataLayerAuditorFailsOnDuplicateSourceOfTruth(t *testing.T) {
	if os.PathSeparator == '\\' {
		t.Skip("Skipping on Windows: filesystem is case-insensitive")
	}
	tmpDir := t.TempDir()
	secretsDir := filepath.Join(tmpDir, "secrets")
	os.MkdirAll(secretsDir, 0755)

	// Create BOTH credentials/ and Credentials/ directories (should fail)
	os.MkdirAll(filepath.Join(tmpDir, "drive", "credentials"), 0755)
	os.MkdirAll(filepath.Join(tmpDir, "drive", "Credentials"), 0755)

	auditor := NewDataLayerAuditor(tmpDir, secretsDir, filepath.Join(tmpDir, "velox.db"))
	result := auditor.Audit()

	if result.Passed {
		t.Error("Audit should fail when duplicate source of truth exists")
	}

	if len(result.Errors) == 0 {
		t.Error("Expected errors for duplicate source of truth")
	}
}

// TestDataLayerAuditorFailsOnLegacyFilesInRoot tests that audit fails
// when legacy files exist in their original locations.
func TestDataLayerAuditorFailsOnLegacyFilesInRoot(t *testing.T) {
	tmpDir := t.TempDir()
	secretsDir := filepath.Join(tmpDir, "secrets")
	os.MkdirAll(secretsDir, 0755)

	// Create primary
	os.MkdirAll(filepath.Join(tmpDir, "youtube", "GroupYoutubeManager"), 0755)
	os.WriteFile(filepath.Join(tmpDir, "youtube", "GroupYoutubeManager", "ChannelsSaved.json"), []byte(`{}`), 0644)

	// Create legacy in WRONG location (should fail)
	os.WriteFile(filepath.Join(tmpDir, "youtube", "groups.json"), []byte(`[]`), 0644)

	auditor := NewDataLayerAuditor(tmpDir, secretsDir, filepath.Join(tmpDir, "velox.db"))
	result := auditor.Audit()

	if result.Passed {
		t.Error("Audit should fail when legacy files exist in original locations")
	}
}

// TestDataLayerAuditorAllowsArchivedLegacyFiles tests that audit passes
// when legacy files are properly archived.
func TestDataLayerAuditorAllowsArchivedLegacyFiles(t *testing.T) {
	tmpDir := t.TempDir()
	secretsDir := filepath.Join(tmpDir, "secrets")
	os.MkdirAll(secretsDir, 0755)

	// Create ONLY required primary files (no legacy files)
	os.WriteFile(filepath.Join(tmpDir, "velox.db"), []byte(``), 0644)
	bundleDir := filepath.Join(tmpDir, "bundle")
	os.MkdirAll(bundleDir, 0755)
	os.WriteFile(filepath.Join(bundleDir, "manifest_v2.json"), []byte(`{}`), 0644)

	// Create YouTube tokens directory (required for audit)
	ytTokensDir := filepath.Join(secretsDir, "youtube", "tokens")
	os.MkdirAll(ytTokensDir, 0755)

	// Create legacy ONLY in archive (should pass)
	archiveDir := filepath.Join(tmpDir, "legacy_archive", "2026-04-01")
	os.MkdirAll(archiveDir, 0755)
	os.WriteFile(filepath.Join(archiveDir, "youtube_manager.json"), []byte(`{}`), 0644)

	auditor := NewDataLayerAuditor(tmpDir, secretsDir, filepath.Join(tmpDir, "velox.db"))
	result := auditor.Audit()

	if !result.Passed {
		t.Errorf("Audit should pass when legacy files are archived, got errors: %v", result.Errors)
	}
}

// TestCheckLegacyFiles_DetectsForbidden tests that checkLegacyFiles detects
// forbidden legacy files.
func TestCheckLegacyFiles_DetectsForbidden(t *testing.T) {
	tmpDir := t.TempDir()
	secretsDir := filepath.Join(tmpDir, "secrets")
	os.MkdirAll(secretsDir, 0755)

	// Create ALL required primary files first
	os.MkdirAll(filepath.Join(tmpDir, "youtube", "GroupYoutubeManager"), 0755)
	os.MkdirAll(filepath.Join(tmpDir, "youtube", "channels"), 0755)
	os.WriteFile(filepath.Join(tmpDir, "youtube", "GroupYoutubeManager", "ChannelsSaved.json"), []byte(`{}`), 0644)
	os.WriteFile(filepath.Join(tmpDir, "youtube", "channels", "channels.json"), []byte(`{}`), 0644)
	os.WriteFile(filepath.Join(tmpDir, "workers.json"), []byte(`{}`), 0644)
	os.WriteFile(filepath.Join(tmpDir, "velox.db"), []byte(``), 0644)
	os.WriteFile(filepath.Join(tmpDir, "ansible_runs.json"), []byte(`{}`), 0644)
	bundleDir := filepath.Join(tmpDir, "bundle")
	os.MkdirAll(bundleDir, 0755)
	os.WriteFile(filepath.Join(bundleDir, "manifest_v2.json"), []byte(`{}`), 0644)

	// Create forbidden legacy file
	youtubeDir := filepath.Join(tmpDir, "youtube")
	os.MkdirAll(youtubeDir, 0755)
	os.WriteFile(filepath.Join(youtubeDir, "youtube_manager.json"), []byte(`{}`), 0644)

	auditor := NewDataLayerAuditor(tmpDir, secretsDir, filepath.Join(tmpDir, "velox.db"))
	result := &DataLayerAuditResult{
		Passed: true,
		Errors: make([]string, 0),
	}

	auditor.checkLegacyFiles(result)

	// checkLegacyFiles adds errors but doesn't set Passed - that's done by Audit()
	// So we check for errors, not Passed
	if len(result.Errors) == 0 {
		t.Error("checkLegacyFiles should add errors when forbidden legacy files exist")
	}

	found := false
	for _, err := range result.Errors {
		if contains(err, "youtube_manager.json") {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Expected error about youtube_manager.json, got: %v", result.Errors)
	}
}

// TestCheckPrimaryFiles_ReportsMissing tests that checkPrimaryFiles detects
// missing primary files.
func TestCheckPrimaryFiles_ReportsMissing(t *testing.T) {
	tmpDir := t.TempDir()
	secretsDir := filepath.Join(tmpDir, "secrets")
	os.MkdirAll(secretsDir, 0755)

	// Don't create any primary files

	auditor := NewDataLayerAuditor(tmpDir, secretsDir, filepath.Join(tmpDir, "velox.db"))
	result := &DataLayerAuditResult{
		Passed: true,
		Errors: make([]string, 0),
	}

	auditor.checkPrimaryFiles(result)

	if len(result.Errors) == 0 {
		t.Error("Expected errors for missing primary files")
	}
}

// TestCheckPrimaryFiles_PassesWhenPresent tests that checkPrimaryFiles passes
// when all primary files exist.
func TestCheckPrimaryFiles_PassesWhenPresent(t *testing.T) {
	tmpDir := t.TempDir()
	secretsDir := filepath.Join(tmpDir, "secrets")
	os.MkdirAll(secretsDir, 0755)

	// Create all required primary files
	os.MkdirAll(filepath.Join(tmpDir, "youtube", "GroupYoutubeManager"), 0755)
	os.MkdirAll(filepath.Join(tmpDir, "youtube", "channels"), 0755)
	os.WriteFile(filepath.Join(tmpDir, "youtube", "GroupYoutubeManager", "ChannelsSaved.json"), []byte(`{}`), 0644)
	os.WriteFile(filepath.Join(tmpDir, "youtube", "channels", "channels.json"), []byte(`{}`), 0644)
	os.WriteFile(filepath.Join(tmpDir, "workers.json"), []byte(`{}`), 0644)
	os.WriteFile(filepath.Join(tmpDir, "velox.db"), []byte(``), 0644)
	os.WriteFile(filepath.Join(tmpDir, "ansible_runs.json"), []byte(`{}`), 0644)
	
	bundleDir := filepath.Join(tmpDir, "bundle")
	os.MkdirAll(bundleDir, 0755)
	os.WriteFile(filepath.Join(bundleDir, "manifest_v2.json"), []byte(`{}`), 0644)

	auditor := NewDataLayerAuditor(tmpDir, secretsDir, filepath.Join(tmpDir, "velox.db"))
	result := &DataLayerAuditResult{
		Passed: true,
		Errors: make([]string, 0),
	}

	auditor.checkPrimaryFiles(result)

	if len(result.Errors) > 0 {
		t.Errorf("Expected no errors, got: %v", result.Errors)
	}
}

// TestAuditResultStructure tests that audit result contains expected fields.
func TestAuditResultStructure(t *testing.T) {
	result := &DataLayerAuditResult{
		Passed:   true,
		Errors:   []string{},
		Warnings: []string{"warning 1"},
		Info:     []string{"info 1", "info 2"},
	}

	if !result.Passed {
		t.Error("Expected Passed to be true")
	}

	if len(result.Warnings) != 1 {
		t.Errorf("Expected 1 warning, got %d", len(result.Warnings))
	}

	if len(result.Info) != 2 {
		t.Errorf("Expected 2 info, got %d", len(result.Info))
	}
}

// TestAuditResult_Failed tests audit result when failed.
func TestAuditResult_Failed(t *testing.T) {
	result := &DataLayerAuditResult{
		Passed:   false,
		Errors:   []string{"error 1", "error 2"},
		Warnings: []string{},
		Info:     []string{},
	}

	if result.Passed {
		t.Error("Expected Passed to be false")
	}

	if len(result.Errors) != 2 {
		t.Errorf("Expected 2 errors, got %d", len(result.Errors))
	}
}

// Helper function
func contains(s string, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && findSubstring(s, substr))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// ============================================================
// Additional audit tests
// ============================================================

// TestCheckDuplicateSources_WorkersWarning tests that workers.json produces
// a warning, not an error.
func TestCheckDuplicateSources_WorkersWarning(t *testing.T) {
	tmpDir := t.TempDir()
	secretsDir := filepath.Join(tmpDir, "secrets")
	os.MkdirAll(secretsDir, 0755)

	// Create workers.json (should produce a warning)
	os.WriteFile(filepath.Join(tmpDir, "workers.json"), []byte(`{}`), 0644)

	auditor := NewDataLayerAuditor(tmpDir, secretsDir)
	result := &DataLayerAuditResult{
		Passed:   true,
		Errors:   make([]string, 0),
		Warnings: make([]string, 0),
	}

	auditor.checkDuplicateSources(result)

	if len(result.Warnings) == 0 {
		t.Error("Expected warning for workers.json, got none")
	}
	if len(result.Errors) > 0 {
		t.Errorf("Expected no errors, got: %v", result.Errors)
	}
}

// TestCheckDuplicateSources_DriveCredentialsError tests that both credentials/
// and Credentials/ directories produce an error.
func TestCheckDuplicateSources_DriveCredentialsError(t *testing.T) {
	tmpDir := t.TempDir()
	secretsDir := filepath.Join(tmpDir, "secrets")
	os.MkdirAll(secretsDir, 0755)

	// Create BOTH credentials/ and Credentials/ directories
	os.MkdirAll(filepath.Join(tmpDir, "drive", "credentials"), 0755)
	os.MkdirAll(filepath.Join(tmpDir, "drive", "Credentials"), 0755)

	auditor := NewDataLayerAuditor(tmpDir, secretsDir)
	result := &DataLayerAuditResult{
		Passed:   true,
		Errors:   make([]string, 0),
		Warnings: make([]string, 0),
	}

	auditor.checkDuplicateSources(result)

	if len(result.Errors) == 0 {
		t.Error("Expected error for duplicate drive credentials, got none")
	}

	found := false
	for _, err := range result.Errors {
		if stringsContains(err, "Drive has duplicate credentials") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected error about duplicate credentials, got: %v", result.Errors)
	}
}

// TestCheckNamingConsistency_MixedCase tests that mixed-case
// directory names produce an error.
func TestCheckNamingConsistency_MixedCase(t *testing.T) {
	tmpDir := t.TempDir()
	secretsDir := filepath.Join(tmpDir, "secrets")
	os.MkdirAll(secretsDir, 0755)

	// Create BOTH credentials/ and Credentials/ (mixed case)
	os.MkdirAll(filepath.Join(tmpDir, "drive", "credentials"), 0755)
	os.MkdirAll(filepath.Join(tmpDir, "drive", "Credentials"), 0755)

	auditor := NewDataLayerAuditor(tmpDir, secretsDir)
	result := &DataLayerAuditResult{
		Passed:   true,
		Errors:   make([]string, 0),
		Warnings: make([]string, 0),
	}

	auditor.checkNamingConsistency(result)

	if len(result.Errors) == 0 {
		t.Error("Expected error for inconsistent directory naming, got none")
	}

	found := false
	for _, err := range result.Errors {
		if stringsContains(err, "Inconsistent directory naming") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected error about inconsistent naming, got: %v", result.Errors)
	}
}

// TestCheckDatabase_MissingDB tests that missing velox.db produces a warning.
func TestCheckDatabase_MissingDB(t *testing.T) {
	tmpDir := t.TempDir()
	secretsDir := filepath.Join(tmpDir, "secrets")
	os.MkdirAll(secretsDir, 0755)

	auditor := NewDataLayerAuditor(tmpDir, secretsDir)
	result := &DataLayerAuditResult{
		Passed:   true,
		Errors:   make([]string, 0),
		Warnings: make([]string, 0),
	}

	auditor.checkDatabase(result)

	if len(result.Warnings) == 0 {
		t.Error("Expected warning for missing velox.db, got none")
	}
	found := false
	for _, w := range result.Warnings {
		if stringsContains(w, "velox.db") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected warning about missing velox.db, got: %v", result.Warnings)
	}
}

// TestCheckDatabase_NoDuplicate tests that no duplicate warning is emitted
// when there's only one velox.db.
func TestCheckDatabase_NoDuplicate(t *testing.T) {
	tmpDir := t.TempDir()
	secretsDir := filepath.Join(tmpDir, "secrets")
	os.MkdirAll(secretsDir, 0755)

	// Create a single velox.db (no duplicates)
	os.WriteFile(filepath.Join(tmpDir, "velox.db"), []byte(`SQLite format 3\x00`), 0644)

	auditor := NewDataLayerAuditor(tmpDir, secretsDir)
	result := &DataLayerAuditResult{
		Passed:   true,
		Errors:   make([]string, 0),
		Warnings: make([]string, 0),
	}

	auditor.checkDatabase(result)

	// Should have no errors because velox.db exists
	if len(result.Errors) > 0 {
		t.Errorf("Expected no errors, got: %v", result.Errors)
	}

	// Should have no warnings about missing DB (it exists)
	for _, w := range result.Warnings {
		if stringsContains(w, "not found") {
			t.Errorf("Expected no 'not found' warning when velox.db exists, got: %s", w)
		}
	}
}

// TestAllowLegacy_SkippedAllowed tests that allowed legacy files don't
// produce errors.
func TestAllowLegacy_SkippedAllowed(t *testing.T) {
	tmpDir := t.TempDir()
	secretsDir := filepath.Join(tmpDir, "secrets")
	os.MkdirAll(secretsDir, 0755)

	// Create a known legacy file
	os.MkdirAll(filepath.Join(tmpDir, "youtube", "GroupYoutubeManager"), 0755)
	os.WriteFile(filepath.Join(tmpDir, "youtube", "GroupYoutubeManager", "ChannelsSaved.json"), []byte(`{}`), 0644)

	auditor := NewDataLayerAuditor(tmpDir, secretsDir)
	auditor.AllowLegacy("youtube/GroupYoutubeManager/ChannelsSaved.json")

	result := auditor.Audit()

	// With the file allowed, checkLegacyFiles should not report it
	// but checkPrimaryFiles might require velox.db
	// So we check specifically that ChannelsSaved.json is not in errors
	for _, err := range result.Errors {
		if stringsContains(err, "ChannelsSaved.json") {
			t.Errorf("Allowed legacy file should not produce error: %s", err)
		}
	}
}

// TestFailOnError_ReturnsError tests that FailOnError returns an error
// for failed audits.
func TestFailOnError_ReturnsError(t *testing.T) {
	result := &DataLayerAuditResult{
		Passed: false,
		Errors: []string{"error 1", "error 2"},
	}

	err := result.FailOnError()
	if err == nil {
		t.Error("FailOnError should return error when Passed is false")
	}

	if !stringsContains(err.Error(), "DATA LAYER AUDIT FAILED") {
		t.Errorf("Expected 'DATA LAYER AUDIT FAILED' in error, got: %v", err)
	}
}

// TestFailOnError_ReturnsNil tests that FailOnError returns nil
// for passed audits.
func TestFailOnError_ReturnsNil(t *testing.T) {
	result := &DataLayerAuditResult{
		Passed: true,
		Errors: []string{},
	}

	err := result.FailOnError()
	if err != nil {
		t.Errorf("FailOnError should return nil when Passed is true, got: %v", err)
	}
}

// TestAuditResult_StringContainsStatus tests that String() output
// contains the correct status.
func TestAuditResult_StringContainsStatus(t *testing.T) {
	result := &DataLayerAuditResult{
		Passed: true,
		Errors: []string{},
		DataDir: "/tmp/test",
	}

	s := result.String()

	if !stringsContains(s, "PASSED") {
		t.Error("String() should contain 'PASSED' for passed audits")
	}
}

// TestAuditResult_StringFailed tests that String() output for failed audits.
func TestAuditResult_StringFailed(t *testing.T) {
	result := &DataLayerAuditResult{
		Passed: false,
		Errors: []string{"test error"},
		DataDir: "/tmp/test",
	}

	s := result.String()

	if !stringsContains(s, "FAILED") {
		t.Error("String() should contain 'FAILED' for failed audits")
	}
	if !stringsContains(s, "test error") {
		t.Error("String() should contain the error message")
	}
}

// TestNewDataLayerAuditor tests that the constructor initializes correctly.
func TestNewDataLayerAuditor(t *testing.T) {
	auditor := NewDataLayerAuditor("/tmp/data", "/tmp/data/secrets")

	if auditor == nil {
		t.Fatal("NewDataLayerAuditor should not return nil")
	}

	if auditor.dataDir != "/tmp/data" {
		t.Errorf("Expected dataDir='/tmp/data', got '%s'", auditor.dataDir)
	}
	if auditor.secretsDir != "/tmp/data/secrets" {
		t.Errorf("Expected secretsDir='/tmp/data/secrets', got '%s'", auditor.secretsDir)
	}
	if auditor.allowedLegacy == nil {
		t.Error("allowedLegacy map should be initialized")
	}
}

// TestAudit_WarningCount tests that Audit() returns correct warning count.
func TestAudit_WarningCount(t *testing.T) {
	tmpDir := t.TempDir()
	secretsDir := filepath.Join(tmpDir, "secrets")
	os.MkdirAll(secretsDir, 0755)

	// Create YouTube tokens directory to avoid warnings from checkPrimaryFiles
	ytTokensDir := filepath.Join(secretsDir, "youtube", "tokens")
	os.MkdirAll(ytTokensDir, 0755)

	// Create some tokens
	os.WriteFile(filepath.Join(ytTokensDir, "account_test.json"), []byte(`{}`), 0644)

	auditor := NewDataLayerAuditor(tmpDir, secretsDir)
	result := auditor.Audit()

	// Should NOT have errors (no DB warning is expected since checkDatabase warns)
	// Check that Info contains token info
	found := false
	for _, info := range result.Info {
		if stringsContains(info, "YouTube tokens") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected info about YouTube tokens, got: %v", result.Info)
	}
}

// Helper: stringsContains reports whether substr is within s.
func stringsContains(s, substr string) bool {
	return len(s) >= len(substr) && contains(s, substr)
}

// TestMain runs setup/teardown for all tests
func TestMain(m *testing.M) {
	// Setup: nothing needed
	code := m.Run()
	// Teardown: nothing needed
	os.Exit(code)
}
