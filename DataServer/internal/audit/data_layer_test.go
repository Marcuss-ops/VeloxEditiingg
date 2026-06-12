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

	// Create ALL required primary files
	os.MkdirAll(filepath.Join(tmpDir, "youtube", "GroupYoutubeManager"), 0755)
	os.WriteFile(filepath.Join(tmpDir, "youtube", "GroupYoutubeManager", "ChannelsSaved.json"), []byte(`{}`), 0644)
	os.MkdirAll(filepath.Join(tmpDir, "youtube", "channels"), 0755)
	os.WriteFile(filepath.Join(tmpDir, "youtube", "channels", "channels.json"), []byte(`{}`), 0644)
	os.WriteFile(filepath.Join(tmpDir, "workers.json"), []byte(`{}`), 0644)
	os.WriteFile(filepath.Join(tmpDir, "velox.db"), []byte(``), 0644)
	os.WriteFile(filepath.Join(tmpDir, "ansible_runs.json"), []byte(`{}`), 0644)
	bundleDir := filepath.Join(tmpDir, "bundle")
	os.MkdirAll(bundleDir, 0755)
	os.WriteFile(filepath.Join(bundleDir, "manifest_v2.json"), []byte(`{}`), 0644)

	// Create legacy archive (allowed)
	archiveDir := filepath.Join(tmpDir, "legacy_archive", "2026-04-01")
	os.MkdirAll(archiveDir, 0755)
	os.WriteFile(filepath.Join(archiveDir, "youtube_manager.json"), []byte(`{}`), 0644)

	auditor := NewDataLayerAuditor(tmpDir, secretsDir)
	result := auditor.Audit()

	if !result.Passed {
		t.Errorf("Audit should pass with clean structure, got errors: %v", result.Errors)
	}

	if len(result.Errors) > 0 {
		t.Errorf("Expected no errors, got: %v", result.Errors)
	}
}

// TestDataLayerAuditorFailsOnDuplicateSourceOfTruth tests that audit fails
// when duplicate sources of truth are detected.
func TestDataLayerAuditorFailsOnDuplicateSourceOfTruth(t *testing.T) {
	tmpDir := t.TempDir()
	secretsDir := filepath.Join(tmpDir, "secrets")
	os.MkdirAll(secretsDir, 0755)

	// Create BOTH primary AND legacy workers files (should fail)
	os.WriteFile(filepath.Join(tmpDir, "workers.json"), []byte(`{}`), 0644)
	os.MkdirAll(filepath.Join(tmpDir, "workers"), 0755)
	os.WriteFile(filepath.Join(tmpDir, "workers", "workers.json"), []byte(`{}`), 0644)

	auditor := NewDataLayerAuditor(tmpDir, secretsDir)
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

	auditor := NewDataLayerAuditor(tmpDir, secretsDir)
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

	// Create ALL required primary files
	os.MkdirAll(filepath.Join(tmpDir, "youtube", "GroupYoutubeManager"), 0755)
	os.WriteFile(filepath.Join(tmpDir, "youtube", "GroupYoutubeManager", "ChannelsSaved.json"), []byte(`{}`), 0644)
	os.MkdirAll(filepath.Join(tmpDir, "youtube", "channels"), 0755)
	os.WriteFile(filepath.Join(tmpDir, "youtube", "channels", "channels.json"), []byte(`{}`), 0644)
	os.WriteFile(filepath.Join(tmpDir, "workers.json"), []byte(`{}`), 0644)
	os.WriteFile(filepath.Join(tmpDir, "velox.db"), []byte(``), 0644)
	os.WriteFile(filepath.Join(tmpDir, "ansible_runs.json"), []byte(`{}`), 0644)
	bundleDir := filepath.Join(tmpDir, "bundle")
	os.MkdirAll(bundleDir, 0755)
	os.WriteFile(filepath.Join(bundleDir, "manifest_v2.json"), []byte(`{}`), 0644)

	// Create legacy ONLY in archive (should pass)
	archiveDir := filepath.Join(tmpDir, "legacy_archive", "2026-04-01")
	os.MkdirAll(archiveDir, 0755)
	os.WriteFile(filepath.Join(archiveDir, "youtube_manager.json"), []byte(`{}`), 0644)

	auditor := NewDataLayerAuditor(tmpDir, secretsDir)
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

	auditor := NewDataLayerAuditor(tmpDir, secretsDir)
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

	auditor := NewDataLayerAuditor(tmpDir, secretsDir)
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

	auditor := NewDataLayerAuditor(tmpDir, secretsDir)
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

// TestMain runs setup/teardown for all tests
func TestMain(m *testing.M) {
	// Setup: nothing needed
	code := m.Run()
	// Teardown: nothing needed
	os.Exit(code)
}
