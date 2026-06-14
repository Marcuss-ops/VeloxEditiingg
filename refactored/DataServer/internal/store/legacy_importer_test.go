package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"velox-shared/payload"
)

// ============================================================
// countJSONRecords tests
// ============================================================

func TestCountJSONRecords_MapFormat(t *testing.T) {
	data := []byte(`{"a": 1, "b": 2, "c": 3}`)
	count, err := countJSONRecords("workers", data)
	if err != nil {
		t.Fatalf("countJSONRecords failed: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected 3 records, got %d", count)
	}
}

func TestCountJSONRecords_ArrayFormat(t *testing.T) {
	data := []byte(`[{"name": "a"}, {"name": "b"}]`)
	count, err := countJSONRecords("youtube_groups", data)
	if err != nil {
		t.Fatalf("countJSONRecords failed: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 records, got %d", count)
	}
}

func TestCountJSONRecords_EmptyObject(t *testing.T) {
	data := []byte(`{}`)
	count, err := countJSONRecords("workers", data)
	if err != nil {
		t.Fatalf("countJSONRecords failed: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 records for empty object, got %d", count)
	}
}

func TestCountJSONRecords_EmptyArray(t *testing.T) {
	data := []byte(`[]`)
	count, err := countJSONRecords("youtube_groups", data)
	if err != nil {
		t.Fatalf("countJSONRecords failed: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 records for empty array, got %d", count)
	}
}

func TestCountJSONRecords_InvalidJSON(t *testing.T) {
	data := []byte(`{invalid json}`)
	count, err := countJSONRecords("workers", data)
	if err != nil {
		// Error is acceptable — function returns 0 on error
		t.Logf("Got expected error for invalid JSON: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 records for invalid JSON, got %d", count)
	}
}

// ============================================================
// createJSONBackup tests
// ============================================================

func TestCreateJSONBackup_Success(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.json")
	originalData := []byte(`{"key": "value"}`)

	// Create original file
	if err := os.WriteFile(filePath, originalData, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	backupPath, err := createJSONBackup(filePath, originalData)
	if err != nil {
		t.Fatalf("createJSONBackup failed: %v", err)
	}

	// Verify backup exists
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		t.Fatalf("Backup file not created at %s", backupPath)
	}

	// Verify backup contains original data
	backupData, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatalf("Failed to read backup: %v", err)
	}
	if string(backupData) != string(originalData) {
		t.Errorf("Backup content mismatch: got %s, expected %s", backupData, originalData)
	}

	// Verify backup has .bak extension and timestamp
	if !strings.HasSuffix(backupPath, ".bak") {
		t.Errorf("Backup path should end with .bak, got: %s", backupPath)
	}

	// Verify original still intact
	origData, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read original: %v", err)
	}
	if string(origData) != string(originalData) {
		t.Errorf("Original file modified: got %s, expected %s", origData, originalData)
	}
}

func TestCreateJSONBackup_NonExistentDir(t *testing.T) {
	tmpDir := t.TempDir()
	// Use a non-existent subdirectory to test failure
	filePath := filepath.Join(tmpDir, "nonexistent", "test.json")
	data := []byte(`{"key": "value"}`)

	_, err := createJSONBackup(filePath, data)
	if err == nil {
		t.Error("Expected error for non-existent directory, got nil")
	}
}

// ============================================================
// toInt64 tests
// ============================================================

func TestToInt64_Float64(t *testing.T) {
	result := toInt64(float64(42))
	if result != 42 {
		t.Errorf("Expected 42, got %d", result)
	}
}

func TestToInt64_Int64(t *testing.T) {
	result := toInt64(int64(100))
	if result != 100 {
		t.Errorf("Expected 100, got %d", result)
	}
}

func TestToInt64_Int(t *testing.T) {
	result := toInt64(200)
	if result != 200 {
		t.Errorf("Expected 200, got %d", result)
	}
}

func TestToInt64_JsonNumber(t *testing.T) {
	result := toInt64(json.Number("300"))
	if result != 300 {
		t.Errorf("Expected 300, got %d", result)
	}
}

func TestToInt64_ZeroForUnsupported(t *testing.T) {
	result := toInt64("not a number")
	if result != 0 {
		t.Errorf("Expected 0 for unsupported type, got %d", result)
	}
}

func TestToInt64_Nil(t *testing.T) {
	result := toInt64(nil)
	if result != 0 {
		t.Errorf("Expected 0 for nil, got %d", result)
	}
}

// ============================================================
// payload.FirstNonEmpty tests
// ============================================================

func TestFirstNonEmpty_First(t *testing.T) {
	result := payload.FirstNonEmpty("hello", "", "world")
	if result != "hello" {
		t.Errorf("Expected 'hello', got '%s'", result)
	}
}

func TestFirstNonEmpty_Second(t *testing.T) {
	result := payload.FirstNonEmpty("", "world")
	if result != "world" {
		t.Errorf("Expected 'world', got '%s'", result)
	}
}

func TestFirstNonEmpty_AllEmpty(t *testing.T) {
	result := payload.FirstNonEmpty("", "", "")
	if result != "" {
		t.Errorf("Expected empty string, got '%s'", result)
	}
}

func TestFirstNonEmpty_NoArgs(t *testing.T) {
	result := payload.FirstNonEmpty()
	if result != "" {
		t.Errorf("Expected empty string, got '%s'", result)
	}
}

// ============================================================
// sanitizeFilename tests
// ============================================================

func TestSanitizeFilename_Colon(t *testing.T) {
	result := sanitizeFilename("host:name")
	if result != "host_name" {
		t.Errorf("Expected 'host_name', got '%s'", result)
	}
}

func TestSanitizeFilename_Slash(t *testing.T) {
	result := sanitizeFilename("host/name")
	if result != "host_name" {
		t.Errorf("Expected 'host_name', got '%s'", result)
	}
}

func TestSanitizeFilename_Space(t *testing.T) {
	result := sanitizeFilename("host name")
	if result != "host_name" {
		t.Errorf("Expected 'host_name', got '%s'", result)
	}
}

func TestSanitizeFilename_Clean(t *testing.T) {
	result := sanitizeFilename("myhost-01")
	if result != "myhost-01" {
		t.Errorf("Expected 'myhost-01', got '%s'", result)
	}
}

func TestSanitizeFilename_Empty(t *testing.T) {
	result := sanitizeFilename("")
	if result != "" {
		t.Errorf("Expected empty string, got '%s'", result)
	}
}

// ============================================================
// legacyJSONSources tests
// ============================================================

func TestLegacyJSONSources_Count(t *testing.T) {
	sources := legacyJSONSources()
	if len(sources) != 8 {
		t.Errorf("Expected 8 legacy JSON sources, got %d", len(sources))
	}
}

func TestLegacyJSONSources_Domains(t *testing.T) {
	sources := legacyJSONSources()
	domains := make(map[string]bool)
	for _, s := range sources {
		if s.Domain == "" {
			t.Errorf("Source %q has empty domain", s.Name)
		}
		if s.Path == "" {
			t.Errorf("Source %q has empty path", s.Name)
		}
		if domains[s.Domain] {
			t.Errorf("Duplicate domain: %s", s.Domain)
		}
		domains[s.Domain] = true
	}

	expectedDomains := []string{"workers", "youtube_channels", "youtube_groups", "youtube_manager",
		"ansible_hosts", "ansible_runs", "analytics_cache", "youtube_cache"}
	for _, d := range expectedDomains {
		if !domains[d] {
			t.Errorf("Missing expected domain: %s", d)
		}
	}
}

// ============================================================
// importWorkersJSON test (with real SQLite)
// ============================================================

func TestImportWorkersJSON(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	data := []byte(`{
		"worker-1": {"name": "Worker 1", "status": "online"},
		"worker-2": {"name": "Worker 2", "status": "offline"}
	}`)

	imported, err := importWorkersJSON(s, data)
	if err != nil {
		t.Fatalf("importWorkersJSON failed: %v", err)
	}
	if imported != 2 {
		t.Errorf("Expected 2 imported workers, got %d", imported)
	}
}

func TestImportWorkersJSON_InvalidJSON(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	data := []byte(`{invalid}`)

	_, err := importWorkersJSON(s, data)
	if err == nil {
		t.Error("Expected error for invalid JSON, got nil")
	}
}

func TestImportWorkersJSON_Empty(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	data := []byte(`{}`)

	imported, err := importWorkersJSON(s, data)
	if err != nil {
		t.Fatalf("importWorkersJSON failed: %v", err)
	}
	if imported != 0 {
		t.Errorf("Expected 0 imported workers for empty data, got %d", imported)
	}
}

// ============================================================
// importYouTubeChannelsJSON test (with real SQLite)
// ============================================================

func TestImportYouTubeChannelsJSON(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	data := []byte(`{
		"UC_aaa": {"title": "Channel A", "display_name": "Chan A", "language": "en"},
		"UC_bbb": {"title": "Channel B", "language": "it"}
	}`)

	imported, err := importYouTubeChannelsJSON(s, data)
	if err != nil {
		t.Fatalf("importYouTubeChannelsJSON failed: %v", err)
	}
	if imported != 2 {
		t.Errorf("Expected 2 imported channels, got %d", imported)
	}
}

func TestImportYouTubeChannelsJSON_InvalidJSON(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	data := []byte(`{invalid}`)

	_, err := importYouTubeChannelsJSON(s, data)
	if err == nil {
		t.Error("Expected error for invalid JSON, got nil")
	}
}

// ============================================================
// importYouTubeGroupsJSON test (with real SQLite)
// ============================================================

func TestImportYouTubeGroupsJSON(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	data := []byte(`[
		{"name": "Group A", "description": "First group", "privacy": "public", "channels": ["UC_aaa"]},
		{"name": "Group B", "description": "Second group", "privacy": "unlisted"}
	]`)

	imported, err := importYouTubeGroupsJSON(s, data)
	if err != nil {
		t.Fatalf("importYouTubeGroupsJSON failed: %v", err)
	}
	if imported != 2 {
		t.Errorf("Expected 2 imported groups, got %d", imported)
	}
}

func TestImportYouTubeGroupsJSON_MissingName(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	// Group without name should be skipped
	data := []byte(`[
		{"name": "Group 1", "description": "Valid"},
		{"description": "Missing name"},
		{"name": "Group 2", "description": "Valid"}
	]`)

	imported, err := importYouTubeGroupsJSON(s, data)
	if err != nil {
		t.Fatalf("importYouTubeGroupsJSON failed: %v", err)
	}
	if imported != 2 {
		t.Errorf("Expected 2 imported groups (1 skipped), got %d", imported)
	}
}

func TestImportYouTubeGroupsJSON_InvalidJSON(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	data := []byte(`{invalid}`)

	_, err := importYouTubeGroupsJSON(s, data)
	if err == nil {
		t.Error("Expected error for invalid JSON, got nil")
	}
}

// ============================================================
// importJSONData dispatch tests
// ============================================================

func TestImportJSONData_UnknownDomain(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	_, err := importJSONData(s, "unknown_domain", []byte(`{}`), "/tmp/test.json")
	if err == nil {
		t.Error("Expected error for unknown domain, got nil")
	}
	if !strings.Contains(err.Error(), "unknown import domain") {
		t.Errorf("Expected 'unknown import domain' error, got: %v", err)
	}
}

// ============================================================
// LegacyImportResult tests
// ============================================================

func TestLegacyImportResult_DefaultValues(t *testing.T) {
	r := LegacyImportResult{}
	if r.Status != "" {
		t.Errorf("Expected empty status, got '%s'", r.Status)
	}
	if r.SHA256 != "" {
		t.Errorf("Expected empty SHA256, got '%s'", r.SHA256)
	}
	if r.Error != "" {
		t.Errorf("Expected empty Error, got '%s'", r.Error)
	}
}

func TestLegacyImportResult_JSONSerialization(t *testing.T) {
	r := LegacyImportResult{
		Status:   "imported",
		SHA256:   "abc123",
		Records:  10,
		Imported: 10,
		Backup:   "/tmp/test.json.20260101T000000Z.bak",
	}

	data, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("Failed to marshal LegacyImportResult: %v", err)
	}

	var decoded LegacyImportResult
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal LegacyImportResult: %v", err)
	}

	if decoded.Status != "imported" {
		t.Errorf("Expected status 'imported', got '%s'", decoded.Status)
	}
	if decoded.SHA256 != "abc123" {
		t.Errorf("Expected SHA256 'abc123', got '%s'", decoded.SHA256)
	}
}

// ============================================================
// Integration: ImportLegacyJSON with mock data
// ============================================================

func TestImportLegacyJSON_NoDataDir(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	results, err := s.ImportLegacyJSON("")
	if err != nil {
		t.Fatalf("ImportLegacyJSON with empty dataDir failed: %v", err)
	}
	if results != nil {
		t.Errorf("Expected nil results for empty dataDir, got %d items", len(results))
	}
}

func TestImportLegacyJSON_EmptyDataDir(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	tmpDir := t.TempDir()

	results, err := s.ImportLegacyJSON(tmpDir)
	if err != nil {
		t.Fatalf("ImportLegacyJSON with empty directory failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results for empty directory, got %d", len(results))
	}
}

func TestImportLegacyJSON_WithWorkersFile(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	tmpDir := t.TempDir()

	// Create a workers.json file
	workersDir := tmpDir
	workersData := []byte(`{"w1": {"name": "Worker 1"}, "w2": {"name": "Worker 2"}}`)
	if err := os.WriteFile(filepath.Join(workersDir, "workers.json"), workersData, 0644); err != nil {
		t.Fatalf("Failed to create workers.json: %v", err)
	}

	results, err := s.ImportLegacyJSON(tmpDir)
	if err != nil {
		t.Fatalf("ImportLegacyJSON failed: %v", err)
	}

	// Should find and import the workers.json file
	found := false
	for _, r := range results {
		if r.Source.Domain == "workers" && r.Status == "imported" {
			found = true
			if r.Imported != 2 {
				t.Errorf("Expected 2 imported workers, got %d", r.Imported)
			}
			break
		}
	}
	if !found {
		t.Errorf("Expected workers import result, got: %+v", results)
	}
}

func TestImportLegacyJSON_Idempotent(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()

	tmpDir := t.TempDir()

	// Create a workers.json file
	workersData := []byte(`{"w1": {"name": "Worker 1"}}`)
	if err := os.WriteFile(filepath.Join(tmpDir, "workers.json"), workersData, 0644); err != nil {
		t.Fatalf("Failed to create workers.json: %v", err)
	}

	// First import — should import. After import, the file is archived to legacy_archive/
	results1, err := s.ImportLegacyJSON(tmpDir)
	if err != nil {
		t.Fatalf("First ImportLegacyJSON failed: %v", err)
	}

	imported1 := 0
	for _, r := range results1 {
		if r.Status == "imported" {
			imported1++
		}
	}
	if imported1 == 0 {
		t.Error("Expected at least one imported file on first import")
	}

	// Recreate the file with the same content to test idempotency
	// (ImportLegacyJSON archives the file after successful import)
	if err := os.WriteFile(filepath.Join(tmpDir, "workers.json"), workersData, 0644); err != nil {
		t.Fatalf("Failed to recreate workers.json: %v", err)
	}

	// Second import — should be skipped (idempotent, same checksum)
	results2, err := s.ImportLegacyJSON(tmpDir)
	if err != nil {
		t.Fatalf("Second ImportLegacyJSON failed: %v", err)
	}

	skipped := 0
	for _, r := range results2 {
		if r.Status == "skipped" {
			skipped++
		}
	}
	if skipped == 0 {
		t.Errorf("Expected at least one skipped file on second import, got: %+v", results2)
	}
}
