package migrations

import (
	"crypto/sha256"
	"database/sql"
	"embed"
	"fmt"
	"sort"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed *.sql
var testMigrationsFS embed.FS

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name()))
	if err != nil {
		t.Fatalf("open in-memory db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	_, _ = db.Exec("PRAGMA foreign_keys = ON")
	return db
}

// loadAllMigrations returns all discovered migrations (shortcut for tests).
func loadAllMigrations(t *testing.T) []Migration {
	t.Helper()
	migs, err := discoverMigrations(testMigrationsFS, ".")
	if err != nil {
		t.Fatalf("discoverMigrations: %v", err)
	}
	if len(migs) != 24 {
		t.Fatalf("expected 24 migrations, got %d", len(migs))
	}
	expected := []struct {
		Version int
		Name    string
	}{
		{1, "initial"}, {2, "legacy_imports"}, {3, "youtube_canonical"},
		{4, "ansible"}, {5, "legacy_cleanup"}, {6, "drive_links_source_of_truth"},
		{7, "queue_persistence"}, {8, "drop_legacy_tables"}, {9, "drop_legacy_tables"},
		{10, "job_attempts_and_artifacts"}, {11, "youtube_oauth_tokens"},
		{12, "youtube_groups_rename"}, {13, "delivery_targets"},
		{14, "orchestrator_outbox"}, {15, "delivery_and_revision"},
		{16, "job_indices"}, {17, "job_normalization"},
		{18, "metadata_json_backfill"}, {19, "drop_metadata_json"},
		{20, "worker_control_plane"}, {21, "artifact_states"},
		{22, "split_deliveries"}, {23, "add_job_columns"},
		{24, "legacy_state_cleanup"},
	}
	for i, exp := range expected {
		if migs[i].Version != exp.Version || migs[i].Name != exp.Name {
			t.Errorf("migration[%d]: got %d/%q, want %d/%q", i, migs[i].Version, migs[i].Name, exp.Version, exp.Name)
		}
		if m.Name == "" {
			t.Errorf("migration[%d]: name is empty", i)
		}
		if m.SQL == "" {
			t.Errorf("migration[%d]: SQL is empty", i)
		}
		if m.Checksum == "" {
			t.Errorf("migration[%d]: checksum is empty", i)
		}
	}
}

func TestDiscoverMigrations_NoDuplicateVersions(t *testing.T) {
	migs := loadAllMigrations(t)
	seen := make(map[int]int) // version → count
	for _, m := range migs {
		seen[m.Version]++
	}
	for v, count := range seen {
		if count > 1 {
			t.Errorf("duplicate migration version %03d: %d occurrences", v, count)
		}
	}
}

func TestDiscoverMigrations_StrictlyIncreasing(t *testing.T) {
	migs := loadAllMigrations(t)
	for i := 1; i < len(migs); i++ {
		prev := migs[i-1].Version
		curr := migs[i].Version
		if curr <= prev {
			t.Errorf("migrations not strictly increasing: %03d followed by %03d", prev, curr)
		}
	}
}

func TestDiscoverMigrations_GapsAllowedIfDocumented(t *testing.T) {
	migs := loadAllMigrations(t)
	if len(migs) == 0 {
		return
	}

	// Build the full range from first to last.
	first := migs[0].Version
	last := migs[len(migs)-1].Version

	present := make(map[int]bool)
	for _, m := range migs {
		present[m.Version] = true
	}

	// Gaps that are explicitly allowed (documented with reason).
	// Add entries here when a version is intentionally skipped.
	allowlist := map[int]string{
		// Example: 5: "skipped — schema was stable, no changes needed"
	}

	var gaps []int
	for v := first; v <= last; v++ {
		if !present[v] {
			gaps = append(gaps, v)
		}
	}
	if len(gaps) == 0 {
		return
	}

	// Report undocumented gaps.
	var undocumented []int
	for _, g := range gaps {
		if _, ok := allowlist[g]; !ok {
			undocumented = append(undocumented, g)
		}
	}
	if len(undocumented) > 0 {
		t.Errorf("undocumented gap(s) in migration versions: %v. "+
			"If intentional, add to the allowlist in the test with a comment explaining why.", undocumented)
	}
}

func TestDiscoverMigrations_MigrationNamesAreValid(t *testing.T) {
	migs := loadAllMigrations(t)
	for _, m := range migs {
		if m.Name == "" {
			t.Errorf("migration %03d: name is empty", m.Version)
		}
		if strings.HasPrefix(m.Name, "_") || strings.HasSuffix(m.Name, "_") {
			t.Errorf("migration %03d: name %q has leading/trailing underscore", m.Version, m.Name)
		}
	}
}

// ---- Run Tests ----

func TestRunMigrations_FreshDatabase(t *testing.T) {
	db := openTestDB(t)
	migs := loadAllMigrations(t)
	expectedCount := len(migs)

	if err := RunMigrations(db, testMigrationsFS, "."); err != nil {
		t.Fatalf("RunMigrations failed: %v", err)
	}
	var count int
	db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count)
	if count != expectedCount {
		t.Fatalf("expected %d schema_migrations, got %d", expectedCount, count)
	}
}

func TestRunMigrations_Idempotent(t *testing.T) {
	db := openTestDB(t)
	migs := loadAllMigrations(t)
	expectedCount := len(migs)

	if err := RunMigrations(db, testMigrationsFS, "."); err != nil {
		t.Fatalf("first RunMigrations: %v", err)
	}
	if err := RunMigrations(db, testMigrationsFS, "."); err != nil {
		t.Fatalf("second RunMigrations (idempotent): %v", err)
	}
	var count int
	db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count)
	if count != expectedCount {
		t.Errorf("expected %d after idempotent run, got %d", expectedCount, count)
	}
}

func TestRunMigrations_ChecksumMismatch(t *testing.T) {
	db := openTestDB(t)
	if err := RunMigrations(db, testMigrationsFS, "."); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	if _, err := db.Exec(`UPDATE schema_migrations SET checksum = 'tampered' WHERE version = 1`); err != nil {
		t.Fatalf("update checksum: %v", err)
	}
	if err := RunMigrations(db, testMigrationsFS, "."); err == nil {
		t.Fatal("expected checksum mismatch error, got nil")
	} else {
		t.Logf("expected error received: %v", err)
	}
}

func TestRunMigrations_UpgradeFromPreviousSnapshot(t *testing.T) {
	db := openTestDB(t)
	migs := loadAllMigrations(t)

	// Apply only the first N migrations (simulate upgrade from old snapshot).
	if len(migs) < 2 {
		t.Skip("need at least 2 migrations for upgrade test")
	}
	half := len(migs) / 2
	for _, m := range migs[:half] {
		if err := EnsureApplied(db, m); err != nil {
			t.Fatalf("EnsureApplied(%03d): %v", m.Version, err)
		}
	}

	// Verify half applied.
	var count int
	db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count)
	if count != half {
		t.Fatalf("expected %d applied after partial upgrade, got %d", half, count)
	}

	// Apply remaining migrations via RunMigrations.
	if err := RunMigrations(db, testMigrationsFS, "."); err != nil {
		t.Fatalf("RunMigrations upgrade from %d → %d: %v", half, len(migs), err)
	}

	var finalCount int
	db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&finalCount)
	if finalCount != len(migs) {
		t.Fatalf("expected %d after full upgrade, got %d", len(migs), finalCount)
	}
}

// ---- EnsureApplied tests ----

func TestEnsureApplied(t *testing.T) {
	db := openTestDB(t)
	mig := Migration{Version: 99, Name: "test_mig", SQL: `CREATE TABLE IF NOT EXISTS t99 (id INTEGER PRIMARY KEY)`, Checksum: "test"}

	if err := EnsureApplied(db, mig); err != nil {
		t.Fatalf("first EnsureApplied: %v", err)
	}
	if !tableExists(t, db, "t99") {
		t.Error("t99 should exist")
	}
	if err := EnsureApplied(db, mig); err != nil {
		t.Fatalf("second EnsureApplied (idempotent): %v", err)
	}
	var count int
	db.QueryRow(`SELECT COUNT(*) FROM schema_migrations WHERE version = 99`).Scan(&count)
	if count != 1 {
		t.Errorf("expected 1 entry for version 99, got %d", count)
	}
	mig.Checksum = "modified"
	if err := EnsureApplied(db, mig); err == nil {
		t.Fatal("expected checksum mismatch error, got nil")
	}
}

// ---- Version tracking tests ----

func TestAppliedVersions(t *testing.T) {
	db := openTestDB(t)
	migs := loadAllMigrations(t)
	applyAllMigrations(t, db)

	versions, err := AppliedVersions(db)
	if err != nil {
		t.Fatalf("AppliedVersions: %v", err)
	}
	if len(versions) != len(migs) {
		t.Fatalf("expected %d versions, got %d", len(migs), len(versions))
	}
	for i, v := range versions {
		if v != migs[i].Version {
			t.Errorf("versions[%d]: got %d, want %d", i, v, migs[i].Version)
		}
	}
}

func TestPendingVersions(t *testing.T) {
	db := openTestDB(t)
	migs := loadAllMigrations(t)

	EnsureSchemaTable(db)
	pending, _ := PendingVersions(db, testMigrationsFS, ".")
	if len(pending) != len(migs) {
		t.Fatalf("expected %d pending, got %d", len(migs), len(pending))
	}

	if err := RunMigrations(db, testMigrationsFS, "."); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	pending, _ = PendingVersions(db, testMigrationsFS, ".")
	if len(pending) != 0 {
		t.Fatalf("expected 0 pending after apply, got %d", len(pending))
	}
}

// ---- End-to-end integration tests ----

func TestIntegration_MigrationRunner_EndToEnd(t *testing.T) {
	db := openTestDB(t)
	applyAllMigrations(t, db)

	// Verify all tables from migration 001 exist
	tables := []string{
		"jobs", "job_history", "job_logs", "workers", "worker_flags",
		"analytics_cache", "drive_links", "youtube_api_cache",
		"dark_editor_projects", "dark_editor_folders", "dark_editor_assets",
		"dark_editor_templates", "dark_editor_temp_files", "dark_editor_generations",
		"youtube_channel_metrics", "youtube_revenue_metrics", "youtube_video_metrics",
		"youtube_quota_usage", "calendar_events", "worker_validations",
	}
	for _, table := range tables {
		if !tableExists(t, db, table) {
			t.Errorf("table %s missing", table)
		}
	}

	// Verify specific migration columns
	type colCheck struct{ table, col string }
	for _, cc := range []colCheck{
		{"workers", "display_name"}, {"workers", "ip_address"}, {"workers", "first_seen"},
		{"workers", "current_job"}, {"workers", "code_version"}, {"workers", "bundle_version"},
		{"workers", "bundle_hash"}, {"workers", "protocol_version"}, {"workers", "engine_version"},
		{"workers", "capabilities"},
	} {
		if !columnExists(t, db, cc.table, cc.col) {
			t.Errorf("column %s.%s missing", cc.table, cc.col)
		}
	}

	// YouTube CRUD
	db.Exec(`INSERT INTO youtube_channels (channel_id, title, created_at, updated_at) VALUES ('UC_a', 'Alpha', datetime('now'), datetime('now'))`)
	db.Exec(`INSERT INTO youtube_groups_v2 (name, group_type, created_at, updated_at) VALUES ('Sports', 'manager', datetime('now'), datetime('now'))`)
	db.Exec(`INSERT INTO youtube_group_channels (group_id, channel_id, position, added_at) VALUES (1, 'UC_a', 0, datetime('now'))`)
	db.Exec(`INSERT INTO youtube_tracked_niches (niche, created_at) VALUES ('basketball', datetime('now'))`)

	var title string
	db.QueryRow(`SELECT title FROM youtube_channels WHERE channel_id='UC_a'`).Scan(&title)
	if title != "Alpha" {
		t.Errorf("channel title: got %q, want %q", title, "Alpha")
	}

	// FK cascade delete
	db.Exec(`DELETE FROM youtube_channels WHERE channel_id='UC_a'`)
	var memberCount int
	db.QueryRow(`SELECT COUNT(*) FROM youtube_group_channels WHERE channel_id='UC_a'`).Scan(&memberCount)
	if memberCount != 0 {
		t.Errorf("expected 0 memberships after cascade, got %d", memberCount)
	}

	// Ansible CRUD
	db.Exec(`INSERT INTO ansible_hosts (host, ansible_user, enabled, created_at, updated_at) VALUES ('vm-01', 'pierone', 1, datetime('now'), datetime('now'))`)
	db.Exec(`INSERT INTO ansible_runs (run_id, action, status, created_at) VALUES ('run-01', 'deploy', 'success', datetime('now'))`)
	db.Exec(`INSERT INTO ansible_run_hosts (run_id, host) VALUES ('run-01', 'vm-01')`)
	db.Exec(`DELETE FROM ansible_runs WHERE run_id='run-01'`)
	var runHostCount int
	db.QueryRow(`SELECT COUNT(*) FROM ansible_run_hosts WHERE run_id='run-01'`).Scan(&runHostCount)
	if runHostCount != 0 {
		t.Errorf("expected 0 run hosts after CASCADE, got %d", runHostCount)
	}

	// Legacy tables dropped (migration 009)
	// Note: youtube_groups is NOT in this list because migration 012
	// renames youtube_groups_v2 → youtube_groups (canonical name).
	for _, table := range []string{"youtube_channel_metadata", "youtube_manager_channels", "youtube_manager_groups", "ansible_computers"} {
		if tableExists(t, db, table) {
			t.Errorf("migration 009 should have dropped %s", table)
		}
	}
}

func TestIntegration_NewSQLiteStore_AutoMigration(t *testing.T) {
	t.Parallel()
	migs := loadAllMigrations(t)
	expectedCount := len(migs)
	dbPath := t.TempDir() + "/test.db"

	db, _ := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000&_journal_mode=WAL")
	RunMigrations(db, testMigrationsFS, ".")
	var c1 int
	db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&c1)
	if c1 != expectedCount {
		t.Fatalf("first open: expected %d, got %d", expectedCount, c1)
	}
	db.Close()

	db2, _ := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000&_journal_mode=WAL")
	defer db2.Close()
	RunMigrations(db2, testMigrationsFS, ".")
	var c2 int
	db2.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&c2)
	if c2 != expectedCount {
		t.Fatalf("second open: expected %d, got %d", expectedCount, c2)
	}
}

func TestMigration008_UpgradeEndToEnd(t *testing.T) {
	db := openTestDB(t)
	migs := loadAllMigrations(t)

	// Apply migrations 1-7
	for _, m := range migs {
		if m.Version >= 8 {
			break
		}
		EnsureApplied(db, m)
	}

	// Insert legacy data
	db.Exec(`INSERT INTO youtube_channel_metadata (channel_id, title, token_path, language, added_date, last_used, raw_json, updated_at) VALUES ('UC_a', 'Alpha', '/t/a.json', 'en', '2023-01-15', '2024-06-01', '{}', datetime('now'))`)
	db.Exec(`INSERT INTO youtube_channel_metadata (channel_id, title, token_path, language, added_date, last_used, raw_json, updated_at) VALUES ('UC_b', 'Beta', '/t/b.json', 'it', '2023-03-20', '2024-05-15', '{}', datetime('now'))`)
	db.Exec(`INSERT INTO youtube_groups (name, description, privacy, channels_json, updated_at) VALUES ('WNBA', 'highlights', 'public', '["UC_a","UC_b"]', datetime('now'))`)
	db.Exec(`INSERT INTO youtube_manager_channels (channel_id, group_name, url, title, name, thumbnail, language, view_count, sub_count, raw_json, updated_at) VALUES ('UC_c', 'Sports', 'https://youtube.com/@s', 'Sports', 'Sports', 't.jpg', 'en', 50000, 2000, '{}', datetime('now'))`)
	db.Exec(`INSERT INTO youtube_manager_groups (name, created_at, group_type, tracked_niches_json, updated_at) VALUES ('Sports', '2023-01-01', 'manager', '["basketball"]', datetime('now'))`)
	db.Exec(`INSERT INTO ansible_computers (host, raw_json, updated_at) VALUES ('srv-01', '{"host":"srv-01","ansible_user":"pierone","enabled":true}', datetime('now'))`)

	// Apply migration 008 (data copy)
	EnsureApplied(db, migs[7])
	for _, table := range []string{"youtube_channel_metadata", "youtube_groups", "youtube_manager_channels", "youtube_manager_groups", "ansible_computers"} {
		if !tableExists(t, db, table) {
			t.Errorf("008 should NOT have dropped %s", table)
		}
	}

	// Apply migration 009 (DROP)
	EnsureApplied(db, migs[8])
	for _, table := range []string{"youtube_channel_metadata", "youtube_groups", "youtube_manager_channels", "youtube_manager_groups", "ansible_computers"} {
		if tableExists(t, db, table) {
			t.Errorf("009 should have dropped %s", table)
		}
	}

	// Verify zero data loss
	var chCount int
	db.QueryRow(`SELECT COUNT(*) FROM youtube_channels`).Scan(&chCount)
	if chCount != 3 {
		t.Errorf("expected 3 channels (2 metadata + 1 manager), got %d", chCount)
	}
	var grpCount int
	db.QueryRow(`SELECT COUNT(*) FROM youtube_groups_v2`).Scan(&grpCount)
	if grpCount < 2 {
		t.Errorf("expected at least 2 groups, got %d", grpCount)
	}
	var hostCount int
	db.QueryRow(`SELECT COUNT(*) FROM ansible_hosts`).Scan(&hostCount)
	if hostCount != 1 {
		t.Errorf("expected 1 ansible host, got %d", hostCount)
	}

	// FK + integrity check
	fkRows, _ := db.Query("PRAGMA foreign_key_check")
	defer fkRows.Close()
	var fkViolations int
	for fkRows.Next() {
		fkViolations++
	}
	if fkViolations > 0 {
		t.Errorf("FK violations after migration: %d", fkViolations)
	}
}

// ---- Integration: checksum validation ----

func TestRunMigrations_ChecksumMatchesContent(t *testing.T) {
	migs := loadAllMigrations(t)
	for _, m := range migs {
		expected := fmt.Sprintf("%x", sha256.Sum256([]byte(m.SQL)))
		if m.Checksum != expected {
			t.Errorf("migration %03d: checksum does not match SQL content", m.Version)
		}
	}
}

// ---- Helpers ----

func applyAllMigrations(t *testing.T, db *sql.DB) {
	t.Helper()
	if err := RunMigrations(db, testMigrationsFS, "."); err != nil {
		t.Fatalf("RunMigrations failed: %v", err)
	}
}

func tableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var count int
	db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, name).Scan(&count)
	return count > 0
}

func columnExists(t *testing.T, db *sql.DB, table, col string) bool {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return false
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, colType string
		var notnull int
		var dflt sql.NullString
		var pk int
		rows.Scan(&cid, &name, &colType, &notnull, &dflt, &pk)
		if name == col {
			return true
		}
	}
	return false
}

// Ensure sort is used (for future tests that may sort slices).
var _ = sort.Ints
