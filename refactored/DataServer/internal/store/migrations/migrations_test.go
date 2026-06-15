package migrations

import (
	"database/sql"
	"embed"
	"fmt"
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

func TestDiscoverMigrations_AllVersions(t *testing.T) {
	migs, err := discoverMigrations(testMigrationsFS, ".")
	if err != nil {
		t.Fatalf("discoverMigrations failed: %v", err)
	}
	if len(migs) != 9 {
		t.Fatalf("expected 9 migrations, got %d", len(migs))
	}
	expected := []struct {
		Version int
		Name    string
	}{
		{1, "initial"}, {2, "legacy_imports"}, {3, "youtube_canonical"},
		{4, "ansible"}, {5, "legacy_cleanup"}, {6, "drive_links_source_of_truth"},
		{7, "queue_persistence"}, {8, "drop_legacy_tables"}, {9, "drop_legacy_tables"},
	}
	for i, exp := range expected {
		if migs[i].Version != exp.Version || migs[i].Name != exp.Name {
			t.Errorf("migration[%d]: got %d/%q, want %d/%q", i, migs[i].Version, migs[i].Name, exp.Version, exp.Name)
		}
		if migs[i].Checksum == "" || migs[i].SQL == "" {
			t.Errorf("migration[%d]: empty checksum or SQL", i)
		}
	}
}

func TestRunMigrations_FullLifecycle(t *testing.T) {
	db := openTestDB(t)
	if err := RunMigrations(db, testMigrationsFS, "."); err != nil {
		t.Fatalf("RunMigrations failed: %v", err)
	}
	expectedCount := len(discoverMigrationsOrFatal(t))
	var count int
	db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count)
	if count != expectedCount {
		t.Fatalf("expected %d schema_migrations, got %d", expectedCount, count)
	}
}

func TestRunMigrations_Idempotent(t *testing.T) {
	db := openTestDB(t)
	expectedCount := len(discoverMigrationsOrFatal(t))
	RunMigrations(db, testMigrationsFS, ".")
	RunMigrations(db, testMigrationsFS, ".")
	var count int
	db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count)
	if count != expectedCount {
		t.Errorf("expected %d after idempotent run, got %d", expectedCount, count)
	}
}

func TestRunMigrations_ChecksumMismatch(t *testing.T) {
	db := openTestDB(t)
	RunMigrations(db, testMigrationsFS, ".")
	db.Exec(`UPDATE schema_migrations SET checksum = 'tampered' WHERE version = 3`)
	if err := RunMigrations(db, testMigrationsFS, "."); err == nil {
		t.Fatal("expected checksum mismatch error, got nil")
	}
}

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

func TestAppliedVersions(t *testing.T) {
	db := openTestDB(t)
	applyAllMigrations(t, db)
	expected := discoverMigrationsOrFatal(t)
	versions, err := AppliedVersions(db)
	if err != nil {
		t.Fatalf("AppliedVersions: %v", err)
	}
	if len(versions) != len(expected) {
		t.Fatalf("expected %d versions, got %d", len(expected), len(versions))
	}
	for i, v := range versions {
		if v != expected[i].Version {
			t.Errorf("versions[%d]: got %d, want %d", i, v, expected[i].Version)
		}
	}
}

func TestPendingVersions(t *testing.T) {
	db := openTestDB(t)
	expected := discoverMigrationsOrFatal(t)
	ensureSchemaTable(db)
	pending, _ := PendingVersions(db, testMigrationsFS, ".")
	if len(pending) != len(expected) {
		t.Fatalf("expected %d pending, got %d", len(expected), len(pending))
	}
	RunMigrations(db, testMigrationsFS, ".")
	pending, _ = PendingVersions(db, testMigrationsFS, ".")
	if len(pending) != 0 {
		t.Fatalf("expected 0 pending after apply, got %d", len(pending))
	}
}

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

	// Verify columns from migration 002
	for _, cc := range []struct{ table, col string }{
		{"workers", "display_name"}, {"workers", "ip_address"}, {"workers", "first_seen"},
		{"workers", "current_job"}, {"workers", "code_version"}, {"workers", "bundle_version"},
		{"workers", "bundle_hash"}, {"workers", "protocol_version"}, {"workers", "engine_version"},
		{"workers", "capabilities"},
	} {
		if !columnExists(t, db, cc.table, cc.col) {
			t.Errorf("column %s.%s missing", cc.table, cc.col)
		}
	}

	// YouTube CRUD (migration 003)
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

	// Ansible CRUD (migration 004)
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
	for _, table := range []string{"youtube_channel_metadata", "youtube_groups", "youtube_manager_channels", "youtube_manager_groups", "ansible_computers"} {
		if tableExists(t, db, table) {
			t.Errorf("migration 009 should have dropped %s", table)
		}
	}
	if !tableExists(t, db, "legacy_json_registry") {
		t.Error("legacy_json_registry missing")
	}
}

func TestIntegration_NewSQLiteStore_AutoMigration(t *testing.T) {
	t.Parallel()
	expectedCount := len(discoverMigrationsOrFatal(t))
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
	migs, _ := discoverMigrations(testMigrationsFS, ".")

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
	// Legacy tables still present after 008
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

func applyAllMigrations(t *testing.T, db *sql.DB) {
	t.Helper()
	if err := RunMigrations(db, testMigrationsFS, "."); err != nil {
		t.Fatalf("RunMigrations failed: %v", err)
	}
}

func discoverMigrationsOrFatal(t *testing.T) []Migration {
	t.Helper()
	migs, err := discoverMigrations(testMigrationsFS, ".")
	if err != nil {
		t.Fatalf("discoverMigrations: %v", err)
	}
	return migs
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
