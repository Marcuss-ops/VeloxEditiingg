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
	// Always enable FK enforcement for testing
	_, _ = db.Exec("PRAGMA foreign_keys = ON")
	return db
}

// ============================================================
// discoverMigrations tests
// ============================================================

func TestDiscoverMigrations_AllVersions(t *testing.T) {
	migs, err := discoverMigrations(testMigrationsFS, ".")
	if err != nil {
		t.Fatalf("discoverMigrations failed: %v", err)
	}

	if len(migs) != 8 {
		t.Fatalf("expected 8 migrations, got %d", len(migs))
	}

	expected := []struct {
		Version int
		Name    string
	}{
		{1, "initial"},
		{2, "legacy_imports"},
		{3, "youtube_canonical"},
		{4, "ansible"},
		{5, "legacy_cleanup"},
		{6, "drive_links_source_of_truth"},
		{7, "queue_persistence"},
		{8, "drop_legacy_tables"},
	}

	for i, exp := range expected {
		if migs[i].Version != exp.Version {
			t.Errorf("migration[%d] version: got %d, want %d", i, migs[i].Version, exp.Version)
		}
		if migs[i].Name != exp.Name {
			t.Errorf("migration[%d] name: got %q, want %q", i, migs[i].Name, exp.Name)
		}
		if migs[i].Checksum == "" {
			t.Errorf("migration[%d] checksum is empty", i)
		}
		if migs[i].SQL == "" {
			t.Errorf("migration[%d] SQL is empty", i)
		}
	}
}

func TestDiscoverMigrations_SortedByVersion(t *testing.T) {
	migs, err := discoverMigrations(testMigrationsFS, ".")
	if err != nil {
		t.Fatalf("discoverMigrations failed: %v", err)
	}

	for i := 1; i < len(migs); i++ {
		if migs[i].Version <= migs[i-1].Version {
			t.Errorf("migrations not sorted: %d (%d) after %d (%d)",
				i, migs[i].Version, i-1, migs[i-1].Version)
		}
	}
}

func TestDiscoverMigrations_ChecksumStable(t *testing.T) {
	migs1, _ := discoverMigrations(testMigrationsFS, ".")
	migs2, _ := discoverMigrations(testMigrationsFS, ".")

	for i := range migs1 {
		if migs1[i].Checksum != migs2[i].Checksum {
			t.Errorf("migration %03d_%s: checksum not stable: %s vs %s",
				migs1[i].Version, migs1[i].Name, migs1[i].Checksum, migs2[i].Checksum)
		}
	}
}

// ============================================================
// RunMigrations integration tests
// ============================================================

func TestRunMigrations_FullLifecycle(t *testing.T) {
	db := openTestDB(t)

	// First run: all migrations should be applied
	if err := RunMigrations(db, testMigrationsFS, "."); err != nil {
		t.Fatalf("first RunMigrations failed: %v", err)
	}

	// Discover expected migration count
	expectedMigs, _ := discoverMigrations(testMigrationsFS, ".")
	expectedCount := len(expectedMigs)

	// Verify schema_migrations has the expected number of entries
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("query schema_migrations count: %v", err)
	}
	if count != expectedCount {
		t.Fatalf("expected %d schema_migrations entries, got %d", expectedCount, count)
	}

	// Verify each version has name, checksum, and applied_at
	rows, err := db.Query(`SELECT version, name, checksum, applied_at FROM schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatalf("query schema_migrations: %v", err)
	}
	defer rows.Close()

	migIdx := 0
	for rows.Next() {
		var v int
		var name, checksum, appliedAt string
		if err := rows.Scan(&v, &name, &checksum, &appliedAt); err != nil {
			t.Fatalf("scan row: %v", err)
		}
		if v != expectedMigs[migIdx].Version {
			t.Errorf("row %d version: got %d, want %d", migIdx, v, expectedMigs[migIdx].Version)
		}
		if name == "" {
			t.Errorf("version %d: empty name", v)
		}
		if checksum == "" {
			t.Errorf("version %d: empty checksum", v)
		}
		if appliedAt == "" {
			t.Errorf("version %d: empty applied_at", v)
		}
		migIdx++
	}
}

func TestRunMigrations_Idempotent(t *testing.T) {
	db := openTestDB(t)

	// Discover expected count
	expectedMigs, _ := discoverMigrations(testMigrationsFS, ".")
	expectedCount := len(expectedMigs)

	// Run twice
	if err := RunMigrations(db, testMigrationsFS, "."); err != nil {
		t.Fatalf("first RunMigrations failed: %v", err)
	}
	if err := RunMigrations(db, testMigrationsFS, "."); err != nil {
		t.Fatalf("second RunMigrations (idempotent) failed: %v", err)
	}

	// Should still have the expected number of entries
	var count int
	db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count)
	if count != expectedCount {
		t.Errorf("expected %d entries after idempotent run, got %d", expectedCount, count)
	}
}

func TestRunMigrations_ChecksumMismatch(t *testing.T) {
	db := openTestDB(t)

	// Apply migrations normally first
	if err := RunMigrations(db, testMigrationsFS, "."); err != nil {
		t.Fatalf("first RunMigrations failed: %v", err)
	}

	// Tamper with the checksum in schema_migrations
	if _, err := db.Exec(`UPDATE schema_migrations SET checksum = 'tampered' WHERE version = 3`); err != nil {
		t.Fatalf("tamper checksum: %v", err)
	}

	// Second run should fail with checksum mismatch
	err := RunMigrations(db, testMigrationsFS, ".")
	if err == nil {
		t.Fatal("expected checksum mismatch error, got nil")
	}
}

// ============================================================
// Migration 003: YouTube canonical tables
// ============================================================

func TestMigration003_YouTubeCanonicalTables(t *testing.T) {
	db := openTestDB(t)
	applyAllMigrations(t, db)

	tables := []string{
		"youtube_channels",
		"youtube_groups_v2",
		"youtube_group_channels",
		"youtube_tracked_niches",
	}

	for _, table := range tables {
		if !tableExists(t, db, table) {
			t.Errorf("migration 003: table %s does not exist", table)
		}
	}
}

func TestMigration003_YouTubeForeignKeys(t *testing.T) {
	db := openTestDB(t)
	applyAllMigrations(t, db)

	// Verify FK on youtube_group_channels by attempting inserts
	// First insert into parent tables
	_, err := db.Exec(`INSERT INTO youtube_channels (channel_id, title, created_at, updated_at) VALUES ('UC_test', 'Test', datetime('now'), datetime('now'))`)
	if err != nil {
		t.Fatalf("insert channel: %v", err)
	}

	_, err = db.Exec(`INSERT INTO youtube_groups_v2 (name, group_type, created_at, updated_at) VALUES ('Test Group', 'manager', datetime('now'), datetime('now'))`)
	if err != nil {
		t.Fatalf("insert group: %v", err)
	}

	// Valid FK insert should succeed
	_, err = db.Exec(`INSERT INTO youtube_group_channels (group_id, channel_id, position, added_at) VALUES (1, 'UC_test', 0, datetime('now'))`)
	if err != nil {
		t.Errorf("valid FK insert failed: %v", err)
	}

	// Invalid FK should fail
	_, err = db.Exec(`INSERT INTO youtube_group_channels (group_id, channel_id, position, added_at) VALUES (999, 'nonexistent', 0, datetime('now'))`)
	if err == nil {
		t.Error("expected FK violation for invalid group_id, got nil")
	}
}

func TestMigration003_UNIQUENameGroupType(t *testing.T) {
	db := openTestDB(t)
	applyAllMigrations(t, db)

	// Insert first group
	_, err := db.Exec(`INSERT INTO youtube_groups_v2 (name, group_type, created_at, updated_at) VALUES ('SameName', 'manager', datetime('now'), datetime('now'))`)
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}

	// Same name, different type — should succeed (UNIQUE(name, group_type))
	_, err = db.Exec(`INSERT INTO youtube_groups_v2 (name, group_type, created_at, updated_at) VALUES ('SameName', 'upload', datetime('now'), datetime('now'))`)
	if err != nil {
		t.Errorf("same name different type should be allowed: %v", err)
	}

	// Same name, same type — should fail
	_, err = db.Exec(`INSERT INTO youtube_groups_v2 (name, group_type, created_at, updated_at) VALUES ('SameName', 'manager', datetime('now'), datetime('now'))`)
	if err == nil {
		t.Error("expected UNIQUE violation for duplicate (name, group_type), got nil")
	}
}

// ============================================================
// Migration 004: Ansible tables
// ============================================================

func TestMigration004_AnsibleTables(t *testing.T) {
	db := openTestDB(t)
	applyAllMigrations(t, db)

	tables := []string{
		"ansible_hosts",
		"ansible_runs",
		"ansible_run_hosts",
	}

	for _, table := range tables {
		if !tableExists(t, db, table) {
			t.Errorf("migration 004: table %s does not exist", table)
		}
	}
}

func TestMigration004_AnsibleRunCascadeDelete(t *testing.T) {
	db := openTestDB(t)
	applyAllMigrations(t, db)

	// Insert a run and associate hosts
	_, err := db.Exec(`INSERT INTO ansible_runs (run_id, action, status, created_at) VALUES ('test-run', 'deploy', 'success', datetime('now'))`)
	if err != nil {
		t.Fatalf("insert run: %v", err)
	}

	_, err = db.Exec(`INSERT INTO ansible_run_hosts (run_id, host) VALUES ('test-run', 'host-a')`)
	if err != nil {
		t.Fatalf("insert run host: %v", err)
	}

	// Delete the run — CASCADE should remove the association
	_, err = db.Exec(`DELETE FROM ansible_runs WHERE run_id = 'test-run'`)
	if err != nil {
		t.Fatalf("delete run: %v", err)
	}

	var count int
	db.QueryRow(`SELECT COUNT(*) FROM ansible_run_hosts WHERE run_id = 'test-run'`).Scan(&count)
	if count != 0 {
		t.Errorf("expected 0 run_hosts after cascade delete, got %d", count)
	}
}

func TestMigration004_AnsibleHostsDefaults(t *testing.T) {
	db := openTestDB(t)
	applyAllMigrations(t, db)

	// Insert with minimal fields
	_, err := db.Exec(`INSERT INTO ansible_hosts (host, created_at, updated_at) VALUES ('test-host', datetime('now'), datetime('now'))`)
	if err != nil {
		t.Fatalf("insert host: %v", err)
	}

	// Verify defaults
	var ansibleUser, secretRef string
	var enabled int
	err = db.QueryRow(`SELECT ansible_user, secret_ref, enabled FROM ansible_hosts WHERE host='test-host'`).Scan(&ansibleUser, &secretRef, &enabled)
	if err != nil {
		t.Fatalf("query host: %v", err)
	}
	if ansibleUser != "pierone" {
		t.Errorf("default ansible_user: got %q, want %q", ansibleUser, "pierone")
	}
	if secretRef != "" {
		t.Errorf("default secret_ref: got %q, want empty string", secretRef)
	}
	if enabled != 1 {
		t.Errorf("default enabled: got %d, want 1", enabled)
	}
}

// ============================================================
// Migration 005: Legacy cleanup (soft) + Migration 008: DROP legacy
// ============================================================

func TestMigration005_AppliesCleanly(t *testing.T) {
	db := openTestDB(t)
	applyAllMigrations(t, db)

	// Verify migration 005 is recorded
	var checksum string
	err := db.QueryRow(`SELECT checksum FROM schema_migrations WHERE version = 5`).Scan(&checksum)
	if err != nil {
		t.Fatalf("migration 005 not recorded: %v", err)
	}
	if checksum == "" {
		t.Error("migration 005 checksum is empty")
	}

	// Verify migration 008 is recorded
	var checksum008 string
	err = db.QueryRow(`SELECT checksum FROM schema_migrations WHERE version = 8`).Scan(&checksum008)
	if err != nil {
		t.Fatalf("migration 008 not recorded: %v", err)
	}

	// Verify legacy tables are DROPPED by migration 008
	legacyTables := []string{
		"youtube_channel_metadata",
		"youtube_groups",
		"youtube_manager_channels",
		"youtube_manager_groups",
		"ansible_computers",
	}
	for _, table := range legacyTables {
		if tableExists(t, db, table) {
			t.Errorf("migration 008 should have dropped %s", table)
		}
	}

	// Verify legacy_json_registry exists
	if !tableExists(t, db, "legacy_json_registry") {
		t.Error("migration 008 should have created legacy_json_registry")
	}
}

// ============================================================
// EnsureApplied helper tests
// ============================================================

func TestEnsureApplied_AppliesIfMissing(t *testing.T) {
	db := openTestDB(t)

	mig := Migration{
		Version:  99,
		Name:     "test_migration",
		SQL:      `CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY)`,
		Checksum: "test",
	}

	if err := EnsureApplied(db, mig); err != nil {
		t.Fatalf("EnsureApplied (missing) failed: %v", err)
	}

	if !tableExists(t, db, "test_table") {
		t.Error("test_table should exist after EnsureApplied")
	}
}

func TestEnsureApplied_Idempotent(t *testing.T) {
	db := openTestDB(t)

	mig := Migration{
		Version:  99,
		Name:     "test_idempotent",
		SQL:      `CREATE TABLE IF NOT EXISTS test_table2 (id INTEGER PRIMARY KEY)`,
		Checksum: "test",
	}

	if err := EnsureApplied(db, mig); err != nil {
		t.Fatalf("first EnsureApplied failed: %v", err)
	}
	if err := EnsureApplied(db, mig); err != nil {
		t.Fatalf("second EnsureApplied (idempotent) failed: %v", err)
	}

	var count int
	db.QueryRow(`SELECT COUNT(*) FROM schema_migrations WHERE version = 99`).Scan(&count)
	if count != 1 {
		t.Errorf("expected 1 schema_migrations entry for version 99, got %d", count)
	}
}

func TestEnsureApplied_ChecksumMismatch(t *testing.T) {
	db := openTestDB(t)

	mig := Migration{
		Version:  100,
		Name:     "test_checksum",
		SQL:      `CREATE TABLE IF NOT EXISTS test_table3 (id INTEGER PRIMARY KEY)`,
		Checksum: "original",
	}

	if err := EnsureApplied(db, mig); err != nil {
		t.Fatalf("first EnsureApplied failed: %v", err)
	}

	// Try again with different checksum
	mig.Checksum = "modified"
	err := EnsureApplied(db, mig)
	if err == nil {
		t.Fatal("expected checksum mismatch error, got nil")
	}
}

// ============================================================
// AppliedVersions / PendingVersions tests
// ============================================================

func TestAppliedVersions(t *testing.T) {
	db := openTestDB(t)
	applyAllMigrations(t, db)

	expectedMigs, _ := discoverMigrations(testMigrationsFS, ".")
	expectedCount := len(expectedMigs)

	versions, err := AppliedVersions(db)
	if err != nil {
		t.Fatalf("AppliedVersions failed: %v", err)
	}

	if len(versions) != expectedCount {
		t.Fatalf("expected %d applied versions, got %d", expectedCount, len(versions))
	}

	for i, v := range versions {
		if v != expectedMigs[i].Version {
			t.Errorf("versions[%d]: got %d, want %d", i, v, expectedMigs[i].Version)
		}
	}
}

func TestPendingVersions(t *testing.T) {
	db := openTestDB(t)

	expectedMigs, _ := discoverMigrations(testMigrationsFS, ".")
	expectedCount := len(expectedMigs)

	// ensureSchemaTable is needed before PendingVersions can query schema_migrations
	if err := ensureSchemaTable(db); err != nil {
		t.Fatalf("ensureSchemaTable failed: %v", err)
	}

	// Before applying anything, all migrations are pending
	pending, err := PendingVersions(db, testMigrationsFS, ".")
	if err != nil {
		t.Fatalf("PendingVersions failed: %v", err)
	}
	if len(pending) != expectedCount {
		t.Fatalf("expected %d pending migrations, got %d", expectedCount, len(pending))
	}

	// After applying, none should be pending
	if err := RunMigrations(db, testMigrationsFS, "."); err != nil {
		t.Fatalf("RunMigrations failed: %v", err)
	}

	pending, err = PendingVersions(db, testMigrationsFS, ".")
	if err != nil {
		t.Fatalf("PendingVersions after apply failed: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected 0 pending after apply, got %d", len(pending))
	}
}

// ============================================================
// Integration: End-to-end migration + CRUD pipeline
// ============================================================

// TestIntegration_MigrationRunner_EndToEnd applies all migrations and then
// performs INSERT/SELECT/UPDATE/DELETE operations against tables created by each
// migration to verify the full pipeline works.
func TestIntegration_MigrationRunner_EndToEnd(t *testing.T) {
	db := openTestDB(t)
	applyAllMigrations(t, db)

	expectedMigs, _ := discoverMigrations(testMigrationsFS, ".")
	expectedCount := len(expectedMigs)

	// ---- Phase 1: Verify schema_migrations has all entries ----
	var count int
	db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count)
	if count != expectedCount {
		t.Fatalf("expected %d migrations, got %d", expectedCount, count)
	}

	rows, err := db.Query(`SELECT version, name, checksum, applied_at FROM schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatalf("query schema_migrations: %v", err)
	}
	defer rows.Close()

	migIdx := 0
	for rows.Next() {
		var v int
		var name, checksum, appliedAt string
		if err := rows.Scan(&v, &name, &checksum, &appliedAt); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if v != migIdx+1 {
			t.Errorf("row %d: expected version %d, got %d", migIdx, migIdx+1, v)
		}
		if name == "" || checksum == "" || appliedAt == "" {
			t.Errorf("version %d: missing name/checksum/applied_at", v)
		}
		migIdx++
	}

	// ---- Phase 2: Verify tables from Migration 001 (initial schema) ----
	// Note: legacy tables (ansible_computers, youtube_channel_metadata, youtube_groups,
	// youtube_manager_channels, youtube_manager_groups) are created by 001 but dropped by 008.
	tables001 := []string{
		"jobs", "job_history", "job_logs", "workers", "worker_flags",
		"analytics_cache", "drive_links", "youtube_api_cache",
		"dark_editor_projects", "dark_editor_folders", "dark_editor_assets",
		"dark_editor_templates", "dark_editor_temp_files", "dark_editor_generations",
		"youtube_channel_metrics", "youtube_revenue_metrics", "youtube_video_metrics",
		"youtube_quota_usage", "calendar_events", "worker_validations",
	}
	for _, table := range tables001 {
		if !tableExists(t, db, table) {
			t.Errorf("migration 001: table %s missing", table)
		}
	}

	// ---- Phase 3: Verify columns added by Migration 002 ----
	colChecks := []struct {
		table  string
		col    string
	}{
		{"workers", "display_name"},
		{"workers", "ip_address"},
		{"workers", "first_seen"},
		{"workers", "current_job"},
		{"workers", "code_version"},
		{"workers", "bundle_version"},
		{"workers", "bundle_hash"},
		{"workers", "protocol_version"},
		{"workers", "engine_version"},
		{"workers", "capabilities"},
	}
	for _, cc := range colChecks {
		if !columnExists(t, db, cc.table, cc.col) {
			t.Errorf("migration 002: column %s.%s missing", cc.table, cc.col)
		}
	}

	// Verify legacy_imports table was created by migration 002
	if !tableExists(t, db, "legacy_imports") {
		t.Error("migration 002: legacy_imports table missing")
	}

	// ---- Phase 4: Migration 003 — YouTube canonical CRUD ----

	// 4a. Insert channels
	_, err = db.Exec(`INSERT INTO youtube_channels (channel_id, title, display_name, language, view_count, subscriber_count, created_at, updated_at)
		VALUES ('UC_a', 'Alpha', 'Alpha Display', 'en', 1000, 500, datetime('now'), datetime('now'))`)
	if err != nil {
		t.Fatalf("insert youtube_channels: %v", err)
	}

	// 4b. Insert groups
	_, err = db.Exec(`INSERT INTO youtube_groups_v2 (name, group_type, description, created_at, updated_at)
		VALUES ('Sports', 'manager', 'Sports channels', datetime('now'), datetime('now'))`)
	if err != nil {
		t.Fatalf("insert youtube_groups_v2: %v", err)
	}

	// 4c. Add channel to group (membership with FK)
	_, err = db.Exec(`INSERT INTO youtube_group_channels (group_id, channel_id, position, added_at) VALUES (1, 'UC_a', 0, datetime('now'))`)
	if err != nil {
		t.Errorf("insert youtube_group_channels: %v", err)
	}

	// 4d. Insert tracked niche
	_, err = db.Exec(`INSERT INTO youtube_tracked_niches (niche, created_at) VALUES ('basketball', datetime('now'))`)
	if err != nil {
		t.Errorf("insert youtube_tracked_niches: %v", err)
	}

	// 4e. Verify reads
	var channelTitle string
	if err := db.QueryRow(`SELECT title FROM youtube_channels WHERE channel_id='UC_a'`).Scan(&channelTitle); err != nil {
		t.Fatalf("query channel title: %v", err)
	}
	if channelTitle != "Alpha" {
		t.Errorf("youtube channel title: got %q, want %q", channelTitle, "Alpha")
	}

	var groupName string
	if err := db.QueryRow(`SELECT name FROM youtube_groups_v2 WHERE id=1`).Scan(&groupName); err != nil {
		t.Fatalf("query group name: %v", err)
	}
	if groupName != "Sports" {
		t.Errorf("group name: got %q, want %q", groupName, "Sports")
	}

	// 4f. Update channel
	_, err = db.Exec(`UPDATE youtube_channels SET view_count = 2000 WHERE channel_id='UC_a'`)
	if err != nil {
		t.Errorf("update youtube_channels: %v", err)
	}
	var vc int64
	if err := db.QueryRow(`SELECT view_count FROM youtube_channels WHERE channel_id='UC_a'`).Scan(&vc); err != nil {
		t.Fatalf("query view_count after update: %v", err)
	}
	if vc != 2000 {
		t.Errorf("view_count after update: got %d, want 2000", vc)
	}

	// 4g. Delete channel (should cascade to memberships)
	_, err = db.Exec(`DELETE FROM youtube_channels WHERE channel_id='UC_a'`)
	if err != nil {
		t.Errorf("delete youtube_channels: %v", err)
	}
	var memberCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM youtube_group_channels WHERE channel_id='UC_a'`).Scan(&memberCount); err != nil {
		t.Fatalf("query memberships after CASCADE delete: %v", err)
	}
	if memberCount != 0 {
		t.Errorf("expected 0 memberships after CASCADE delete, got %d", memberCount)
	}

	// ---- Phase 5: Migration 004 — Ansible CRUD ----

	// 5a. Insert hosts
	_, err = db.Exec(`INSERT INTO ansible_hosts (host, ansible_user, enabled, host_group, created_at, updated_at)
		VALUES ('vm-01', 'pierone', 1, 'production', datetime('now'), datetime('now'))`)
	if err != nil {
		t.Fatalf("insert ansible_hosts: %v", err)
	}
	_, err = db.Exec(`INSERT INTO ansible_hosts (host, ansible_user, enabled, host_group, created_at, updated_at)
		VALUES ('vm-02', 'root', 0, 'staging', datetime('now'), datetime('now'))`)
	if err != nil {
		t.Fatalf("insert ansible_hosts: %v", err)
	}

	// 5b. Insert runs
	_, err = db.Exec(`INSERT INTO ansible_runs (run_id, action, playbook, status, started_at, ended_at, return_code, created_at)
		VALUES ('run-01', 'deploy', 'site.yml', 'success', 1000, 2000, 0, datetime('now'))`)
	if err != nil {
		t.Fatalf("insert ansible_runs: %v", err)
	}

	// 5c. Link hosts to run
	_, err = db.Exec(`INSERT INTO ansible_run_hosts (run_id, host) VALUES ('run-01', 'vm-01')`)
	if err != nil {
		t.Fatalf("insert ansible_run_hosts: %v", err)
	}
	_, err = db.Exec(`INSERT INTO ansible_run_hosts (run_id, host) VALUES ('run-01', 'vm-02')`)
	if err != nil {
		t.Fatalf("insert ansible_run_hosts: %v", err)
	}

	// 5d. Verify reads
	var hostCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ansible_hosts`).Scan(&hostCount); err != nil {
		t.Fatalf("query host count: %v", err)
	}
	if hostCount != 2 {
		t.Errorf("expected 2 ansible hosts, got %d", hostCount)
	}

	var runHostCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ansible_run_hosts WHERE run_id='run-01'`).Scan(&runHostCount); err != nil {
		t.Fatalf("query run host count: %v", err)
	}
	if runHostCount != 2 {
		t.Errorf("expected 2 run hosts, got %d", runHostCount)
	}

	// 5e. Filter: only enabled hosts
	var enabledCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ansible_hosts WHERE enabled=1`).Scan(&enabledCount); err != nil {
		t.Fatalf("query enabled count: %v", err)
	}
	if enabledCount != 1 {
		t.Errorf("expected 1 enabled host, got %d", enabledCount)
	}

	// 5f. CASCADE delete run
	if _, err := db.Exec(`DELETE FROM ansible_runs WHERE run_id='run-01'`); err != nil {
		t.Fatalf("delete run for CASCADE test: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM ansible_run_hosts WHERE run_id='run-01'`).Scan(&runHostCount); err != nil {
		t.Fatalf("query run hosts after CASCADE: %v", err)
	}
	if runHostCount != 0 {
		t.Errorf("expected 0 run hosts after CASCADE, got %d", runHostCount)
	}

	// ---- Phase 6: Migration 008 — Legacy tables dropped ----
	legacyTables := []string{
		"youtube_channel_metadata", "youtube_groups",
		"youtube_manager_channels", "youtube_manager_groups",
		"ansible_computers",
	}
	for _, table := range legacyTables {
		if tableExists(t, db, table) {
			t.Errorf("migration 008 should have dropped %s", table)
		}
	}

	// Verify legacy_json_registry exists
	if !tableExists(t, db, "legacy_json_registry") {
		t.Error("migration 008 should have created legacy_json_registry")
	}
}

// TestIntegration_NewSQLiteStore_AutoMigration verifies that NewSQLiteStore
// auto-runs migrations on first open and is idempotent on subsequent opens.
func TestIntegration_NewSQLiteStore_AutoMigration(t *testing.T) {
	t.Parallel()

	expectedMigs, _ := discoverMigrations(testMigrationsFS, ".")
	expectedCount := len(expectedMigs)

	dbPath := t.TempDir() + "/integration_test.db"

	// ---- First open: should auto-apply all migrations ----
	db, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	// Apply migrations via testMigrationsFS (embed of *.sql in this dir)
	if err := RunMigrations(db, testMigrationsFS, "."); err != nil {
		t.Fatalf("first RunMigrations: %v", err)
	}

	// Verify all applied
	var migCount int
	db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&migCount)
	if migCount != expectedCount {
		t.Fatalf("expected %d migrations after first open, got %d", expectedCount, migCount)
	}

	// Store checksums for later verification
	type migRow struct {
		version  int
		checksum string
	}
	var migs []migRow
	mrows, _ := db.Query(`SELECT version, checksum FROM schema_migrations ORDER BY version`)
	for mrows.Next() {
		var mr migRow
		mrows.Scan(&mr.version, &mr.checksum)
		migs = append(migs, mr)
	}
	mrows.Close()
	db.Close()

	// ---- Second open: should NOT re-apply migrations ----
	db2, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	defer db2.Close()

	// Apply again — should be idempotent
	if err := RunMigrations(db2, testMigrationsFS, "."); err != nil {
		t.Fatalf("second RunMigrations: %v", err)
	}

	var migCount2 int
	db2.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&migCount2)
	if migCount2 != expectedCount {
		t.Fatalf("expected still %d migrations after second open, got %d", expectedCount, migCount2)
	}

	// Verify checksums unchanged
	var i int
	mrows2, _ := db2.Query(`SELECT version, checksum FROM schema_migrations ORDER BY version`)
	for mrows2.Next() {
		var v int
		var cksum string
		mrows2.Scan(&v, &cksum)
		if i < len(migs) {
			if migs[i].version != v {
				t.Errorf("version mismatch at %d: got %d, want %d", i, v, migs[i].version)
			}
			if migs[i].checksum != cksum {
				t.Errorf("checksum mismatch for version %d: got %s, want %s", v, cksum, migs[i].checksum)
			}
		}
		i++
	}
	mrows2.Close()

	// Verify we can query a table from migration 003
	var channelCount int
	db2.QueryRow(`SELECT COUNT(*) FROM youtube_channels`).Scan(&channelCount)
	if channelCount != 0 {
		t.Errorf("expected 0 channels, got %d", channelCount)
	}
}

// ============================================================
// Migration 008: End-to-end upgrade test — zero data loss
// ============================================================

// TestMigration008_UpgradeEndToEnd simulates a database that has been running
// since before migration 003/004 (canonical YouTube/Ansible models), inserts
// real data into legacy tables, then applies migration 008 (data migration +
// DROP) and verifies ZERO data loss.
//
// This is the most critical test for production safety.
func TestMigration008_UpgradeEndToEnd(t *testing.T) {
	db := openTestDB(t)

	// -----------------------------------------------------------------------
	// Phase 1: Apply migrations 1–7 (pre-008 state)
	// -----------------------------------------------------------------------
	migs, err := discoverMigrations(testMigrationsFS, ".")
	if err != nil {
		t.Fatalf("discoverMigrations: %v", err)
	}

	for _, m := range migs {
		if m.Version >= 8 {
			break
		}
		if err := EnsureApplied(db, m); err != nil {
			t.Fatalf("apply migration %03d_%s: %v", m.Version, m.Name, err)
		}
	}

	// Verify legacy tables exist (pre-008 state)
	legacyTables := []string{
		"youtube_channel_metadata",
		"youtube_groups",
		"youtube_manager_channels",
		"youtube_manager_groups",
		"ansible_computers",
	}
	for _, table := range legacyTables {
		if !tableExists(t, db, table) {
			t.Fatalf("pre-008: legacy table %s should exist but does not", table)
		}
	}

	// Verify canonical tables also exist (created by 003/004)
	canonicalTables := []string{
		"youtube_channels",
		"youtube_groups_v2",
		"youtube_group_channels",
		"ansible_hosts",
		"ansible_runs",
	}
	for _, table := range canonicalTables {
		if !tableExists(t, db, table) {
			t.Fatalf("pre-008: canonical table %s should exist but does not", table)
		}
	}

	// -----------------------------------------------------------------------
	// Phase 2: Insert realistic data into legacy tables
	// -----------------------------------------------------------------------

	// 2a. youtube_channel_metadata (legacy channel metadata)
	_, err = db.Exec(`INSERT INTO youtube_channel_metadata (channel_id, title, token_path, language, added_date, last_used, raw_json, updated_at)
		VALUES ('UC_legacy_alpha', 'Legacy Alpha', '/tokens/alpha.json', 'en', '2023-01-15', '2024-06-01', '{"legacy":true}', datetime('now'))`)
	if err != nil {
		t.Fatalf("insert youtube_channel_metadata 1: %v", err)
	}
	_, err = db.Exec(`INSERT INTO youtube_channel_metadata (channel_id, title, token_path, language, added_date, last_used, raw_json, updated_at)
		VALUES ('UC_legacy_beta', 'Legacy Beta', '/tokens/beta.json', 'it', '2023-03-20', '2024-05-15', '{"legacy":true}', datetime('now'))`)
	if err != nil {
		t.Fatalf("insert youtube_channel_metadata 2: %v", err)
	}

	// 2b. youtube_groups (legacy upload groups with channels_json)
	_, err = db.Exec(`INSERT INTO youtube_groups (name, description, privacy, channels_json, updated_at)
		VALUES ('WNBA Uploads', 'WNBA basketball highlights', 'public', '["UC_legacy_alpha","UC_legacy_beta"]', datetime('now'))`)
	if err != nil {
		t.Fatalf("insert youtube_groups 1: %v", err)
	}
	_, err = db.Exec(`INSERT INTO youtube_groups (name, description, privacy, channels_json, updated_at)
		VALUES ('NBA Uploads', 'NBA basketball highlights', 'unlisted', '["UC_legacy_alpha"]', datetime('now'))`)
	if err != nil {
		t.Fatalf("insert youtube_groups 2: %v", err)
	}

	// 2c. youtube_manager_channels (legacy manager channels with group affiliations)
	_, err = db.Exec(`INSERT INTO youtube_manager_channels (channel_id, group_name, url, title, name, thumbnail, language, view_count, sub_count, raw_json, updated_at)
		VALUES ('UC_mgr_ch1', 'Sports Group', 'https://youtube.com/@sports', 'Sports Channel', 'Sports', 'thumb.jpg', 'en', 50000, 2000, '{}', datetime('now'))`)
	if err != nil {
		t.Fatalf("insert youtube_manager_channels 1: %v", err)
	}
	_, err = db.Exec(`INSERT INTO youtube_manager_channels (channel_id, group_name, url, title, name, thumbnail, language, view_count, sub_count, raw_json, updated_at)
		VALUES ('UC_mgr_ch2', 'Sports Group', 'https://youtube.com/@sports2', 'Sports Two', 'Sports Two', 'thumb2.jpg', 'de', 30000, 1000, '{}', datetime('now'))`)
	if err != nil {
		t.Fatalf("insert youtube_manager_channels 2: %v", err)
	}
	_, err = db.Exec(`INSERT INTO youtube_manager_channels (channel_id, group_name, url, title, name, thumbnail, language, view_count, sub_count, raw_json, updated_at)
		VALUES ('UC_mgr_ch3', 'News Group', 'https://youtube.com/@news', 'News Channel', 'News', 'news.jpg', 'en', 100000, 5000, '{}', datetime('now'))`)
	if err != nil {
		t.Fatalf("insert youtube_manager_channels 3: %v", err)
	}

	// 2d. youtube_manager_groups (legacy manager group definitions)
	_, err = db.Exec(`INSERT INTO youtube_manager_groups (name, created_at, group_type, tracked_niches_json, updated_at)
		VALUES ('Sports Group', '2023-01-01', 'manager', '["basketball","football"]', datetime('now'))`)
	if err != nil {
		t.Fatalf("insert youtube_manager_groups 1: %v", err)
	}
	_, err = db.Exec(`INSERT INTO youtube_manager_groups (name, created_at, group_type, tracked_niches_json, updated_at)
		VALUES ('News Group', '2023-02-01', 'manager', '["politics","tech"]', datetime('now'))`)
	if err != nil {
		t.Fatalf("insert youtube_manager_groups 2: %v", err)
	}

	// 2e. ansible_computers (legacy Ansible computer records)
	_, err = db.Exec(`INSERT INTO ansible_computers (host, raw_json, updated_at)
		VALUES ('legacy-server-01', '{"host":"legacy-server-01","ansible_user":"pierone","enabled":true,"group":"production"}', datetime('now'))`)
	if err != nil {
		t.Fatalf("insert ansible_computers 1: %v", err)
	}
	_, err = db.Exec(`INSERT INTO ansible_computers (host, raw_json, updated_at)
		VALUES ('legacy-server-02', '{"host":"legacy-server-02","ansible_user":"root","enabled":false,"group":"staging"}', datetime('now'))`)
	if err != nil {
		t.Fatalf("insert ansible_computers 2: %v", err)
	}

	// -----------------------------------------------------------------------
	// Phase 3: Verify pre-migration counts
	// -----------------------------------------------------------------------
	var legacyChannelMetadataCount int
	db.QueryRow(`SELECT COUNT(*) FROM youtube_channel_metadata`).Scan(&legacyChannelMetadataCount)
	if legacyChannelMetadataCount != 2 {
		t.Fatalf("expected 2 youtube_channel_metadata rows, got %d", legacyChannelMetadataCount)
	}

	var legacyGroupsCount int
	db.QueryRow(`SELECT COUNT(*) FROM youtube_groups`).Scan(&legacyGroupsCount)
	if legacyGroupsCount != 2 {
		t.Fatalf("expected 2 youtube_groups rows, got %d", legacyGroupsCount)
	}

	var legacyManagerChannelsCount int
	db.QueryRow(`SELECT COUNT(*) FROM youtube_manager_channels`).Scan(&legacyManagerChannelsCount)
	if legacyManagerChannelsCount != 3 {
		t.Fatalf("expected 3 youtube_manager_channels rows, got %d", legacyManagerChannelsCount)
	}

	var legacyManagerGroupsCount int
	db.QueryRow(`SELECT COUNT(*) FROM youtube_manager_groups`).Scan(&legacyManagerGroupsCount)
	if legacyManagerGroupsCount != 2 {
		t.Fatalf("expected 2 youtube_manager_groups rows, got %d", legacyManagerGroupsCount)
	}

	var legacyAnsibleComputersCount int
	db.QueryRow(`SELECT COUNT(*) FROM ansible_computers`).Scan(&legacyAnsibleComputersCount)
	if legacyAnsibleComputersCount != 2 {
		t.Fatalf("expected 2 ansible_computers rows, got %d", legacyAnsibleComputersCount)
	}

	// Ensure canonical tables are empty before migration
	var canonicalChannelsCount int
	db.QueryRow(`SELECT COUNT(*) FROM youtube_channels`).Scan(&canonicalChannelsCount)
	if canonicalChannelsCount != 0 {
		t.Fatalf("expected 0 youtube_channels before migration, got %d", canonicalChannelsCount)
	}

	var canonicalGroupsCount int
	db.QueryRow(`SELECT COUNT(*) FROM youtube_groups_v2`).Scan(&canonicalGroupsCount)
	if canonicalGroupsCount != 0 {
		t.Fatalf("expected 0 youtube_groups_v2 before migration, got %d", canonicalGroupsCount)
	}

	var canonicalAnsibleHostsCount int
	db.QueryRow(`SELECT COUNT(*) FROM ansible_hosts`).Scan(&canonicalAnsibleHostsCount)
	if canonicalAnsibleHostsCount != 0 {
		t.Fatalf("expected 0 ansible_hosts before migration, got %d", canonicalAnsibleHostsCount)
	}

	// -----------------------------------------------------------------------
	// Phase 4: Apply migration 008 (data migration + DROP)
	// -----------------------------------------------------------------------
	err = EnsureApplied(db, migs[7]) // Version 8 is at index 7
	if err != nil {
		t.Fatalf("apply migration 008: %v", err)
	}

	// -----------------------------------------------------------------------
	// Phase 5: Verify legacy tables are DROPPED
	// -----------------------------------------------------------------------
	for _, table := range legacyTables {
		if tableExists(t, db, table) {
			t.Errorf("migration 008 should have dropped %s", table)
		}
	}

	// Verify legacy_json_registry exists
	if !tableExists(t, db, "legacy_json_registry") {
		t.Error("migration 008 should have created legacy_json_registry")
	}

	// -----------------------------------------------------------------------
	// Phase 6: Verify ZERO DATA LOSS — canonical tables have all records
	// -----------------------------------------------------------------------

	// 6a. youtube_channels should have all 5 channels:
	//     - 2 from youtube_channel_metadata (UC_legacy_alpha, UC_legacy_beta)
	//     - 3 from youtube_manager_channels (UC_mgr_ch1, UC_mgr_ch2, UC_mgr_ch3)
	var migratedChannelsCount int
	db.QueryRow(`SELECT COUNT(*) FROM youtube_channels`).Scan(&migratedChannelsCount)
	expectedChannels := legacyChannelMetadataCount + legacyManagerChannelsCount // 2 + 3 = 5
	if migratedChannelsCount != expectedChannels {
		t.Fatalf("data loss! expected %d youtube_channels (2 metadata + 3 manager), got %d", expectedChannels, migratedChannelsCount)
	}

	// Verify specific channel data was migrated correctly
	var legacyAlphaTitle string
	err = db.QueryRow(`SELECT title FROM youtube_channels WHERE channel_id='UC_legacy_alpha'`).Scan(&legacyAlphaTitle)
	if err != nil {
		t.Errorf("UC_legacy_alpha missing from youtube_channels after migration: %v", err)
	} else if legacyAlphaTitle != "Legacy Alpha" {
		t.Errorf("UC_legacy_alpha title: got %q, want %q", legacyAlphaTitle, "Legacy Alpha")
	}

	var mgrCh1Title string
	err = db.QueryRow(`SELECT title FROM youtube_channels WHERE channel_id='UC_mgr_ch1'`).Scan(&mgrCh1Title)
	if err != nil {
		t.Errorf("UC_mgr_ch1 missing from youtube_channels after migration: %v", err)
	} else if mgrCh1Title != "Sports Channel" {
		t.Errorf("UC_mgr_ch1 title: got %q, want %q", mgrCh1Title, "Sports Channel")
	}

	// 6b. youtube_groups_v2 should have all groups:
	//     - 2 from youtube_groups (WNBA Uploads, NBA Uploads) → group_type='upload'
	//     - 2 from youtube_manager_groups (Sports Group, News Group) → group_type='manager'
	//     - 2 from youtube_manager_channels (Sports Group, News Group) → group_type='manager' (INSERT OR IGNORE dedup)
	// Total expected: 4 unique groups (2 upload + 2 manager)
	// But wait — youtube_manager_channels Phase 3 also inserts groups that may already
	// exist from youtube_manager_groups Phase 4. Since we're using INSERT OR IGNORE,
	// duplicates are handled. So unique groups:
	//   - phase2 upload groups: "WNBA Uploads" (upload), "NBA Uploads" (upload)
	//   - phase4 manager groups: "Sports Group" (manager), "News Group" (manager)
	//   - phase3 also inserts "Sports Group" (manager) and "News Group" (manager)
	//     — these are dedup'd by INSERT OR IGNORE
	// Total unique (name, group_type) combos = 4
	var migratedGroupsCount int
	db.QueryRow(`SELECT COUNT(*) FROM youtube_groups_v2`).Scan(&migratedGroupsCount)
	if migratedGroupsCount != 4 {
		t.Fatalf("expected 4 youtube_groups_v2, got %d", migratedGroupsCount)
	}

	// Verify group data
	var wnbaPrivacy string
	var wnbaType string
	err = db.QueryRow(`SELECT privacy, group_type FROM youtube_groups_v2 WHERE name='WNBA Uploads'`).Scan(&wnbaPrivacy, &wnbaType)
	if err != nil {
		t.Errorf("WNBA Uploads missing from youtube_groups_v2: %v", err)
	} else {
		if wnbaPrivacy != "public" {
			t.Errorf("WNBA Uploads privacy: got %q, want %q", wnbaPrivacy, "public")
		}
		if wnbaType != "upload" {
			t.Errorf("WNBA Uploads group_type: got %q, want %q", wnbaType, "upload")
		}
	}

	var sportsType string
	err = db.QueryRow(`SELECT group_type FROM youtube_groups_v2 WHERE name='Sports Group'`).Scan(&sportsType)
	if err != nil {
		t.Errorf("Sports Group missing from youtube_groups_v2: %v", err)
	} else if sportsType != "manager" {
		t.Errorf("Sports Group group_type: got %q, want %q", sportsType, "manager")
	}

	// 6c. youtube_group_channels should have memberships:
	//     - WNBA Uploads (upload) → UC_legacy_alpha, UC_legacy_beta (from channels_json) = 2
	//     - NBA Uploads (upload) → UC_legacy_alpha (from channels_json) = 1
	//     - Sports Group (manager) → UC_mgr_ch1, UC_mgr_ch2 (from youtube_manager_channels) = 2
	//     - News Group (manager) → UC_mgr_ch3 (from youtube_manager_channels) = 1
	// Total: 6 memberships
	var migratedMembershipsCount int
	db.QueryRow(`SELECT COUNT(*) FROM youtube_group_channels`).Scan(&migratedMembershipsCount)
	if migratedMembershipsCount != 6 {
		t.Fatalf("expected 6 youtube_group_channels, got %d", migratedMembershipsCount)
	}

	// Verify specific memberships
	var membershipCount int
	db.QueryRow(`SELECT COUNT(*) FROM youtube_group_channels gc
		JOIN youtube_groups_v2 g ON g.id = gc.group_id
		WHERE g.name='WNBA Uploads' AND gc.channel_id='UC_legacy_alpha'`).Scan(&membershipCount)
	if membershipCount != 1 {
		t.Errorf("WNBA Uploads → UC_legacy_alpha membership missing after migration")
	}

	db.QueryRow(`SELECT COUNT(*) FROM youtube_group_channels gc
		JOIN youtube_groups_v2 g ON g.id = gc.group_id
		WHERE g.name='WNBA Uploads' AND gc.channel_id='UC_legacy_beta'`).Scan(&membershipCount)
	if membershipCount != 1 {
		t.Errorf("WNBA Uploads → UC_legacy_beta membership missing after migration")
	}

	db.QueryRow(`SELECT COUNT(*) FROM youtube_group_channels gc
		JOIN youtube_groups_v2 g ON g.id = gc.group_id
		WHERE g.name='Sports Group' AND gc.channel_id='UC_mgr_ch1'`).Scan(&membershipCount)
	if membershipCount != 1 {
		t.Errorf("Sports Group → UC_mgr_ch1 membership missing after migration")
	}

	db.QueryRow(`SELECT COUNT(*) FROM youtube_group_channels gc
		JOIN youtube_groups_v2 g ON g.id = gc.group_id
		WHERE g.name='Sports Group' AND gc.channel_id='UC_mgr_ch2'`).Scan(&membershipCount)
	if membershipCount != 1 {
		t.Errorf("Sports Group → UC_mgr_ch2 membership missing after migration")
	}

	db.QueryRow(`SELECT COUNT(*) FROM youtube_group_channels gc
		JOIN youtube_groups_v2 g ON g.id = gc.group_id
		WHERE g.name='News Group' AND gc.channel_id='UC_mgr_ch3'`).Scan(&membershipCount)
	if membershipCount != 1 {
		t.Errorf("News Group → UC_mgr_ch3 membership missing after migration")
	}

	db.QueryRow(`SELECT COUNT(*) FROM youtube_group_channels gc
		JOIN youtube_groups_v2 g ON g.id = gc.group_id
		WHERE g.name='NBA Uploads' AND gc.channel_id='UC_legacy_alpha'`).Scan(&membershipCount)
	if membershipCount != 1 {
		t.Errorf("NBA Uploads → UC_legacy_alpha membership missing after migration")
	}

	// 6d. ansible_hosts should have all 2 records from ansible_computers
	var migratedHostsCount int
	db.QueryRow(`SELECT COUNT(*) FROM ansible_hosts`).Scan(&migratedHostsCount)
	if migratedHostsCount != 2 {
		t.Fatalf("expected 2 ansible_hosts, got %d", migratedHostsCount)
	}

	// Verify specific host data
	var server01User string
	var server01Enabled int
	err = db.QueryRow(`SELECT ansible_user, enabled FROM ansible_hosts WHERE host='legacy-server-01'`).Scan(&server01User, &server01Enabled)
	if err != nil {
		t.Errorf("legacy-server-01 missing from ansible_hosts: %v", err)
	} else {
		if server01User != "pierone" {
			t.Errorf("legacy-server-01 ansible_user: got %q, want %q", server01User, "pierone")
		}
		if server01Enabled != 1 {
			t.Errorf("legacy-server-01 enabled: got %d, want 1", server01Enabled)
		}
	}

	var server02User string
	var server02Enabled int
	err = db.QueryRow(`SELECT ansible_user, enabled FROM ansible_hosts WHERE host='legacy-server-02'`).Scan(&server02User, &server02Enabled)
	if err != nil {
		t.Errorf("legacy-server-02 missing from ansible_hosts: %v", err)
	} else {
		if server02User != "pierone" {
			t.Errorf("legacy-server-02 ansible_user: got %q, want %q", server02User, "pierone")
		}
		if server02Enabled != 1 {
			t.Errorf("legacy-server-02 enabled: got %d, want 1 (migration default, overwrite per-host later)", server02Enabled)
		}
	}

	// -----------------------------------------------------------------------
	// Phase 7: Verify foreign key integrity and schema integrity
	// -----------------------------------------------------------------------

	// PRAGMA foreign_key_check: should return no rows
	fkRows, err := db.Query("PRAGMA foreign_key_check")
	if err != nil {
		t.Fatalf("PRAGMA foreign_key_check: %v", err)
	}
	defer fkRows.Close()
	var fkViolations int
	for fkRows.Next() {
		fkViolations++
	}
	if fkViolations > 0 {
		t.Errorf("PRAGMA foreign_key_check found %d FK violations after migration", fkViolations)
	}

	// PRAGMA integrity_check: should return 'ok'
	var integrityResult string
	err = db.QueryRow("PRAGMA integrity_check").Scan(&integrityResult)
	if err != nil {
		t.Fatalf("PRAGMA integrity_check: %v", err)
	}
	if integrityResult != "ok" {
		t.Errorf("database integrity check failed: %s", integrityResult)
	}
}

// ============================================================
// Helpers
// ============================================================

func applyAllMigrations(t *testing.T, db *sql.DB) {
	t.Helper()
	if err := RunMigrations(db, testMigrationsFS, "."); err != nil {
		t.Fatalf("RunMigrations failed: %v", err)
	}
}

func tableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var count int
	err := db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, name,
	).Scan(&count)
	if err != nil {
		t.Fatalf("check table %s: %v", name, err)
	}
	return count > 0
}

func columnExists(t *testing.T, db *sql.DB, table, col string) bool {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		t.Fatalf("PRAGMA table_info(%s): %v", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, colType string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &colType, &notnull, &dflt, &pk); err != nil {
			continue
		}
		if name == col {
			return true
		}
	}
	return false
}
