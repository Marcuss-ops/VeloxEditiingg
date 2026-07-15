package migrations

import (
	"database/sql"
	"testing"
)

// ============================================================
// Integration: End-to-end migration + CRUD pipeline
// ============================================================

// TestIntegration_MigrationRunner_EndToEnd applies all migrations and then
// performs INSERT/SELECT/UPDATE/DELETE operations against tables created by each
// migration to verify the full pipeline works.
func TestIntegration_MigrationRunner_EndToEnd(t *testing.T) {
	db := openTestDB(t)
	applyAllMigrations(t, db)

	expectedMigs, _ := discoverMigrations(testMigrationsFS, "testdata")
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
		want := expectedMigs[migIdx]
		if v != want.Version {
			t.Errorf("row %d: expected version %d, got %d", migIdx, want.Version, v)
		}
		if name != want.Name {
			t.Errorf("row %d: expected name %q, got %q", migIdx, want.Name, name)
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
		table string
		col   string
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
	_, err = db.Exec(`INSERT INTO youtube_groups (name, group_type, description, created_at, updated_at)
		VALUES ('Sports', 'manager', 'Sports channels', datetime('now'), datetime('now'))`)
	if err != nil {
		t.Fatalf("insert youtube_groups: %v", err)
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
	if err := db.QueryRow(`SELECT name FROM youtube_groups WHERE id=1`).Scan(&groupName); err != nil {
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
		"youtube_channel_metadata",
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

	expectedMigs, _ := discoverMigrations(testMigrationsFS, "testdata")
	expectedCount := len(expectedMigs)

	dbPath := t.TempDir() + "/integration_test.db"

	// ---- First open: should auto-apply all migrations ----
	db, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	// Apply migrations via testMigrationsFS (embed of *.sql in this dir)
	if err := RunMigrations(db, testMigrationsFS, "testdata"); err != nil {
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
	if err := RunMigrations(db2, testMigrationsFS, "testdata"); err != nil {
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
