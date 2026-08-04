package migrations

import (
	"database/sql"
	"fmt"
	"testing"
)

// ============================================================
// Migration 090: YouTube domain dropped
// ============================================================

// TestMigration090_YouTubeDomainDropped applies the chain through
// migration 090 and pins the YouTube cleanup. The Dark Editor schema is
// not part of the fresh baseline; legacy Dark Editor tables are removed
// by migration 128.
func TestMigration090_YouTubeDomainDropped(t *testing.T) {
	db := openTestDB(t)
	applyMigrationsUpTo(t, db, 90)

	youtubeTables := []string{
		"youtube_channels",
		"youtube_groups",
		"youtube_group_channels",
		"youtube_tracked_niches",
		"youtube_oauth_tokens",
		"youtube_channel_metrics",
		"youtube_revenue_metrics",
		"youtube_video_metrics",
		"youtube_quota_usage",
		"youtube_api_cache",
	}
	for _, table := range youtubeTables {
		if tableExists(t, db, table) {
			t.Errorf("migration 090 should have dropped %s", table)
		}
	}

	// MIGRATION 090 must also drop the historical YouTube columns on
	// domain tables. The cleanup of these columns is part of the YouTube
	// domain exit; pin their absence here so a future schema drift is
	// caught by the suite rather than discovered at runtime.
	youtubeColumns := []struct {
		table string
		col   string
	}{
		{"calendar_events", "youtube_group"},
		{"calendar_events", "youtube_links_json"},
	}
	for _, cc := range youtubeColumns {
		if columnExists(t, db, cc.table, cc.col) {
			t.Errorf("migration 090 should have dropped column %s.%s", cc.table, cc.col)
		}
	}

}

// ============================================================
// Migration 128: Dark Editor domain dropped (final schema)
// ============================================================

// TestMigration128_DarkEditorDomainDropped applies the full chain and
// pins the end state: zero Dark Editor tables in the final Velox
// schema.
func TestMigration128_DarkEditorDomainDropped(t *testing.T) {
	db := openTestDB(t)
	applyAllMigrations(t, db)

	darkEditorTables := []string{
		"dark_editor_temp_files",
		"dark_editor_generations",
		"dark_editor_assets",
		"dark_editor_templates",
		"dark_editor_projects",
		"dark_editor_folders",
	}
	for _, table := range darkEditorTables {
		if tableExists(t, db, table) {
			t.Errorf("migration 128 should have dropped %s", table)
		}
	}
}

// TestMigration128_UpgradeFromPreDropState simulates an existing install
// that already applied migrations through 090 (Dark Editor tables live,
// populated with data) and then upgrades onto the 128 drop. It verifies
// the Dark Editor data is removed while core Velox tables survive.
func TestMigration128_UpgradeFromPreDropState(t *testing.T) {
	db := openTestDB(t)
	applyMigrationsUpTo(t, db, 90)

	// Seed every legacy Dark Editor table that migration 128 must remove.
	legacyDarkEditorTables := []string{
		"dark_editor_projects",
		"dark_editor_folders",
		"dark_editor_assets",
		"dark_editor_templates",
		"dark_editor_temp_files",
		"dark_editor_generations",
	}
	for _, table := range legacyDarkEditorTables {
		if _, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id TEXT PRIMARY KEY, name TEXT)", table)); err != nil {
			t.Fatalf("create legacy %s: %v", table, err)
		}
	}
	if _, err := db.Exec(`INSERT INTO dark_editor_folders (id, name) VALUES ('de-folder-1', 'legacy')`); err != nil {
		t.Fatalf("seed dark_editor_folders: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO dark_editor_projects (id, name) VALUES ('de-proj-1', 'legacy')`); err != nil {
		t.Fatalf("seed dark_editor_projects: %v", err)
	}

	// Seed a core Velox job that must survive the upgrade.
	if _, err := db.Exec(`INSERT INTO jobs (job_id, status, migrated_at) VALUES ('upgrade-job', 'queued', datetime('now'))`); err != nil {
		t.Fatalf("seed jobs: %v", err)
	}

	// Apply the remainder of the chain (091..120..128).
	applyMigrationsUpTo(t, db, 100000)

	darkEditorTables := []string{
		"dark_editor_temp_files",
		"dark_editor_generations",
		"dark_editor_assets",
		"dark_editor_templates",
		"dark_editor_projects",
		"dark_editor_folders",
	}
	for _, table := range darkEditorTables {
		if tableExists(t, db, table) {
			t.Errorf("migration 128 should have dropped %s", table)
		}
	}

	// Core Velox tables and rows intact.
	if !tableExists(t, db, "jobs") {
		t.Error("jobs table must survive the Dark Editor drop")
	}
	var jobCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE job_id = 'upgrade-job'`).Scan(&jobCount); err != nil {
		t.Fatalf("query seeded job: %v", err)
	}
	if jobCount != 1 {
		t.Errorf("expected seeded job to survive, got count=%d", jobCount)
	}

	// 128 recorded in schema_migrations.
	var recorded int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations WHERE version = 128`).Scan(&recorded); err != nil {
		t.Fatalf("query schema_migrations for 128: %v", err)
	}
	if recorded != 1 {
		t.Errorf("expected migration 128 recorded, got count=%d", recorded)
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
