package migrations

import (
	"database/sql"
	"fmt"
	"testing"
)

// ============================================================
// Migration 090: YouTube domain dropped
// ============================================================

func TestMigration090_YouTubeDomainDropped(t *testing.T) {
	db := openTestDB(t)
	applyAllMigrations(t, db)

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
