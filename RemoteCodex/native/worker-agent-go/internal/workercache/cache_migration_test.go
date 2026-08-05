package workercache

import (
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func openLegacyCacheDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "legacy-cache.db"))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(`
CREATE TABLE cached_assets (
    drive_file_id TEXT PRIMARY KEY,
    local_path TEXT NOT NULL,
    size_bytes INTEGER NOT NULL DEFAULT 0,
    active_job_id TEXT,
    download_complete INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    last_used_at TEXT NOT NULL
);
CREATE INDEX idx_cached_assets_active_job_id ON cached_assets(active_job_id);
CREATE INDEX idx_cached_assets_last_used_at ON cached_assets(last_used_at);
CREATE TABLE cached_asset_leases (
    drive_file_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    PRIMARY KEY (drive_file_id, job_id)
);
CREATE INDEX idx_cached_asset_leases_asset ON cached_asset_leases(drive_file_id);
PRAGMA user_version = 1;
INSERT INTO cached_assets
    (drive_file_id, local_path, size_bytes, active_job_id, download_complete, created_at, last_used_at)
VALUES ('LEGACY', '/cache/legacy.mp4', 42, 'old-job', 1,
        '2026-08-01T00:00:00Z', '2026-08-01T00:01:00Z'),
       ('FREE', '/cache/free.mp4', 7, NULL, 1,
        '2026-08-01T00:00:00Z', '2026-08-01T00:02:00Z')`)
	if err != nil {
		t.Fatalf("seed legacy DB: %v", err)
	}
	return db
}

func hasColumn(t *testing.T, db *sql.DB, table, column string) bool {
	t.Helper()
	ok, err := columnExists(db, table, column)
	if err != nil {
		t.Fatalf("columnExists(%s.%s): %v", table, column, err)
	}
	return ok
}

func TestApplySchema_UpgradeBackfillsLeaseAndDropsLegacyColumn(t *testing.T) {
	db := openLegacyCacheDB(t)
	if err := applySchema(db); err != nil {
		t.Fatalf("applySchema: %v", err)
	}
	if hasColumn(t, db, "cached_assets", "active_job_id") {
		t.Fatal("legacy active_job_id column remains after forward migration")
	}
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	if version != currentSchemaVersion {
		t.Fatalf("user_version=%d, want %d", version, currentSchemaVersion)
	}
	var job string
	if err := db.QueryRow(`SELECT job_id FROM cached_asset_leases WHERE asset_key = 'LEGACY'`).Scan(&job); err != nil {
		t.Fatalf("backfilled lease: %v", err)
	}
	if job != "old-job" {
		t.Fatalf("backfilled job=%q, want old-job", job)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM cached_assets`).Scan(&count); err != nil {
		t.Fatalf("asset count: %v", err)
	}
	if count != 2 {
		t.Fatalf("asset count=%d, want 2", count)
	}
}

// openV2CacheDB seeds the v2 schema: asset rows already keyed by
// drive_file_id WITHOUT the legacy active_job_id mirror column. The
// forward migration must still converge to asset_key and restore
// existing leases.
func openV2CacheDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "v2-cache.db"))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(`
CREATE TABLE cached_assets (
    drive_file_id TEXT PRIMARY KEY,
    local_path TEXT NOT NULL,
    size_bytes INTEGER NOT NULL DEFAULT 0,
    download_complete INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    last_used_at TEXT NOT NULL
);
CREATE INDEX idx_cached_assets_last_used_at ON cached_assets(last_used_at);
CREATE TABLE cached_asset_leases (
    drive_file_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    PRIMARY KEY (drive_file_id, job_id),
    FOREIGN KEY (drive_file_id) REFERENCES cached_assets(drive_file_id) ON DELETE CASCADE
);
CREATE INDEX idx_cached_asset_leases_asset ON cached_asset_leases(drive_file_id);
PRAGMA user_version = 2;
INSERT INTO cached_assets
    (drive_file_id, local_path, size_bytes, download_complete, created_at, last_used_at)
VALUES ('V2KEY', '/cache/v2.mp4', 99, 1,
        '2026-08-01T00:00:00Z', '2026-08-01T00:01:00Z');
INSERT INTO cached_asset_leases (drive_file_id, job_id, acquired_at)
VALUES ('V2KEY', 'job-v2', '2026-08-01T00:01:00Z')`)
	if err != nil {
		t.Fatalf("seed v2 DB: %v", err)
	}
	return db
}

func TestApplySchema_UpgradeFromV2_RestoresAssetKeyAndLeases(t *testing.T) {
	db := openV2CacheDB(t)
	if err := applySchema(db); err != nil {
		t.Fatalf("applySchema: %v", err)
	}
	if hasColumn(t, db, "cached_assets", "drive_file_id") {
		t.Fatal("legacy drive_file_id column remains after forward migration")
	}
	if hasColumn(t, db, "cached_assets", "active_job_id") {
		t.Fatal("legacy active_job_id column remains after forward migration")
	}
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	if version != currentSchemaVersion {
		t.Fatalf("user_version=%d, want %d", version, currentSchemaVersion)
	}
	var key, job string
	if err := db.QueryRow(`SELECT asset_key FROM cached_assets WHERE local_path = '/cache/v2.mp4'`).Scan(&key); err != nil {
		t.Fatalf("migrated asset key: %v", err)
	}
	if key != "V2KEY" {
		t.Fatalf("migrated asset_key=%q, want V2KEY", key)
	}
	if err := db.QueryRow(`SELECT job_id FROM cached_asset_leases WHERE asset_key = 'V2KEY'`).Scan(&job); err != nil {
		t.Fatalf("restored lease: %v", err)
	}
	if job != "job-v2" {
		t.Fatalf("restored lease job=%q, want job-v2", job)
	}
}

func TestApplySchema_UpgradeIsIdempotent(t *testing.T) {
	db := openLegacyCacheDB(t)
	if err := applySchema(db); err != nil {
		t.Fatalf("first applySchema: %v", err)
	}
	if err := applySchema(db); err != nil {
		t.Fatalf("second applySchema: %v", err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM cached_asset_leases WHERE asset_key = 'LEGACY' AND job_id = 'old-job'`).Scan(&count); err != nil {
		t.Fatalf("lease count: %v", err)
	}
	if count != 1 {
		t.Fatalf("backfilled lease count=%d, want 1", count)
	}
}

func TestMigrateLegacySchema_RollsBackAfterDestructiveDDL(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "rollback-cache.db"))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`
CREATE TABLE cached_assets (
    drive_file_id TEXT PRIMARY KEY,
    local_path TEXT NOT NULL,
    size_bytes INTEGER NOT NULL DEFAULT 0,
    active_job_id TEXT,
    download_complete INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    last_used_at TEXT NOT NULL
);
CREATE TABLE cached_asset_leases (
    drive_file_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    PRIMARY KEY (drive_file_id, job_id)
);
CREATE INDEX idx_cached_assets_active_job_id ON cached_assets(active_job_id);
INSERT INTO cached_assets
    (drive_file_id, local_path, active_job_id, created_at, last_used_at)
VALUES ('ROLLBACK', '/cache/rollback.mp4', 'job-rollback',
        '2026-08-01T00:00:00Z', '2026-08-01T00:00:00Z')`)
	if err != nil {
		t.Fatalf("seed rollback DB: %v", err)
	}

	err = migrateLegacySchemaWithHook(db, func() error {
		return errors.New("test failure after assets rebuild")
	})
	if err == nil {
		t.Fatal("migrateLegacySchema unexpectedly succeeded")
	}
	if !hasColumn(t, db, "cached_assets", "active_job_id") {
		t.Fatal("rollback lost legacy active_job_id column")
	}
	var job string
	if scanErr := db.QueryRow(`SELECT active_job_id FROM cached_assets WHERE drive_file_id = 'ROLLBACK'`).Scan(&job); scanErr != nil {
		t.Fatalf("read rolled-back asset: %v", scanErr)
	}
	if job != "job-rollback" {
		t.Fatalf("rolled-back active_job_id=%q, want job-rollback", job)
	}
	if !hasColumn(t, db, "cached_asset_leases", "job_id") {
		t.Fatal("rollback lost the original lease table")
	}
	var version int
	if scanErr := db.QueryRow(`PRAGMA user_version`).Scan(&version); scanErr != nil {
		t.Fatalf("read rolled-back user_version: %v", scanErr)
	}
	if version != 0 {
		t.Fatalf("rolled-back user_version=%d, want 0", version)
	}
	var indexName string
	if scanErr := db.QueryRow(`SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx_cached_assets_active_job_id'`).Scan(&indexName); scanErr != nil {
		t.Fatalf("legacy index missing after rollback: %v", scanErr)
	}
	// The exact SQLite error is implementation-specific; this guard makes
	// the test explicit that the failure came from the migration path.
	if err.Error() == "" {
		t.Fatal("migration returned an empty error")
	}
}
