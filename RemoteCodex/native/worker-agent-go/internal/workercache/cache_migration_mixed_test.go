package workercache

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestApplySchema_UpgradeMixedAssetAndCanonicalLeaseTables(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "mixed-cache.db"))
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
    download_complete INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    last_used_at TEXT NOT NULL
);
CREATE TABLE cached_asset_leases (
    asset_key TEXT NOT NULL,
    job_id TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    PRIMARY KEY (asset_key, job_id)
);
INSERT INTO cached_assets
    (drive_file_id, local_path, size_bytes, created_at, last_used_at)
VALUES ('MIXED', '/cache/mixed.mp4', 10,
        '2026-08-01T00:00:00Z', '2026-08-01T00:01:00Z');
INSERT INTO cached_asset_leases (asset_key, job_id, acquired_at)
VALUES ('MIXED', 'mixed-job', '2026-08-01T00:01:00Z')`)
	if err != nil {
		t.Fatalf("seed mixed DB: %v", err)
	}

	if err := applySchema(db); err != nil {
		t.Fatalf("applySchema: %v", err)
	}
	var job string
	if err := db.QueryRow(`SELECT job_id FROM cached_asset_leases WHERE asset_key = 'MIXED'`).Scan(&job); err != nil {
		t.Fatalf("migrated lease: %v", err)
	}
	if job != "mixed-job" {
		t.Fatalf("migrated lease job=%q, want mixed-job", job)
	}
	if hasColumn(t, db, "cached_assets", "drive_file_id") {
		t.Fatal("legacy drive_file_id column remains")
	}
}
