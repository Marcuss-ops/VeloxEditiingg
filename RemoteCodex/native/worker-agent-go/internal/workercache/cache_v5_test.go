package workercache

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"velox-shared/assetref"
)

const v5SharedSHA = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// TestApplySchema_UpgradeFromV4_SplitsIntoContentDeduplicatedBlobs proves the
// v4 → v5 split: physical columns move to cached_blobs, two assets sharing a
// content_hash collapse into one blob row, and a hashless row becomes a
// per-asset legacy blob that the read model reports with an empty ContentHash.
func TestApplySchema_UpgradeFromV4_SplitsIntoContentDeduplicatedBlobs(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "v4-cache.db"))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`
CREATE TABLE cached_assets (
    asset_key TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL DEFAULT '',
    local_path TEXT NOT NULL,
    size_bytes INTEGER NOT NULL DEFAULT 0,
    download_complete INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    last_used_at TEXT NOT NULL
);
CREATE INDEX idx_cached_assets_last_used_at ON cached_assets(last_used_at);
CREATE TABLE cached_asset_leases (
    asset_key TEXT NOT NULL,
    job_id TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    PRIMARY KEY (asset_key, job_id),
    FOREIGN KEY (asset_key) REFERENCES cached_assets(asset_key) ON DELETE CASCADE
);
CREATE INDEX idx_cached_asset_leases_asset ON cached_asset_leases(asset_key);
PRAGMA user_version = 4;
INSERT INTO cached_assets
    (asset_key, content_hash, local_path, size_bytes, download_complete, created_at, last_used_at)
VALUES ('A', '` + v5SharedSHA + `', '/cache/a.mp4', 10, 1, '2026-08-01T00:00:00Z', '2026-08-01T00:01:00Z'),
       ('B', '` + v5SharedSHA + `', '/cache/b.mp4', 10, 1, '2026-08-01T00:00:00Z', '2026-08-01T00:02:00Z'),
       ('C', '', '/cache/c.mp4', 5, 1, '2026-08-01T00:00:00Z', '2026-08-01T00:03:00Z')`)
	if err != nil {
		t.Fatalf("seed v4 DB: %v", err)
	}

	if err := applySchema(db); err != nil {
		t.Fatalf("applySchema: %v", err)
	}
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatalf("read user_version: %v", err)
	}
	if version != currentSchemaVersion {
		t.Fatalf("user_version=%d, want %d", version, currentSchemaVersion)
	}

	var assetCount, blobCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM cached_assets`).Scan(&assetCount); err != nil {
		t.Fatalf("asset count: %v", err)
	}
	if assetCount != 3 {
		t.Fatalf("asset count=%d, want 3", assetCount)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM cached_blobs`).Scan(&blobCount); err != nil {
		t.Fatalf("blob count: %v", err)
	}
	if blobCount != 2 {
		t.Fatalf("blob count=%d, want 2 (one shared SHA + one legacy)", blobCount)
	}

	// The shared SHA collapsed to a single blob (first-writer path wins).
	var sharedPath string
	if err := db.QueryRow(`SELECT local_path FROM cached_blobs WHERE content_hash = ?`, v5SharedSHA).Scan(&sharedPath); err != nil {
		t.Fatalf("shared blob: %v", err)
	}
	if sharedPath != "/cache/a.mp4" && sharedPath != "/cache/b.mp4" {
		t.Fatalf("shared blob path=%q, want /cache/a.mp4 or /cache/b.mp4", sharedPath)
	}

	// The hashless row became a legacy per-asset blob reported as empty hash.
	var legacyKey string
	if err := db.QueryRow(`SELECT content_hash FROM cached_assets WHERE asset_key = 'C'`).Scan(&legacyKey); err != nil {
		t.Fatalf("legacy mapping: %v", err)
	}
	if legacyKey != legacyBlobKey("C") {
		t.Fatalf("legacy blob key=%q, want %q", legacyKey, legacyBlobKey("C"))
	}
	if got := displayContentHash(legacyKey); got != "" {
		t.Fatalf("displayContentHash(%q)=%q, want empty", legacyKey, got)
	}
}

// TestCache_SharedContentHashKeepsOneBlobUntilLastAssetEvicted proves the
// physical dedup contract: two assets resolving to the same content hash share
// one blob row and one file, and the file survives until the last asset
// referencing it is evicted.
func TestCache_SharedContentHashKeepsOneBlobUntilLastAssetEvicted(t *testing.T) {
	cache := newTestCache(t)
	ctx := context.Background()
	dir := t.TempDir()
	payload := []byte("shared bytes")
	hash := acceptanceContentHash(payload)
	path := filepath.Join(dir, "blob.mp4")
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("write blob: %v", err)
	}

	for _, key := range []string{"ASSET-A", "ASSET-B"} {
		if err := cache.Store(ctx, Entry{
			AssetKey:         assetref.AssetKey(key),
			ContentHash:      hash,
			LocalPath:        path,
			SizeBytes:        int64(len(payload)),
			DownloadComplete: true,
		}); err != nil {
			t.Fatalf("Store %s: %v", key, err)
		}
	}

	var blobCount int
	if err := cache.DB().QueryRow(`SELECT COUNT(1) FROM cached_blobs`).Scan(&blobCount); err != nil {
		t.Fatalf("blob count: %v", err)
	}
	if blobCount != 1 {
		t.Fatalf("blob count=%d, want 1 (shared content hash)", blobCount)
	}

	if err := cache.EvictIfUnleased(ctx, "ASSET-A", path); err != nil {
		t.Fatalf("evict A: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("shared file removed while ASSET-B still references it: %v", err)
	}
	var remaining int
	if err := cache.DB().QueryRow(`SELECT COUNT(1) FROM cached_blobs WHERE content_hash = ?`, string(hash)).Scan(&remaining); err != nil {
		t.Fatalf("blob probe: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("blob removed while a second asset still references it: remaining=%d", remaining)
	}

	if err := cache.EvictIfUnleased(ctx, "ASSET-B", path); err != nil {
		t.Fatalf("evict B: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("blob file should be gone after last asset evicted: %v", err)
	}
	if err := cache.DB().QueryRow(`SELECT COUNT(1) FROM cached_blobs`).Scan(&remaining); err != nil {
		t.Fatalf("final blob count: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("blob row leaked after last asset evicted: %d", remaining)
	}
}
