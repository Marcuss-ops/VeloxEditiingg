package workercache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"velox-shared/assetref"
)

// These are the cross-cutting acceptance tests for the content-addressed
// cache and its pressure controller:
//
//   - three asset IDs resolving to the same SHA-256 collapse into ONE
//     physical blob (one row, one file);
//   - below the high watermark the pressure pass evicts NOTHING;
//   - leased / reserved / snapshot-protected blobs are never removed, even
//     under pressure.

// TestCacheAcceptance_ThreeAssetIDsSameSHAOneBlob proves the dedup contract:
// three distinct asset IDs that resolve to the same content hash share one
// cached_blobs row and exactly one physical file (first writer wins the
// canonical path; the other two paths never materialise).
func TestCacheAcceptance_ThreeAssetIDsSameSHAOneBlob(t *testing.T) {
	cache := newTestCache(t)
	ctx := context.Background()
	dir := t.TempDir()
	payload := []byte("identical bytes shared by three asset ids")
	hash := acceptanceContentHash(payload)

	path1 := filepath.Join(dir, "blob-1.mp4")
	if err := os.WriteFile(path1, payload, 0o644); err != nil {
		t.Fatalf("write canonical blob: %v", err)
	}
	// path2 / path3 are the distinct paths the later assets would try to
	// claim; the dedup layer must keep path1 and never materialise them.
	path2 := filepath.Join(dir, "blob-2.mp4")
	path3 := filepath.Join(dir, "blob-3.mp4")

	keys := []string{"ASSET-1", "ASSET-2", "ASSET-3"}
	paths := []string{path1, path2, path3}
	for i, key := range keys {
		if err := cache.Store(ctx, Entry{
			AssetKey:         assetref.AssetKey(key),
			ContentHash:      hash,
			LocalPath:        paths[i],
			SizeBytes:        int64(len(payload)),
			DownloadComplete: true,
		}); err != nil {
			t.Fatalf("Store %s: %v", key, err)
		}
	}

	var blobCount, assetCount int
	if err := cache.DB().QueryRow(`SELECT COUNT(1) FROM cached_blobs`).Scan(&blobCount); err != nil {
		t.Fatalf("blob count: %v", err)
	}
	if err := cache.DB().QueryRow(`SELECT COUNT(1) FROM cached_assets`).Scan(&assetCount); err != nil {
		t.Fatalf("asset count: %v", err)
	}
	if blobCount != 1 {
		t.Fatalf("blob count = %d, want 1 (three asset IDs share one blob)", blobCount)
	}
	if assetCount != 3 {
		t.Fatalf("asset count = %d, want 3 (logical mappings retained)", assetCount)
	}

	var canonicalPath string
	if err := cache.DB().QueryRow(`SELECT local_path FROM cached_blobs WHERE content_hash = ?`, string(hash)).Scan(&canonicalPath); err != nil {
		t.Fatalf("canonical blob path: %v", err)
	}
	if canonicalPath != path1 {
		t.Fatalf("canonical blob path = %q, want first writer %q", canonicalPath, path1)
	}

	// Exactly one physical file exists for the three asset IDs.
	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("canonical blob file missing: %v", err)
	}
	if _, err := os.Stat(path2); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("path2 should not exist as a separate physical file, stat err=%v", err)
	}
	if _, err := os.Stat(path3); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("path3 should not exist as a separate physical file, stat err=%v", err)
	}
}

// TestCacheAcceptance_NoEvictionBelowHighWatermark proves the pressure pass is
// a no-op while disk usage is below the high watermark, even when the cache
// holds warm, unprotected, complete blobs that the legacy cleaner would have
// deleted.
func TestCacheAcceptance_NoEvictionBelowHighWatermark(t *testing.T) {
	cache := newTestCache(t)
	ctx := context.Background()
	dir := t.TempDir()

	for _, key := range []string{"COLD-A", "COLD-B", "COLD-C"} {
		path := filepath.Join(dir, key+".mp4")
		if err := os.WriteFile(path, []byte(key+" bytes"), 0o644); err != nil {
			t.Fatalf("write %s: %v", key, err)
		}
		if err := cache.Store(ctx, Entry{
			AssetKey:         assetref.AssetKey(key),
			ContentHash:      acceptanceContentHash([]byte(key + " bytes")),
			LocalPath:        path,
			SizeBytes:        int64(len(key + " bytes")),
			DownloadComplete: true,
		}); err != nil {
			t.Fatalf("Store %s: %v", key, err)
		}
	}

	stats, err := EvictUnderPressure(ctx, cache, PressureEvictionConfig{
		HighWatermarkPercent: 80,
		LowWatermarkPercent:  72,
		BatchSize:            128,
	}, func() int { return 79 }, nil)
	if err != nil {
		t.Fatalf("EvictUnderPressure: %v", err)
	}
	if stats.Removed != 0 || stats.Attempted != 0 {
		t.Fatalf("stats = %+v, want zero evictions below the high watermark", stats)
	}
	for _, key := range []string{"COLD-A", "COLD-B", "COLD-C"} {
		if _, err := os.Stat(filepath.Join(dir, key+".mp4")); err != nil {
			t.Fatalf("blob %s was evicted below the high watermark: %v", key, err)
		}
	}
}

// TestCacheAcceptance_PressureEvictsLRUOnlyAndNeverProtected proves the
// pressure pass: at/above the high watermark it removes unleased LRU blobs
// until usage falls to the low watermark, while leased blobs and
// snapshot-protected blobs survive.
func TestCacheAcceptance_PressureEvictsLRUOnlyAndNeverProtected(t *testing.T) {
	cache := newTestCache(t)
	ctx := context.Background()
	dir := t.TempDir()

	store := func(key, path string) assetref.ContentHash {
		hash := acceptanceContentHash([]byte(key + " bytes"))
		if err := cache.Store(ctx, Entry{
			AssetKey:         assetref.AssetKey(key),
			ContentHash:      hash,
			LocalPath:        path,
			SizeBytes:        int64(len(key + " bytes")),
			DownloadComplete: true,
		}); err != nil {
			t.Fatalf("Store %s: %v", key, err)
		}
		return hash
	}

	lruPath := filepath.Join(dir, "LRU.mp4")
	if err := os.WriteFile(lruPath, []byte("LRU bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	store("LRU", lruPath)

	leasedPath := filepath.Join(dir, "LEASED.mp4")
	if err := os.WriteFile(leasedPath, []byte("LEASED bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	store("LEASED", leasedPath)
	if err := cache.Acquire(ctx, "LEASED", "job-active"); err != nil {
		t.Fatalf("Acquire LEASED: %v", err)
	}

	snapshotPath := filepath.Join(dir, "SNAPSHOT.mp4")
	if err := os.WriteFile(snapshotPath, []byte("SNAPSHOT bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	store("SNAPSHOT", snapshotPath)

	// The fake usage probe reports above the high watermark once, then below
	// the low watermark (simulating the freed bytes of the evicted LRU blob).
	calls := 0
	usage := func() int {
		calls++
		if calls == 1 {
			return 85
		}
		return 70
	}
	stats, err := EvictUnderPressure(ctx, cache, PressureEvictionConfig{
		HighWatermarkPercent: 80,
		LowWatermarkPercent:  72,
		BatchSize:            128,
	}, usage, map[string]struct{}{"SNAPSHOT": {}})
	if err != nil {
		t.Fatalf("EvictUnderPressure: %v", err)
	}
	if stats.Removed != 1 {
		t.Fatalf("Removed = %d, want 1 (only the unleased LRU blob)", stats.Removed)
	}
	if _, err := os.Stat(lruPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("LRU blob should be evicted, stat err=%v", err)
	}
	if _, err := os.Stat(leasedPath); err != nil {
		t.Fatalf("leased blob was removed: %v", err)
	}
	if _, err := os.Stat(snapshotPath); err != nil {
		t.Fatalf("snapshot-protected blob was removed: %v", err)
	}
}

// TestCacheAcceptance_ReservationProtectsBlobUnderPressure proves the
// blob-level barrier: a blob referenced by a future-job reservation is not a
// pressure candidate even with no lease and no snapshot entry.
func TestCacheAcceptance_ReservationProtectsBlobUnderPressure(t *testing.T) {
	cache := newTestCache(t)
	ctx := context.Background()
	dir := t.TempDir()

	reservedPath := filepath.Join(dir, "RESERVED.mp4")
	if err := os.WriteFile(reservedPath, []byte("RESERVED bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := cache.Store(ctx, Entry{
		AssetKey:         "RESERVED",
		ContentHash:      acceptanceContentHash([]byte("RESERVED bytes")),
		LocalPath:        reservedPath,
		SizeBytes:        int64(len("RESERVED bytes")),
		DownloadComplete: true,
	}); err != nil {
		t.Fatalf("Store RESERVED: %v", err)
	}
	if err := cache.Reserve(ctx, "RESERVED", "future-job", time.Now().UTC().Add(time.Hour)); err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	stats, err := EvictUnderPressure(ctx, cache, PressureEvictionConfig{
		HighWatermarkPercent: 80,
		LowWatermarkPercent:  72,
		BatchSize:            128,
	}, func() int { return 85 }, nil)
	if err != nil {
		t.Fatalf("EvictUnderPressure: %v", err)
	}
	if stats.Removed != 0 {
		t.Fatalf("Removed = %d, want 0 (reservation protects the blob)", stats.Removed)
	}
	if _, err := os.Stat(reservedPath); err != nil {
		t.Fatalf("reserved blob was removed: %v", err)
	}
}
