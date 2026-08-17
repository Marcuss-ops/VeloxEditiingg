package worker

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"velox-shared/assetref"
	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/internal/workercache"
	"velox-worker-agent/pkg/config"
)

// TestCheck_IndexCompleteButBlobGone_ClassifiesMissExpired pins the
// MISS_EXPIRED classification: the durable index claims a complete entry but
// the physical blob is gone. It uses a root-less cache (workercache.Open) so
// the cache layer's Find skips its own stat and Check's stat is the only gate.
func TestCheck_IndexCompleteButBlobGone_ClassifiesMissExpired(t *testing.T) {
	cache, err := workercache.Open(filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatalf("open cache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	gonePath := filepath.Join(t.TempDir(), "gone-blob.mp4") // intentionally never created

	w := &Worker{
		config:              &config.WorkerConfig{StateDir: t.TempDir()},
		canonicalAssetCache: workercache.NewCanonicalAssetStore(cache),
	}
	if err := cache.Store(context.Background(), workercache.Entry{
		AssetKey:         assetref.AssetKey("asset-gone"),
		LocalPath:        gonePath,
		SizeBytes:        1234,
		DownloadComplete: true,
	}); err != nil {
		t.Fatalf("seed entry: %v", err)
	}

	tfer := &masterAssetTransferer{w: w}
	got, err := tfer.Check(context.Background(), context.Background(), downloader.DownloadRequest{
		AssetKey:  assetref.AssetKey("asset-gone"),
		AssetID:   "asset-gone",
		SizeBytes: 1234,
	})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if got.Outcome != downloader.CacheOutcomeMissExpired {
		t.Fatalf("outcome = %q, want %q", got.Outcome, downloader.CacheOutcomeMissExpired)
	}
}

// TestCheck_IndexCompleteAndBlobPresent_ClassifiesHitValid is the positive
// control: the same root-less index entry with a REAL file is a hit.
func TestCheck_IndexCompleteAndBlobPresent_ClassifiesHitValid(t *testing.T) {
	cache, err := workercache.Open(filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatalf("open cache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	blobPath := filepath.Join(t.TempDir(), "present-blob.mp4")
	if err := os.WriteFile(blobPath, []byte("bytes"), 0o644); err != nil {
		t.Fatal(err)
	}

	w := &Worker{
		config:              &config.WorkerConfig{StateDir: t.TempDir()},
		canonicalAssetCache: workercache.NewCanonicalAssetStore(cache),
	}
	if err := cache.Store(context.Background(), workercache.Entry{
		AssetKey:         assetref.AssetKey("asset-present"),
		LocalPath:        blobPath,
		SizeBytes:        5,
		DownloadComplete: true,
	}); err != nil {
		t.Fatalf("seed entry: %v", err)
	}

	tfer := &masterAssetTransferer{w: w}
	got, err := tfer.Check(context.Background(), context.Background(), downloader.DownloadRequest{
		AssetKey:  assetref.AssetKey("asset-present"),
		AssetID:   "asset-present",
		SizeBytes: 5,
	})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if got.Outcome != downloader.CacheOutcomeHitValid {
		t.Fatalf("outcome = %q, want %q", got.Outcome, downloader.CacheOutcomeHitValid)
	}
	if got.LocalPath != blobPath {
		t.Fatalf("local path = %q, want %q", got.LocalPath, blobPath)
	}
}
