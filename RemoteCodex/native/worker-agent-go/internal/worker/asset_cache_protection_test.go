package worker

import (
	"context"
	"path/filepath"
	"testing"

	"velox-shared/assetref"
	"velox-worker-agent/internal/workercache"
)

func TestSyncClipCache_CacheHitPreservesExistingVerifiedHash(t *testing.T) {
	ctx := context.Background()
	cache, err := workercache.Open(filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatalf("Open cache: %v", err)
	}
	defer cache.Close()

	assetID := "cache-hit-hash-invariant"
	assetKey := assetref.AssetKey(assetID)
	verifiedHash := assetref.ContentHash("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	localPath := filepath.Join(t.TempDir(), "asset.mp4")
	store := cache.AsCanonicalStore()
	if err := store.Store(ctx, workercache.Entry{
		AssetKey:         assetKey,
		ContentHash:      verifiedHash,
		LocalPath:        localPath,
		SizeBytes:        123,
		DownloadComplete: true,
	}); err != nil {
		t.Fatalf("seed verified cache entry: %v", err)
	}

	// A cache hit may not carry fresh integrity metadata. The duplicate
	// Store path must therefore preserve the hash already verified and
	// persisted in the canonical cache rather than replacing it with empty.
	w := &Worker{canonicalAssetCache: store}
	if err := w.syncClipCache(ctx, assetID, localPath, 123, ""); err != nil {
		t.Fatalf("sync cache hit: %v", err)
	}

	entry, found, err := store.Find(ctx, assetKey)
	if err != nil || !found {
		t.Fatalf("Find after cache-hit sync: found=%v err=%v", found, err)
	}
	if entry.ContentHash != verifiedHash {
		t.Fatalf("ContentHash after cache-hit sync=%q, want unchanged %q", entry.ContentHash, verifiedHash)
	}
	if !entry.DownloadComplete {
		t.Fatal("cache-hit sync unexpectedly cleared DownloadComplete")
	}
}
