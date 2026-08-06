package workercache

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"velox-shared/assetref"
)

func TestCanonicalAssetStoreTypedBoundaryPreservesHashAndProtection(t *testing.T) {
	cache, err := Open(filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cache.Close()

	store := cache.AsCanonicalStore()
	if store == nil {
		t.Fatal("AsCanonicalStore returned nil")
	}
	var registry AssetRegistry = store
	var contentCache ContentAddressedCache = store
	var protections LeaseReservationStore = store
	ctx := context.Background()
	key := assetref.AssetKey("canonical-asset")
	hash := assetref.ContentHash("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")

	if err := registry.Store(ctx, Entry{AssetKey: key, LocalPath: filepath.Join(t.TempDir(), "asset.mp4")}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := contentCache.MarkDownloadCompleteWithHash(ctx, key, filepath.Join(t.TempDir(), "asset.mp4"), 64, hash); err != nil {
		t.Fatalf("MarkDownloadCompleteWithHash: %v", err)
	}
	entry, found, err := registry.Find(ctx, key)
	if err != nil || !found {
		t.Fatalf("Find: found=%v err=%v", found, err)
	}
	if entry.AssetKey != key || entry.ContentHash != hash {
		t.Fatalf("entry identity/hash = %q/%q, want %q/%q", entry.AssetKey, entry.ContentHash, key, hash)
	}

	if err := protections.Reserve(ctx, key, "future-job", time.Now().UTC().Add(time.Hour)); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if err := contentCache.EvictIfUnleased(ctx, key, entry.LocalPath); !errors.Is(err, ErrNotFound) {
		t.Fatalf("protected eviction err=%v, want ErrNotFound", err)
	}
	if _, found, err := registry.Find(ctx, key); err != nil || !found {
		t.Fatalf("protected asset disappeared: found=%v err=%v", found, err)
	}
	if err := protections.ReleaseReservation(ctx, key, "future-job"); err != nil {
		t.Fatalf("ReleaseReservation: %v", err)
	}
	if err := protections.Acquire(ctx, key, "active-job"); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := contentCache.EvictIfUnleased(ctx, key, entry.LocalPath); !errors.Is(err, ErrNotFound) {
		t.Fatalf("leased eviction err=%v, want ErrNotFound", err)
	}
	if err := protections.Release(ctx, key, "active-job"); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := contentCache.EvictIfUnleased(ctx, key, entry.LocalPath); err != nil {
		t.Fatalf("unprotected eviction: %v", err)
	}
	if _, found, err := registry.Find(ctx, key); err != nil || found {
		t.Fatalf("entry remains after unprotected eviction: found=%v err=%v", found, err)
	}
}
