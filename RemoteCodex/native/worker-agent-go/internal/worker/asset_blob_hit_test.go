package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"velox-shared/assetref"
	"velox-worker-agent/internal/workercache"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/config"
)

// TestDownloadAsset_CrossAssetContentHashBlobHit proves the resolver routes a
// known SHA through the blob store: asset B has no asset_key mapping, but its
// bytes are already cached under asset A's mapping, so B resolves as a
// verified hit (zero upstream requests) and its mapping is registered for the
// next asset-key lookup.
func TestDownloadAsset_CrossAssetContentHashBlobHit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected upstream request: %s", r.URL.Path)
		http.NotFound(w, r)
	}))
	defer server.Close()

	stateDir := t.TempDir()
	w := &Worker{
		config:    &config.WorkerConfig{MasterURL: server.URL, StateDir: stateDir},
		apiClient: api.NewClient(server.URL),
	}
	cache, err := workercache.Open(filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatalf("open cache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	w.canonicalAssetCache = workercache.NewCanonicalAssetStore(cache)

	payload := []byte("shared bytes across assets")
	digest := sha256Hex(payload)
	blobPath := assetBlobPath(w.assetCacheDir(), digest, ".mp4")
	if err := os.MkdirAll(filepath.Dir(blobPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blobPath, payload, 0o644); err != nil {
		t.Fatal(err)
	}

	// Seed only asset A; asset B has no asset_key mapping yet.
	if err := cache.Store(context.Background(), workercache.Entry{
		AssetKey:         assetref.AssetKey("asset-A"),
		ContentHash:      assetref.ContentHash(digest),
		LocalPath:        blobPath,
		SizeBytes:        int64(len(payload)),
		DownloadComplete: true,
	}); err != nil {
		t.Fatalf("seed asset A: %v", err)
	}

	got, err := w.downloadVeloxAssetWithMetadata(context.Background(), "asset-B", digest, int64(len(payload)))
	if err != nil {
		t.Fatalf("download asset-B: %v", err)
	}
	if got != blobPath {
		t.Fatalf("resolved path = %q, want shared blob path %q", got, blobPath)
	}

	// The caller's syncClipCache registers the asset-B → SHA mapping so a
	// later asset-key lookup resolves to the same blob.
	entry, found, err := cache.Find(context.Background(), "asset-B")
	if err != nil || !found {
		t.Fatalf("asset-B mapping after hit: found=%v err=%v", found, err)
	}
	if entry.ContentHash != assetref.ContentHash(digest) {
		t.Fatalf("asset-B content hash = %q, want %q", entry.ContentHash, digest)
	}
}
