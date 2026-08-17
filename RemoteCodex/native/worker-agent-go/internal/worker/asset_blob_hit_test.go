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

// TestDownloadVeloxAsset_IgnoresLegacyFlatFile locks the post-v5 removal of
// the pre-v5 flat filesystem fallback: a legacy <assetID>_<sha12>.ext file
// carrying the CORRECT bytes is still a miss. The only recognized physical
// identity is the content-addressed blob path, so the asset is re-downloaded
// and promoted to <sha[:2]>/<sha><ext>; the orphaned flat file is ignored,
// never resolved and never deleted by the lookup.
func TestDownloadVeloxAsset_IgnoresLegacyFlatFile(t *testing.T) {
	assetID := "legacy-flat-asset"
	assetBytes := []byte("legacy flat layout bytes")
	digest := sha256Hex(assetBytes)
	requestCount := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if r.URL.Path != "/api/v1/agent/assets/"+assetID {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "audio/mpeg")
		_, _ = w.Write(assetBytes)
	}))
	defer srv.Close()

	workerDir := t.TempDir()
	w := &Worker{
		config:    &config.WorkerConfig{MasterURL: srv.URL, WorkDir: workerDir},
		apiClient: api.NewClient(srv.URL),
	}
	cacheDir := w.assetCacheDir()
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Seed the pre-v5 flat layout with the CORRECT bytes so a resolver that
	// still honored it would return a valid hit and never contact the master.
	flatPath := filepath.Join(cacheDir, assetPartialKey(assetID, digest)+".mp3")
	if err := os.WriteFile(flatPath, assetBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	size := int64(len(assetBytes))

	// Unit level: the content-addressed probe must not resolve the flat file.
	if got, err := cachedAssetPath(cacheDir, digest, size); err != nil || got != "" {
		t.Fatalf("flat file resolved = %q, err=%v; legacy flat layout must be a miss", got, err)
	}

	// End to end: the resolver re-downloads and promotes to the blob path.
	path, err := w.downloadVeloxAssetWithMetadata(context.Background(), assetID, digest, size)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	if requestCount != 1 {
		t.Fatalf("master request count = %d, want 1 (flat file must not satisfy the lookup)", requestCount)
	}
	if want := assetBlobPath(cacheDir, digest, ".mp3"); path != want {
		t.Fatalf("resolved path = %q, want content-addressed blob %q", path, want)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != string(assetBytes) {
		t.Fatalf("redownloaded bytes = %q, err=%v; want %q", got, err, assetBytes)
	}
	// The legacy flat file is orphaned but not deleted by the lookup.
	if got, err := os.ReadFile(flatPath); err != nil || string(got) != string(assetBytes) {
		t.Fatalf("legacy flat file changed: %q, err=%v", got, err)
	}
}
