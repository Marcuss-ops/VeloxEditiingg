package workercache

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"velox-shared/assetref"
)

const validationSHA = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestFind_InvalidAliasBlobSizeSelfHeals(t *testing.T) {
	root := t.TempDir()
	cache, err := OpenWithRoot(filepath.Join(t.TempDir(), "cache.db"), root)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	path := filepath.Join(root, "ab", validationSHA+".mp4")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("wrong-size"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := cache.Store(context.Background(), Entry{
		AssetKey:         "asset-alias",
		ContentHash:      assetref.ContentHash(validationSHA),
		LocalPath:        path,
		SizeBytes:        999,
		DownloadComplete: true,
	}); err != nil {
		t.Fatal(err)
	}

	entry, found, err := cache.Find(context.Background(), "asset-alias")
	if err != nil {
		t.Fatal(err)
	}
	if !found || entry.DownloadComplete || entry.LocalPath != "" {
		t.Fatalf("Find = %+v found=%v, want retained alias with invalid blob marked miss", entry, found)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("owned invalid blob should be removed, stat err=%v", err)
	}
	if _, found, err := cache.FindBlob(context.Background(), assetref.ContentHash(validationSHA)); err != nil || found {
		t.Fatalf("FindBlob after self-heal = found=%v err=%v, want absent", found, err)
	}

	// The logical mapping remains, so the downloader can associate the miss
	// with the same content identity and redownload it.
	mapping, found, err := cache.Find(context.Background(), "asset-alias")
	if err != nil || !found || mapping.ContentHash != assetref.ContentHash(validationSHA) {
		t.Fatalf("mapping after self-heal = %+v found=%v err=%v", mapping, found, err)
	}
}

func TestFind_InvalidBlobOutsideRootNeverRemovesExternalPath(t *testing.T) {
	root := t.TempDir()
	external := filepath.Join(t.TempDir(), "outside.mp4")
	if err := os.WriteFile(external, []byte("must survive"), 0o644); err != nil {
		t.Fatal(err)
	}
	cache, err := OpenWithRoot(filepath.Join(t.TempDir(), "cache.db"), root)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	if err := cache.Store(context.Background(), Entry{
		AssetKey:         "external-path",
		ContentHash:      assetref.ContentHash(validationSHA),
		LocalPath:        external,
		SizeBytes:        1,
		DownloadComplete: true,
	}); err != nil {
		t.Fatal(err)
	}

	if _, found, err := cache.Find(context.Background(), "external-path"); err != nil || !found {
		t.Fatalf("Find = found=%v err=%v, want retained mapping", found, err)
	}
	if _, err := os.Stat(external); err != nil {
		t.Fatalf("external path was removed: %v", err)
	}
}
