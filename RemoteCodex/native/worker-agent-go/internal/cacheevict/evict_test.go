package cacheevict

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"velox-shared/assetref"
	"velox-worker-agent/internal/workercache"
)

const (
	hashA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	hashB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	hashC = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	hashD = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
)

// blobPath builds a content-addressed (v5) blob location, mirroring the
// worker's assetBlobPath layout: <root>/<sha[:2]>/<sha><ext>.
func blobPath(root, sha, ext string) string {
	return filepath.Join(root, sha[:2], sha+ext)
}

func writeCacheFile(t *testing.T, path string, data []byte) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir for %s: %v", path, err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func openIndex(t *testing.T) *workercache.Cache {
	t.Helper()
	index, err := workercache.Open(filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatalf("open index: %v", err)
	}
	t.Cleanup(func() { _ = index.Close() })
	return index
}

func storeEntry(t *testing.T, index *workercache.Cache, assetKey, sha, localPath string, data []byte) {
	t.Helper()
	if err := index.Store(context.Background(), workercache.Entry{
		AssetKey:         assetref.AssetKey(assetKey),
		ContentHash:      assetref.ContentHash(sha),
		LocalPath:        localPath,
		SizeBytes:        int64(len(data)),
		DownloadComplete: true,
	}); err != nil {
		t.Fatalf("store %s: %v", assetKey, err)
	}
}

func TestRun_DryRunDoesNotMutateFilesOrIndex(t *testing.T) {
	root := t.TempDir()
	path := writeCacheFile(t, blobPath(root, hashA, ".mp4"), []byte("clip"))
	index := openIndex(t)
	storeEntry(t, index, "clip-stock-a", hashA, path, []byte("clip"))

	items, err := Run(context.Background(), Options{Root: root, AssetIDs: []string{"clip-stock-a"}, Index: index})
	if err != nil {
		t.Fatalf("Run dry-run: %v", err)
	}
	if len(items) != 1 || items[0].Action != "dry_run" || items[0].CacheStatus != "present" {
		t.Fatalf("items = %+v, want one present dry-run item", items)
	}
	if items[0].Bytes != int64(len("clip")) {
		t.Fatalf("bytes = %d, want %d", items[0].Bytes, len("clip"))
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("dry-run removed file: %v", err)
	}
	if _, ok, err := index.Find(context.Background(), "clip-stock-a"); err != nil || !ok {
		t.Fatalf("dry-run changed index: ok=%v err=%v", ok, err)
	}
}

func TestRun_ExecuteDeletesOnlyExactSelectedAsset(t *testing.T) {
	root := t.TempDir()
	selected := writeCacheFile(t, blobPath(root, hashA, ".mp4"), []byte("selected"))
	unselected := writeCacheFile(t, blobPath(root, hashB, ".mp4"), []byte("keep"))
	// An unindexed file must be untouched: the index is the sole authority.
	orphan := writeCacheFile(t, filepath.Join(root, "orphan.mp4"), []byte("keep"))
	index := openIndex(t)
	storeEntry(t, index, "clip-stock-a", hashA, selected, []byte("selected"))
	storeEntry(t, index, "clip-stock-ab", hashB, unselected, []byte("keep"))

	items, err := Run(context.Background(), Options{Root: root, AssetIDs: []string{"clip-stock-a"}, Index: index, Execute: true})
	if err != nil {
		t.Fatalf("Run execute: %v", err)
	}
	if len(items) != 1 || items[0].Action != "deleted" {
		t.Fatalf("items = %+v, want deleted item", items)
	}
	if _, err := os.Stat(selected); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("selected file still exists, err=%v", err)
	}
	for _, keep := range []string{unselected, orphan} {
		if _, err := os.Stat(keep); err != nil {
			t.Fatalf("unselected/orphan file %s changed: %v", keep, err)
		}
	}
	if _, ok, err := index.Find(context.Background(), "clip-stock-a"); err != nil || ok {
		t.Fatalf("selected index row remains: ok=%v err=%v", ok, err)
	}
	if _, ok, err := index.Find(context.Background(), "clip-stock-ab"); err != nil || !ok {
		t.Fatalf("unselected index row changed: ok=%v err=%v", ok, err)
	}
}

func TestRun_ExecuteDeletesIndexedPathRegardlessOfExtension(t *testing.T) {
	root := t.TempDir()
	path := writeCacheFile(t, blobPath(root, hashC, ".f4v"), []byte("selected"))
	index := openIndex(t)
	storeEntry(t, index, "clip-stock-a", hashC, path, []byte("selected"))

	items, err := Run(context.Background(), Options{Root: root, AssetIDs: []string{"clip-stock-a"}, Index: index, Execute: true})
	if err != nil {
		t.Fatalf("Run execute: %v", err)
	}
	if len(items) != 1 || items[0].Action != "deleted" || items[0].Bytes == 0 {
		t.Fatalf("items = %+v, want deleted f4v item with bytes", items)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("f4v file still exists, err=%v", err)
	}
}

func TestRun_ExecuteRefusesActiveReservationWithoutMutation(t *testing.T) {
	root := t.TempDir()
	path := writeCacheFile(t, blobPath(root, hashA, ".mp4"), []byte("future"))
	index := openIndex(t)
	storeEntry(t, index, "future-asset", hashA, path, []byte("future"))
	if err := index.Reserve(context.Background(), "future-asset", "future-job", time.Now().UTC().Add(time.Hour)); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	_, err := Run(context.Background(), Options{Root: root, AssetIDs: []string{"future-asset"}, Index: index, Execute: true})
	if !errors.Is(err, ErrActiveReservation) {
		t.Fatalf("Run err=%v, want ErrActiveReservation", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("reserved file removed: %v", err)
	}
}

func TestRun_ExecuteRefusesActiveLeaseWithoutMutation(t *testing.T) {
	root := t.TempDir()
	path := writeCacheFile(t, blobPath(root, hashD, ".mp3"), []byte("voice"))
	index := openIndex(t)
	storeEntry(t, index, "voiceover-smoke", hashD, path, []byte("voice"))
	if err := index.Acquire(context.Background(), "voiceover-smoke", "job-1"); err != nil {
		t.Fatalf("acquire: %v", err)
	}

	_, err := Run(context.Background(), Options{Root: root, AssetIDs: []string{"voiceover-smoke"}, Index: index, Execute: true})
	if !errors.Is(err, ErrActiveLease) {
		t.Fatalf("Run err=%v, want ErrActiveLease", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("leased file removed: %v", err)
	}
}

func TestRun_MissingAssetIsReportedWithoutMutation(t *testing.T) {
	root := t.TempDir()
	index := openIndex(t)
	items, err := Run(context.Background(), Options{Root: root, AssetIDs: []string{"missing-asset"}, Index: index})
	if err != nil {
		t.Fatalf("Run missing: %v", err)
	}
	if len(items) != 1 || items[0].Action != "dry_run" || items[0].CacheStatus != "miss" {
		t.Fatalf("items = %+v, want one missing dry-run item", items)
	}
}

func TestRun_RequiresIndex(t *testing.T) {
	_, err := Run(context.Background(), Options{Root: t.TempDir(), AssetIDs: []string{"safe"}})
	if !errors.Is(err, ErrNoIndex) {
		t.Fatalf("Run without index err=%v, want ErrNoIndex", err)
	}
}

func TestRun_RejectsInvalidIDsAndUnsafeRoots(t *testing.T) {
	root := t.TempDir()
	index := openIndex(t)
	for _, ids := range [][]string{{"../production"}, {"asset/child"}, {""}} {
		if _, err := Run(context.Background(), Options{Root: root, AssetIDs: ids, Index: index}); !errors.Is(err, ErrInvalidID) && !errors.Is(err, ErrNoAssetIDs) {
			t.Errorf("IDs=%q err=%v, want input validation error", ids, err)
		}
	}
	if _, err := Run(context.Background(), Options{Root: "/", AssetIDs: []string{"safe"}, Index: index}); !errors.Is(err, ErrUnsafeRoot) {
		t.Errorf("root=/ err=%v, want ErrUnsafeRoot", err)
	}
}

func TestRun_RejectsIndexedPathOutsideRoot(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.mp4")
	if err := os.WriteFile(outside, []byte("do not remove"), 0o600); err != nil {
		t.Fatal(err)
	}
	index := openIndex(t)
	storeEntry(t, index, "outside", hashA, outside, []byte("do not remove"))
	_, err := Run(context.Background(), Options{Root: root, AssetIDs: []string{"outside"}, Index: index, Execute: true})
	if err == nil {
		t.Fatal("Run unexpectedly accepted indexed path outside cache root")
	}
	if _, statErr := os.Stat(outside); statErr != nil {
		t.Fatalf("outside file changed: %v", statErr)
	}
}

// TestRun_ExecuteKeepsSharedBlobUntilLastAssetEvicted locks the dedup contract
// through the tool boundary: two assets referencing the same content-addressed
// blob share one physical file, and evicting one asset must leave the file
// (and the surviving asset's index row) untouched until the LAST referent is
// evicted.
func TestRun_ExecuteKeepsSharedBlobUntilLastAssetEvicted(t *testing.T) {
	root := t.TempDir()
	payload := []byte("shared blob bytes")
	shared := writeCacheFile(t, blobPath(root, hashA, ".mp4"), payload)
	index := openIndex(t)
	storeEntry(t, index, "asset-a", hashA, shared, payload)
	storeEntry(t, index, "asset-b", hashA, shared, payload)

	// Evict only asset-a: the blob is still referenced by asset-b, so the
	// physical file must survive and asset-b's index row must remain.
	items, err := Run(context.Background(), Options{Root: root, AssetIDs: []string{"asset-a"}, Index: index, Execute: true})
	if err != nil {
		t.Fatalf("Run evict asset-a: %v", err)
	}
	if len(items) != 1 || items[0].Action != "deleted" {
		t.Fatalf("items = %+v, want one deleted item", items)
	}
	if _, err := os.Stat(shared); err != nil {
		t.Fatalf("shared file removed while asset-b still references it: %v", err)
	}
	if _, ok, err := index.Find(context.Background(), "asset-a"); err != nil || ok {
		t.Fatalf("asset-a index row remains: ok=%v err=%v", ok, err)
	}
	if _, ok, err := index.Find(context.Background(), "asset-b"); err != nil || !ok {
		t.Fatalf("asset-b index row changed: ok=%v err=%v", ok, err)
	}

	// Evict asset-b (the last referent): the blob is now orphaned and the
	// physical file must be removed.
	if _, err := Run(context.Background(), Options{Root: root, AssetIDs: []string{"asset-b"}, Index: index, Execute: true}); err != nil {
		t.Fatalf("Run evict asset-b: %v", err)
	}
	if _, err := os.Stat(shared); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("shared file still exists after last referent evicted: %v", err)
	}
	if _, ok, err := index.Find(context.Background(), "asset-b"); err != nil || ok {
		t.Fatalf("asset-b index row remains: ok=%v err=%v", ok, err)
	}
}
