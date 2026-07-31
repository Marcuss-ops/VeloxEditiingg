package cacheevict

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"velox-worker-agent/internal/workercache"
)

func writeCacheFile(t *testing.T, root, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(root, name)
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

func TestRun_DryRunDoesNotMutateFilesOrIndex(t *testing.T) {
	root := t.TempDir()
	path := writeCacheFile(t, root, "clip-stock-a_0123456789ab.mp4", []byte("clip"))
	index := openIndex(t)
	if err := index.Store(context.Background(), workercache.Entry{DriveFileID: "clip-stock-a", LocalPath: path, DownloadComplete: true}); err != nil {
		t.Fatalf("store index: %v", err)
	}

	items, err := Run(context.Background(), Options{Root: root, AssetIDs: []string{"clip-stock-a"}, Index: index})
	if err != nil {
		t.Fatalf("Run dry-run: %v", err)
	}
	if len(items) != 1 || items[0].Action != "dry_run" || items[0].CacheStatus != "present" {
		t.Fatalf("items = %+v, want one present dry-run item", items)
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
	selected := writeCacheFile(t, root, "clip-stock-a_0123456789ab.mp4", []byte("selected"))
	unselected := writeCacheFile(t, root, "clip-stock-ab_0123456789ab.mp4", []byte("keep"))
	other := writeCacheFile(t, root, "production-asset.mp4", []byte("keep"))
	index := openIndex(t)
	if err := index.Store(context.Background(), workercache.Entry{DriveFileID: "clip-stock-a", LocalPath: selected, DownloadComplete: true}); err != nil {
		t.Fatalf("store selected: %v", err)
	}
	if err := index.Store(context.Background(), workercache.Entry{DriveFileID: "clip-stock-ab", LocalPath: unselected, DownloadComplete: true}); err != nil {
		t.Fatalf("store unselected: %v", err)
	}

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
	for _, keep := range []string{unselected, other} {
		if _, err := os.Stat(keep); err != nil {
			t.Fatalf("unselected file %s changed: %v", keep, err)
		}
	}
	if _, ok, err := index.Find(context.Background(), "clip-stock-a"); err != nil || ok {
		t.Fatalf("selected index row remains: ok=%v err=%v", ok, err)
	}
	if _, ok, err := index.Find(context.Background(), "clip-stock-ab"); err != nil || !ok {
		t.Fatalf("unselected index row changed: ok=%v err=%v", ok, err)
	}
}

func TestRun_ExecuteRefusesActiveLeaseWithoutMutation(t *testing.T) {
	root := t.TempDir()
	path := writeCacheFile(t, root, "voiceover-smoke_deadbeefdead.mp3", []byte("voice"))
	index := openIndex(t)
	if err := index.Store(context.Background(), workercache.Entry{DriveFileID: "voiceover-smoke", LocalPath: path, DownloadComplete: true}); err != nil {
		t.Fatalf("store: %v", err)
	}
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
	items, err := Run(context.Background(), Options{Root: root, AssetIDs: []string{"missing-asset"}})
	if err != nil {
		t.Fatalf("Run missing: %v", err)
	}
	if len(items) != 1 || items[0].Action != "dry_run" || items[0].CacheStatus != "miss" {
		t.Fatalf("items = %+v, want one missing dry-run item", items)
	}
}

func TestRun_RejectsInvalidIDsAndUnsafeRoots(t *testing.T) {
	root := t.TempDir()
	for _, ids := range [][]string{{"../production"}, {"asset/child"}, {""}} {
		if _, err := Run(context.Background(), Options{Root: root, AssetIDs: ids}); !errors.Is(err, ErrInvalidID) && !errors.Is(err, ErrNoAssetIDs) {
			t.Errorf("IDs=%q err=%v, want input validation error", ids, err)
		}
	}
	if _, err := Run(context.Background(), Options{Root: "/", AssetIDs: []string{"safe"}}); !errors.Is(err, ErrUnsafeRoot) {
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
	if err := index.Store(context.Background(), workercache.Entry{DriveFileID: "outside", LocalPath: outside, DownloadComplete: true}); err != nil {
		t.Fatalf("store: %v", err)
	}
	_, err := Run(context.Background(), Options{Root: root, AssetIDs: []string{"outside"}, Index: index, Execute: true})
	if err == nil {
		t.Fatal("Run unexpectedly accepted indexed path outside cache root")
	}
	if _, statErr := os.Stat(outside); statErr != nil {
		t.Fatalf("outside file changed: %v", statErr)
	}
}
