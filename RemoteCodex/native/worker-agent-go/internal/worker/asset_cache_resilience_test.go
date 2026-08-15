package worker

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAssetPartialCleanupRemovesOnlyStalePartials(t *testing.T) {
	cacheDir := t.TempDir()
	partialDir := filepath.Join(cacheDir, "partial")
	if err := os.MkdirAll(partialDir, 0o755); err != nil {
		t.Fatal(err)
	}
	oldPath := filepath.Join(partialDir, "old.part")
	newPath := filepath.Join(partialDir, "new.part")
	if err := os.WriteFile(oldPath, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(newPath, []byte("new"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(oldPath, old, old); err != nil {
		t.Fatal(err)
	}

	removed, err := cleanupOrphanedAssetPartials(cacheDir, 24*time.Hour)
	if err != nil {
		t.Fatalf("cleanup partials: %v", err)
	}
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("old partial still exists, stat err=%v", err)
	}
	if _, err := os.Stat(newPath); err != nil {
		t.Fatalf("recent partial should remain: %v", err)
	}
}

func TestAssetPartialCleanupKeepsActivePartial(t *testing.T) {
	cacheDir := t.TempDir()
	partPath := assetPartialPath(cacheDir, "active-asset", "")
	if err := os.MkdirAll(filepath.Dir(partPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(partPath, []byte("active"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(partPath, old, old); err != nil {
		t.Fatal(err)
	}
	deactivate := markAssetPartialActive(partPath)
	defer deactivate()

	removed, err := cleanupOrphanedAssetPartials(cacheDir, 24*time.Hour)
	if err != nil {
		t.Fatalf("cleanup active partial: %v", err)
	}
	if removed != 0 {
		t.Fatalf("removed = %d, want 0 for active partial", removed)
	}
	if _, err := os.Stat(partPath); err != nil {
		t.Fatalf("active partial removed: %v", err)
	}
}

func TestWriteAssetCacheAtOffsetPromotesOnlyCompleteVerifiedPartial(t *testing.T) {
	cacheDir := t.TempDir()
	assetID := "asset-atomic"
	data := []byte("complete verified asset")
	partPath := assetPartialPath(cacheDir, assetID, "")
	if err := os.MkdirAll(filepath.Dir(partPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(partPath, data[:7], 0o644); err != nil {
		t.Fatal(err)
	}

	path, size, _, _, err := writeVeloxAssetStreamToCacheAtOffset(cacheDir, assetID, "", int64(len(data)), ioReadCloser{Reader: bytes.NewReader(data[7:])}, 7, "", int64(len(data)), syncAssetDirectory)
	if err != nil {
		t.Fatalf("append/promote partial: %v", err)
	}
	if size != int64(len(data)) {
		t.Fatalf("size = %d, want %d", size, len(data))
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read promoted file: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("promoted bytes = %q, want %q", got, data)
	}
	if _, err := os.Stat(partPath); !os.IsNotExist(err) {
		t.Fatalf("partial still exists after promotion, stat err=%v", err)
	}
}

func TestWriteAssetCacheAtOffsetRejectsHashAndRemovesPartial(t *testing.T) {
	cacheDir := t.TempDir()
	assetID := "asset-hash"
	_, _, _, _, err := writeVeloxAssetStreamToCacheAtOffset(cacheDir, assetID, "0000000000000000000000000000000000000000000000000000000000000000", 11, ioReadCloser{Reader: bytes.NewReader([]byte("wrong bytes"))}, 0, "", 0, syncAssetDirectory)
	if err == nil {
		t.Fatal("hash mismatch should fail")
	}
	if _, statErr := os.Stat(assetPartialPath(cacheDir, assetID, "0000000000000000000000000000000000000000000000000000000000000000")); !os.IsNotExist(statErr) {
		t.Fatalf("mismatched partial should be removed, stat err=%v", statErr)
	}
}

type ioReadCloser struct{ *bytes.Reader }

func (r ioReadCloser) Close() error { return nil }
