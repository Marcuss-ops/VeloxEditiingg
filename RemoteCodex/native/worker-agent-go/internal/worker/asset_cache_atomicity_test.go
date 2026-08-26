package worker

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteAssetCacheAtOffsetRestoresPreviousFinalOnDirectorySyncFailure(t *testing.T) {
	cacheDir := t.TempDir()
	assetID := "atomic-rollback"
	oldData := []byte("previous valid bytes")
	newData := []byte("new verified bytes")
	expectedSHA := testAssetDigest(newData)
	oldPath := assetBlobPath(cacheDir, expectedSHA, ".mp4")
	if err := os.MkdirAll(filepath.Dir(oldPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(oldPath, oldData, 0o644); err != nil {
		t.Fatal(err)
	}
	calls := 0
	syncDir := func(path string) error {
		calls++
		if calls == 2 {
			return errors.New("injected directory fsync failure")
		}
		return syncAssetDirectory(path)
	}
	_, _, _, _, _, err := writeVeloxAssetStreamToCacheAtOffset(cacheDir, assetID, expectedSHA, int64(len(newData)), ioReadCloser{Reader: bytes.NewReader(newData)}, 0, "video/mp4", int64(len(newData)), syncDir)
	if err == nil {
		t.Fatal("injected fsync failure must fail promotion")
	}
	got, readErr := os.ReadFile(oldPath)
	if readErr != nil {
		t.Fatalf("previous final missing after rollback: %v", readErr)
	}
	if !bytes.Equal(got, oldData) {
		t.Fatalf("previous final = %q, want %q", got, oldData)
	}
}
