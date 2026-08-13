package worker

import (
	"bytes"
	"errors"
	"net/http"
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
	oldPath := filepath.Join(cacheDir, cacheKeyPrefix(assetID, expectedSHA)+".mp4")
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
	resp := &http.Response{
		StatusCode:    http.StatusOK,
		ContentLength: int64(len(newData)),
		Header:        http.Header{"Content-Type": []string{"video/mp4"}},
		Body:          ioReadCloser{Reader: bytes.NewReader(newData)},
	}
	_, _, _, _, err := writeVeloxAssetToCacheAtOffset(cacheDir, assetID, expectedSHA, int64(len(newData)), resp, 0, syncDir)
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
