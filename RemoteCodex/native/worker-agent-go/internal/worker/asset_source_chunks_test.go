package worker

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"velox-shared/contract"
)

func TestVideoSourceChunksAreComputedWithCompleteDigestAndReloaded(t *testing.T) {
	cacheDir := t.TempDir()
	partialDir := filepath.Join(cacheDir, "partial")
	if err := os.MkdirAll(partialDir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(partialDir, "source.part")
	data := make([]byte, contract.VideoSourceChunkSize+3)
	for i := range data {
		data[i] = byte(i * 31)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	full := sha256.Sum256(data)
	finalPath, written, gotSHA, _, _, err := verifyAndPromoteVeloxAsset(cacheDir, hex.EncodeToString(full[:]), int64(len(data)), path, ".mp4", syncAssetDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if written != int64(len(data)) || gotSHA != hex.EncodeToString(full[:]) {
		t.Fatalf("promoted source = size %d sha %s", written, gotSHA)
	}
	reloaded, ok := readVideoSourceChunks(finalPath, gotSHA, int64(len(data)))
	if !ok || reloaded == nil || reloaded.Validate(int64(len(data))) != nil || len(reloaded.SHA256) != 2 {
		t.Fatalf("reloaded source blocks = %+v, valid=%t", reloaded, ok)
	}
	if _, ok := readVideoSourceChunks(finalPath, hex.EncodeToString(make([]byte, 32)), int64(len(data))); ok {
		t.Fatal("source blocks were accepted under a different full-file digest")
	}
}

func TestVideoSourceChunkHashingSkipsNonVideoFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audio.aac")
	if err := os.WriteFile(path, []byte("audio"), 0o600); err != nil {
		t.Fatal(err)
	}
	sha, chunks, err := sha256FileWithVideoChunks(path, 5, ".aac")
	if err != nil || sha == "" || chunks != nil {
		t.Fatalf("non-video digest/chunks = %s/%+v err=%v", sha, chunks, err)
	}
}
