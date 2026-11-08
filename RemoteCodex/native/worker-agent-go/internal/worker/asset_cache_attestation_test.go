package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func writeVerifiedCacheBlob(t *testing.T, root string, content []byte) (string, string) {
	t.Helper()
	sum := sha256.Sum256(content)
	sha := hex.EncodeToString(sum[:])
	path := assetBlobPath(root, sha, ".mp4")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0o444); err != nil {
		t.Fatal(err)
	}
	return sha, path
}

func clearCacheAttestationMemory() {
	cacheAttestationMemory.Range(func(key, _ interface{}) bool {
		cacheAttestationMemory.Delete(key)
		return true
	})
}

func TestCachedAssetPathAttestsRepeatedHit(t *testing.T) {
	root := t.TempDir()
	content := []byte("verified asset bytes")
	sha, path := writeVerifiedCacheBlob(t, root, content)
	calls := 0
	original := sha256FileForCache
	sha256FileForCache = func(path string) (string, error) {
		calls++
		return original(path)
	}
	defer func() { sha256FileForCache = original }()

	got, _, err := cachedAssetPathTimedWithContext(context.Background(), root, sha, int64(len(content)))
	if err != nil || got != path {
		t.Fatalf("first probe = %q, %v; want %q", got, err, path)
	}
	if calls != 1 {
		t.Fatalf("first probe hashed %d times, want 1", calls)
	}
	got, _, err = cachedAssetPathTimedWithContext(context.Background(), root, sha, int64(len(content)))
	if err != nil || got != path {
		t.Fatalf("second probe = %q, %v; want %q", got, err, path)
	}
	if calls != 1 {
		t.Fatalf("second probe re-hashed the blob (%d calls)", calls)
	}
}

func TestCachedAssetPathUsesDurableAttestation(t *testing.T) {
	root := t.TempDir()
	content := []byte("durable asset bytes")
	sha, path := writeVerifiedCacheBlob(t, root, content)
	calls := 0
	original := sha256FileForCache
	sha256FileForCache = func(path string) (string, error) {
		calls++
		return original(path)
	}
	defer func() { sha256FileForCache = original }()

	if _, _, err := cachedAssetPathTimedWithContext(context.Background(), root, sha, int64(len(content))); err != nil {
		t.Fatal(err)
	}
	clearCacheAttestationMemory()
	got, _, err := cachedAssetPathTimedWithContext(context.Background(), root, sha, int64(len(content)))
	if err != nil || got != path {
		t.Fatalf("durable probe = %q, %v; want %q", got, err, path)
	}
	if calls != 1 {
		t.Fatalf("durable attestation did not survive a memory reset (%d hash calls)", calls)
	}
}

func TestCachedAssetPathInvalidatesAttestationOnMutation(t *testing.T) {
	root := t.TempDir()
	content := []byte("verified asset bytes")
	sha, path := writeVerifiedCacheBlob(t, root, content)
	if _, _, err := cachedAssetPathTimedWithContext(context.Background(), root, sha, int64(len(content))); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("tampered asset bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, _, err := cachedAssetPathTimedWithContext(context.Background(), root, sha, int64(len(content)))
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Fatalf("tampered blob accepted as a hit: %q", got)
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("tampered blob was not evicted, stat err = %v", statErr)
	}
}
