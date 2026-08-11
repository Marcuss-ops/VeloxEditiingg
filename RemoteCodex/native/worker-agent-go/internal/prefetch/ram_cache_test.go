package prefetch

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"velox-shared/assetref"
	"velox-worker-agent/internal/downloader"
)

func TestRAMCache_PublishesVerifiedCopyAndFallsBackAfterL1Eviction(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source.bin")
	data := []byte("verified-prefetch-payload")
	if err := os.WriteFile(source, data, 0o600); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(data)
	key := assetref.AssetKey("asset-ram")
	c := NewRAMCache(filepath.Join(dir, "ram"), int64(len(data))*2, int64(len(data)))
	req := downloader.DownloadRequest{AssetKey: key, AssetID: "asset-ram", SHA256: assetref.ContentHash(fmt.Sprintf("%x", hash)), SizeBytes: int64(len(data))}
	asset := downloader.DownloadedAsset{AssetKey: key, AssetID: req.AssetID, LocalPath: source, SHA256: req.SHA256, SizeBytes: req.SizeBytes}
	if err := c.Put(context.Background(), req, asset); err != nil {
		t.Fatal(err)
	}
	hit, ok, err := c.Find(context.Background(), req)
	if err != nil || !ok {
		t.Fatalf("RAM find hit=%v err=%v", ok, err)
	}
	if hit.LocalPath == source || hit.SizeBytes != int64(len(data)) {
		t.Fatalf("RAM hit=%+v", hit)
	}
	if err := os.Remove(hit.LocalPath); err != nil {
		t.Fatal(err)
	}
	_, ok, err = c.Find(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("deleted RAM copy remained a hit")
	}
}
