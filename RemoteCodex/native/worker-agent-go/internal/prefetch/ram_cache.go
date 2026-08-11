package prefetch

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"velox-shared/assetref"
	"velox-worker-agent/internal/downloader"
)

// RAMCache is a bounded L1 copy over the durable cache. It never removes the
// NVMe backing and publishes an entry only after size/hash verification.
type RAMCache struct {
	mu                     sync.Mutex
	dir                    string
	budget, maxAsset, used int64
	entries                map[assetref.AssetKey]ramEntry
}
type ramEntry struct {
	path string
	size int64
	hash assetref.ContentHash
	used time.Time
}

func NewRAMCache(dir string, budget, maxAsset int64) *RAMCache {
	if dir == "" || budget <= 0 || maxAsset <= 0 {
		return nil
	}
	return &RAMCache{dir: dir, budget: budget, maxAsset: maxAsset, entries: make(map[assetref.AssetKey]ramEntry)}
}

func (c *RAMCache) Find(_ context.Context, req downloader.DownloadRequest) (downloader.DownloadedAsset, bool, error) {
	if c == nil {
		return downloader.DownloadedAsset{}, false, nil
	}
	c.mu.Lock()
	e, ok := c.entries[req.AssetKey]
	c.mu.Unlock()
	if !ok {
		return downloader.DownloadedAsset{}, false, nil
	}
	if (req.SizeBytes > 0 && req.SizeBytes != e.size) || (req.SHA256 != "" && req.SHA256 != e.hash) {
		return downloader.DownloadedAsset{}, false, nil
	}
	info, err := os.Stat(e.path)
	if err != nil || info.Size() != e.size {
		c.remove(req.AssetKey)
		return downloader.DownloadedAsset{}, false, nil
	}
	c.mu.Lock()
	e.used = time.Now()
	c.entries[req.AssetKey] = e
	c.mu.Unlock()
	return downloader.DownloadedAsset{AssetKey: req.AssetKey, AssetID: req.AssetID, LocalPath: e.path, SHA256: e.hash, SizeBytes: e.size, CacheHit: true}, true, nil
}

func (c *RAMCache) Put(_ context.Context, req downloader.DownloadRequest, asset downloader.DownloadedAsset) error {
	if c == nil || asset.LocalPath == "" || req.SizeBytes <= 0 || req.SizeBytes > c.maxAsset {
		return nil
	}
	if asset.SHA256 == "" || (req.SHA256 != "" && asset.SHA256 != req.SHA256) {
		return fmt.Errorf("ram cache: source hash mismatch for %s", req.AssetKey)
	}
	if err := os.MkdirAll(c.dir, 0o700); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.used+req.SizeBytes > c.budget {
		if !c.evictOneLocked() {
			return nil
		}
	}
	name := fmt.Sprintf("%x", sha256.Sum256([]byte(req.AssetKey)))
	tmp := filepath.Join(c.dir, "."+name+".part")
	in, err := os.Open(asset.LocalPath)
	if err != nil {
		return err
	}
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		_ = in.Close()
		return err
	}
	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(out, h), in)
	_ = in.Close()
	closeErr := out.Close()
	if copyErr != nil {
		_ = os.Remove(tmp)
		return copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return closeErr
	}
	if n != req.SizeBytes || (req.SHA256 != "" && fmt.Sprintf("%x", h.Sum(nil)) != string(req.SHA256)) {
		_ = os.Remove(tmp)
		return fmt.Errorf("ram cache: verification failed for %s", req.AssetKey)
	}
	final := filepath.Join(c.dir, name)
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if old, ok := c.entries[req.AssetKey]; ok {
		c.used -= old.size
	}
	c.entries[req.AssetKey] = ramEntry{path: final, size: n, hash: asset.SHA256, used: time.Now()}
	c.used += n
	return nil
}

func (c *RAMCache) evictOneLocked() bool {
	var key assetref.AssetKey
	var oldest time.Time
	for k, e := range c.entries {
		if key == "" || e.used.Before(oldest) {
			key = k
			oldest = e.used
		}
	}
	if key == "" {
		return false
	}
	c.removeLocked(key)
	return true
}
func (c *RAMCache) remove(key assetref.AssetKey) { c.mu.Lock(); c.removeLocked(key); c.mu.Unlock() }
func (c *RAMCache) removeLocked(key assetref.AssetKey) {
	if e, ok := c.entries[key]; ok {
		_ = os.Remove(e.path)
		c.used -= e.size
		delete(c.entries, key)
	}
}
