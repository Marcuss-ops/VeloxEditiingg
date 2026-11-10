package assets

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// keyframeIndexVersion invalidates sidecars written by an older probe schema.
const keyframeIndexVersion = 1

type keyframeIndexRecord struct {
	Version   int        `json:"version"`
	SHA256    string     `json:"sha256"`
	Size      int64      `json:"size_bytes"`
	Probe     VideoProbe `json:"probe"`
	Keyframes []float64  `json:"keyframes"`
}

// KeyframeIndex is the durable per-immutable-source probe index. A Drive source
// is downloaded and hashed before it is probed; that SHA-256 is its identity,
// so the whole probe (including every keyframe timestamp) can be reused across
// requests, retries and process restarts. Only complete, size-matching records
// are returned as hits.
type KeyframeIndex struct {
	dir     string
	mu      sync.Mutex
	metrics *KeyframeIndexMetrics
}

// NewKeyframeIndex creates an index rooted at dir. An empty dir disables the
// index (Lookup always misses, Store is a no-op) so callers without a writable
// cache can keep the in-memory probe path.
func NewKeyframeIndex(dir string) *KeyframeIndex {
	return &KeyframeIndex{dir: strings.TrimSpace(dir), metrics: NewKeyframeIndexMetrics()}
}

// DefaultKeyframeIndex resolves the production cache directory. VELOX_KEYFRAME_INDEX_DIR
// takes precedence; otherwise the user cache directory is used. An unavailable
// directory disables persistence rather than failing source preparation.
func DefaultKeyframeIndex() *KeyframeIndex {
	if configured := strings.TrimSpace(os.Getenv("VELOX_KEYFRAME_INDEX_DIR")); configured != "" {
		return NewKeyframeIndex(configured)
	}
	root, err := os.UserCacheDir()
	if err != nil || strings.TrimSpace(root) == "" {
		return NewKeyframeIndex("")
	}
	return NewKeyframeIndex(filepath.Join(root, "velox", "keyframes"))
}

func (k *KeyframeIndex) path(sourceSHA string) string {
	return filepath.Join(k.dir, strings.ToLower(strings.TrimSpace(sourceSHA))+".json")
}

// Lookup returns the persisted probe for an immutable source. A missing,
// malformed, stale or size-mismatched record is a miss, never an error: the
// index is an optimization and a corrupt entry must not fail a job.
func (k *KeyframeIndex) Lookup(_ context.Context, sourceSHA string, sizeBytes int64) (VideoProbe, bool) {
	if k == nil || k.dir == "" || len(strings.TrimSpace(sourceSHA)) != 64 || sizeBytes <= 0 {
		return VideoProbe{}, false
	}
	k.mu.Lock()
	data, err := os.ReadFile(k.path(sourceSHA))
	if err != nil {
		k.mu.Unlock()
		k.recordLookup(false)
		return VideoProbe{}, false
	}
	var record keyframeIndexRecord
	if err := json.Unmarshal(data, &record); err != nil || record.Version != keyframeIndexVersion ||
		!strings.EqualFold(record.SHA256, sourceSHA) || record.Size != sizeBytes ||
		len(record.Keyframes) == 0 || record.Probe.DurationSeconds <= 0 ||
		record.Probe.Width <= 0 || record.Probe.Height <= 0 || record.Probe.FPSNum <= 0 || record.Probe.FPSDen <= 0 {
		k.mu.Unlock()
		k.recordLookup(false)
		return VideoProbe{}, false
	}
	k.mu.Unlock()
	k.recordLookup(true)
	return record.Probe, true
}

func (k *KeyframeIndex) recordLookup(hit bool) {
	if k != nil && k.metrics != nil {
		k.metrics.Observe(hit)
	}
}

// Store writes the probe atomically. Failures are returned to the caller so a
// misconfigured cache directory is visible in logs, but callers may treat them
// as non-fatal because the fresh probe is still usable.
func (k *KeyframeIndex) Store(_ context.Context, sourceSHA string, sizeBytes int64, probe VideoProbe) error {
	if k == nil || k.dir == "" {
		return nil
	}
	if len(strings.TrimSpace(sourceSHA)) != 64 || sizeBytes <= 0 {
		return fmt.Errorf("keyframe index: source SHA-256 and size are required")
	}
	if len(probe.Keyframes) == 0 {
		return fmt.Errorf("keyframe index: probe has no keyframes")
	}
	if err := os.MkdirAll(k.dir, 0o750); err != nil {
		return fmt.Errorf("keyframe index: create directory: %w", err)
	}
	record := keyframeIndexRecord{
		Version: keyframeIndexVersion, SHA256: strings.ToLower(strings.TrimSpace(sourceSHA)),
		Size: sizeBytes, Probe: probe, Keyframes: append([]float64(nil), probe.Keyframes...),
	}
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("keyframe index: encode: %w", err)
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	tmp, err := os.CreateTemp(k.dir, ".probe-*.tmp")
	if err != nil {
		return fmt.Errorf("keyframe index: stage: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("keyframe index: write: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("keyframe index: sync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("keyframe index: close: %w", err)
	}
	if err := os.Rename(tmpName, k.path(sourceSHA)); err != nil {
		return fmt.Errorf("keyframe index: promote: %w", err)
	}
	return nil
}
