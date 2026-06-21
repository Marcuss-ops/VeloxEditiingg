// Package cache provides a persistent, content-addressed local cache
// with LRU eviction, leases/pins, and SHA-256 hash verification.
//
// PR-3.7 design invariants:
//   - SHA-256 keyed (industry default; matches git-style content addressing).
//   - On-disk layout: $root/<2-hex-prefix>/<full-hex> for sharded directories.
//   - LRU eviction: byte-budget based. Default 256 MiB, configurable via
//     CacheOptions. Pinned entries are exempt.
//   - Leases/pins: in-memory optimistic refcount (Pin/Unpin) shared
//     between online-use and long-term holds. PinCount > 0 exempts from
//     eviction.
//   - Corruption detection: on Get, file content is re-hashed. Mismatch
//     deletes the file (async), bumps corruption counter, returns
//     ErrCacheCorruption.
//   - Counters: all atomic so Stats() is race-free under -race; index
//     reads/writes go through a single mutex; file IO is dispatched
//     outside the mutex to avoid blocking concurrent Gets.
//
// The cache satisfies executor.LocalCache (Get/Put) and exposes a
// StatsProvider surface used by the taskrunner for surfacing metrics
// into TaskExecutionReport.Metrics.
package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ErrCacheCorruption is returned by Get when the on-disk content's
// hash does not match the requested hash. Caller should treat as a
// cache miss and re-acquire from upstream.
var ErrCacheCorruption = errors.New("cache: on-disk content hash mismatch")

// CacheStats is a snapshot of PersistedLocalCache counters and gauges.
// All counters are monotonic; gauges are point-in-time.
type CacheStats struct {
	Hits          int64 `json:"hits"`
	Misses        int64 `json:"misses"`
	Evictions     int64 `json:"evictions"`
	Corruptions   int64 `json:"corruptions"`
	PinAcquires   int64 `json:"pin_acquires"`
	PinReleases   int64 `json:"pin_releases"`
	BytesUsed     int64 `json:"bytes_used"`
	Entries       int   `json:"entries"`
	PinnedEntries int   `json:"pinned_entries"`
}

// CacheOptions configures PersistedLocalCache.
type CacheOptions struct {
	// Root is the on-disk directory. Required.
	Root string
	// ByteBudget is the soft cap for total bytes cached. Default 256 MiB.
	ByteBudget int64
}

// PersistedLocalCache is a thread-safe, content-addressed, LRU-evicted
// persistent local cache.
type PersistedLocalCache struct {
	root       string
	byteBudget int64

	mu    sync.Mutex
	index map[string]*entry

	hits        atomic.Int64
	misses      atomic.Int64
	evictions   atomic.Int64
	corruptions atomic.Int64
	pinAcquires atomic.Int64
	pinReleases atomic.Int64
}

type entry struct {
	hash         string
	sizeBytes    int64
	pinCount     int32
	createdNs    int64
	lastAccessNs int64
}

// NewPersistedLocalCache constructs a cache rooted at opts.Root. The
// directory is created if missing. ByteBudget defaults to 256 MiB
// when omitted or non-positive.
func NewPersistedLocalCache(opts CacheOptions) (*PersistedLocalCache, error) {
	if opts.Root == "" {
		return nil, errors.New("cache: Root is required")
	}
	if opts.ByteBudget <= 0 {
		opts.ByteBudget = 256 * 1024 * 1024
	}
	if err := os.MkdirAll(opts.Root, 0o755); err != nil {
		return nil, fmt.Errorf("cache: create root: %w", err)
	}
	return &PersistedLocalCache{
		root:       opts.Root,
		byteBudget: opts.ByteBudget,
		index:      make(map[string]*entry),
	}, nil
}

// Stats returns a snapshot of counters and gauges.
func (c *PersistedLocalCache) Stats() CacheStats {
	c.mu.Lock()
	var bytes int64
	nEntries := len(c.index)
	nPinned := 0
	for _, e := range c.index {
		bytes += e.sizeBytes
		if e.pinCount > 0 {
			nPinned++
		}
	}
	c.mu.Unlock()
	return CacheStats{
		Hits:          c.hits.Load(),
		Misses:        c.misses.Load(),
		Evictions:     c.evictions.Load(),
		Corruptions:   c.corruptions.Load(),
		PinAcquires:   c.pinAcquires.Load(),
		PinReleases:   c.pinReleases.Load(),
		BytesUsed:     bytes,
		Entries:       nEntries,
		PinnedEntries: nPinned,
	}
}

// Get returns the bytes stored under hash, or (nil, false, nil) on miss.
// On corruption, returns (nil, false, ErrCacheCorruption).
func (c *PersistedLocalCache) Get(_ context.Context, hash string) ([]byte, bool, error) {
	if !isValidHash(hash) {
		return nil, false, errors.New("cache: invalid hash format")
	}
	path := c.entryPath(hash)

	// Bump lastAccessNs in the index under lock. A miss is cheap.
	c.mu.Lock()
	e, hasIndex := c.index[hash]
	if hasIndex {
		e.lastAccessNs = time.Now().UnixNano()
	}
	c.mu.Unlock()

	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if hasIndex {
				c.mu.Lock()
				delete(c.index, hash)
				c.mu.Unlock()
			}
			c.misses.Add(1)
			return nil, false, nil
		}
		c.misses.Add(1)
		return nil, false, fmt.Errorf("cache: read %s: %w", hash, err)
	}

	// Always verify hash on read (PR-3.7 invariant).
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != hash {
		c.corruptions.Add(1)
		// Detach the corrupt file (async; never the hot path's mutex).
		go func(p string) { _ = os.Remove(p) }(path)
		if hasIndex {
			c.mu.Lock()
			delete(c.index, hash)
			c.mu.Unlock()
		}
		return nil, false, ErrCacheCorruption
	}

	c.hits.Add(1)

	if !hasIndex {
		// Survived-restart on-disk entry; lazily index.
		c.mu.Lock()
		if _, exists := c.index[hash]; !exists {
			c.index[hash] = &entry{
				hash:         hash,
				sizeBytes:    int64(len(data)),
				createdNs:    time.Now().UnixNano(),
				lastAccessNs: time.Now().UnixNano(),
			}
		}
		c.mu.Unlock()
	}

	return data, true, nil
}

// Put writes data under its expected SHA-256 hash. Returns an error
// if the hash does not match data or if the underlying IO fails. May
// trigger LRU eviction to fit the byte budget.
//
// Concurrency: os.CreateTemp yields a unique tmp name per Put call,
// so concurrent goroutines writing the same hash do not collide on a
// bare path+".tmp" rename.
func (c *PersistedLocalCache) Put(_ context.Context, hash string, data []byte) error {
	if !isValidHash(hash) {
		return errors.New("cache: invalid hash format")
	}
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != hash {
		return fmt.Errorf("cache: hash mismatch (got %s, want %s)", got, hash)
	}

	path := c.entryPath(hash)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("cache: mkdir prefix: %w", err)
	}
	// os.CreateTemp guarantees a unique tmp name even under concurrent
	// Puts to the same hash (last writer wins on rename to `path`).
	tmpFile, err := os.CreateTemp(filepath.Dir(path), ".tmp.*")
	if err != nil {
		return fmt.Errorf("cache: create temp: %w", err)
	}
	tmpPath := tmpFile.Name()
	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("cache: write tmp: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("cache: close tmp: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("cache: rename: %w", err)
	}

	c.mu.Lock()
	var newPin int32
	if existing, ok := c.index[hash]; ok {
		newPin = existing.pinCount
	}
	c.index[hash] = &entry{
		hash:         hash,
		sizeBytes:    int64(len(data)),
		pinCount:     newPin,
		createdNs:    time.Now().UnixNano(),
		lastAccessNs: time.Now().UnixNano(),
	}
	c.mu.Unlock()

	// Eviction runs OUTSIDE the hot-path mutex: takes the lock, decides,
	// releases, then deletes the chosen paths asynchronously.
	c.evictIfOver()
	return nil
}

// Pin increments pinCount for hash. Hydrates from disk if entry absent.
// Pinned entries cannot be evicted until Unpin brings pinCount to 0.
func (c *PersistedLocalCache) Pin(hash string) error {
	if !isValidHash(hash) {
		return errors.New("cache: invalid hash format")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.index[hash]
	if !ok {
		// Hydrate from disk if file exists.
		if _, err := os.Stat(c.entryPath(hash)); err != nil {
			return errors.New("cache: cannot pin missing entry")
		}
		c.index[hash] = &entry{
			hash:         hash,
			createdNs:    time.Now().UnixNano(),
			lastAccessNs: time.Now().UnixNano(),
		}
		e = c.index[hash]
	}
	e.pinCount++
	c.pinAcquires.Add(1)
	return nil
}

// Unpin decrements pinCount for hash. Returns error if pinCount <= 0.
func (c *PersistedLocalCache) Unpin(hash string) error {
	if !isValidHash(hash) {
		return errors.New("cache: invalid hash format")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.index[hash]
	if !ok {
		return errors.New("cache: cannot unpin absent entry")
	}
	if e.pinCount <= 0 {
		return errors.New("cache: pinCount already zero")
	}
	e.pinCount--
	c.pinReleases.Add(1)
	return nil
}

// entryPath returns the on-disk path for a given hash.
func (c *PersistedLocalCache) entryPath(hash string) string {
	return filepath.Join(c.root, hash[:2], hash)
}

// evictIfOver triggers LRU eviction when bytesUsed > byteBudget. Pinned
// entries are exempt. File deletions happen ASYNC post-mutex so the
// hot path is not blocked by IO.
func (c *PersistedLocalCache) evictIfOver() {
	c.mu.Lock()
	var bytes int64
	for _, e := range c.index {
		bytes += e.sizeBytes
	}
	if bytes <= c.byteBudget {
		c.mu.Unlock()
		return
	}

	type cand struct {
		hash string
		size int64
		last int64
	}
	cands := make([]cand, 0, len(c.index))
	for h, e := range c.index {
		if e.pinCount > 0 {
			continue
		}
		cands = append(cands, cand{h, e.sizeBytes, e.lastAccessNs})
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].last < cands[j].last })

	evictedPaths := make([]string, 0, len(cands))
	for _, k := range cands {
		if bytes <= c.byteBudget {
			break
		}
		delete(c.index, k.hash)
		evictedPaths = append(evictedPaths, c.entryPath(k.hash))
		bytes -= k.size
		c.evictions.Add(1)
	}
	c.mu.Unlock()

	// Sync deletion (post-mutex): correctness > latency under
	// sustained eviction. The cache file is true-gone on Put return;
	// a concurrent Get sees the same on-disk state as the in-memory
	// index. Operators observe immediate on-disk absence via
	// Stats().BytesUsed.
	for _, p := range evictedPaths {
		_ = os.Remove(p)
	}
}

// isValidHash returns true iff s is a 64-char lowercase hex string.
func isValidHash(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}
