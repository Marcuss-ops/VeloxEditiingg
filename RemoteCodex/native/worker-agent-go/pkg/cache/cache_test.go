// PR-3.7 — tests for PersistedLocalCache. Use t.TempDir() to isolate
// each test's on-disk root; counters and stats are observed under
// concurrent access for race-detector compatibility.
package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// testHash returns the canonical SHA-256 hex of data.
func testHash(t *testing.T, data []byte) string {
	t.Helper()
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func makeCache(t *testing.T, budget int64) *PersistedLocalCache {
	t.Helper()
	c, err := NewPersistedLocalCache(CacheOptions{
		Root:       t.TempDir(),
		ByteBudget: budget,
	})
	if err != nil {
		t.Fatalf("NewPersistedLocalCache: %v", err)
	}
	return c
}

func writeOnDisk(t *testing.T, c *PersistedLocalCache, hash string, data []byte) {
	t.Helper()
	path := c.entryPath(hash)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func TestCache_HitMissRoundtrip(t *testing.T) {
	c := makeCache(t, 0)
	ctx := context.Background()

	data := []byte("hello-world")
	hash := testHash(t, data)

	// Initial Get = miss.
	if _, found, err := c.Get(ctx, hash); err != nil || found {
		t.Fatalf("first Get: want (nil,false,nil) got (_,%v,%v)", found, err)
	}
	if s := c.Stats(); s.Misses != 1 || s.Hits != 0 {
		t.Fatalf("after miss: stats=%+v want misses=1 hits=0", s)
	}

	// Put then Get = hit.
	if err := c.Put(ctx, hash, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, found, err := c.Get(ctx, hash)
	if err != nil || !found {
		t.Fatalf("after Put Get: (_,%v,%v)", found, err)
	}
	if string(got) != string(data) {
		t.Fatalf("Get data mismatch: got %q want %q", got, data)
	}
	if s := c.Stats(); s.Hits != 1 || s.Entries != 1 {
		t.Fatalf("after hit: stats=%+v want hits=1 entries=1", s)
	}
}

func TestCache_LRUEvictionPicksLeastRecentlyUsed(t *testing.T) {
	// Tight budget: each entry is 10 bytes; budget = 20 → after 3
	// puts (30 bytes total), the cache is over budget and must evict
	// one entry. With Get(A) between Put B and Put C, A's
	// lastAccessNs is the most-recent of A/B, so the LRU victim is B.
	budget := int64(20)
	c := makeCache(t, budget)
	ctx := context.Background()

	mk := func(content string) (string, []byte) {
		data := []byte(content)
		return testHash(t, data), data
	}

	hashA, dataA := mk("AAAAAAAAAA")
	hashB, dataB := mk("BBBBBBBBBB")
	hashC, dataC := mk("CCCCCCCCCC")

	// Put A, B, C with budget tight enough that all three don't fit.
	if err := c.Put(ctx, hashA, dataA); err != nil {
		t.Fatalf("Put A: %v", err)
	}
	if err := c.Put(ctx, hashB, dataB); err != nil {
		t.Fatalf("Put B: %v", err)
	}
	// Touch A so it's the MOST-recently-used.
	if _, _, err := c.Get(ctx, hashA); err != nil {
		t.Fatalf("Get A: %v", err)
	}
	if err := c.Put(ctx, hashC, dataC); err != nil {
		t.Fatalf("Put C: %v", err)
	}

	s := c.Stats()
	if s.Evictions < 1 {
		t.Fatalf("expected at least 1 eviction, got %+v", s)
	}
	// B should be the victim (least-recently-used).
	if _, found, err := c.Get(ctx, hashB); err != nil || found {
		t.Fatalf("victim B should be evicted; found=%v err=%v", found, err)
	}
	// A and C should survive.
	if _, found, err := c.Get(ctx, hashA); err != nil || !found {
		t.Fatalf("A should still be present; found=%v err=%v", found, err)
	}
	if _, found, err := c.Get(ctx, hashC); err != nil || !found {
		t.Fatalf("C should still be present; found=%v err=%v", found, err)
	}
}

func TestCache_PinPreventsEviction(t *testing.T) {
	// Tight budget so the pinned entry plus the flood fit cleanly:
	// pinned (12 bytes) + 20 * 3-byte entries ≈ 72 bytes; budget 80.
	budget := int64(80)
	c := makeCache(t, budget)
	ctx := context.Background()

	data := []byte("pinned-content")
	hash := testHash(t, data)
	if err := c.Put(ctx, hash, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := c.Pin(hash); err != nil {
		t.Fatalf("Pin: %v", err)
	}

	// Flood the cache with other entries to force eviction attempts.
	for i := 0; i < 20; i++ {
		data := []byte{byte(i + 1), byte(i + 2), byte(i + 3)}
		hash := testHash(t, data)
		if err := c.Put(ctx, hash, data); err != nil {
			t.Fatalf("Put[%d]: %v", i, err)
		}
	}

	if _, found, err := c.Get(ctx, hash); err != nil || !found {
		t.Fatalf("pinned entry should survive eviction; found=%v err=%v", found, err)
	}
	if s := c.Stats(); s.PinnedEntries != 1 {
		t.Fatalf("PinnedEntries should be 1, got %d", s.PinnedEntries)
	}
	if err := c.Unpin(hash); err != nil {
		t.Fatalf("Unpin: %v", err)
	}
}

func TestCache_CorruptionDetectedOnGet(t *testing.T) {
	c := makeCache(t, 0)
	ctx := context.Background()

	data := []byte("original")
	hash := testHash(t, data)
	if err := c.Put(ctx, hash, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	// Tamper with the file on disk after Put.
	tampered := []byte("taaaaaampered")
	writeOnDisk(t, c, hash, tampered)

	_, found, err := c.Get(ctx, hash)
	if !errors.Is(err, ErrCacheCorruption) {
		t.Fatalf("want ErrCacheCorruption, got %v", err)
	}
	if found {
		t.Fatalf("want found=false on corruption")
	}
	if s := c.Stats(); s.Corruptions != 1 {
		t.Fatalf("Corruptions should be 1, got %d", s.Corruptions)
	}
}

func TestCache_RejectPutWithWrongHash(t *testing.T) {
	c := makeCache(t, 0)
	ctx := context.Background()

	data := []byte("hello")
	wrongHash := testHash(t, []byte("something-else"))
	err := c.Put(ctx, wrongHash, data)
	if err == nil {
		t.Fatalf("Put with bad hash should fail")
	}
	if _, found, _ := c.Get(ctx, wrongHash); found {
		t.Fatalf("nothing should be cached after rejected Put")
	}
}

func TestCache_RaceSafePutGet(t *testing.T) {
	c := makeCache(t, 0)
	ctx := context.Background()

	entries := make([][2]string, 16)
	for i := range entries {
		data := []byte(string(rune('a' + i)))
		entries[i] = [2]string{testHash(t, data), string(data)}
	}

	const goroutines = 8
	const perGoroutine = 50

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				k := (seed*perGoroutine + i) % len(entries)
				h, d := entries[k][0], entries[k][1]
				if err := c.Put(ctx, h, []byte(d)); err != nil {
					t.Errorf("Put: %v", err)
				}
				if _, _, err := c.Get(ctx, h); err != nil && !errors.Is(err, ErrCacheCorruption) {
					t.Errorf("Get: %v", err)
				}
			}
		}(g)
	}
	wg.Wait()

	s := c.Stats()
	if s.Hits+s.Misses == 0 {
		t.Fatalf("expected traffic, got %+v", s)
	}
}
