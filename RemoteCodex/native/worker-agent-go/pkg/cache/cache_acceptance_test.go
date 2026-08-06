package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"
)

func acceptanceHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// TestCacheAcceptance_MissHitHashAndPin exercises the worker cache contract
// as one lifecycle: an absent content-addressed entry is a miss, a verified
// Put becomes a hit, tampering is detected by SHA-256 on read, and a pinned
// entry survives byte-pressure eviction.
func TestCacheAcceptance_MissHitHashAndPin(t *testing.T) {
	ctx := context.Background()
	c, err := NewPersistedLocalCache(CacheOptions{
		Root:       t.TempDir(),
		ByteBudget: 32,
	})
	if err != nil {
		t.Fatalf("NewPersistedLocalCache: %v", err)
	}

	data := []byte("verified cache payload")
	hash := acceptanceHash(data)
	if _, found, err := c.Get(ctx, hash); err != nil || found {
		t.Fatalf("initial Get = (found=%v, err=%v), want miss without error", found, err)
	}

	if err := c.Put(ctx, hash, data); err != nil {
		t.Fatalf("Put verified payload: %v", err)
	}
	got, found, err := c.Get(ctx, hash)
	if err != nil || !found {
		t.Fatalf("verified Get = (found=%v, err=%v), want hit", found, err)
	}
	if string(got) != string(data) {
		t.Fatalf("verified Get payload = %q, want %q", got, data)
	}

	// A byte-level tamper must be treated as corruption, never as a hit.
	if err := os.WriteFile(c.entryPath(hash), []byte("tampered payload"), 0o644); err != nil {
		t.Fatalf("tamper cache entry: %v", err)
	}
	if _, found, err := c.Get(ctx, hash); !errors.Is(err, ErrCacheCorruption) || found {
		t.Fatalf("tampered Get = (found=%v, err=%v), want corruption and found=false", found, err)
	}
	// Corruption removal is intentionally asynchronous; wait until the
	// invalid file is detached before restoring the verified bytes.
	deadline := time.Now().Add(time.Second)
	for {
		_, statErr := os.Stat(c.entryPath(hash))
		if errors.Is(statErr, os.ErrNotExist) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("corrupt cache entry was not removed")
		}
		time.Sleep(time.Millisecond)
	}

	// Reinsert the verified bytes, pin them, then force pressure with a
	// second entry. The pinned entry must remain available.
	if err := c.Put(ctx, hash, data); err != nil {
		t.Fatalf("restore verified payload: %v", err)
	}
	if err := c.Pin(hash); err != nil {
		t.Fatalf("Pin: %v", err)
	}
	defer func() {
		if err := c.Unpin(hash); err != nil {
			t.Errorf("Unpin: %v", err)
		}
	}()

	flood := []byte("evict-me-now")
	floodHash := acceptanceHash(flood)
	if err := c.Put(ctx, floodHash, flood); err != nil {
		t.Fatalf("Put pressure entry: %v", err)
	}
	if _, found, err := c.Get(ctx, hash); err != nil || !found {
		t.Fatalf("pinned entry after pressure = (found=%v, err=%v), want hit", found, err)
	}
	stats := c.Stats()
	if stats.PinnedEntries != 1 {
		t.Fatalf("PinnedEntries=%d, want 1", stats.PinnedEntries)
	}
	if stats.Misses < 1 || stats.Hits < 2 || stats.Corruptions != 1 {
		t.Fatalf("cache lifecycle stats=%+v, want miss, hits, and one corruption", stats)
	}
}
