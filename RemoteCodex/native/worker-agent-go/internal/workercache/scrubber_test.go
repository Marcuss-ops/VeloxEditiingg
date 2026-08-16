// Package workercache — background integrity scrubber test matrix.

package workercache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"velox-shared/assetref"
)

func scrubHash(data []byte) assetref.ContentHash {
	sum := sha256.Sum256(data)
	return assetref.ContentHash(hex.EncodeToString(sum[:]))
}

// seedScrubBlob writes payload to <dir>/<assetKey>.bin and registers a
// complete, content-hashed blob at that path. verifiedAt sets the blob's
// verified_at via LastUsedAt (Store mirrors LastUsedAt into verified_at for
// a download_complete entry), so callers can control the scrub queue order.
func seedScrubBlob(t *testing.T, c *Cache, dir, assetKey string, payload []byte, verifiedAt time.Time) assetref.ContentHash {
	t.Helper()
	hash := scrubHash(payload)
	path := filepath.Join(dir, assetKey+".bin")
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("write blob %s: %v", assetKey, err)
	}
	if err := c.Store(context.Background(), Entry{
		AssetKey:         assetref.AssetKey(assetKey),
		ContentHash:      hash,
		LocalPath:        path,
		SizeBytes:        int64(len(payload)),
		DownloadComplete: true,
		CreatedAt:        verifiedAt,
		LastUsedAt:       verifiedAt,
	}); err != nil {
		t.Fatalf("store blob %s: %v", assetKey, err)
	}
	return hash
}

func newScrubFixture(t *testing.T) (*Cache, string) {
	t.Helper()
	cache, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open :memory:: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	return cache, t.TempDir()
}

// TestScrubPass_VerifiesInOrderAndBumpsVerifiedAt: the pass re-verifies the
// oldest-verified blob first and, on success, bumps verified_at so the blob
// moves to the back of the queue (round-robin coverage, no re-scrub of the
// same blob every tick).
func TestScrubPass_VerifiesInOrderAndBumpsVerifiedAt(t *testing.T) {
	cache, dir := newScrubFixture(t)
	ctx := context.Background()
	T0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	payloadA := []byte("blob A bytes")
	payloadB := []byte("blob B bytes")
	seedScrubBlob(t, cache, dir, "A", payloadA, T0)
	seedScrubBlob(t, cache, dir, "B", payloadB, T0.Add(time.Hour))

	// Oldest-verified first: A, then B.
	first, err := cache.NextScrubBlobs(ctx, 2)
	if err != nil {
		t.Fatalf("NextScrubBlobs: %v", err)
	}
	if len(first) != 2 || first[0].ContentHash != scrubHash(payloadA) {
		t.Fatalf("initial scrub order = %+v, want A then B", first)
	}

	// The default hasher must reproduce the recorded digest for untouched
	// bytes; with MaxBlobsPerPass=1 only A is verified this pass.
	stats, err := NewIntegrityScrubber().ScrubPass(ctx, cache, ScrubConfig{
		BytesPerPass:    1 << 20,
		MaxBlobsPerPass: 1,
	})
	if err != nil {
		t.Fatalf("ScrubPass: %v", err)
	}
	if stats.Scanned != 1 || stats.Corrupt != 0 {
		t.Fatalf("stats=%+v, want one clean scan", stats)
	}

	// A's verified_at is now the wall clock, later than B's seed, so the next
	// pass starts with B.
	second, err := cache.NextScrubBlobs(ctx, 2)
	if err != nil {
		t.Fatalf("NextScrubBlobs after pass: %v", err)
	}
	if len(second) != 2 || second[0].ContentHash != scrubHash(payloadB) {
		t.Fatalf("post-pass scrub order = %+v, want B first (A bumped to back)", second)
	}
}

// TestScrubPass_InvalidatesCorruptBlobAndRetainsMapping: a blob whose bytes no
// longer match its digest is physically removed + its cached_blobs row
// deleted, while the asset_key → content_hash mapping survives so a resolve
// later re-downloads.
func TestScrubPass_InvalidatesCorruptBlobAndRetainsMapping(t *testing.T) {
	cache, dir := newScrubFixture(t)
	ctx := context.Background()
	T0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	payload := []byte("pristine bytes")
	seedScrubBlob(t, cache, dir, "ASSET", payload, T0)

	// Corrupt the file in place (out-of-band corruption).
	path := filepath.Join(dir, "ASSET.bin")
	if err := os.WriteFile(path, []byte("corrupted!"), 0o644); err != nil {
		t.Fatalf("corrupt file: %v", err)
	}

	stats, err := NewIntegrityScrubber().ScrubPass(ctx, cache, ScrubConfig{
		BytesPerPass:    1 << 20,
		MaxBlobsPerPass: 8,
	})
	if err != nil {
		t.Fatalf("ScrubPass: %v", err)
	}
	if stats.Corrupt != 1 || stats.CorruptBytes != int64(len(payload)) {
		t.Fatalf("stats=%+v, want one corrupt invalidated", stats)
	}

	// Physical bytes gone.
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupt file should be removed, stat err=%v", err)
	}
	// Blob row gone.
	if _, found, err := cache.FindBlob(ctx, scrubHash(payload)); err != nil || found {
		t.Fatalf("FindBlob after invalidation: found=%v err=%v, want absent", found, err)
	}
	// Mapping retained: Find still resolves the asset to the (now-missing)
	// digest so the downloader knows it must re-download.
	entry, found, err := cache.Find(ctx, "ASSET")
	if err != nil || !found {
		t.Fatalf("Find mapping after invalidation: found=%v err=%v, want present", found, err)
	}
	if entry.ContentHash != scrubHash(payload) {
		t.Fatalf("retained ContentHash=%q, want %q", entry.ContentHash, scrubHash(payload))
	}
}

// TestScrubPass_SkipsLeasedAndReservedBlobs: the scrubber never reads a blob
// that an active job is using (I/O contention avoidance), regardless of the
// budget.
func TestScrubPass_SkipsLeasedAndReservedBlobs(t *testing.T) {
	cache, dir := newScrubFixture(t)
	ctx := context.Background()
	T0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	seedScrubBlob(t, cache, dir, "LEASED", []byte("leased bytes"), T0)
	seedScrubBlob(t, cache, dir, "RESERVED", []byte("reserved bytes"), T0)
	if err := cache.Acquire(ctx, "LEASED", "job-1"); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := cache.Reserve(ctx, "RESERVED", "future", time.Now().UTC().Add(time.Hour)); err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	blobs, err := cache.NextScrubBlobs(ctx, 8)
	if err != nil {
		t.Fatalf("NextScrubBlobs: %v", err)
	}
	if len(blobs) != 0 {
		t.Fatalf("NextScrubBlobs = %+v, want empty (leased/reserved excluded)", blobs)
	}

	stats, err := NewIntegrityScrubber().ScrubPass(ctx, cache, ScrubConfig{
		BytesPerPass:    1 << 20,
		MaxBlobsPerPass: 8,
	})
	if err != nil {
		t.Fatalf("ScrubPass: %v", err)
	}
	if stats.Scanned != 0 || stats.Corrupt != 0 {
		t.Fatalf("stats=%+v, want zero scans while leased/reserved", stats)
	}
}

// TestScrubPass_ExcludesLegacyBlobs: a legacy blob (no verified digest, keyed
// by the synthetic legacy:<asset> identity) has nothing to compare against and
// must never be re-hashed into a false "corrupt".
func TestScrubPass_ExcludesLegacyBlobs(t *testing.T) {
	cache, dir := newScrubFixture(t)
	ctx := context.Background()

	// Store with no ContentHash → legacy blob key.
	path := filepath.Join(dir, "LEGACY.mp3")
	if err := os.WriteFile(path, []byte("legacy bytes"), 0o644); err != nil {
		t.Fatalf("write legacy file: %v", err)
	}
	if err := cache.Store(ctx, Entry{
		AssetKey:         assetref.AssetKey("LEGACY"),
		LocalPath:        path,
		SizeBytes:        int64(len("legacy bytes")),
		DownloadComplete: true,
	}); err != nil {
		t.Fatalf("Store legacy: %v", err)
	}

	blobs, err := cache.NextScrubBlobs(ctx, 8)
	if err != nil {
		t.Fatalf("NextScrubBlobs: %v", err)
	}
	if len(blobs) != 0 {
		t.Fatalf("NextScrubBlobs = %+v, want empty (legacy blob excluded)", blobs)
	}
}

// TestScrubPass_BudgetThrottleStopsBeforeBudget: with a byte budget that fits
// only one blob, the pass re-reads exactly one blob and defers the rest (no
// unbounded NVMe read).
func TestScrubPass_BudgetThrottleStopsBeforeBudget(t *testing.T) {
	cache, dir := newScrubFixture(t)
	ctx := context.Background()
	T0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	payloadA := []byte("first blob content")
	payloadB := []byte("second blob content")
	seedScrubBlob(t, cache, dir, "A", payloadA, T0)
	seedScrubBlob(t, cache, dir, "B", payloadB, T0.Add(time.Hour))

	// Budget fits exactly one blob (the first).
	stats, err := NewIntegrityScrubber().ScrubPass(ctx, cache, ScrubConfig{
		BytesPerPass:    int64(len(payloadA)),
		MaxBlobsPerPass: 8,
	})
	if err != nil {
		t.Fatalf("ScrubPass: %v", err)
	}
	if stats.Scanned != 1 {
		t.Fatalf("stats=%+v, want exactly one blob scanned under the byte budget", stats)
	}
}

// TestScrubPass_MissingFileInvalidates: a blob whose physical file vanished is
// a stale entry, so the pass invalidates it (a resolve would otherwise hit a
// path that does not exist).
func TestScrubPass_MissingFileInvalidates(t *testing.T) {
	cache, dir := newScrubFixture(t)
	ctx := context.Background()
	T0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	payload := []byte("soon to disappear")
	hash := seedScrubBlob(t, cache, dir, "GONE", payload, T0)
	if err := os.Remove(filepath.Join(dir, "GONE.bin")); err != nil {
		t.Fatalf("remove file: %v", err)
	}

	stats, err := NewIntegrityScrubber().ScrubPass(ctx, cache, ScrubConfig{
		BytesPerPass:    1 << 20,
		MaxBlobsPerPass: 8,
	})
	if err != nil {
		t.Fatalf("ScrubPass: %v", err)
	}
	if stats.Corrupt != 1 {
		t.Fatalf("stats=%+v, want missing file counted as corrupt (invalidated)", stats)
	}
	if _, found, err := cache.FindBlob(ctx, hash); err != nil || found {
		t.Fatalf("FindBlob after missing-file invalidation: found=%v err=%v, want absent", found, err)
	}
}

// TestScrubLoop_Run_RespectsContextDone: Run returns ctx.Err on cancellation
// (long interval so the only exit path is cancellation).
func TestScrubLoop_Run_RespectsContextDone(t *testing.T) {
	cache, _ := newScrubFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	loop := &ScrubLoop{
		Cache:    cache,
		Interval: time.Hour,
		Config:   ScrubConfig{BytesPerPass: 1 << 20, MaxBlobsPerPass: 8},
	}

	done := make(chan error, 1)
	go func() { done <- loop.Run(ctx) }()

	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run err=%v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return within 1s of ctx cancel")
	}
}

// TestScrubLoop_runTick_ReportsOnTick: the per-tick wrapper delegates to
// TickOnce and invokes OnTick exactly once with the resulting stats.
func TestScrubLoop_runTick_ReportsOnTick(t *testing.T) {
	cache, dir := newScrubFixture(t)
	T0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seedScrubBlob(t, cache, dir, "A", []byte("a"), T0)

	var got ScrubStats
	var tickErr error
	loop := &ScrubLoop{
		Cache:    cache,
		Interval: time.Hour,
		Config:   ScrubConfig{BytesPerPass: 1 << 20, MaxBlobsPerPass: 8},
		OnTick:   func(s ScrubStats, err error) { got, tickErr = s, err },
	}
	loop.runTick(context.Background())
	if got.Scanned != 1 || tickErr != nil {
		t.Fatalf("OnTick stats=%+v err=%v, want one scan reported", got, tickErr)
	}
}
