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

func acceptanceContentHash(data []byte) assetref.ContentHash {
	sum := sha256.Sum256(data)
	return assetref.ContentHash(hex.EncodeToString(sum[:]))
}

// TestCacheAcceptance_DownloadLeaseReservationLifecycle proves that an asset
// is not eligible for cleanup while it is incomplete, leased by an active job,
// or reserved for a future job. Only after the verified download is complete
// and both protections are released may cleanup remove the file and index row.
func TestCacheAcceptance_DownloadLeaseReservationLifecycle(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cache, err := Open(filepath.Join(dir, "cache.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	assetKey := assetref.AssetKey("ACCEPTANCE-ASSET")
	partialPath := filepath.Join(dir, "asset.part")
	finalPath := filepath.Join(dir, "asset.mp4")
	payload := []byte("verified media bytes")
	if err := os.WriteFile(partialPath, payload[:8], 0o644); err != nil {
		t.Fatalf("write partial: %v", err)
	}
	if err := cache.Store(ctx, Entry{
		AssetKey:  assetKey,
		LocalPath: partialPath,
	}); err != nil {
		t.Fatalf("Store incomplete entry: %v", err)
	}

	// A malformed digest cannot make an entry ready.
	if err := cache.MarkDownloadCompleteWithHash(ctx, string(assetKey), finalPath, int64(len(payload)), "bad-digest"); !errors.Is(err, ErrInvalidContentHash) {
		t.Fatalf("invalid completion hash error=%v, want ErrInvalidContentHash", err)
	}
	entry, found, err := cache.Find(ctx, string(assetKey))
	if err != nil || !found {
		t.Fatalf("Find after invalid completion: found=%v err=%v", found, err)
	}
	if entry.DownloadComplete {
		t.Fatal("invalid hash must not transition entry to download_complete")
	}

	if err := os.WriteFile(finalPath, payload, 0o644); err != nil {
		t.Fatalf("write final: %v", err)
	}
	if err := cache.MarkDownloadCompleteWithHash(ctx, string(assetKey), finalPath, int64(len(payload)), acceptanceContentHash(payload)); err != nil {
		t.Fatalf("verified completion: %v", err)
	}
	entry, found, err = cache.Find(ctx, string(assetKey))
	if err != nil || !found || !entry.DownloadComplete {
		t.Fatalf("verified entry = %+v, found=%v err=%v; want complete", entry, found, err)
	}
	if entry.ContentHash != acceptanceContentHash(payload) {
		t.Fatalf("ContentHash=%q, want %q", entry.ContentHash, acceptanceContentHash(payload))
	}

	if err := cache.Acquire(ctx, string(assetKey), "job-active"); err != nil {
		t.Fatalf("Acquire active lease: %v", err)
	}
	if err := cache.Reserve(ctx, string(assetKey), "reservation-future", time.Now().UTC().Add(time.Hour)); err != nil {
		t.Fatalf("Reserve future job: %v", err)
	}
	entry, _, _ = cache.Find(ctx, string(assetKey))
	if entry.ActiveLeaseCount != 1 || entry.ActiveReservationCount != 1 {
		t.Fatalf("protection counts=%d leases/%d reservations, want 1/1", entry.ActiveLeaseCount, entry.ActiveReservationCount)
	}

	stats, err := Cleanup(ctx, cache, nil)
	if err != nil {
		t.Fatalf("Cleanup while protected: %v", err)
	}
	if stats.Removed != 0 || stats.SkippedLeased != 1 {
		t.Fatalf("Cleanup while protected stats=%+v, want retained asset", stats)
	}
	if _, err := os.Stat(finalPath); err != nil {
		t.Fatalf("protected final file missing: %v", err)
	}

	if err := cache.Release(ctx, string(assetKey), "job-active"); err != nil {
		t.Fatalf("Release lease: %v", err)
	}
	if err := cache.ReleaseReservation(ctx, string(assetKey), "reservation-future"); err != nil {
		t.Fatalf("Release reservation: %v", err)
	}
	stats, err = Cleanup(ctx, cache, nil)
	if err != nil {
		t.Fatalf("Cleanup after release: %v", err)
	}
	if stats.Removed != 1 {
		t.Fatalf("Cleanup after release stats=%+v, want one removal", stats)
	}
	if _, found, err := cache.Find(ctx, string(assetKey)); err != nil || found {
		t.Fatalf("entry after cleanup: found=%v err=%v, want absent", found, err)
	}
	if _, err := os.Stat(finalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("final file after cleanup err=%v, want not-exist", err)
	}
}
