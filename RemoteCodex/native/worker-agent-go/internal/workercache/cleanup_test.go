// Package workercache — Cleanup test matrix.
//	// Each fixture pre-populates the cache via Store+MarkDownloadComplete
// so the tested rule (lease, download_complete, protected-set)
// is the only thing varying.

package workercache

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"velox-shared/assetref"
)

// cleanupFixture returns a Cache backed by a t.TempDir-backed SQLite
// file plus a t.TempDir-backed on-disk file for each entry it adds.
//
// Defensive convention: every entry gets a UNIQUE on-disk path so
// parallel subtests (if any are added later) cannot race-removing
// each other's files. The cache.Close is registered via t.Cleanup.
func cleanupFixture(t *testing.T) (*Cache, string) {
	t.Helper()
	dir := t.TempDir()
	cache, err := Open(filepath.Join(dir, "cache.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	return cache, dir
}

// storeSeeded inserts a fully-downloaded entry with a real on-disk
// file so os.Remove in Cleanup has something to do.
func storeSeeded(t *testing.T, c *Cache, dir, assetKey string) {
	t.Helper()
	path := filepath.Join(dir, assetKey+".mp4")
	if err := os.WriteFile(path, []byte("FAKE VIDEO BYTES "+assetKey), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	if err := c.Store(context.Background(), Entry{
		AssetKey:         assetref.AssetKey(assetKey),
		LocalPath:        path,
		SizeBytes:        int64(len("FAKE VIDEO BYTES " + assetKey)),
		DownloadComplete: true,
	}); err != nil {
		t.Fatalf("Store %s: %v", assetKey, err)
	}
	if err := c.MarkDownloadComplete(context.Background(), assetKey, path, int64(len("FAKE VIDEO BYTES "+assetKey))); err != nil {
		t.Fatalf("MarkDownloadComplete %s: %v", assetKey, err)
	}
}

func TestCleanup_EmptyCache_NoOp(t *testing.T) {
	cache, _ := cleanupFixture(t)
	stats, err := Cleanup(context.Background(), cache, nil)
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if stats.Inspected != 0 || stats.Removed != 0 {
		t.Errorf("stats.Inspected=%d Removed=%d want 0/0", stats.Inspected, stats.Removed)
	}
}

func TestCleanup_NilCache_Errors(t *testing.T) {
	if _, err := Cleanup(context.Background(), nil, nil); err == nil {
		t.Errorf("Cleanup(nil, nil) returned nil err, want non-nil")
	}
}

func TestCleanup_LeasedAssetsNeverRemoved(t *testing.T) {
	cache, dir := cleanupFixture(t)
	ctx := context.Background()

	storeSeeded(t, cache, dir, "TYSON001")
	storeSeeded(t, cache, dir, "ALI002")
	// Lease only TYSON001 — ALI002 must be removable.
	if err := cache.Acquire(ctx, "TYSON001", "JOB-IN-FLIGHT"); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	stats, err := Cleanup(ctx, cache, nil)
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if stats.Inspected != 2 {
		t.Errorf("Inspected=%d want 2", stats.Inspected)
	}
	if stats.SkippedLeased != 1 {
		t.Errorf("SkippedLeased=%d want 1 (TYSON001)", stats.SkippedLeased)
	}
	if stats.Removed != 1 {
		t.Errorf("Removed=%d want 1 (ALI002)", stats.Removed)
	}

	// Verify the leased entry is still on disk + in index.
	if _, ok, _ := cache.Find(ctx, "TYSON001"); !ok {
		t.Errorf("TYSON001 disappeared from index; leased row must survive")
	}
	if _, ok, _ := cache.Find(ctx, "ALI002"); ok {
		t.Errorf("ALI002 still in index after Cleanup; must have been removed")
	}
}

func TestCleanup_InFlightAssetsNeverRemoved(t *testing.T) {
	cache, dir := cleanupFixture(t)
	ctx := context.Background()

	// Insert a row WITHOUT download_complete=true (a download is in flight).
	path := filepath.Join(dir, "INFLIGHT.mp4.part")
	if err := os.WriteFile(path, []byte("half-downloaded bytes"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := cache.Store(ctx, Entry{
		AssetKey:  assetref.AssetKey("INFLIGHT"),
		LocalPath: path,
		SizeBytes: 20,
	}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	// Note: no MarkDownloadComplete — row stays at download_complete=0.

	stats, err := Cleanup(ctx, cache, nil)
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if stats.SkippedInFlight != 1 {
		t.Errorf("SkippedInFlight=%d want 1", stats.SkippedInFlight)
	}
	if stats.Removed != 0 {
		t.Errorf("Removed=%d want 0 (in-flight asset must NOT be removed)", stats.Removed)
	}
	if _, ok, _ := cache.Find(ctx, "INFLIGHT"); !ok {
		t.Errorf("INFLIGHT disappeared from index; in-flight row must survive")
	}
}

func TestCleanup_ProtectedAssetsNeverRemoved(t *testing.T) {
	cache, dir := cleanupFixture(t)
	ctx := context.Background()

	storeSeeded(t, cache, dir, "TYSON001")
	storeSeeded(t, cache, dir, "BOXING005")

	protected := map[string]struct{}{"TYSON001": {}}

	stats, err := Cleanup(ctx, cache, protected)
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if stats.SkippedProtected != 1 {
		t.Errorf("SkippedProtected=%d want 1 (TYSON001)", stats.SkippedProtected)
	}
	if stats.Removed != 1 {
		t.Errorf("Removed=%d want 1 (BOXING005)", stats.Removed)
	}
	if _, ok, _ := cache.Find(ctx, "TYSON001"); !ok {
		t.Errorf("TYSON001 disappeared from index; protected row must survive")
	}
}

func TestCleanup_AllSkippedProtected_NoOp(t *testing.T) {
	cache, dir := cleanupFixture(t)
	ctx := context.Background()

	storeSeeded(t, cache, dir, "A")
	storeSeeded(t, cache, dir, "B")
	protected := map[string]struct{}{"A": {}, "B": {}}

	stats, err := Cleanup(ctx, cache, protected)
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if stats.Removed != 0 || stats.SkippedProtected != 2 {
		t.Errorf("Removed=%d SkippedProtected=%d want 0/2", stats.Removed, stats.SkippedProtected)
	}
}

func TestCleanup_PhysicalRemoveFailureRetainsIndexRow(t *testing.T) {
	cache, dir := cleanupFixture(t)
	ctx := context.Background()

	// Point the index at a non-empty directory. os.Remove cannot remove it,
	// so eviction must fail without deleting the durable index row.
	path := filepath.Join(dir, "not-empty")
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(path, "child"), []byte("keep"), 0o644); err != nil {
		t.Fatalf("write child: %v", err)
	}
	if err := cache.Store(ctx, Entry{AssetKey: "REMOVE-FAIL", LocalPath: path, DownloadComplete: true}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := cache.MarkDownloadComplete(ctx, "REMOVE-FAIL", path, 4); err != nil {
		t.Fatalf("MarkDownloadComplete: %v", err)
	}

	stats, err := Cleanup(ctx, cache, nil)
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if stats.RemoveErrors != 1 || stats.Removed != 0 {
		t.Fatalf("stats=%+v, want one physical remove error and no removal", stats)
	}
	if _, ok, err := cache.Find(ctx, "REMOVE-FAIL"); err != nil || !ok {
		t.Fatalf("failed eviction lost index row: ok=%v err=%v", ok, err)
	}
}

func TestCleanup_LocalPathMissingButIndexPresent_StillDeletesIndexRow(t *testing.T) {
	cache, dir := cleanupFixture(t)
	ctx := context.Background()

	storeSeeded(t, cache, dir, "PRESENT")
	storeSeeded(t, cache, dir, "GHOST")
	// Pre-delete the on-disk file for GHOST — exercise the
	// errors.Is(err, os.ErrNotExist) branch.
	if err := os.Remove(filepath.Join(dir, "GHOST.mp4")); err != nil {
		t.Fatalf("pre-rm ghost: %v", err)
	}

	stats, err := Cleanup(ctx, cache, nil)
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if stats.Removed != 2 {
		t.Errorf("Removed=%d want 2 (PRESENT + GHOST, both index-deleted)", stats.Removed)
	}
	if stats.RemoveErrors != 0 {
		t.Errorf("RemoveErrors=%d want 0 (os.ErrNotExist is non-fatal)", stats.RemoveErrors)
	}
}
