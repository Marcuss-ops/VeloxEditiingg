// Package worker — ClipLease integration test.
//
// This is the canonical Pass 9 test the user asked for: a "job"
// with one reused Drive clip (already downloaded by an earlier job)
// plus one new Drive clip (just downloaded by THIS job's resolver
// before AcquireJobClips fires). Walks the full lifecycle:
//
//  1. Pre-seed assetA as if downloaded by an earlier job;
//     pre-insert assetB as if THIS job's resolver just finished
//     downloading it.
//  2. AcquireJobClips for JOB-100 → both rows' active_job_id = JOB-100.
//  3. Cleanup with empty protected set: must NOT delete either row
//     (SkippedLeased == 2).
//  4. ReleaseAll: both rows' active_job_id cleared.
//  5. Cleanup with empty protected set: deletes both rows.
//
// This is the canonical demonstration that Pass 9 keeps an in-flight
// job's clips on disk even when the master snapshot does NOT include
// them.

package worker_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"velox-worker-agent/internal/worker"
	"velox-worker-agent/internal/workercache"

	"velox-shared/assetref"
)

// TestClipLease_ReusedAndNewClip is the Pass 9 lifecycle test.
func TestClipLease_ReusedAndNewClip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	cache, err := workercache.Open(filepath.Join(dir, "cache.db"))
	if err != nil {
		t.Fatalf("Open cache: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	// 1a. Asset A was downloaded by an earlier job ("JOB-PREV").
	pathA := filepath.Join(dir, "TYSON001.mp4")
	if err := os.WriteFile(pathA, []byte("FAKE VIDEO BYTES A"), 0o644); err != nil {
		t.Fatalf("write A: %v", err)
	}
	if err := cache.Store(ctx, workercache.Entry{
		DriveFileID:      "TYSON001",
		LocalPath:        pathA,
		SizeBytes:        int64(len("FAKE VIDEO BYTES A")),
		DownloadComplete: true,
	}); err != nil {
		t.Fatalf("Store TYSON001: %v", err)
	}
	if err := cache.MarkDownloadComplete(ctx, "TYSON001", pathA, int64(len("FAKE VIDEO BYTES A"))); err != nil {
		t.Fatalf("MarkDownloadComplete TYSON001: %v", err)
	}

	// 1b. Drive IDs are extracted from the job payload via the
	// canonical Pass 4 extractor. The fixture uses two distinct
	// canonical IDs (TYSON001 reused, NEW-CLIP just-resolved).
	payload := json.RawMessage(`{
		"scenes": [
			{"clip_link": "https://drive.google.com/file/d/TYSON001/view"},
			{"clip_link": "https://drive.google.com/file/d/NEW-CLIP/view?usp=sharing"}
		]
	}`)
	idSet := assetref.ExtractDriveFileIDs(payload)
	if len(idSet) != 2 {
		t.Fatalf("ExtractDriveFileIDs returned %d IDs, want 2 (%v)", len(idSet), idSet)
	}
	driveIDs := make([]string, 0, len(idSet))
	for id := range idSet {
		driveIDs = append(driveIDs, id)
	}
	sort.Strings(driveIDs) // deterministic order for assertions

	// 1c. NEW-CLIP was just downloaded by THIS job's resolver —
	// representing the Store+MarkDownloadComplete path that runs
	// in Pass 10 (download .part + rename + mark). We mirror that
	// contract here so the lease integration test is hermetic.
	pathB := filepath.Join(dir, "NEW-CLIP.mp4")
	if err := os.WriteFile(pathB, []byte("FAKE VIDEO BYTES B"), 0o644); err != nil {
		t.Fatalf("write B: %v", err)
	}
	if err := cache.Store(ctx, workercache.Entry{
		DriveFileID:      "NEW-CLIP",
		LocalPath:        pathB,
		SizeBytes:        int64(len("FAKE VIDEO BYTES B")),
		DownloadComplete: true,
	}); err != nil {
		t.Fatalf("Store NEW-CLIP: %v", err)
	}
	if err := cache.MarkDownloadComplete(ctx, "NEW-CLIP", pathB, int64(len("FAKE VIDEO BYTES B"))); err != nil {
		t.Fatalf("MarkDownloadComplete NEW-CLIP: %v", err)
	}

	// 2. AcquireJobClips: both rows get active_job_id = JOB-100.
	const jobID = "JOB-100"
	lease, err := worker.AcquireJobClips(ctx, cache, jobID, driveIDs)
	if err != nil {
		t.Fatalf("AcquireJobClips: %v", err)
	}
	if lease == nil {
		t.Fatalf("AcquireJobClips returned nil lease on success")
	}
	t.Cleanup(func() { _ = lease.ReleaseAll(ctx) })

	for _, id := range driveIDs {
		e, ok, err := cache.Find(ctx, id)
		if err != nil || !ok {
			t.Fatalf("Find(%s) after Acquire: ok=%v err=%v", id, ok, err)
		}
		if e.ActiveJobID != jobID {
			t.Errorf("Asset %q after Acquire has ActiveJobID=%q want %q", id, e.ActiveJobID, jobID)
		}
		if !e.DownloadComplete {
			t.Errorf("Asset %q after Acquire has DownloadComplete=false want true", id)
		}
	}

	// 3. Cleanup with EMPTY protected set: BOTH rows are leased and
	// must survive. This is the canonical Pass 9 rule.
	stats, err := workercache.Cleanup(ctx, cache, nil)
	if err != nil {
		t.Fatalf("Cleanup(leased): %v", err)
	}
	if stats.Removed != 0 {
		t.Errorf("Cleanup while leased Removed=%d want 0 (both rows are leased)", stats.Removed)
	}
	if stats.SkippedLeased != 2 {
		t.Errorf("Cleanup while leased SkippedLeased=%d want 2", stats.SkippedLeased)
	}

	// 4. ReleaseAll: clear leases (success branch — this is also
	// what defer would do on an error return).
	if err := lease.ReleaseAll(ctx); err != nil {
		t.Fatalf("ReleaseAll: %v", err)
	}
	for _, id := range driveIDs {
		e, ok, err := cache.Find(ctx, id)
		if err != nil || !ok {
			t.Fatalf("Find(%s) after Release: ok=%v err=%v", id, ok, err)
		}
		if e.ActiveJobID != "" {
			t.Errorf("Asset %q after Release has ActiveJobID=%q want empty", id, e.ActiveJobID)
		}
	}

	// 5. Second Cleanup: now neither row is leased → both removed.
	stats, err = workercache.Cleanup(ctx, cache, nil)
	if err != nil {
		t.Fatalf("Cleanup after release: %v", err)
	}
	if stats.Removed != 2 {
		t.Errorf("Cleanup after release Removed=%d want 2", stats.Removed)
	}
	for _, id := range driveIDs {
		if _, ok, _ := cache.Find(ctx, id); ok {
			t.Errorf("Asset %q still in index after Cleanup; expected removed", id)
		}
	}
}

// TestClipLease_AcquireMidLoopFailureRollsBackPartial exercises the
// "partial acquire must not leak" rule: if one of the driveIDs is
// missing from the cache, AcquireJobClips must release every row it
// already acquired in this loop and surface ErrNotFound.
func TestClipLease_AcquireMidLoopFailureRollsBackPartial(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	cache, err := workercache.Open(filepath.Join(dir, "cache.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	// First two exist, third does NOT.
	for _, id := range []string{"EXISTS-A", "EXISTS-B"} {
		path := filepath.Join(dir, id+".mp4")
		if err := os.WriteFile(path, []byte("FAKE "+id), 0o644); err != nil {
			t.Fatalf("write %s: %v", id, err)
		}
		if err := cache.Store(ctx, workercache.Entry{
			DriveFileID:      id,
			LocalPath:        path,
			SizeBytes:        int64(len("FAKE " + id)),
			DownloadComplete: true,
		}); err != nil {
			t.Fatalf("Store %s: %v", id, err)
		}
	}

	const jobID = "JOB-FAIL"
	driveIDs := []string{"EXISTS-A", "EXISTS-B", "DOES-NOT-EXIST"}

	lease, err := worker.AcquireJobClips(ctx, cache, jobID, driveIDs)
	if err == nil {
		t.Fatalf("AcquireJobClips succeeded; expected ErrNotFound on DOES-NOT-EXIST")
	}
	if lease != nil {
		// Defensive cleanup: if for some reason the helper returned
		// a partial lease, ensure it is released so the next test
		// in this file does not observe leaked state.
		_ = lease.ReleaseAll(ctx)
	}

	// Verify the rollback: the two existing rows must NOT be leased.
	for _, id := range []string{"EXISTS-A", "EXISTS-B"} {
		e, ok, fErr := cache.Find(ctx, id)
		if fErr != nil || !ok {
			t.Fatalf("Find(%s) post-rollback: ok=%v err=%v", id, ok, fErr)
		}
		if e.ActiveJobID != "" {
			t.Errorf("Asset %q post-rollback has ActiveJobID=%q want empty (rollback failed)", id, e.ActiveJobID)
		}
	}
}

// TestClipLease_EmptyOrNilGuards enforces the constructor contracts.
func TestClipLease_EmptyOrNilGuards(t *testing.T) {
	ctx := context.Background()
	cache, err := workercache.Open(":memory:")
	if err != nil {
		t.Fatalf("Open :memory:: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	if _, err := worker.AcquireJobClips(ctx, nil, "JOB-EMPTY", []string{"X"}); err == nil {
		t.Errorf("AcquireJobClips(nil cache) returned nil err")
	}
	if _, err := worker.AcquireJobClips(ctx, cache, "", []string{"X"}); err == nil {
		t.Errorf("AcquireJobClips(empty jobID) returned nil err")
	}

	l := &worker.ClipLease{} // nil cache → no-op
	if err := l.ReleaseAll(ctx); err != nil {
		t.Errorf("nil-lease ReleaseAll returned %v", err)
	}
}

// TestClipLease_ReleaseAllIdempotent verifies the explicit
// documented contract: calling ReleaseAll twice in a row is safe and
// returns nil on the second call (because workercache.Release treats
// "row with lease != jobID" as a benign no-op).
func TestClipLease_ReleaseAllIdempotent(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cache, err := workercache.Open(filepath.Join(dir, "cache.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	path := filepath.Join(dir, "X.mp4")
	if err := os.WriteFile(path, []byte("X"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := cache.Store(ctx, workercache.Entry{
		DriveFileID: "X", LocalPath: path, SizeBytes: 1, DownloadComplete: true,
	}); err != nil {
		t.Fatalf("Store X: %v", err)
	}

	lease, err := worker.AcquireJobClips(ctx, cache, "JOB-IDEMPOTENT", []string{"X"})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := lease.ReleaseAll(ctx); err != nil {
		t.Fatalf("first ReleaseAll: %v", err)
	}
	if err := lease.ReleaseAll(ctx); err != nil {
		t.Errorf("second ReleaseAll returned %v, want nil (idempotent)", err)
	}
}
