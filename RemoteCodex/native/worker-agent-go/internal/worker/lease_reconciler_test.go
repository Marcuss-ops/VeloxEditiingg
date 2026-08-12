package worker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"velox-worker-agent/internal/workercache"
)

func TestReconcileLeaseReleasesOnce_CleansPersistedLeaseAfterReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cache.db")
	first, err := workercache.Open(path)
	if err != nil {
		t.Fatalf("open first cache: %v", err)
	}
	assetPath := filepath.Join(t.TempDir(), "audio.asset")
	if err := os.WriteFile(assetPath, []byte("audio"), 0o640); err != nil {
		t.Fatalf("write asset: %v", err)
	}
	if err := first.Store(ctx, workercache.Entry{
		AssetKey: "audio-master", LocalPath: assetPath, SizeBytes: 5, DownloadComplete: true,
	}); err != nil {
		t.Fatalf("store asset: %v", err)
	}
	if err := first.Acquire(ctx, "audio-master", "job-restart"); err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	if err := first.EnqueueLeaseRelease(ctx, "audio-master", "job-restart", nowUTC()); err != nil {
		t.Fatalf("enqueue release: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first cache: %v", err)
	}

	second, err := workercache.Open(path)
	if err != nil {
		t.Fatalf("reopen cache: %v", err)
	}
	defer second.Close()
	worker := &Worker{clipCache: second}
	if err := worker.reconcileLeaseReleasesOnce(ctx); err != nil {
		t.Fatalf("reconcile persisted release: %v", err)
	}

	entry, found, err := second.Find(ctx, "audio-master")
	if err != nil || !found {
		t.Fatalf("find asset after reconcile: found=%v err=%v", found, err)
	}
	if entry.ActiveLeaseCount != 0 || entry.ActiveJobID != "" {
		t.Fatalf("lease retained after reconcile: job=%q count=%d", entry.ActiveJobID, entry.ActiveLeaseCount)
	}
	count, err := second.PendingLeaseReleaseCount(ctx)
	if err != nil || count != 0 {
		t.Fatalf("queue count after reconcile = %d err=%v, want 0", count, err)
	}
}

func nowUTC() time.Time {
	return time.Now().UTC()
}

func TestClipLease_ReleaseAllEnqueuesAfterRetryExhaustion(t *testing.T) {
	ctx := context.Background()
	cache, err := workercache.Open(":memory:")
	if err != nil {
		t.Fatalf("open cache: %v", err)
	}
	defer cache.Close()

	assetPath := filepath.Join(t.TempDir(), "video.asset")
	if err := os.WriteFile(assetPath, []byte("video"), 0o640); err != nil {
		t.Fatalf("write asset: %v", err)
	}
	if err := cache.Store(ctx, workercache.Entry{
		AssetKey: "video-1", LocalPath: assetPath, SizeBytes: 5, DownloadComplete: true,
	}); err != nil {
		t.Fatalf("store asset: %v", err)
	}
	if err := cache.Acquire(ctx, "video-1", "job-release"); err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	if _, err := cache.DB().Exec(`
CREATE TRIGGER fail_release_update
BEFORE UPDATE ON cached_assets
BEGIN
  SELECT RAISE(ABORT, 'forced release failure');
END;`); err != nil {
		t.Fatalf("create release failure trigger: %v", err)
	}

	lease := &ClipLease{cache: cache, jobID: "job-release", assetKeys: []string{"video-1"}}
	if err := lease.ReleaseAll(ctx); err == nil {
		t.Fatal("ReleaseAll returned nil, want exhausted release error")
	}
	count, err := cache.PendingLeaseReleaseCount(ctx)
	if err != nil {
		t.Fatalf("pending count: %v", err)
	}
	if count != 1 {
		t.Fatalf("pending durable releases = %d, want 1", count)
	}
}

func TestLeaseReconciliationLoop_StartsAndStopsCleanly(t *testing.T) {
	ctx := context.Background()
	cache, err := workercache.Open(":memory:")
	if err != nil {
		t.Fatalf("open cache: %v", err)
	}
	defer cache.Close()

	assetPath := filepath.Join(t.TempDir(), "loop.asset")
	if err := os.WriteFile(assetPath, []byte("loop"), 0o640); err != nil {
		t.Fatalf("write asset: %v", err)
	}
	if err := cache.Store(ctx, workercache.Entry{
		AssetKey: "loop-asset", LocalPath: assetPath, SizeBytes: 4, DownloadComplete: true,
	}); err != nil {
		t.Fatalf("store asset: %v", err)
	}
	if err := cache.Acquire(ctx, "loop-asset", "job-loop"); err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	if err := cache.EnqueueLeaseRelease(ctx, "loop-asset", "job-loop", nowUTC()); err != nil {
		t.Fatalf("enqueue release: %v", err)
	}

	worker := &Worker{clipCache: cache, stopChan: make(chan struct{})}
	worker.startLeaseReconciliationLoop(ctx)
	deadline := time.Now().Add(time.Second)
	for {
		count, countErr := cache.PendingLeaseReleaseCount(ctx)
		if countErr != nil {
			t.Fatalf("pending count: %v", countErr)
		}
		if count == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("reconciliation loop did not process the initial durable item")
		}
		time.Sleep(time.Millisecond)
	}
	close(worker.stopChan)
	waitDone := make(chan struct{})
	go func() {
		worker.wg.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("reconciliation loop did not stop after stop signal")
	}
}

func TestReconcileLeaseReleasesOnce_PersistsRetryAfterReleaseFailure(t *testing.T) {
	ctx := context.Background()
	cache, err := workercache.Open(":memory:")
	if err != nil {
		t.Fatalf("open cache: %v", err)
	}
	defer cache.Close()

	assetPath := filepath.Join(t.TempDir(), "audio.asset")
	if err := os.WriteFile(assetPath, []byte("audio"), 0o640); err != nil {
		t.Fatalf("write asset: %v", err)
	}
	if err := cache.Store(ctx, workercache.Entry{
		AssetKey: "audio-retry", LocalPath: assetPath, SizeBytes: 5, DownloadComplete: true,
	}); err != nil {
		t.Fatalf("store asset: %v", err)
	}
	if err := cache.Acquire(ctx, "audio-retry", "job-retry"); err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	if err := cache.EnqueueLeaseRelease(ctx, "audio-retry", "job-retry", nowUTC()); err != nil {
		t.Fatalf("enqueue release: %v", err)
	}
	if _, err := cache.DB().Exec(`
CREATE TRIGGER fail_reconcile_update
BEFORE UPDATE ON cached_assets
BEGIN
  SELECT RAISE(ABORT, 'forced reconcile failure');
END;`); err != nil {
		t.Fatalf("create reconcile failure trigger: %v", err)
	}

	worker := &Worker{clipCache: cache}
	if err := worker.reconcileLeaseReleasesOnce(ctx); err != nil {
		t.Fatalf("reconcile pass: %v", err)
	}
	entries, err := cache.ListDueLeaseReleases(ctx, time.Now().UTC().Add(time.Minute), 10)
	if err != nil {
		t.Fatalf("list persisted retry: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("persisted retry entries = %d, want 1", len(entries))
	}
	if entries[0].AttemptCount != 1 {
		t.Fatalf("attempt_count = %d, want 1", entries[0].AttemptCount)
	}
	if entries[0].LastError == "" {
		t.Fatal("last_error is empty after failed reconciliation")
	}
	if !entries[0].NextAttemptAt.After(time.Now().UTC()) {
		t.Fatalf("next_attempt_at = %s, want future retry", entries[0].NextAttemptAt)
	}
}
