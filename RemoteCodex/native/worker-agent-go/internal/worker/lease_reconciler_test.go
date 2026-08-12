package worker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"velox-worker-agent/internal/telemetry"
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

func leaseMetricValue(t *testing.T, name, labelName, labelValue string) float64 {
	t.Helper()
	prefix := fmt.Sprintf("%s{%s=\"%s\"} ", name, labelName, labelValue)
	for _, line := range strings.Split(telemetry.GetPrometheusMetrics().ExportPrometheus(), "\n") {
		if strings.HasPrefix(line, prefix) {
			value, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, prefix)), 64)
			if err != nil {
				t.Fatalf("parse metric %s: %v", name, err)
			}
			return value
		}
	}
	t.Fatalf("metric %s{%s=%q} missing", name, labelName, labelValue)
	return 0
}

func nowUTC() time.Time {
	return time.Now().UTC()
}

func TestAcquireJobClipsRecordsLeaseAcquireAndReleaseMetrics(t *testing.T) {
	ctx := context.Background()
	cache, err := workercache.Open(":memory:")
	if err != nil {
		t.Fatalf("open cache: %v", err)
	}
	defer cache.Close()
	assetPath := filepath.Join(t.TempDir(), "metrics.asset")
	if err := os.WriteFile(assetPath, []byte("data"), 0o640); err != nil {
		t.Fatalf("write asset: %v", err)
	}
	if err := cache.Store(ctx, workercache.Entry{AssetKey: "metrics-asset", LocalPath: assetPath, SizeBytes: 4, DownloadComplete: true}); err != nil {
		t.Fatalf("store asset: %v", err)
	}
	acquiresBefore := leaseMetricValue(t, "velox_cache_lease_acquires_total", "result", "success")
	releasesBefore := leaseMetricValue(t, "velox_cache_lease_releases_total", "result", "success")
	lease, err := AcquireJobClips(ctx, cache, "metrics-job", []string{"metrics-asset"})
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	if err := lease.ReleaseAll(ctx); err != nil {
		t.Fatalf("release lease: %v", err)
	}
	if got := leaseMetricValue(t, "velox_cache_lease_acquires_total", "result", "success"); got != acquiresBefore+1 {
		t.Fatalf("lease acquire metric = %v, want %v", got, acquiresBefore+1)
	}
	if got := leaseMetricValue(t, "velox_cache_lease_releases_total", "result", "success"); got != releasesBefore+1 {
		t.Fatalf("lease release metric = %v, want %v", got, releasesBefore+1)
	}
}

func TestClipLeaseRenewAll_ContinuesAfterIndividualFailure(t *testing.T) {
	ctx := context.Background()
	cache, err := workercache.Open(":memory:")
	if err != nil {
		t.Fatalf("open cache: %v", err)
	}
	defer cache.Close()
	for _, key := range []string{"renew-bad", "renew-good"} {
		assetPath := filepath.Join(t.TempDir(), key)
		if err := os.WriteFile(assetPath, []byte(key), 0o640); err != nil {
			t.Fatalf("write %s: %v", key, err)
		}
		if err := cache.Store(ctx, workercache.Entry{AssetKey: workercache.AssetKey(key), LocalPath: assetPath, SizeBytes: int64(len(key)), DownloadComplete: true}); err != nil {
			t.Fatalf("store %s: %v", key, err)
		}
		if err := cache.Acquire(ctx, key, "renew-job"); err != nil {
			t.Fatalf("acquire %s: %v", key, err)
		}
	}
	old := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	if _, err := cache.DB().Exec(`UPDATE cached_assets SET last_used_at = ?`, old); err != nil {
		t.Fatalf("age assets: %v", err)
	}
	beforeGood, found, err := cache.Find(ctx, "renew-good")
	if err != nil || !found {
		t.Fatalf("find good asset before renew: found=%v err=%v", found, err)
	}
	renewSuccessBefore := leaseMetricValue(t, "velox_cache_lease_renewals_total", "result", "success")
	renewFailureBefore := leaseMetricValue(t, "velox_cache_lease_renewals_total", "result", "failure")
	if _, err := cache.DB().Exec(`
CREATE TRIGGER fail_one_renew
BEFORE UPDATE ON cached_assets
WHEN NEW.asset_key = 'renew-bad'
BEGIN
  SELECT RAISE(ABORT, 'forced renewal failure');
END;`); err != nil {
		t.Fatalf("create renewal failure trigger: %v", err)
	}
	lease := &ClipLease{cache: cache, jobID: "renew-job", assetKeys: []string{"renew-bad", "renew-good"}}
	if err := lease.RenewAll(ctx); err == nil {
		t.Fatal("RenewAll returned nil, want individual renewal failure")
	}
	good, found, err := cache.Find(ctx, "renew-good")
	if err != nil || !found {
		t.Fatalf("find renewed good asset: found=%v err=%v", found, err)
	}
	if !good.LastUsedAt.After(beforeGood.LastUsedAt) {
		t.Fatalf("successful renewal LastUsedAt = %s, want after %s", good.LastUsedAt, beforeGood.LastUsedAt)
	}
	if got := leaseMetricValue(t, "velox_cache_lease_renewals_total", "result", "success"); got < renewSuccessBefore+1 {
		t.Fatalf("renewal success metric = %v, want at least %v", got, renewSuccessBefore+1)
	}
	if got := leaseMetricValue(t, "velox_cache_lease_renewals_total", "result", "failure"); got < renewFailureBefore+1 {
		t.Fatalf("renewal failure metric = %v, want at least %v", got, renewFailureBefore+1)
	}
}

func TestClipLeaseRenewalLoop_StopsOnCancellation(t *testing.T) {
	ctx := context.Background()
	cache, err := workercache.Open(":memory:")
	if err != nil {
		t.Fatalf("open cache: %v", err)
	}
	defer cache.Close()
	assetPath := filepath.Join(t.TempDir(), "renew-loop.asset")
	if err := os.WriteFile(assetPath, []byte("loop"), 0o640); err != nil {
		t.Fatalf("write asset: %v", err)
	}
	if err := cache.Store(ctx, workercache.Entry{AssetKey: "renew-loop", LocalPath: assetPath, SizeBytes: 4, DownloadComplete: true}); err != nil {
		t.Fatalf("store asset: %v", err)
	}
	if err := cache.Acquire(ctx, "renew-loop", "renew-loop-job"); err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	old := time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano)
	if _, err := cache.DB().Exec(`UPDATE cached_assets SET last_used_at = ? WHERE asset_key = 'renew-loop'`, old); err != nil {
		t.Fatalf("age asset: %v", err)
	}
	before, _, err := cache.Find(ctx, "renew-loop")
	if err != nil {
		t.Fatalf("find before loop: %v", err)
	}
	lease := &ClipLease{cache: cache, jobID: "renew-loop-job", assetKeys: []string{"renew-loop"}}
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		lease.runRenewalLoop(loopCtx, 5*time.Millisecond, nil)
		close(done)
	}()
	deadline := time.Now().Add(time.Second)
	var after workercache.Entry
	for {
		after, _, err = cache.Find(ctx, "renew-loop")
		if err != nil {
			t.Fatalf("find during loop: %v", err)
		}
		if after.LastUsedAt.After(before.LastUsedAt) || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("lease renewal loop did not stop after cancellation")
	}
	if !after.LastUsedAt.After(before.LastUsedAt) {
		t.Fatalf("LastUsedAt = %s, want after %s", after.LastUsedAt, before.LastUsedAt)
	}
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
	releaseFailuresBefore := leaseMetricValue(t, "velox_cache_lease_releases_total", "result", "failure")
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
	if got := leaseMetricValue(t, "velox_cache_lease_releases_total", "result", "failure"); got < releaseFailuresBefore+3 {
		t.Fatalf("release failure metric = %v, want at least %v", got, releaseFailuresBefore+3)
	}
	if leaseMetricValue(t, "velox_cache_lease_retries_total", "source", "release_all") < 2 {
		t.Fatal("release retry metric did not record exhausted in-memory retries")
	}
	if leaseMetricValue(t, "velox_cache_lease_cleanup_failures_total", "stage", "release") < 1 {
		t.Fatal("lease cleanup failure metric did not record failed release")
	}
}

func TestReconcileLeaseReleasesOnce_RecordsListFailure(t *testing.T) {
	cache, err := workercache.Open(":memory:")
	if err != nil {
		t.Fatalf("open cache: %v", err)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("close cache: %v", err)
	}
	before := leaseMetricValue(t, "velox_cache_lease_cleanup_failures_total", "stage", "reconcile_list")
	worker := &Worker{clipCache: cache}
	if err := worker.reconcileLeaseReleasesOnce(context.Background()); err == nil {
		t.Fatal("reconcile with closed cache returned nil")
	}
	if got := leaseMetricValue(t, "velox_cache_lease_cleanup_failures_total", "stage", "reconcile_list"); got < before+1 {
		t.Fatalf("reconcile list failure metric = %v, want at least %v", got, before+1)
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

	releaseFailuresBefore := leaseMetricValue(t, "velox_cache_lease_releases_total", "result", "failure")
	worker := &Worker{clipCache: cache}
	if err := worker.reconcileLeaseReleasesOnce(ctx); err != nil {
		t.Fatalf("reconcile pass: %v", err)
	}
	if got := leaseMetricValue(t, "velox_cache_lease_releases_total", "result", "failure"); got < releaseFailuresBefore+1 {
		t.Fatalf("reconcile release failure metric = %v, want at least %v", got, releaseFailuresBefore+1)
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
	if leaseMetricValue(t, "velox_cache_lease_retries_total", "source", "reconciler") < 1 {
		t.Fatal("reconciler retry metric did not increase")
	}
	if leaseMetricValue(t, "velox_cache_lease_cleanup_failures_total", "stage", "reconcile_release") < 1 {
		t.Fatal("reconciler cleanup failure metric did not increase")
	}
	if entries[0].LastError == "" {
		t.Fatal("last_error is empty after failed reconciliation")
	}
	if !entries[0].NextAttemptAt.After(time.Now().UTC()) {
		t.Fatalf("next_attempt_at = %s, want future retry", entries[0].NextAttemptAt)
	}
}
