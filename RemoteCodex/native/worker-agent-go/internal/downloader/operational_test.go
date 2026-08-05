package downloader

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestManager_OperationalSnapshotProjectsLowCardinalityMetrics(t *testing.T) {
	var mu sync.Mutex
	var latest OperationalSnapshot
	updates := 0
	release := make(chan struct{})
	cacheHit := true
	tf := &fakeTransferer{
		check: func(ctx context.Context, reportCtx context.Context, req DownloadRequest) (CacheCheckResult, error) {
			if req.AssetKey == "cached" && cacheHit {
				return CacheCheckResult{CacheHit: true, LocalPath: "/cache/cached"}, nil
			}
			return CacheCheckResult{}, nil
		},
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest, onProgress func(int64)) (TransferResult, error) {
			if req.AssetKey == "failed" {
				return TransferResult{}, errors.New("upstream failed")
			}
			select {
			case <-release:
			case <-ctx.Done():
				return TransferResult{}, ctx.Err()
			}
			onProgress(req.SizeBytes / 2)
			return TransferResult{LocalPath: "/cache/" + req.AssetKey, Bytes: req.SizeBytes, SHA256: "verified"}, nil
		},
	}
	m := NewManager(Config{
		Concurrency:  1,
		PublishBytes: 1,
		OnOperationalSnapshot: func(snapshot OperationalSnapshot) {
			mu.Lock()
			latest = snapshot
			updates++
			mu.Unlock()
		},
	}, tf)
	defer m.Close()

	cachedDone := make(chan error, 1)
	go func() {
		_, err := m.Resolve(context.Background(), DownloadRequest{JobID: "cache-job", TaskID: "cache-task", AssetKey: "cached", SizeBytes: 100})
		cachedDone <- err
	}()
	if err := <-cachedDone; err != nil {
		t.Fatalf("cache-hit resolve: %v", err)
	}

	var wg sync.WaitGroup
	resolve := func(job, task, key string) {
		defer wg.Done()
		_, _ = m.Resolve(context.Background(), DownloadRequest{JobID: job, TaskID: task, AssetKey: key, SizeBytes: 100})
	}
	wg.Add(1)
	go resolve("active-1", "task-1", "active")
	waitFor(t, "active transfer", func() bool {
		snap, ok := m.Snapshot("active")
		if ok && snap.State == DownloadRunning {
			return true
		}
		return false
	})
	wg.Add(1)
	go resolve("active-2", "task-2", "active")
	waitFor(t, "coalesced active waiter", func() bool {
		snap, ok := m.Snapshot("active")
		return ok && snap.SharedWaiters == 2
	})
	mu.Lock()
	active := latest
	mu.Unlock()
	if active.ActiveTransfers != 1 || active.QueuedTransfers != 0 {
		t.Fatalf("active/queued = %d/%d, want 1/0", active.ActiveTransfers, active.QueuedTransfers)
	}
	if active.CoalescedRequestsTotal < 1 {
		t.Fatalf("coalesced = %d, want at least 1", active.CoalescedRequestsTotal)
	}

	// Release the active transfer before starting the failed request: the
	// manager is intentionally configured with one slot, so a queued failed
	// transfer must not be awaited while the slot is held by the gate above.
	close(release)
	wg.Wait()

	failedDone := make(chan error, 1)
	go func() {
		_, err := m.Resolve(context.Background(), DownloadRequest{JobID: "failed-job", TaskID: "failed-task", AssetKey: "failed", SizeBytes: 50})
		failedDone <- err
	}()
	if err := <-failedDone; err == nil {
		t.Fatal("failed transfer must return an error")
	}
	waitFor(t, "failed operational snapshot", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return latest.FailedTransfers == 1
	})

	waitFor(t, "ready operational snapshot", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return latest.ReadyTransfers >= 1 && latest.CacheHitTransfers >= 1
	})

	mu.Lock()
	final := latest
	count := updates
	mu.Unlock()
	if count == 0 {
		t.Fatal("operational callback never fired")
	}
	if final.ReadyTransfers < 2 {
		t.Fatalf("ready transfers = %d, want cached + active", final.ReadyTransfers)
	}
	if final.CacheHitTransfers != 1 {
		t.Fatalf("cache hits = %d, want 1", final.CacheHitTransfers)
	}
	if final.BytesDownloaded != 100 || final.BytesTotal != 250 {
		t.Fatalf("bytes downloaded/total = %d/%d, want 100/250 (failed transfer contributes expected bytes, not downloaded bytes)", final.BytesDownloaded, final.BytesTotal)
	}
	if final.ActiveTransfers != 0 || final.FailedTransfers != 1 {
		t.Fatalf("final active/failed = %d/%d, want 0/1", final.ActiveTransfers, final.FailedTransfers)
	}
}
