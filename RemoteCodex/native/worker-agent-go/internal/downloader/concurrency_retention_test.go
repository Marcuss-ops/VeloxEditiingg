package downloader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"velox-shared/assetref"
)

func TestManager_ErrTransferCancelledRetriesWithLiveCaller(t *testing.T) {
	firstCheckStarted := make(chan struct{})
	firstCheckReturned := make(chan struct{})
	secondCheckStarted := make(chan struct{})
	releaseSecondCheck := make(chan struct{})
	var checkCalls atomic.Int32

	tf := &fakeTransferer{
		check: func(ctx context.Context, _ context.Context, _ DownloadRequest) (CacheCheckResult, error) {
			if checkCalls.Add(1) == 1 {
				close(firstCheckStarted)
				<-ctx.Done()
				close(firstCheckReturned)
				return CacheCheckResult{}, ctx.Err()
			}
			if checkCalls.Load() == 2 {
				close(secondCheckStarted)
				<-releaseSecondCheck
			}
			return CacheCheckResult{}, nil
		},
		transfer: func(_ context.Context, _ context.Context, req DownloadRequest, _ func(int64)) (TransferResult, error) {
			return TransferResult{LocalPath: "/verified/retried.mp4", Bytes: req.SizeBytes, SHA256: "sha"}, nil
		},
	}
	m := newTestManager(t, tf)

	result := make(chan struct {
		asset DownloadedAsset
		err   error
	}, 1)
	go func() {
		asset, err := m.Resolve(context.Background(), DownloadRequest{
			JobID: "retry-job", TaskID: "retry-task", AssetKey: "retry-asset", AssetID: "retry-asset", SizeBytes: 1024,
		})
		result <- struct {
			asset DownloadedAsset
			err   error
		}{asset: asset, err: err}
	}()

	<-firstCheckStarted
	transfer := m.registry.Get(assetref.AssetKey("retry-asset"))
	if transfer == nil {
		t.Fatal("first transfer was not registered")
	}
	firstSnapshot := transfer.snapshot(time.Now())

	// Cancel the transfer-owned context while Resolve's caller remains live.
	// The first cache probe observes that cancellation and the run loop settles
	// the generation as CANCELLED; Resolve must then retry on a fresh transfer
	// instead of exposing errTransferCancelled to its caller.
	transfer.requestCancel()
	<-firstCheckReturned
	waitFor(t, "first transfer cancellation", func() bool {
		snapshot := transfer.snapshot(time.Now())
		return snapshot.State == DownloadCancelled
	})
	<-secondCheckStarted
	close(releaseSecondCheck)

	select {
	case out := <-result:
		if out.err != nil {
			t.Fatalf("resolve after cancellation race: %v", out.err)
		}
		if out.asset.LocalPath != "/verified/retried.mp4" {
			t.Fatalf("retried path = %q, want /verified/retried.mp4", out.asset.LocalPath)
		}
		finalSnapshot, ok := m.Snapshot(assetref.AssetKey("retry-asset"))
		if !ok || finalSnapshot.State != DownloadReady {
			t.Fatalf("final retry snapshot = %#v, want READY", finalSnapshot)
		}
		if finalSnapshot.TransferGeneration <= firstSnapshot.TransferGeneration {
			t.Fatalf("retry generation = %d, first generation = %d; want a fresh transfer", finalSnapshot.TransferGeneration, firstSnapshot.TransferGeneration)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("resolve did not complete after errTransferCancelled retry")
	}
	if got := checkCalls.Load(); got != 2 {
		t.Fatalf("cache checks = %d, want first cancelled generation plus one retry", got)
	}
	if got := tf.transferCalls.Load(); got != 1 {
		t.Fatalf("transfer calls = %d, want one successful retry", got)
	}
}

func TestManager_ConcurrentCloseSettlesAllWaiters(t *testing.T) {
	const resolveCount = 16
	const closeCount = 12

	tf := &fakeTransferer{
		check: func(ctx context.Context, _ context.Context, _ DownloadRequest) (CacheCheckResult, error) {
			<-ctx.Done()
			return CacheCheckResult{}, ctx.Err()
		},
	}
	m := NewManager(Config{
		Concurrency: 2,
		Now:         func() time.Time { return time.Unix(1_700_000_000, 0) },
	}, tf)
	t.Cleanup(m.Close)

	results := make(chan error, resolveCount)
	var resolveWG sync.WaitGroup
	for i := 0; i < resolveCount; i++ {
		resolveWG.Add(1)
		go func(i int) {
			defer resolveWG.Done()
			_, err := m.Resolve(context.Background(), DownloadRequest{
				JobID:     fmt.Sprintf("close-job-%02d", i),
				TaskID:    fmt.Sprintf("close-task-%02d", i),
				AssetKey:  assetref.AssetKey(fmt.Sprintf("close-asset-%02d", i)),
				AssetID:   fmt.Sprintf("close-asset-%02d", i),
				SizeBytes: 4096,
			})
			results <- err
		}(i)
	}

	waitFor(t, "all close-test waiters", func() bool {
		registered := 0
		for i := 0; i < resolveCount; i++ {
			if snapshot, ok := m.Snapshot(assetref.AssetKey(fmt.Sprintf("close-asset-%02d", i))); ok && snapshot.SharedWaiters == 1 {
				registered++
			}
		}
		return registered == resolveCount
	})

	var closeWG sync.WaitGroup
	closeWG.Add(closeCount)
	for i := 0; i < closeCount; i++ {
		go func() {
			defer closeWG.Done()
			m.Close()
		}()
	}
	closeWG.Wait()

	resolved := make(chan struct{})
	go func() {
		resolveWG.Wait()
		close(resolved)
	}()
	select {
	case <-resolved:
	case <-time.After(10 * time.Second):
		t.Fatal("resolve waiters did not settle after concurrent Manager.Close calls")
	}
	close(results)
	for err := range results {
		if err == nil {
			t.Fatal("resolve unexpectedly succeeded after manager close")
		}
		if !errors.Is(err, errTransferCancelled) {
			t.Fatalf("resolve error = %v, want errTransferCancelled", err)
		}
	}
	for i := 0; i < resolveCount; i++ {
		snapshot, ok := m.Snapshot(assetref.AssetKey(fmt.Sprintf("close-asset-%02d", i)))
		if !ok {
			t.Fatalf("closed transfer %d snapshot missing", i)
		}
		if snapshot.State != DownloadCancelled {
			t.Fatalf("closed transfer %d state = %s, want CANCELLED", i, snapshot.State)
		}
		if snapshot.SharedWaiters != 0 {
			t.Fatalf("closed transfer %d waiters = %d, want 0", i, snapshot.SharedWaiters)
		}
	}
}

func TestManager_MaxRetainedTransfersRetainsJobRefsWithBoundedHistory(t *testing.T) {
	var ticks atomic.Int64
	clock := func() time.Time {
		return time.Unix(1_700_000_000, ticks.Add(1))
	}
	tf := &fakeTransferer{
		check: func(context.Context, context.Context, DownloadRequest) (CacheCheckResult, error) {
			return CacheCheckResult{CacheHit: true, LocalPath: "/cache/retained.mp4", SHA256: "sha"}, nil
		},
	}
	m := NewManager(Config{
		Concurrency:          1,
		Now:                  clock,
		MaxRetainedTransfers: 2,
	}, tf)
	t.Cleanup(m.Close)

	for i := 0; i < 4; i++ {
		key := assetref.AssetKey(fmt.Sprintf("retained-asset-%d", i))
		_, err := m.Resolve(context.Background(), DownloadRequest{
			JobID:     fmt.Sprintf("retained-job-%d", i),
			TaskID:    fmt.Sprintf("retained-task-%d", i),
			AssetKey:  key,
			AssetID:   string(key),
			SceneIDs:  []string{fmt.Sprintf("scene-%d", i)},
			SizeBytes: 1024,
		})
		if err != nil {
			t.Fatalf("resolve %d: %v", i, err)
		}
	}

	terminalCount := 0
	m.registry.Each(func(_ assetref.AssetKey, transfer *Transfer) {
		if transfer.isTerminal() {
			terminalCount++
		}
	})
	if terminalCount != 2 {
		t.Fatalf("retained terminal transfers = %d, want 2", terminalCount)
	}

	for i := 0; i < 2; i++ {
		if _, ok := m.Snapshot(assetref.AssetKey(fmt.Sprintf("retained-asset-%d", i))); ok {
			t.Fatalf("old transfer %d remained after retention pruning", i)
		}
		job := m.JobSnapshot(fmt.Sprintf("retained-job-%d", i))
		if job.AssetsTotal != 0 {
			t.Fatalf("pruned job %d still has %d assets", i, job.AssetsTotal)
		}
	}
	for i := 2; i < 4; i++ {
		key := assetref.AssetKey(fmt.Sprintf("retained-asset-%d", i))
		snapshot, ok := m.Snapshot(key)
		if !ok {
			t.Fatalf("retained transfer %d missing", i)
		}
		if len(snapshot.JobRefs) != 1 {
			t.Fatalf("retained transfer %d job refs = %d, want 1", i, len(snapshot.JobRefs))
		}
		ref := snapshot.JobRefs[0]
		if ref.JobID != fmt.Sprintf("retained-job-%d", i) || ref.TaskID != fmt.Sprintf("retained-task-%d", i) {
			t.Fatalf("retained transfer %d ref = %#v", i, ref)
		}
		if len(ref.SceneIDs) != 1 || ref.SceneIDs[0] != fmt.Sprintf("scene-%d", i) {
			t.Fatalf("retained transfer %d scene refs = %#v", i, ref.SceneIDs)
		}
		job := m.JobSnapshot(fmt.Sprintf("retained-job-%d", i))
		if job.AssetsTotal != 1 || job.AssetsReady != 1 || job.BytesDownloaded != 1024 {
			t.Fatalf("retained job %d snapshot = %#v", i, job)
		}
	}
}
