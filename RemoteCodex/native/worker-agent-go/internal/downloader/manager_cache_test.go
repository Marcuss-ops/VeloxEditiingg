package downloader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"velox-shared/assetref"
)

func TestManager_CacheHit(t *testing.T) {
	tf := &fakeTransferer{
		check: func(ctx context.Context, reportCtx context.Context, req DownloadRequest) (CacheCheckResult, error) {
			return CacheCheckResult{CacheHit: true, LocalPath: "/cache/verified.mp4", SHA256: "verified-cache-sha"}, nil
		},
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest, _ func(downloadedBytes int64)) (TransferResult, error) {
			return TransferResult{}, errors.New("transfer must not run on cache hit")
		},
	}
	m := newTestManager(t, tf)

	asset, err := m.Resolve(context.Background(), DownloadRequest{
		JobID: "job-1", AssetKey: "stock-a", AssetID: "stock-a",
		SHA256: "abc", SizeBytes: 1024, Priority: DefaultPriority,
	})
	if err != nil {
		t.Fatalf("cache-hit resolve: %v", err)
	}
	if asset.LocalPath != "/cache/verified.mp4" {
		t.Fatalf("path = %q, want /cache/verified.mp4", asset.LocalPath)
	}
	if asset.SHA256 != "verified-cache-sha" {
		t.Fatalf("cache-hit SHA256 = %q, want verified-cache-sha", asset.SHA256)
	}
	if !asset.CacheHit {
		t.Fatal("asset must be reported as a cache hit")
	}
	if tf.transferCalls.Load() != 0 {
		t.Fatalf("transfer ran %d times on a cache hit, want 0", tf.transferCalls.Load())
	}
	snap, ok := m.Snapshot("stock-a")
	if !ok {
		t.Fatal("snapshot missing after resolve")
	}
	if snap.State != DownloadReady {
		t.Fatalf("state = %s, want READY", snap.State)
	}
	if !snap.CacheHit {
		t.Fatal("snapshot must report cache hit")
	}
	if snap.BytesDownloaded != 0 {
		t.Fatalf("bytes_downloaded = %d, want 0 (cache hit)", snap.BytesDownloaded)
	}
}

// TestManager_TwoJobsSameAsset_OneUpstream: two concurrent requests for the
// same asset must produce ONE upstream transfer, shared progress with two
// waiters, and the same local path for both jobs.
func TestManager_TwoJobsSameAsset_OneUpstream(t *testing.T) {
	release := make(chan struct{})
	tf := &fakeTransferer{
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest, _ func(downloadedBytes int64)) (TransferResult, error) {
			select {
			case <-release:
			case <-ctx.Done():
				return TransferResult{}, ctx.Err()
			}
			return TransferResult{LocalPath: "/shared/stock-b.mp4", Bytes: req.SizeBytes, SHA256: "sha"}, nil
		},
	}
	m := newTestManager(t, tf)

	type out struct {
		asset DownloadedAsset
		err   error
	}
	results := make([]out, 2)
	ctxs := []context.Context{context.Background(), context.Background()}
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i].asset, results[i].err = m.Resolve(ctxs[i], DownloadRequest{
				JobID: fmt.Sprintf("job-%d", i), TaskID: fmt.Sprintf("task-%d", i),
				AssetKey: "stock-b", AssetID: "stock-b", SizeBytes: 2048, Priority: DefaultPriority,
			})
		}(i)
	}

	// Both waiters must be attached to ONE shared transfer before releasing.
	waitFor(t, "two waiters on one transfer", func() bool {
		snap, ok := m.Snapshot("stock-b")
		return ok && snap.SharedWaiters == 2
	})
	// The single dispatcher may not have dequeued the transfer yet; wait until
	// it is actually in flight (the release gate keeps it blocked at 1).
	waitFor(t, "transfer started", func() bool { return tf.transferCalls.Load() == 1 })
	close(release)
	wg.Wait()

	for i := range results {
		if results[i].err != nil {
			t.Fatalf("resolve[%d]: %v", i, results[i].err)
		}
		if results[i].asset.LocalPath != "/shared/stock-b.mp4" {
			t.Fatalf("resolve[%d] path = %q, want /shared/stock-b.mp4", i, results[i].asset.LocalPath)
		}
	}
	if got := tf.transferCalls.Load(); got != 1 {
		t.Fatalf("transfer calls = %d, want exactly 1 (dedup invariant)", got)
	}
	snap, _ := m.Snapshot("stock-b")
	if snap.State != DownloadReady {
		t.Fatalf("final state = %s, want READY", snap.State)
	}
}

// TestManager_CoalescedRequestHook: the manager reports the second caller
// through the canonical coalescing hook, preserving duplicate-download
// accounting without a separate singleflight resolver.
func TestManager_CoalescedRequestHook(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int32
	var size atomic.Int64
	tf := &fakeTransferer{
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest, _ func(int64)) (TransferResult, error) {
			select {
			case <-release:
			case <-ctx.Done():
				return TransferResult{}, ctx.Err()
			}
			return TransferResult{LocalPath: "/shared/hook.mp4", Bytes: req.SizeBytes}, nil
		},
	}
	m := NewManager(Config{
		Concurrency: 1,
		OnCoalescedRequest: func(bytes int64, _ context.Context) {
			calls.Add(1)
			size.Store(bytes)
		},
	}, tf)
	t.Cleanup(m.Close)

	request := DownloadRequest{AssetKey: "hook", AssetID: "hook", SizeBytes: 4096}
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := m.Resolve(context.Background(), request)
			results <- err
		}()
	}
	waitFor(t, "coalesced waiter", func() bool {
		snap, ok := m.Snapshot(request.AssetKey)
		return ok && snap.SharedWaiters == 2
	})
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatalf("resolve: %v", err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("coalesced hook calls = %d, want 1", calls.Load())
	}
	if size.Load() != request.SizeBytes {
		t.Fatalf("coalesced hook size = %d, want %d", size.Load(), request.SizeBytes)
	}
}

// TestManager_25Requests12AssetsSingleFlight pins the cold-wave contract used
// by the performance baseline: 25 logical requests for 12 assets produce
// exactly 12 physical upstream transfers and 12 assets' worth of physical
// bytes, while the 13 coalesced waiters are measurable as AVOIDED bytes.
//
// Metric semantics (the reason this test asserts both sides):
// `duplicate_download_bytes` / OnCoalescedRequest counts the bytes a coalesced
// waiter WOULD have downloaded, i.e. bytes the singleflight SAVED. A non-zero
// value therefore means "the dedupe avoided these bytes", never "these bytes
// were physically transferred twice". The physical side is pinned separately
// (upstreamBytes == one copy of each unique asset), so the two halves of the
// acceptance can no longer be confused for each other.
func TestManager_25Requests12AssetsSingleFlight(t *testing.T) {
	release := make(chan struct{})
	var upstream atomic.Int32
	var upstreamBytes atomic.Int64
	var coalescedAvoidedBytes atomic.Int64
	tf := &fakeTransferer{
		transfer: func(ctx context.Context, _ context.Context, req DownloadRequest, _ func(int64)) (TransferResult, error) {
			upstream.Add(1)
			upstreamBytes.Add(req.SizeBytes)
			select {
			case <-release:
			case <-ctx.Done():
				return TransferResult{}, ctx.Err()
			}
			return TransferResult{LocalPath: "/shared/" + string(req.AssetKey), Bytes: req.SizeBytes, SHA256: "sha"}, nil
		},
	}
	m := NewManager(Config{
		Concurrency: 12,
		OnCoalescedRequest: func(bytes int64, _ context.Context) {
			coalescedAvoidedBytes.Add(bytes)
		},
	}, tf)
	t.Cleanup(m.Close)

	const assetCount = 12
	const requests = 25
	const assetSize = int64(1024)
	results := make(chan error, requests)
	for i := 0; i < requests; i++ {
		asset := i % assetCount
		go func(index, asset int) {
			_, err := m.Resolve(context.Background(), DownloadRequest{
				JobID: fmt.Sprintf("dedupe-job-%02d", index), TaskID: fmt.Sprintf("dedupe-task-%02d", index),
				AssetKey: assetref.AssetKey(fmt.Sprintf("asset-%02d", asset)), AssetID: fmt.Sprintf("asset-%02d", asset),
				SizeBytes: assetSize, Priority: DefaultPriority,
			})
			results <- err
		}(i, asset)
	}
	waitFor(t, "12 active single-flight transfers", func() bool {
		return upstream.Load() == assetCount
	})
	waitFor(t, "13 coalesced waiters", func() bool {
		return coalescedAvoidedBytes.Load() == 13*assetSize
	})
	close(release)
	for i := 0; i < requests; i++ {
		if err := <-results; err != nil {
			t.Fatalf("resolve[%d]: %v", i, err)
		}
	}

	// Physical side: exactly 12 upstream requests and exactly one copy of each
	// unique asset's bytes. This is the assertion that must hold for the
	// singleflight to be certified; `duplicate_download_bytes=0` alone is NOT
	// the acceptance criterion (a value > 0 is expected and healthy when
	// waiters are coalesced).
	if got := upstream.Load(); got != assetCount {
		t.Fatalf("physical upstream transfers = %d, want %d", got, assetCount)
	}
	if got := upstreamBytes.Load(); got != assetCount*assetSize {
		t.Fatalf("physical upstream bytes = %d, want %d (each unique asset once)", got, assetCount*assetSize)
	}

	// Avoided side: 13 waiters were coalesced, and each contributed the bytes
	// the dedupe saved. Those bytes were never transferred upstream.
	if got := coalescedAvoidedBytes.Load(); got != 13*assetSize {
		t.Fatalf("coalesced avoided bytes = %d, want %d", got, 13*assetSize)
	}
	if got := m.LatestOperational().CoalescedRequestsTotal; got != 13 {
		t.Fatalf("CoalescedRequestsTotal = %d, want 13", got)
	}
}

// TestManager_SameTaskConcurrentWaiters: two independent Resolve calls with
// identical job/task metadata still represent two waiters. Cancelling one must
// not remove the other or cancel the shared transfer.
func TestManager_SameTaskConcurrentWaiters(t *testing.T) {
	release := make(chan struct{})
	tf := &fakeTransferer{
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest, _ func(int64)) (TransferResult, error) {
			select {
			case <-release:
			case <-ctx.Done():
				return TransferResult{}, ctx.Err()
			}
			return TransferResult{LocalPath: "/shared/same-task.mp4", Bytes: req.SizeBytes, SHA256: "sha"}, nil
		},
	}
	m := newTestManager(t, tf)
	ctxA, cancelA := context.WithCancel(context.Background())
	outA := make(chan error, 1)
	outB := make(chan DownloadedAsset, 1)
	request := DownloadRequest{JobID: "same-job", TaskID: "same-task", AssetKey: "same-asset", AssetID: "same-asset", SizeBytes: 4096}
	go func() {
		_, err := m.Resolve(ctxA, request)
		outA <- err
	}()
	waitFor(t, "first same-task waiter", func() bool {
		snap, ok := m.Snapshot(request.AssetKey)
		return ok && snap.SharedWaiters == 1
	})
	go func() {
		asset, err := m.Resolve(context.Background(), request)
		if err != nil {
			outB <- DownloadedAsset{}
			return
		}
		outB <- asset
	}()
	waitFor(t, "two same-task waiters", func() bool {
		snap, ok := m.Snapshot(request.AssetKey)
		return ok && snap.SharedWaiters == 2
	})

	cancelA()
	if err := <-outA; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled same-task waiter error = %v, want context.Canceled", err)
	}
	waitFor(t, "surviving same-task waiter", func() bool {
		snap, _ := m.Snapshot(request.AssetKey)
		return snap.SharedWaiters == 1 && snap.State != DownloadCancelled
	})
	close(release)
	asset := <-outB
	if asset.LocalPath != "/shared/same-task.mp4" {
		t.Fatalf("surviving waiter path = %q, want shared path", asset.LocalPath)
	}
	if got := tf.transferCalls.Load(); got != 1 {
		t.Fatalf("transfer calls = %d, want 1", got)
	}
}

// TestManager_CancelOneWaiter_KeepsDownload: cancelling job A while job B
// still uses the asset must NOT interrupt the shared download — the waiter is
// removed, the transfer continues, and B still receives the path.
func TestManager_CancelOneWaiter_KeepsDownload(t *testing.T) {
	release := make(chan struct{})
	tf := &fakeTransferer{
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest, _ func(downloadedBytes int64)) (TransferResult, error) {
			select {
			case <-release:
			case <-ctx.Done():
				return TransferResult{}, ctx.Err()
			}
			return TransferResult{LocalPath: "/shared/stock-c.mp4", Bytes: req.SizeBytes, SHA256: "sha"}, nil
		},
	}
	m := newTestManager(t, tf)

	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB := context.Background()

	type resolveOut struct {
		asset DownloadedAsset
		err   error
	}
	outA := make(chan resolveOut, 1)
	outB := make(chan resolveOut, 1)
	go func() {
		a, err := m.Resolve(ctxA, DownloadRequest{
			JobID: "job-A", TaskID: "task-A", AssetKey: "stock-c", AssetID: "stock-c", SizeBytes: 4096, Priority: DefaultPriority,
		})
		outA <- resolveOut{a, err}
	}()
	waitFor(t, "first waiter attached", func() bool {
		snap, ok := m.Snapshot("stock-c")
		return ok && snap.SharedWaiters == 1
	})
	go func() {
		b, err := m.Resolve(ctxB, DownloadRequest{
			JobID: "job-B", TaskID: "task-B", AssetKey: "stock-c", AssetID: "stock-c", SizeBytes: 4096, Priority: DefaultPriority,
		})
		outB <- resolveOut{b, err}
	}()
	waitFor(t, "two waiters attached", func() bool {
		snap, ok := m.Snapshot("stock-c")
		return ok && snap.SharedWaiters == 2
	})

	cancelA()
	// Job A observes its own cancellation while the transfer keeps running.
	rA := <-outA
	if rA.err == nil || !errors.Is(rA.err, context.Canceled) {
		t.Fatalf("job A err = %v, want context.Canceled", rA.err)
	}
	// ...but the transfer kept running for B and settled READY.
	close(release)
	rB := <-outB
	if rB.err != nil {
		t.Fatalf("job B resolve: %v", rB.err)
	}
	if rB.asset.LocalPath != "/shared/stock-c.mp4" {
		t.Fatalf("job B path = %q", rB.asset.LocalPath)
	}
	snap, _ := m.Snapshot("stock-c")
	if snap.State != DownloadReady {
		t.Fatalf("state after one-waiter cancel = %s, want READY (download must continue)", snap.State)
	}
}

// TestManager_HashMismatch_FailsNeverReady: a verified hash mismatch must
// surface as FAILED — never READY — and the caller must receive the error.
