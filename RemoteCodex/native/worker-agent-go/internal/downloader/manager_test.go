package downloader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeTransferer is the byte-pipeline fake used by the manager tests. Each
// test wires its own check/transfer behaviour; zero-value means always-miss
// check and a transfer that completes instantly with a deterministic path.
type fakeTransferer struct {
	check    func(ctx context.Context, reportCtx context.Context, req DownloadRequest) (CacheCheckResult, error)
	transfer func(ctx context.Context, reportCtx context.Context, req DownloadRequest) (TransferResult, error)

	transferCalls atomic.Int32
}

func (f *fakeTransferer) Check(ctx context.Context, reportCtx context.Context, req DownloadRequest) (CacheCheckResult, error) {
	if f.check != nil {
		return f.check(ctx, reportCtx, req)
	}
	return CacheCheckResult{}, nil
}

func (f *fakeTransferer) Transfer(ctx context.Context, reportCtx context.Context, req DownloadRequest) (TransferResult, error) {
	f.transferCalls.Add(1)
	if f.transfer != nil {
		return f.transfer(ctx, reportCtx, req)
	}
	return TransferResult{LocalPath: "/fake/" + req.AssetKey + ".mp4", Bytes: req.SizeBytes, SHA256: "sha"}, nil
}

// newTestManager builds a manager with a fixed clock and one dispatcher.
func newTestManager(t *testing.T, tf *fakeTransferer) *Manager {
	t.Helper()
	m := NewManager(Config{Concurrency: 1, Now: func() time.Time { return time.Unix(1_700_000_000, 0) }}, tf)
	t.Cleanup(m.Close)
	return m
}

// waitFor polls a predicate until it holds or the test times out.
func waitFor(t *testing.T, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestManager_CacheHit: an asset already verified must resolve as CACHE_HIT,
// with zero bytes downloaded, no upstream request (Transfer never called) and
// an immediate READY.
func TestManager_CacheHit(t *testing.T) {
	tf := &fakeTransferer{
		check: func(ctx context.Context, reportCtx context.Context, req DownloadRequest) (CacheCheckResult, error) {
			return CacheCheckResult{CacheHit: true, LocalPath: "/cache/verified.mp4"}, nil
		},
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest) (TransferResult, error) {
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
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest) (TransferResult, error) {
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

// TestManager_CancelOneWaiter_KeepsDownload: cancelling job A while job B
// still uses the asset must NOT interrupt the shared download — the waiter is
// removed, the transfer continues, and B still receives the path.
func TestManager_CancelOneWaiter_KeepsDownload(t *testing.T) {
	release := make(chan struct{})
	tf := &fakeTransferer{
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest) (TransferResult, error) {
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
func TestManager_HashMismatch_FailsNeverReady(t *testing.T) {
	tf := &fakeTransferer{
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest) (TransferResult, error) {
			return TransferResult{}, fmt.Errorf("%w: got ab12 want cd34", ErrVerify)
		},
	}
	m := newTestManager(t, tf)

	_, err := m.Resolve(context.Background(), DownloadRequest{
		JobID: "job-1", AssetKey: "stock-d", AssetID: "stock-d", SHA256: "cd34", SizeBytes: 512, Priority: DefaultPriority,
	})
	if err == nil {
		t.Fatal("resolve must fail on hash mismatch")
	}
	if !errors.Is(err, ErrVerify) {
		t.Fatalf("error = %v, want ErrVerify", err)
	}
	snap, ok := m.Snapshot("stock-d")
	if !ok {
		t.Fatal("snapshot missing after failure")
	}
	if snap.State != DownloadFailed {
		t.Fatalf("state = %s, want FAILED (a mismatched file must never be READY)", snap.State)
	}
	if snap.ErrorCode != "verify_failed" {
		t.Fatalf("error code = %q, want verify_failed", snap.ErrorCode)
	}
	if snap.CompletedAt.IsZero() {
		t.Fatal("completed_at must be set on failure")
	}
}

// TestManager_LastWaiterCancel_CancelsTransfer: when no job uses the asset
// anymore, the shared transfer is cancelled and the partial download is not
// promoted (CANCELLED state).
func TestManager_LastWaiterCancel_CancelsTransfer(t *testing.T) {
	release := make(chan struct{})
	tf := &fakeTransferer{
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest) (TransferResult, error) {
			select {
			case <-release:
			case <-ctx.Done():
				return TransferResult{}, ctx.Err()
			}
			return TransferResult{LocalPath: "/shared/stock-e.mp4", Bytes: req.SizeBytes, SHA256: "sha"}, nil
		},
	}
	m := newTestManager(t, tf)

	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	outA := make(chan error, 1)
	outB := make(chan error, 1)
	go func() {
		_, err := m.Resolve(ctxA, DownloadRequest{
			JobID: "job-0", TaskID: "task-0", AssetKey: "stock-e", AssetID: "stock-e", SizeBytes: 8192, Priority: DefaultPriority,
		})
		outA <- err
	}()
	waitFor(t, "first waiter attached", func() bool {
		snap, ok := m.Snapshot("stock-e")
		return ok && snap.SharedWaiters == 1
	})
	go func() {
		_, err := m.Resolve(ctxB, DownloadRequest{
			JobID: "job-1", TaskID: "task-1", AssetKey: "stock-e", AssetID: "stock-e", SizeBytes: 8192, Priority: DefaultPriority,
		})
		outB <- err
	}()
	waitFor(t, "two waiters attached", func() bool {
		snap, ok := m.Snapshot("stock-e")
		return ok && snap.SharedWaiters == 2
	})

	cancelA()
	waitFor(t, "one waiter left", func() bool {
		snap, _ := m.Snapshot("stock-e")
		return snap.SharedWaiters == 1
	})
	if err := <-outA; err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("job-0 err = %v, want context.Canceled", err)
	}
	// The transfer must still be live after the first cancel.
	if snap, _ := m.Snapshot("stock-e"); snap.State == DownloadCancelled {
		t.Fatal("transfer cancelled while one waiter remained")
	}

	cancelB() // last waiter: the transfer must be cancelled, not completed
	if err := <-outB; err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("job-1 err = %v, want context.Canceled", err)
	}
	waitFor(t, "transfer cancelled", func() bool {
		snap, _ := m.Snapshot("stock-e")
		return snap.State == DownloadCancelled
	})
	snap, _ := m.Snapshot("stock-e")
	if snap.ErrorCode != "transfer_cancelled" {
		t.Fatalf("error code = %q, want transfer_cancelled", snap.ErrorCode)
	}
	close(release)
}

// TestManager_CloseSettlesQueuedTransfers: closing the manager while a
// transfer is queued/running must settle it (cancelled) so the waiter never
// hangs on a never-run or aborted transfer.
func TestManager_CloseSettlesQueuedTransfers(t *testing.T) {
	release := make(chan struct{})
	tf := &fakeTransferer{
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest) (TransferResult, error) {
			select {
			case <-release:
			case <-ctx.Done():
				return TransferResult{}, ctx.Err()
			}
			return TransferResult{LocalPath: "/x.mp4", Bytes: 1, SHA256: "sha"}, nil
		},
	}
	m := NewManager(Config{Concurrency: 1, Now: func() time.Time { return time.Unix(1_700_000_000, 0) }}, tf)

	done := make(chan error, 1)
	go func() {
		_, err := m.Resolve(context.Background(), DownloadRequest{
			JobID: "j0", AssetKey: "s1", AssetID: "s1", Priority: DefaultPriority,
		})
		done <- err
	}()
	waitFor(t, "transfer in flight", func() bool { return tf.transferCalls.Load() == 1 })

	// Close while the transfer is blocked: the transfer ctx is cancelled and
	// the pool joined; the waiter must observe cancellation, not hang.
	m.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("resolve after close must fail")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("resolve hung after manager close")
	}
	close(release)
}

// TestManager_SubscribeAfterTerminalDeliversSnapshot: subscribing to an
// already-settled transfer must deliver the terminal snapshot exactly once
// and close the channel, never blocking a late consumer.
func TestManager_SubscribeAfterTerminalDeliversSnapshot(t *testing.T) {
	tf := &fakeTransferer{
		check: func(ctx context.Context, reportCtx context.Context, req DownloadRequest) (CacheCheckResult, error) {
			return CacheCheckResult{CacheHit: true, LocalPath: "/cache/v.mp4"}, nil
		},
	}
	m := newTestManager(t, tf)
	if _, err := m.Resolve(context.Background(), DownloadRequest{
		JobID: "j0", AssetKey: "s9", AssetID: "s9", Priority: DefaultPriority,
	}); err != nil {
		t.Fatalf("resolve: %v", err)
	}

	ch, unsub := m.Subscribe("s9")
	if ch == nil {
		t.Fatal("subscribe returned nil channel")
	}
	select {
	case snap, open := <-ch:
		if !open {
			t.Fatal("terminal channel must deliver the snapshot before closing")
		}
		if snap.State != DownloadReady {
			t.Fatalf("terminal snapshot state = %s, want READY", snap.State)
		}
		if _, open = <-ch; open {
			t.Fatal("terminal channel must be closed after the snapshot")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("late subscriber never received the terminal snapshot")
	}
	unsub()
}

// TestManager_QueueOrdering: the pool must dispatch in stable order —
// higher priority first, then older queued_at, then asset_key tie-break —
// while a slower transfer occupies the single slot.
func TestManager_QueueOrdering(t *testing.T) {
	release := make(chan struct{})
	var orderMu sync.Mutex
	var order []string
	tf := &fakeTransferer{
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest) (TransferResult, error) {
			select {
			case <-release:
			case <-ctx.Done():
				return TransferResult{}, ctx.Err()
			}
			orderMu.Lock()
			order = append(order, req.AssetKey)
			orderMu.Unlock()
			return TransferResult{LocalPath: "/o/" + req.AssetKey + ".mp4", Bytes: 1, SHA256: "sha"}, nil
		},
	}
	m := newTestManager(t, tf)

	// Occupies the only slot; everything else queues behind it.
	lowDone := make(chan error, 1)
	go func() {
		_, err := m.Resolve(context.Background(), DownloadRequest{
			JobID: "j0", AssetKey: "low", AssetID: "low", Priority: 50,
		})
		lowDone <- err
	}()
	waitFor(t, "low transfer running", func() bool { return tf.transferCalls.Load() == 1 })

	// Enqueued while the slot is busy: high (200) then medium (100).
	highDone := make(chan error, 1)
	medDone := make(chan error, 1)
	go func() {
		_, err := m.Resolve(context.Background(), DownloadRequest{JobID: "j1", AssetKey: "high", AssetID: "high", Priority: 200})
		highDone <- err
	}()
	go func() {
		_, err := m.Resolve(context.Background(), DownloadRequest{JobID: "j2", AssetKey: "medium", AssetID: "medium", Priority: 100})
		medDone <- err
	}()
	waitFor(t, "two queued behind low", func() bool {
		return m.sched.Size() == 2
	})

	close(release)
	for _, ch := range []<-chan error{lowDone, highDone, medDone} {
		if err := <-ch; err != nil {
			t.Fatalf("queued resolve: %v", err)
		}
	}
	orderMu.Lock()
	defer orderMu.Unlock()
	want := []string{"low", "high", "medium"}
	if len(order) != len(want) {
		t.Fatalf("dispatch order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("dispatch order = %v, want %v", order, want)
		}
	}
}
