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
	transfer func(ctx context.Context, reportCtx context.Context, req DownloadRequest, onProgress func(downloadedBytes int64)) (TransferResult, error)

	transferCalls atomic.Int32
}

func (f *fakeTransferer) Check(ctx context.Context, reportCtx context.Context, req DownloadRequest) (CacheCheckResult, error) {
	if f.check != nil {
		return f.check(ctx, reportCtx, req)
	}
	return CacheCheckResult{}, nil
}

func (f *fakeTransferer) Transfer(ctx context.Context, reportCtx context.Context, req DownloadRequest, onProgress func(downloadedBytes int64)) (TransferResult, error) {
	f.transferCalls.Add(1)
	if f.transfer != nil {
		return f.transfer(ctx, reportCtx, req, onProgress)
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
func TestManager_HashMismatch_FailsNeverReady(t *testing.T) {
	tf := &fakeTransferer{
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest, _ func(downloadedBytes int64)) (TransferResult, error) {
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
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest, _ func(downloadedBytes int64)) (TransferResult, error) {
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
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest, _ func(downloadedBytes int64)) (TransferResult, error) {
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
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest, _ func(downloadedBytes int64)) (TransferResult, error) {
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

// manualClock is a test clock the test controls directly.
type manualClock struct {
	mu sync.Mutex
	t  time.Time
}

func newManualClock(at time.Time) *manualClock { return &manualClock{t: at} }

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// TestManager_Progress_ThroughputAndETA: incremental progress reports must
// refresh bytes, throughput (bytes/sec across samples) and ETA (remaining /
// throughput) in the snapshot. 512 bytes streamed over a 1s window → 512 B/s;
// 1024 of 2048 done → ETA 2s.
func TestManager_Progress_ThroughputAndETA(t *testing.T) {
	clk := newManualClock(time.Unix(1_700_000_000, 0))
	step1 := make(chan struct{})
	step2 := make(chan struct{})
	tf := &fakeTransferer{
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest, onProgress func(downloadedBytes int64)) (TransferResult, error) {
			select {
			case <-step1:
			case <-ctx.Done():
				return TransferResult{}, ctx.Err()
			}
			onProgress(512)
			select {
			case <-step2:
			case <-ctx.Done():
				return TransferResult{}, ctx.Err()
			}
			onProgress(1024)
			return TransferResult{LocalPath: "/p.mp4", Bytes: 1024, SHA256: "sha"}, nil
		},
	}
	m := NewManager(Config{
		Concurrency:  1,
		Now:          clk.Now,
		PublishBytes: 1, // publish on every progress call (deterministic)
	}, tf)

	done := make(chan error, 1)
	go func() {
		_, err := m.Resolve(context.Background(), DownloadRequest{
			JobID: "j0", AssetKey: "p1", AssetID: "p1", SizeBytes: 2048, Priority: DefaultPriority,
		})
		done <- err
	}()
	waitFor(t, "transfer downloading", func() bool {
		snap, ok := m.Snapshot("p1")
		return ok && snap.State == DownloadRunning
	})
	close(step1)
	waitFor(t, "first progress step reported", func() bool {
		snap, ok := m.Snapshot("p1")
		return ok && snap.BytesDownloaded == 512
	})
	// One second elapses while the second half of the transfer streams.
	clk.Advance(time.Second)
	close(step2)
	waitFor(t, "second progress step reported", func() bool {
		snap, ok := m.Snapshot("p1")
		return ok && snap.BytesDownloaded == 1024
	})
	mid, _ := m.Snapshot("p1")
	if mid.ThroughputBytesPerSecond < 500 || mid.ThroughputBytesPerSecond > 525 {
		t.Fatalf("throughput = %.1f B/s, want ~512 B/s", mid.ThroughputBytesPerSecond)
	}
	if mid.ETASeconds != 2 {
		t.Fatalf("ETA = %d s, want 2 (1024 remaining at 512 B/s)", mid.ETASeconds)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("resolve hung")
	}

	snap, ok := m.Snapshot("p1")
	if !ok || snap.State != DownloadReady {
		t.Fatalf("final snapshot = %#v, want READY", snap)
	}
}

// TestManager_ProgressThrottle_ByteThreshold: with a time throttle that never
// fires, subscriber snapshots are published only on >= PublishBytes jumps
// (2048 here) — never once per 32KB-style chunk. The first report and the
// terminal transition still publish immediately.
func TestManager_ProgressThrottle_ByteThreshold(t *testing.T) {
	subReady := make(chan struct{})
	tf := &fakeTransferer{
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest, onProgress func(downloadedBytes int64)) (TransferResult, error) {
			select {
			case <-subReady:
			case <-ctx.Done():
				return TransferResult{}, ctx.Err()
			}
			for _, b := range []int64{512, 1024, 1536, 2048, 2560, 3072, 3584, 4096} {
				select {
				case <-ctx.Done():
					return TransferResult{}, ctx.Err()
				default:
				}
				onProgress(b)
			}
			return TransferResult{LocalPath: "/t.mp4", Bytes: 4096, SHA256: "sha"}, nil
		},
	}
	m := NewManager(Config{
		Concurrency:     1,
		Now:             func() time.Time { return time.Unix(1_700_000_000, 0) },
		PublishInterval: time.Hour, // never fires: byte throttle only
		PublishBytes:    2048,
	}, tf)

	done := make(chan error, 1)
	go func() {
		_, err := m.Resolve(context.Background(), DownloadRequest{
			JobID: "j0", AssetKey: "t1", AssetID: "t1", SizeBytes: 4096, Priority: DefaultPriority,
		})
		done <- err
	}()
	waitFor(t, "transfer downloading", func() bool {
		snap, ok := m.Snapshot("t1")
		return ok && snap.State == DownloadRunning
	})
	ch, unsub := m.Subscribe("t1")
	if ch == nil {
		t.Fatal("subscribe returned nil channel")
	}
	close(subReady)

	var downloaded []int64
	for snap := range ch {
		if snap.State == DownloadRunning {
			downloaded = append(downloaded, snap.BytesDownloaded)
		}
		if snap.State.Terminal() {
			break
		}
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("resolve hung")
	}
	unsub()

	// First report publishes (512), then only the 2048-byte jump (2560); the
	// 512-byte intermediates must be suppressed. Terminal READY (4096) is not
	// DOWNLOADING, so it never lands in this slice.
	want := []int64{512, 2560}
	if len(downloaded) != len(want) {
		t.Fatalf("throttled DOWNLOADING publishes = %v, want %v", downloaded, want)
	}
	for i := range want {
		if downloaded[i] != want[i] {
			t.Fatalf("throttled DOWNLOADING publishes = %v, want %v", downloaded, want)
		}
	}
}

// TestManager_CheckpointThrottle: the durable checkpoint hook fires on the
// first report, then per CheckpointBytes (2048), then once for the terminal
// transition — throttled coarser than publishes.
func TestManager_CheckpointThrottle(t *testing.T) {
	var ckptMu sync.Mutex
	var ckptBytes []int64
	tf := &fakeTransferer{
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest, onProgress func(downloadedBytes int64)) (TransferResult, error) {
			for _, b := range []int64{512, 1024, 1536, 2048, 2560, 3072, 3584, 4096} {
				select {
				case <-ctx.Done():
					return TransferResult{}, ctx.Err()
				default:
				}
				onProgress(b)
			}
			return TransferResult{LocalPath: "/c.mp4", Bytes: 4096, SHA256: "sha"}, nil
		},
	}
	m := NewManager(Config{
		Concurrency:        1,
		Now:                func() time.Time { return time.Unix(1_700_000_000, 0) },
		CheckpointInterval: time.Hour, // never fires: byte throttle only
		CheckpointBytes:    2048,
		OnCheckpoint: func(snap DownloadSnapshot, reportCtx context.Context) {
			ckptMu.Lock()
			ckptBytes = append(ckptBytes, snap.BytesDownloaded)
			ckptMu.Unlock()
		},
	}, tf)

	if _, err := m.Resolve(context.Background(), DownloadRequest{
		JobID: "j0", AssetKey: "c1", AssetID: "c1", SizeBytes: 4096, Priority: DefaultPriority,
	}); err != nil {
		t.Fatalf("resolve: %v", err)
	}

	ckptMu.Lock()
	defer ckptMu.Unlock()
	want := []int64{512, 2560, 4096} // first, 2048-jump, terminal READY
	if len(ckptBytes) != len(want) {
		t.Fatalf("checkpoints = %v, want %v", ckptBytes, want)
	}
	for i := range want {
		if ckptBytes[i] != want[i] {
			t.Fatalf("checkpoints = %v, want %v", ckptBytes, want)
		}
	}
}

// TestManager_JobSnapshot_TwoJobsSharedProgress: two jobs on the same asset
// each get a per-job snapshot reflecting the SAME shared transfer progress
// (one upstream, two snapshots).
func TestManager_JobSnapshot_TwoJobsSharedProgress(t *testing.T) {
	progress := make(chan struct{})
	release := make(chan struct{})
	tf := &fakeTransferer{
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest, onProgress func(downloadedBytes int64)) (TransferResult, error) {
			select {
			case <-progress:
			case <-ctx.Done():
				return TransferResult{}, ctx.Err()
			}
			onProgress(1024)
			select {
			case <-release:
			case <-ctx.Done():
				return TransferResult{}, ctx.Err()
			}
			return TransferResult{LocalPath: "/s.mp4", Bytes: 4096, SHA256: "sha"}, nil
		},
	}
	m := newTestManager(t, tf)

	results := make([]error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, results[i] = m.Resolve(context.Background(), DownloadRequest{
				JobID: fmt.Sprintf("job-%d", i), TaskID: fmt.Sprintf("task-%d", i),
				AssetKey: "sh", AssetID: "sh", SizeBytes: 4096, Priority: DefaultPriority,
			})
		}(i)
	}
	waitFor(t, "two waiters on one transfer", func() bool {
		snap, ok := m.Snapshot("sh")
		return ok && snap.SharedWaiters == 2
	})
	close(progress)
	waitFor(t, "shared progress visible", func() bool {
		snap, _ := m.Snapshot("sh")
		return snap.BytesDownloaded == 1024
	})

	for _, jobID := range []string{"job-0", "job-1"} {
		js := m.JobSnapshot(jobID)
		if js.AssetsTotal != 1 {
			t.Fatalf("%s assets = %d, want 1", jobID, js.AssetsTotal)
		}
		if js.BytesTotal != 4096 || js.BytesDownloaded != 1024 {
			t.Fatalf("%s bytes = %d/%d, want 1024/4096", jobID, js.BytesDownloaded, js.BytesTotal)
		}
		if js.ProgressPercent != 25 {
			t.Fatalf("%s progress = %.1f%%, want 25%%", jobID, js.ProgressPercent)
		}
		if js.ActiveTransfers != 1 {
			t.Fatalf("%s active = %d, want 1", jobID, js.ActiveTransfers)
		}
	}
	close(release)
	wg.Wait()
	for i := range results {
		if results[i] != nil {
			t.Fatalf("resolve[%d]: %v", i, results[i])
		}
	}
}

// TestManager_JobSnapshot_ByteWeighted: job progress is weighted on bytes, not
// file counts. One 1 MiB asset READY plus one 5 GiB asset still downloading
// must report ~0.02%, never 50%.
func TestManager_JobSnapshot_ByteWeighted(t *testing.T) {
	bigRelease := make(chan struct{})
	tf := &fakeTransferer{
		check: func(ctx context.Context, reportCtx context.Context, req DownloadRequest) (CacheCheckResult, error) {
			if req.AssetID == "small" {
				return CacheCheckResult{CacheHit: true, LocalPath: "/c/small.mp4"}, nil
			}
			return CacheCheckResult{}, nil
		},
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest, _ func(downloadedBytes int64)) (TransferResult, error) {
			select {
			case <-bigRelease:
			case <-ctx.Done():
				return TransferResult{}, ctx.Err()
			}
			return TransferResult{LocalPath: "/big.mp4", Bytes: req.SizeBytes, SHA256: "sha"}, nil
		},
	}
	m := newTestManager(t, tf)

	const smallBytes = int64(1 << 20) // 1 MiB
	const bigBytes = int64(5) << 30   // 5 GiB
	results := make([]error, 2)
	var wg sync.WaitGroup
	for i, id := range []string{"small", "big"} {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			size := smallBytes
			if id == "big" {
				size = bigBytes
			}
			_, results[i] = m.Resolve(context.Background(), DownloadRequest{
				JobID: "job-0", AssetKey: id, AssetID: id, SizeBytes: size, Priority: DefaultPriority,
			})
		}(i, id)
	}
	waitFor(t, "big asset downloading with waiter", func() bool {
		snap, ok := m.Snapshot("big")
		return ok && snap.State == DownloadRunning && snap.SharedWaiters == 1
	})

	js := m.JobSnapshot("job-0")
	if js.AssetsTotal != 2 {
		t.Fatalf("assets total = %d, want 2", js.AssetsTotal)
	}
	if js.AssetsReady != 1 || js.AssetsDownloading != 1 {
		t.Fatalf("ready=%d downloading=%d, want 1/1", js.AssetsReady, js.AssetsDownloading)
	}
	if js.BytesTotal != smallBytes+bigBytes {
		t.Fatalf("bytes_total = %d, want %d", js.BytesTotal, smallBytes+bigBytes)
	}
	if js.BytesDownloaded != smallBytes {
		t.Fatalf("bytes_downloaded = %d, want %d (only the small ready asset counts)", js.BytesDownloaded, smallBytes)
	}
	if js.ProgressPercent >= 1 {
		t.Fatalf("progress = %.4f%%, want < 1%% (byte-weighted, not file-count)", js.ProgressPercent)
	}

	close(bigRelease)
	wg.Wait()
	for i := range results {
		if results[i] != nil {
			t.Fatalf("resolve[%d]: %v", i, results[i])
		}
	}
}
