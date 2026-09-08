package downloader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

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
			order = append(order, string(req.AssetKey))
			orderMu.Unlock()
			return TransferResult{LocalPath: "/o/" + string(req.AssetKey) + ".mp4", Bytes: 1, SHA256: "sha"}, nil
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
