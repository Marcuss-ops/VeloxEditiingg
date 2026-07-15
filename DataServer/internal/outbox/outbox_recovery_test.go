package outbox_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"velox-server/internal/outbox"
)

// ── 5. Lock expiry: stale PROCESSING row is reclaimable ────────────────────

func TestOutbox_LockExpiry_ReclaimsStaleLock(t *testing.T) {
	store := newTestStore(t)
	reg := outbox.NewRegistry()

	// Single handler tracking invocations across the post-expiry reclaim —
	// the test must demonstrate "process per claim, not per process
	// invocation". We deliberately do NOT call Dispatcher.Poll() for
	// the first acquisition: Poll() runs handler+MarkProcessed, which
	// would leave the row PROCESSED and ineligible for re-claim. Instead
	// we mimic "dispatcher claimed the row but crashed before processing"
	// by calling store.Claim directly with a bounded lock window.
	var invocations atomic.Int32
	h := &stubHandler{
		eventType: "EVT_LOCK",
		fn: func(ctx context.Context, e outbox.Event) error {
			invocations.Add(1)
			return nil
		},
	}
	_ = reg.Register(h)

	id := insertOne(t, store, "EVT_LOCK", "lock", "lock-1", []byte(`{}`))

	// First claimant "crashed" — manual Claim with 50ms lock + a fake
	// dispatcher id. Handler is NOT invoked (no Dispatcher.Poll ran).
	lockedUntil := time.Now().Add(50 * time.Millisecond)
	if _, err := store.Claim(context.Background(), "dead-dispatcher", lockedUntil, 1); err != nil {
		t.Fatalf("Claim (simulated crash): %v", err)
	}
	if got := invocations.Load(); got != 0 {
		t.Fatalf("after manual claim invocations = %d, want 0 (handler should NOT have fired)", got)
	}

	// Wait for the 50ms lock window to expire.
	time.Sleep(75 * time.Millisecond)

	// Second dispatcher's Poll reclaims the stale PROCESSING row and
	// finally invokes the handler for the first (and only) time.
	d2 := outbox.NewDispatcher(store, reg, outbox.Config{
		PollInterval: 5 * time.Millisecond, BatchSize: 5,
		LockDuration: 5 * time.Second, MaxAttempts: 3,
	})
	ctx := context.Background()
	if err := d2.Poll(ctx); err != nil {
		t.Fatalf("d2.Poll: %v", err)
	}
	if got := invocations.Load(); got != 1 {
		t.Fatalf("after reclaim invocations = %d, want 1 (first successful invocation)", got)
	}
	_, status, err := store.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if status != string(outbox.StatusProcessed) {
		t.Fatalf("after reclaim %q status = %q, want PROCESSED", id, status)
	}
}

// ── 6. Dispatcher crash: row stays PROCESSING, restart reclaims ────────────

func TestOutbox_DispatcherCrash_LeavesProcessingAndRecovers(t *testing.T) {
	store := newTestStore(t)
	reg := outbox.NewRegistry()

	// Handler blocks until released (simulating a crashed/stalled worker).
	released := make(chan struct{})
	// fired is buffered (cap=1) + sent via non-blocking select so the
	// handler can fire on EVERY reclaim. DO NOT replace with `make(chan
	// struct{})` + `close(fired)` — that panics on the second invocation,
	// which is exactly the bug we hit before this rewrite (the test depends
	// on >= 2 invocations: first crash mid-flight, second post-reclaim run).
	fired := make(chan struct{}, 1)
	var invocations atomic.Int32
	_ = reg.Register(&stubHandler{
		eventType: "EVT_CRASH",
		fn: func(ctx context.Context, e outbox.Event) error {
			invocations.Add(1)
			select {
			case fired <- struct{}{}:
			default:
			}
			select {
			case <-released:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		},
	})

	id := insertOne(t, store, "EVT_CRASH", "crash", "c-1", []byte(`{}`))

	// Launch dispatcher; cancel ctx mid-flight (no graceful Stop).
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = outbox.NewDispatcher(store, reg, outbox.Config{
			PollInterval: 5 * time.Millisecond, BatchSize: 5,
			LockDuration: 50 * time.Millisecond, MaxAttempts: 3,
		}).Run(ctx)
	}()

	// Wait for the handler to fire, then cancel (simulating crash mid-handle).
	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatalf("handler did not fire")
	}
	cancel()
	<-done

	// Row should still be PROCESSING (the locked_until was 50ms which has
	// already expired by now, but `MarkProcessed` was never called).
	_, status, err := store.GetByID(context.Background(), id)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if status != string(outbox.StatusProcessing) {
		t.Fatalf("after crash status = %q, want PROCESSING", status)
	}

	// Release the blocked handler so future dispatchers don't deadlock.
	close(released)

	// Wait for locked_until to have definitely expired.
	time.Sleep(80 * time.Millisecond)

	// Restart dispatcher: must reclaim.
	d2 := outbox.NewDispatcher(store, reg, outbox.Config{
		PollInterval: 5 * time.Millisecond, BatchSize: 5,
		LockDuration: 5 * time.Second, MaxAttempts: 5,
	})
	ctx2 := startDispatcher(t, d2)

	if err := waitFor(t, 2*time.Second, func() bool {
		_, st, _ := store.GetByID(ctx2, id)
		return st == string(outbox.StatusProcessed)
	}); err != nil {
		t.Fatalf("reclaim did not progress event to PROCESSED: %v", err)
	}
	// Handler must have fired at least twice (initial crash + reclaim).
	if invocations.Load() < 2 {
		t.Fatalf("invocations = %d, want >= 2", invocations.Load())
	}
}
