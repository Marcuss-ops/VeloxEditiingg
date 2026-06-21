// PR-3.5 — tests for the resizable, cond-based ConcurrencyLimiter.
// Replaces the prior semaphore-channel tests which assumed Acquire
// failed on full capacity rather than queued.
package concurrency

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewConcurrencyLimiter(t *testing.T) {
	cl := NewConcurrencyLimiter(1)
	defer cl.Stop()
	if got := cl.MaxActiveJobs(); got != 1 {
		t.Fatalf("expected MaxActiveJobs==1, got %d", got)
	}
	if cl.ActiveJobCount() != 0 {
		t.Fatalf("expected ActiveJobCount==0, got %d", cl.ActiveJobCount())
	}
}

func TestConcurrencyLimiter_AcquireRelease(t *testing.T) {
	cl := NewConcurrencyLimiter(1)
	defer cl.Stop()

	ctx := context.Background()

	if err := cl.Acquire(ctx, "job-1", 1); err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}
	if cl.ActiveJobCount() != 1 {
		t.Fatalf("ActiveJobCount should be 1, got %d", cl.ActiveJobCount())
	}

	// Second acquire with a tight deadline must surface ctx.Err() rather
	// than block forever on a full limiter.
	shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := cl.Acquire(shortCtx, "job-2", 1); err == nil {
		t.Fatalf("expected Acquire to fail under tight deadline when at capacity")
	}

	// Release and acquire again — must succeed.
	cl.Release()
	if cl.ActiveJobCount() != 0 {
		t.Fatalf("ActiveJobCount should be 0 after Release, got %d", cl.ActiveJobCount())
	}
	if err := cl.Acquire(ctx, "job-3", 1); err != nil {
		t.Fatalf("acquire after release failed: %v", err)
	}
	cl.Release()
}

func TestConcurrencyLimiter_ResizableUpsizeUnblocksWaiters(t *testing.T) {
	// PR-3.5 critical invariant: SetMaxActiveJobs(INCREASE) wakes
	// waiters blocked at the old cap so they get their slot.
	cl := NewConcurrencyLimiter(1)
	defer cl.Stop()

	ctx := context.Background()
	if err := cl.Acquire(ctx, "job-1", 1); err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}

	gotSlot := make(chan struct{})
	go func() {
		_ = cl.Acquire(ctx, "job-blocked", 1)
		close(gotSlot)
	}()

	select {
	case <-gotSlot:
		t.Fatalf("second acquire succeeded unexpectedly; should be queued")
	case <-time.After(100 * time.Millisecond):
	}

	// Raise the cap; second caller's awaitWaiter should admit.
	cl.SetMaxActiveJobs(2)

	select {
	case <-gotSlot:
	case <-time.After(2 * time.Second):
		t.Fatalf("upsize did not wake queued waiter")
	}

	if cl.ActiveJobCount() != 2 {
		t.Fatalf("ActiveJobCount should be 2, got %d", cl.ActiveJobCount())
	}
	cl.Release()
	cl.Release()
}

func TestConcurrencyLimiter_CtxCancelCleansUpWaitQueue(t *testing.T) {
	// PR-3.5 critical invariant: cancelling Acquire context cleanly
	// removes the waiter from waitList and wakes its awaitWaiter
	// goroutine. We assert via the WaitingJobCount delta — if the
	// cleanup path were broken, WaitingJobCount would not return to
	// zero after the cancel completes.
	cl := NewConcurrencyLimiter(1)
	defer cl.Stop()

	if err := cl.Acquire(context.Background(), "job-1", 1); err != nil {
		t.Fatalf("fill limiter failed: %v", err)
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	gotErr := make(chan error, 1)
	go func() {
		gotErr <- cl.Acquire(cancelCtx, "job-cancelled", 1)
	}()

	// Wait until the waiter is actually queued.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cl.WaitingJobCount() == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if cl.WaitingJobCount() != 1 {
		t.Fatalf("expected WaitingJobCount==1 for queued waiter, got %d", cl.WaitingJobCount())
	}

	cancel()

	select {
	case err := <-gotErr:
		if err == nil {
			t.Fatalf("expected ctx.Err(), got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("cancellation did not wake AwaitWaiter in 2s")
	}

	// Give the cleanup a brief window.
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cl.WaitingJobCount() == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if cl.WaitingJobCount() != 0 {
		t.Fatalf("expected WaitingJobCount back to 0 after cancel; got %d", cl.WaitingJobCount())
	}

	cl.Release()
}

func TestConcurrencyLimiter_PriorityIsStatsOnly(t *testing.T) {
	// PR-3.5 (post-review fix): priority is stats-only. Two concurrent
	// priority>=3 callers do not preempt; they queue like everyone else.
	// The previous "bump maxActiveJobs+1 for priority>=3, defer
	// rollback" pattern had a cap-leak race when two priority callers
	// raced: each would snapshot cap=1, both bump to 2, both admit, both
	// defer-rollback, leaking the advertised cap on the wire.
	cl := NewConcurrencyLimiter(1)
	defer cl.Stop()

	if err := cl.Acquire(context.Background(), "job-1", 1); err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}

	// Priority 3 with cap reached cannot preempt; Acquire blocks until
	// the context deadline fires.
	shortCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := cl.Acquire(shortCtx, "job-crit", 3); err == nil {
		t.Fatalf("priority>=3 must NOT preempt; expected timeout error")
	}

	// Wire cap stable across the failed critical attempt.
	if cl.MaxActiveJobs() != 1 {
		t.Fatalf("MaxActiveJobs leaked — should still be 1, got %d", cl.MaxActiveJobs())
	}
	if cl.ActiveJobCount() != 1 {
		t.Fatalf("ActiveJobCount leaked — should still be 1, got %d", cl.ActiveJobCount())
	}

	cl.Release()
}

func TestConcurrencyLimiter_RaceSafeSetAndRead(t *testing.T) {
	// PR-3.5 critical invariant: racy SetMaxActiveJobs interleaved with
	// MaxActiveJobs reads must never produce a torn int (would crash
	// with go test -race if the field had a non-atomic read race).
	cl := NewConcurrencyLimiter(2)
	defer cl.Stop()

	stop := int32(0)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for atomic.LoadInt32(&stop) == 0 {
			cl.SetMaxActiveJobs(8)
			cl.SetMaxActiveJobs(1)
		}
	}()

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for atomic.LoadInt32(&stop) == 0 {
				_ = cl.MaxActiveJobs()
				_ = cl.ActiveJobCount()
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	atomic.StoreInt32(&stop, 1)
	wg.Wait()
}

func TestConcurrencyLimiter_ConcurrentAcquirePressure(t *testing.T) {
	// N goroutines pound Acquire with cap=2. Capacity invariant:
	// ActiveJobs<=MaxActiveJobs at all times.
	cl := NewConcurrencyLimiter(2)
	defer cl.Stop()

	var wg sync.WaitGroup
	var totalAdmitted int32

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			if err := cl.Acquire(sctx, "job", 1); err == nil {
				atomic.AddInt32(&totalAdmitted, 1)
				cl.Release()
			}
		}()
	}
	wg.Wait()

	if cl.ActiveJobCount() != 0 {
		t.Fatalf("ActiveJobCount should be 0 after all releases, got %d", cl.ActiveJobCount())
	}
	if totalAdmitted < 1 {
		t.Fatalf("expected at least 1 admission under contention, got %d", totalAdmitted)
	}
}

func TestConcurrencyLimiter_StopIsIdempotent(t *testing.T) {
	cl := NewConcurrencyLimiter(1)
	cl.Acquire(context.Background(), "job-1", 1)
	cl.Stop()
	cl.Stop() // idempotent
}
