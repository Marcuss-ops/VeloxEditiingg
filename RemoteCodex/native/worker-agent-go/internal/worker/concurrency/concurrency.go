// Package concurrency provides semaphore-based concurrency limiting for job execution.
//
// PR-3.5 redesign: the implementation is now TRULY resizable.
// SetMaxActiveJobs atomically updates the cap AND broadcasts to any
// waiters blocked at capacity so they can evaluate the new ceiling
// immediately. Public API (NewConcurrencyLimiter / Acquire / Release /
// Stats / MaxActiveJobs / SetMaxActiveJobs / CanAcceptJob / Stop) is
// unchanged so call sites in worker.go and worker_comms.go need no
// adjustments.
//
// Concurrency model:
//
//   - maxActiveJobs is int64-backed by atomic.LoadInt64/StoreInt64 so
//     the wire path (Worker.capabilitiesMap → BuildCapabilityReport →
//     pr-3.5 hello or heartbeat) reads a race-free value.
//   - When SetMaxActiveJobs increases the cap, mu.Broadcast() wakes
//     waiters blocked on Cond.Wait() so they can re-enter Acquire.
//   - Acquisition is mutex-guarded; the OS-level goroutine scheduler
//     handles fairness. shouldReject is gone — all priority tiers
//     share one slot counter (kept for backward-compat with stats
//     shape);
//     the historical "priority" caller field is preserved but no
//     longer affects admission decisions.
package concurrency

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// ConcurrencyStats reports concurrency limiter statistics.
type ConcurrencyStats struct {
	MaxActiveJobs int     `json:"max_active_jobs"`
	ActiveJobs    int32   `json:"active_jobs"`
	WaitingJobs   int32   `json:"waiting_jobs"`
	TotalJobs     int64   `json:"total_jobs"`
	RejectedJobs  int64   `json:"rejected_jobs"`
	Utilization   float64 `json:"utilization_pct"`
}

// ConcurrencyLimiter controls the number of concurrent jobs a worker can execute.
type ConcurrencyLimiter struct {
	// maxActiveJobs is int64 + atomic-backed; the wire path reads it
	// via LoadInt64 so it stays race-free per the Go memory model.
	maxActiveJobs int64

	// activeJobs is atomic so stats + non-blocking queries are race-free.
	activeJobs int32

	// State (counters). All atomic.
	waitingJobs  int32
	totalJobs    int64
	rejectedJobs int64

	// mu guards slots, ready, waitList.
	mu sync.Mutex

	// cond is the wait queue signaling primitive. SetMaxActiveJobs
	// triggers cond.Broadcast under mu so newly-eligible waiters
	// can re-evaluate admission.
	cond *sync.Cond

	// Per-job waiters, indexed by pointer for resolved signaling.
	waitList map[*jobWaiter]struct{}

	// Lifecycle.
	stopChan chan struct{}
	stopOnce sync.Once
}

type jobWaiter struct {
	ctx      context.Context
	jobID    string
	ready    chan struct{}
	priority int
	// wg is incremented by admitOrQueue before spawning awaitWaiter and
	// decremented (deferred) by awaitWaiter on its exit. Outer paths
	// use wg.Wait to ensure awaitWaiter's admit-vs-cancel decision is
	// final before deciding whether to Release the slot the waiter
	// may have admitted.
	wg sync.WaitGroup
}

// NewConcurrencyLimiter creates a new concurrency limiter.
func NewConcurrencyLimiter(maxActiveJobs int) *ConcurrencyLimiter {
	if maxActiveJobs <= 0 {
		maxActiveJobs = 1
	}
	cl := &ConcurrencyLimiter{
		maxActiveJobs: int64(maxActiveJobs),
		waitList:      make(map[*jobWaiter]struct{}),
		stopChan:      make(chan struct{}),
	}
	cl.cond = sync.NewCond(&cl.mu)
	return cl
}

// Acquire attempts to acquire a slot for job execution. Returns nil on
// success, ctx.Err() if the caller's context is canceled, or "limiter
// stopped" if the limiter was Stop()ed.
//
// PR-3.5 priority handling (post-review fix): priority is stats-only.
// All callers share one slot counter and one cap. Removing the
// temporary-cap-bump preemption closes a race where two concurrent
// priority>=3 calls could each snapshot the same cap, both bump, and
// both defer-rollback, leaking the advertised cap on the wire.
func (cl *ConcurrencyLimiter) Acquire(ctx context.Context, jobID string, priority int) error {
	return cl.admitOrQueue(ctx, jobID, priority)
}

// admitOrQueue is the common-work path: fast non-blocking CAS admit
// or queue-and-wait via Cond. Used by both normal and critical callers.
func (cl *ConcurrencyLimiter) admitOrQueue(ctx context.Context, jobID string, priority int) error {
	// Fast path: slot available without locking.
	for {
		current := atomic.LoadInt32(&cl.activeJobs)
		cap := atomic.LoadInt64(&cl.maxActiveJobs)
		if int64(current) >= cap {
			break
		}
		if atomic.CompareAndSwapInt32(&cl.activeJobs, current, current+1) {
			atomic.AddInt64(&cl.totalJobs, 1)
			return nil
		}
	}

	// At capacity: enqueue and block on Cond.
	cl.mu.Lock()
	current := atomic.LoadInt32(&cl.activeJobs)
	cap := atomic.LoadInt64(&cl.maxActiveJobs)
	if int64(current) < cap {
		cl.mu.Unlock()
		return cl.admitOrQueue(ctx, jobID, priority)
	}

	waiter := &jobWaiter{ctx: ctx, jobID: jobID, priority: priority, ready: make(chan struct{})}
	cl.waitList[waiter] = struct{}{}
	atomic.AddInt32(&cl.waitingJobs, 1)
	cl.mu.Unlock()

	waiter.wg.Add(1)
	go cl.awaitWaiter(waiter)

	select {
	case <-waiter.ready:
		atomic.AddInt32(&cl.waitingJobs, -1)
		atomic.AddInt64(&cl.totalJobs, 1)
		return nil
	case <-ctx.Done():
		cl.mu.Lock()
		if _, ok := cl.waitList[waiter]; ok {
			delete(cl.waitList, waiter)
			atomic.AddInt32(&cl.waitingJobs, -1)
			atomic.AddInt64(&cl.rejectedJobs, 1)
		}
		cl.mu.Unlock()
		cl.mu.Lock()
		cl.cond.Broadcast()
		cl.mu.Unlock()

		// Block until awaitWaiter observably settled. After this
		// returns, awaitWaiter's admit decision is final: either it
		// was removed from waitList before admit (no activeJobs bump),
		// or it admitted and closed waiter.ready (slot held). Without
		// the wg.Wait barrier, a cancel that races with admit can
		// observe "not in waitList" while the admit is mid-flight and
		// leak the slot.
		waiter.wg.Wait()
		select {
		case <-waiter.ready:
			cl.Release()
		default:
		}
		return ctx.Err()
	case <-cl.stopChan:
		cl.mu.Lock()
		if _, ok := cl.waitList[waiter]; ok {
			delete(cl.waitList, waiter)
			atomic.AddInt32(&cl.waitingJobs, -1)
		}
		cl.mu.Unlock()
		cl.mu.Lock()
		cl.cond.Broadcast()
		cl.mu.Unlock()

		waiter.wg.Wait()
		select {
		case <-waiter.ready:
			cl.Release()
		default:
		}
		return fmt.Errorf("limiter stopped")
	}
}

// awaitWaiter blocks on Cond.Wait and closes waiter.ready when the
// signal arrives. wg.Done is deferred FIRST so the outer cancel/stop
// path's wg.Wait is unblocked even on panic. The goroutine exits
// cleanly when the outer cancel or stop path removes the waitList
// entry; a slot release race that READMITTED a waiter after
// outer-cancel is impossible because the second
// `if _, ok := cl.waitList[w]; !ok` check after cond.Wait returns
// catches the removed-entry case before any state mutation.
func (cl *ConcurrencyLimiter) awaitWaiter(w *jobWaiter) {
	defer w.wg.Done()
	cl.mu.Lock()
	defer cl.mu.Unlock()
	for {
		// stopChan check at iteration boundaries.
		select {
		case <-cl.stopChan:
			return
		default:
		}
		// Initial entry: must be in waitList.
		if _, ok := cl.waitList[w]; !ok {
			return
		}
		cl.cond.Wait()

		// After wakeup: if outer cancelled/removed us, exit without
		// admitting \u2014 prevents slot leak under cancellation races.
		if _, ok := cl.waitList[w]; !ok {
			return
		}

		current := atomic.LoadInt32(&cl.activeJobs)
		cap := atomic.LoadInt64(&cl.maxActiveJobs)
		if int64(current) < cap {
			delete(cl.waitList, w)
			if !atomic.CompareAndSwapInt32(&cl.activeJobs, current, current+1) {
				continue
			}
			close(w.ready)
			return
		}
	}
}

// Release releases a slot after job execution. Broadcasts the cond so
// any waiter at capacity sees the new free slot.
func (cl *ConcurrencyLimiter) Release() {
	atomic.AddInt32(&cl.activeJobs, -1)
	cl.mu.Lock()
	cl.cond.Broadcast()
	cl.mu.Unlock()
}

// MaxActiveJobs returns the configured (advertised) maximum concurrent
// jobs. PR-3.5 race-free: atomic load so concurrent SetMaxActiveJobs
// writes cannot produce torn reads on the wire.
func (cl *ConcurrencyLimiter) MaxActiveJobs() int {
	return int(atomic.LoadInt64(&cl.maxActiveJobs))
}

// SetMaxActiveJobs updates the maximum concurrent jobs limit at runtime.
// PR-3.5 invariant: the new value IS effective capacity. The atomic
// store is paired with cond.Broadcast so any queued waiters blocked at
// the old cap re-evaluate admission.
func (cl *ConcurrencyLimiter) SetMaxActiveJobs(max int) {
	if max <= 0 {
		max = 1
	}
	atomic.StoreInt64(&cl.maxActiveJobs, int64(max))
	cl.mu.Lock()
	cl.cond.Broadcast()
	cl.mu.Unlock()
}

// Stats returns the current concurrency statistics. PR-3.5 race-free
// atomic reads across the board.
func (cl *ConcurrencyLimiter) Stats() ConcurrencyStats {
	active := atomic.LoadInt32(&cl.activeJobs)
	total := atomic.LoadInt64(&cl.totalJobs)
	rejected := atomic.LoadInt64(&cl.rejectedJobs)
	max := atomic.LoadInt64(&cl.maxActiveJobs)
	utilization := float64(0)
	if max > 0 {
		utilization = float64(active) / float64(max) * 100
	}
	return ConcurrencyStats{
		MaxActiveJobs: int(max),
		ActiveJobs:    active,
		WaitingJobs:   atomic.LoadInt32(&cl.waitingJobs),
		TotalJobs:     total,
		RejectedJobs:  rejected,
		Utilization:   utilization,
	}
}

// CanAcceptJob is a best-effort non-blocking check. Returns true if a
// slot is currently free. May become stale immediately on return; use
// Acquire for guaranteed admission. PR-3.5 (post-review fix):
// priority no longer influences admission decisions. Callers that
// need priority semantics should layer their own admission gate
// outside this limiter.
func (cl *ConcurrencyLimiter) CanAcceptJob(priority int) bool {
	_ = priority
	current := atomic.LoadInt32(&cl.activeJobs)
	max := atomic.LoadInt64(&cl.maxActiveJobs)
	return int64(current) < max
}

// ActiveJobCount returns the current number of active jobs.
func (cl *ConcurrencyLimiter) ActiveJobCount() int32 {
	return atomic.LoadInt32(&cl.activeJobs)
}

// WaitingJobCount returns the current number of waiting jobs.
func (cl *ConcurrencyLimiter) WaitingJobCount() int32 {
	return atomic.LoadInt32(&cl.waitingJobs)
}

// Start is a no-op for the resizable cond-based limiter — kept for
// backwards-compat with prior callers (worker.go calls
// w.concurrencyLimiter.Start(ctx)). The cond wakes on Release / Stop
// / SetMaxActiveJobs automatically; no separate start goroutine needed.
func (cl *ConcurrencyLimiter) Start(_ context.Context) {
	// intentional no-op
}

// Stop stops the concurrency limiter and unblocks all waiters with
// an error so their Acquire returns.
func (cl *ConcurrencyLimiter) Stop() {
	cl.stopOnce.Do(func() {
		close(cl.stopChan)
		cl.mu.Lock()
		cl.cond.Broadcast()
		cl.mu.Unlock()
	})
}

// String returns a string representation of the limiter state.
func (cl *ConcurrencyLimiter) String() string {
	stats := cl.Stats()
	return fmt.Sprintf("ConcurrencyLimiter{max=%d, active=%d, waiting=%d, total=%d, rejected=%d, utilization=%.1f%%}",
		stats.MaxActiveJobs, stats.ActiveJobs, stats.WaitingJobs,
		stats.TotalJobs, stats.RejectedJobs, stats.Utilization)
}
