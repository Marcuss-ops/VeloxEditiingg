// Package concurrency provides semaphore-based concurrency limiting for job execution.
//
// This implements Phase 1 deliverable: worker policy (1 VPS = 1 main job + 8-core concurrency).
package concurrency

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// ConcurrencyLimiter controls the number of concurrent jobs a worker can execute.
type ConcurrencyLimiter struct {
	mu sync.Mutex

	// Configuration
	maxActiveJobs int

	// State
	activeJobs   int32
	waitingJobs  int32
	totalJobs    int64
	rejectedJobs int64

	// Channels
	semaphore chan struct{}
	waitQueue chan *jobWaiter

	// Lifecycle
	stopChan chan struct{}
	stopOnce sync.Once
}

// jobWaiter represents a job waiting for execution slot.
type jobWaiter struct {
	ctx      context.Context
	jobID    string
	priority int
	ready    chan struct{}
}

// NewConcurrencyLimiter creates a new concurrency limiter.
func NewConcurrencyLimiter(maxActiveJobs int) *ConcurrencyLimiter {
	if maxActiveJobs <= 0 {
		maxActiveJobs = 1 // Default: 1 main job per VPS
	}

	return &ConcurrencyLimiter{
		maxActiveJobs: maxActiveJobs,
		semaphore:     make(chan struct{}, maxActiveJobs),
		waitQueue:     make(chan *jobWaiter, 100), // Buffer for waiting jobs
		stopChan:      make(chan struct{}),
	}
}

// Acquire attempts to acquire a slot for job execution.
// Returns true if slot acquired, false if rejected.
func (cl *ConcurrencyLimiter) Acquire(ctx context.Context, jobID string, priority int) error {
	// Check if we can acquire immediately
	select {
	case cl.semaphore <- struct{}{}:
		// Slot acquired
		atomic.AddInt32(&cl.activeJobs, 1)
		atomic.AddInt64(&cl.totalJobs, 1)
		return nil
	default:
		// No slot available, check if we should queue or reject
		if cl.shouldReject(priority) {
			atomic.AddInt64(&cl.rejectedJobs, 1)
			return fmt.Errorf("concurrency limit reached: max_active_jobs=%d, active=%d",
				cl.maxActiveJobs, atomic.LoadInt32(&cl.activeJobs))
		}

		// Queue the job
		return cl.queueAndWait(ctx, jobID, priority)
	}
}

// Release releases a slot after job execution.
func (cl *ConcurrencyLimiter) Release() {
	select {
	case <-cl.semaphore:
		atomic.AddInt32(&cl.activeJobs, -1)
	default:
		// Should not happen, but handle gracefully
	}
}

// MaxActiveJobs returns the configured maximum concurrent jobs.
func (cl *ConcurrencyLimiter) MaxActiveJobs() int {
	return cl.maxActiveJobs
}

// SetMaxActiveJobs updates the maximum concurrent jobs limit at runtime.
// The semaphore size is fixed at construction; this method updates the
// logical limit used by Acquire/CanAcceptJob for rejection decisions.
func (cl *ConcurrencyLimiter) SetMaxActiveJobs(max int) {
	if max <= 0 {
		max = 1
	}
	cl.mu.Lock()
	cl.maxActiveJobs = max
	cl.mu.Unlock()
}

// shouldReject determines if a job should be rejected based on priority.
func (cl *ConcurrencyLimiter) shouldReject(priority int) bool {
	// Critical jobs (priority 3) are never rejected
	if priority >= 3 {
		return false
	}

	// High priority jobs (priority 2) have higher acceptance threshold
	if priority >= 2 {
		return atomic.LoadInt32(&cl.activeJobs) >= int32(cl.maxActiveJobs*2)
	}

	// Normal and low priority jobs are rejected if at capacity
	return true
}

// queueAndWait queues a job and waits for a slot.
func (cl *ConcurrencyLimiter) queueAndWait(ctx context.Context, jobID string, priority int) error {
	waiter := &jobWaiter{
		ctx:      ctx,
		jobID:    jobID,
		priority: priority,
		ready:    make(chan struct{}),
	}

	atomic.AddInt32(&cl.waitingJobs, 1)
	defer atomic.AddInt32(&cl.waitingJobs, -1)

	// Try to queue
	select {
	case cl.waitQueue <- waiter:
		// Queued successfully
	case <-ctx.Done():
		return ctx.Err()
	case <-cl.stopChan:
		return fmt.Errorf("limiter stopped")
	}

	// Wait for slot
	select {
	case <-waiter.ready:
		// Slot acquired
		atomic.AddInt32(&cl.activeJobs, 1)
		atomic.AddInt64(&cl.totalJobs, 1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-cl.stopChan:
		return fmt.Errorf("limiter stopped")
	}
}

// Start begins processing the wait queue.
func (cl *ConcurrencyLimiter) Start(ctx context.Context) {
	go cl.processWaitQueue(ctx)
}

// processWaitQueue processes waiting jobs when slots become available.
func (cl *ConcurrencyLimiter) processWaitQueue(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// Drain remaining waiters
			for {
				select {
				case waiter := <-cl.waitQueue:
					close(waiter.ready) // unblock waiter
				default:
					return
				}
			}
		case <-cl.stopChan:
			// Drain remaining waiters
			for {
				select {
				case waiter := <-cl.waitQueue:
					close(waiter.ready) // unblock waiter
				default:
					return
				}
			}
		case waiter := <-cl.waitQueue:
			// Wait for a slot
			select {
			case cl.semaphore <- struct{}{}:
				// Slot acquired, notify waiter
				close(waiter.ready)
			case <-waiter.ctx.Done():
				// Waiter cancelled -- don't consume the slot
				continue
			case <-cl.stopChan:
				// Limiter stopped -- notify waiter to unblock
				close(waiter.ready)
				return
			}
		}
	}
}

// Stop stops the concurrency limiter.
func (cl *ConcurrencyLimiter) Stop() {
	cl.stopOnce.Do(func() {
		close(cl.stopChan)
	})
}

// Stats returns concurrency limiter statistics.
type ConcurrencyStats struct {
	MaxActiveJobs int     `json:"max_active_jobs"`
	ActiveJobs    int32   `json:"active_jobs"`
	WaitingJobs   int32   `json:"waiting_jobs"`
	TotalJobs     int64   `json:"total_jobs"`
	RejectedJobs  int64   `json:"rejected_jobs"`
	Utilization   float64 `json:"utilization_pct"`
}

// Stats returns current concurrency statistics.
func (cl *ConcurrencyLimiter) Stats() ConcurrencyStats {
	active := atomic.LoadInt32(&cl.activeJobs)
	total := atomic.LoadInt64(&cl.totalJobs)
	rejected := atomic.LoadInt64(&cl.rejectedJobs)

	utilization := float64(0)
	if cl.maxActiveJobs > 0 {
		utilization = float64(active) / float64(cl.maxActiveJobs) * 100
	}

	return ConcurrencyStats{
		MaxActiveJobs: cl.maxActiveJobs,
		ActiveJobs:    active,
		WaitingJobs:   atomic.LoadInt32(&cl.waitingJobs),
		TotalJobs:     total,
		RejectedJobs:  rejected,
		Utilization:   utilization,
	}
}

// CanAcceptJob returns true if the limiter can accept a new job.
// This is a non-blocking check that may become stale immediately after return.
// For guaranteed acceptance, use Acquire() instead.
func (cl *ConcurrencyLimiter) CanAcceptJob(priority int) bool {
	// Critical jobs always accepted
	if priority >= 3 {
		return true
	}

	// High priority jobs have higher acceptance threshold
	if priority >= 2 {
		// Check if we're below the high priority threshold (2x max)
		if atomic.LoadInt32(&cl.activeJobs) < int32(cl.maxActiveJobs*2) {
			return true
		}
	}

	// Check semaphore availability for normal priority
	// Note: This is a best-effort check -- the slot may be taken between
	// this check and the actual Acquire() call.
	select {
	case cl.semaphore <- struct{}{}:
		// Slot available, release it immediately
		<-cl.semaphore
		return true
	default:
		// No slot available
		return false
	}
}

// ActiveJobCount returns the current number of active jobs.
func (cl *ConcurrencyLimiter) ActiveJobCount() int32 {
	return atomic.LoadInt32(&cl.activeJobs)
}

// WaitingJobCount returns the current number of waiting jobs.
func (cl *ConcurrencyLimiter) WaitingJobCount() int32 {
	return atomic.LoadInt32(&cl.waitingJobs)
}

// String returns a string representation of the limiter state.
func (cl *ConcurrencyLimiter) String() string {
	stats := cl.Stats()
	return fmt.Sprintf("ConcurrencyLimiter{max=%d, active=%d, waiting=%d, total=%d, rejected=%d, utilization=%.1f%%}",
		stats.MaxActiveJobs, stats.ActiveJobs, stats.WaitingJobs,
		stats.TotalJobs, stats.RejectedJobs, stats.Utilization)
}
