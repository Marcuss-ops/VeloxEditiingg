package worker

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

	if cl.maxActiveJobs != 1 {
		t.Errorf("Expected maxActiveJobs=1, got %d", cl.maxActiveJobs)
	}
}

func TestConcurrencyLimiter_Acquire(t *testing.T) {
	cl := NewConcurrencyLimiter(1)
	defer cl.Stop()

	ctx := context.Background()

	// First acquire should succeed
	err := cl.Acquire(ctx, "job-1", 1)
	if err != nil {
		t.Errorf("First acquire failed: %v", err)
	}

	// Second acquire should fail (at capacity)
	err = cl.Acquire(ctx, "job-2", 1)
	if err == nil {
		t.Error("Second acquire should have failed")
	}

	// Release and try again
	cl.Release()
	err = cl.Acquire(ctx, "job-3", 1)
	if err != nil {
		t.Errorf("Acquire after release failed: %v", err)
	}
}

func TestConcurrencyLimiter_CriticalPriority(t *testing.T) {
	cl := NewConcurrencyLimiter(1)
	defer cl.Stop()

	ctx := context.Background()

	// Fill the limiter
	err := cl.Acquire(ctx, "job-1", 1)
	if err != nil {
		t.Errorf("First acquire failed: %v", err)
	}

	// Critical job should be accepted (bypass rejection)
	// Note: It will still wait for a slot, but won't be rejected
	canAccept := cl.CanAcceptJob(3)
	if !canAccept {
		t.Error("Critical job should be accepted (not rejected)")
	}

	// Release the first job
	cl.Release()

	// Now critical job should be able to acquire
	err = cl.Acquire(ctx, "job-critical", 3)
	if err != nil {
		t.Errorf("Critical job acquire failed: %v", err)
	}
}

func TestConcurrencyLimiter_HighPriority(t *testing.T) {
	cl := NewConcurrencyLimiter(1)
	defer cl.Stop()

	ctx := context.Background()

	// Fill the limiter
	err := cl.Acquire(ctx, "job-1", 1)
	if err != nil {
		t.Errorf("First acquire failed: %v", err)
	}

	// High priority job should be accepted (threshold is 2x max)
	// Note: It will still wait for a slot, but won't be rejected
	canAccept := cl.CanAcceptJob(2)
	if !canAccept {
		t.Error("High priority job should be accepted (not rejected)")
	}

	// Release the first job
	cl.Release()

	// Now high priority job should be able to acquire
	err = cl.Acquire(ctx, "job-high", 2)
	if err != nil {
		t.Errorf("High priority job acquire failed: %v", err)
	}
}

func TestConcurrencyLimiter_Stats(t *testing.T) {
	cl := NewConcurrencyLimiter(2)
	defer cl.Stop()

	ctx := context.Background()

	// Acquire two slots
	cl.Acquire(ctx, "job-1", 1)
	cl.Acquire(ctx, "job-2", 1)

	stats := cl.Stats()
	if stats.MaxActiveJobs != 2 {
		t.Errorf("Expected maxActiveJobs=2, got %d", stats.MaxActiveJobs)
	}
	if stats.ActiveJobs != 2 {
		t.Errorf("Expected activeJobs=2, got %d", stats.ActiveJobs)
	}
	if stats.TotalJobs != 2 {
		t.Errorf("Expected totalJobs=2, got %d", stats.TotalJobs)
	}
	if stats.Utilization != 100 {
		t.Errorf("Expected utilization=100, got %f", stats.Utilization)
	}
}

func TestConcurrencyLimiter_CanAcceptJob(t *testing.T) {
	cl := NewConcurrencyLimiter(1)
	defer cl.Stop()

	ctx := context.Background()

	// Should accept when empty
	if !cl.CanAcceptJob(1) {
		t.Error("Should accept job when empty")
	}

	// Fill the limiter
	cl.Acquire(ctx, "job-1", 1)

	// Should not accept normal priority
	if cl.CanAcceptJob(1) {
		t.Error("Should not accept normal priority when full")
	}

	// Should accept critical priority
	if !cl.CanAcceptJob(3) {
		t.Error("Should accept critical priority when full")
	}
}

func TestConcurrencyLimiter_ConcurrentAccess(t *testing.T) {
	cl := NewConcurrencyLimiter(4)
	defer cl.Stop()

	ctx := context.Background()
	var wg sync.WaitGroup
	var successCount int32
	var failCount int32

	// Try to acquire 10 jobs concurrently
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			err := cl.Acquire(ctx, "job", 1)
			if err != nil {
				atomic.AddInt32(&failCount, 1)
			} else {
				atomic.AddInt32(&successCount, 1)
				time.Sleep(10 * time.Millisecond)
				cl.Release()
			}
		}(i)
	}

	wg.Wait()

	// Should have some successes and some failures
	if successCount == 0 {
		t.Error("Expected some successful acquisitions")
	}
	if failCount == 0 {
		t.Error("Expected some failed acquisitions due to concurrency limit")
	}
}

func TestConcurrencyLimiter_ContextCancellation(t *testing.T) {
	cl := NewConcurrencyLimiter(1)
	defer cl.Stop()

	ctx, cancel := context.WithCancel(context.Background())

	// Fill the limiter
	cl.Acquire(ctx, "job-1", 1)

	// Try to acquire with cancelled context
	cancel()
	err := cl.Acquire(ctx, "job-2", 1)
	if err == nil {
		t.Error("Should fail with cancelled context")
	}
}

func TestConcurrencyLimiter_Stop(t *testing.T) {
	cl := NewConcurrencyLimiter(1)

	ctx := context.Background()
	cl.Acquire(ctx, "job-1", 1)

	// Stop should be idempotent
	cl.Stop()
	cl.Stop()

	// Should not panic
}