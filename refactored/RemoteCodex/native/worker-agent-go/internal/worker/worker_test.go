// Package worker provides the core worker orchestration logic.
package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"velox-worker-agent/pkg/config"
)

// TestNewWorker tests worker creation with default config.
func TestNewWorker(t *testing.T) {
	cfg := &config.WorkerConfig{
		MasterURL:  "http://localhost:8000",
		WorkerID:   "test-worker-001",
		WorkerName: "test-worker",
		WorkDir:    "/tmp/velox",
		VenvPath:   "/tmp/velox/.venv",
		LogLevel:   "debug",
	}

	w := New(cfg, "test-version")

	if w == nil {
		t.Fatal("Expected non-nil worker")
	}

	if w.Status() != StatusIdle {
		t.Errorf("Expected initial status to be idle, got %s", w.Status())
	}

	if w.IsStopped() {
		t.Error("Expected worker to not be stopped initially")
	}

	if w.config.EnableCommandPolling {
		t.Error("Expected command polling to be disabled by default")
	}
}

// TestStopIdempotent tests that calling Stop multiple times doesn't panic.
func TestStopIdempotent(t *testing.T) {
	cfg := config.DefaultConfig("/tmp/velox")
	cfg.WorkerID = "test-worker-idempotent"

	w := New(cfg, "test-version")

	// Call Stop multiple times - should not panic
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.Stop()
		}()
	}
	wg.Wait()

	// Verify worker is stopped
	if !w.IsStopped() {
		t.Error("Expected worker to be stopped after Stop()")
	}

	// Call Stop again after goroutines finish - should be idempotent
	w.Stop()
	w.Stop() // Second call should be no-op

	if !w.IsStopped() {
		t.Error("Expected worker to remain stopped")
	}
}

// TestStatusTransitions tests valid status transitions.
func TestStatusTransitions(t *testing.T) {
	cfg := config.DefaultConfig("/tmp/velox")
	cfg.WorkerID = "test-worker-status"

	w := New(cfg, "test-version")

	// Initial state: Idle
	if w.Status() != StatusIdle {
		t.Errorf("Expected initial status idle, got %s", w.Status())
	}

	// Idle -> Busy (valid)
	if !w.canTransitionTo(StatusBusy) {
		t.Error("Expected idle->busy transition to be valid")
	}

	// Idle -> Error (invalid from idle)
	if w.canTransitionTo(StatusError) {
		t.Error("Expected idle->error transition to be invalid")
	}

	// Idle -> Stopped (valid)
	if !w.canTransitionTo(StatusStopped) {
		t.Error("Expected idle->stopped transition to be valid")
	}

	// Transition to busy
	w.setStatus(StatusBusy)

	// Busy -> Idle (valid)
	if !w.canTransitionTo(StatusIdle) {
		t.Error("Expected busy->idle transition to be valid")
	}

	// Busy -> Error (valid)
	if !w.canTransitionTo(StatusError) {
		t.Error("Expected busy->error transition to be valid")
	}

	// Transition to error
	w.setStatus(StatusError)

	// Error -> Idle (valid)
	if !w.canTransitionTo(StatusIdle) {
		t.Error("Expected error->idle transition to be valid")
	}

	// Transition to stopped
	w.setStatus(StatusStopped)

	// No transitions from stopped
	if w.canTransitionTo(StatusIdle) {
		t.Error("Expected stopped->idle transition to be invalid")
	}
	if w.canTransitionTo(StatusBusy) {
		t.Error("Expected stopped->busy transition to be invalid")
	}
}

// TestGracefulShutdown tests that shutdown waits for goroutines.
func TestGracefulShutdown(t *testing.T) {
	cfg := config.DefaultConfig("/tmp/velox")
	cfg.WorkerID = "test-worker-shutdown"
	cfg.MasterURL = "http://localhost:8000" // Non-existent master

	w := New(cfg, "test-version")

	// Create a context with timeout for the test
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start worker in a goroutine
	started := make(chan struct{})
	var startErr error
	go func() {
		close(started)
		startErr = w.Start(ctx)
	}()

	// Wait for worker to start
	<-started

	// Give worker time to initialize
	time.Sleep(100 * time.Millisecond)

	// Stop the worker
	w.Stop()

	// Verify worker is stopped
	if !w.IsStopped() {
		t.Error("Expected worker to be stopped")
	}

	// Cancel context to clean up
	cancel()

	_ = startErr // Worker may fail to register with non-existent master
}

// TestHeartbeatBackoff tests backoff calculation.
func TestHeartbeatBackoff(t *testing.T) {
	cfg := config.DefaultConfig("/tmp/velox")
	cfg.WorkerID = "test-worker-backoff"

	w := New(cfg, "test-version")

	// Test backoff calculation
	current := 30 * time.Second
	next := w.calculateBackoff(current)

	// Should double
	expected := 60 * time.Second
	if next != expected {
		t.Errorf("Expected backoff %v, got %v", expected, next)
	}

	// Test max cap
	current = 60 * time.Second
	next = w.calculateBackoff(current)
	if next != 60*time.Second {
		t.Errorf("Expected backoff to cap at max %v, got %v", 60*time.Second, next)
	}
}

// TestWorkerStatusThreadSafety tests concurrent status access.
func TestWorkerStatusThreadSafety(t *testing.T) {
	cfg := config.DefaultConfig("/tmp/velox")
	cfg.WorkerID = "test-worker-concurrent"

	w := New(cfg, "test-version")

	var wg sync.WaitGroup

	// Concurrent status reads
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = w.Status()
		}()
	}

	// Concurrent status writes
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			statuses := []Status{StatusIdle, StatusBusy, StatusError}
			w.setStatus(statuses[i%3])
		}(i)
	}

	// Concurrent IsStopped checks
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = w.IsStopped()
		}()
	}

	wg.Wait()
	// If we get here without race condition, test passes
}

// TestStopChanClosedOnce tests that stopChan is only closed once.
func TestStopChanClosedOnce(t *testing.T) {
	cfg := config.DefaultConfig("/tmp/velox")
	cfg.WorkerID = "test-worker-stopchan"

	w := New(cfg, "test-version")

	// First stop should close the channel
	w.Stop()

	// Verify channel is closed
	select {
	case <-w.stopChan:
		// Channel is closed, expected
	default:
		t.Error("Expected stopChan to be closed after Stop()")
	}

	// Second stop should not panic
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Stop() panicked on second call: %v", r)
			}
		}()
		w.Stop()
	}()
}

// BenchmarkStatusTransition benchmarks status transitions.
func BenchmarkStatusTransition(b *testing.B) {
	cfg := config.DefaultConfig("/tmp/velox")
	cfg.WorkerID = "bench-worker"

	w := New(cfg, "test-version")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.setStatus(StatusBusy)
		w.setStatus(StatusIdle)
	}
}

// BenchmarkConcurrentStatusRead benchmarks concurrent status reads.
func BenchmarkConcurrentStatusRead(b *testing.B) {
	cfg := config.DefaultConfig("/tmp/velox")
	cfg.WorkerID = "bench-worker"

	w := New(cfg, "test-version")

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = w.Status()
		}
	})
}
