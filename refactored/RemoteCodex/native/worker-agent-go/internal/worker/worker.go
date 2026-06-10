// Package worker provides the core worker orchestration logic.
// This file serves as the thin orchestrator that coordinates the worker modules.
package worker

import (
	"context"
	"fmt"
	"time"

	"velox-worker-agent/pkg/logger"
)

// Start begins the worker's main loop.
func (w *Worker) Start(ctx context.Context) error {
	// Use structured event for startup
	logger.LogStartup(w.config.WorkerID, w.version, w.config.MasterURL)
	w.logger.Debug("Work Directory: %s", w.config.WorkDir)

	// Phase 1: Start concurrency limiter wait queue processor
	w.concurrencyLimiter.Start(ctx)
	w.logger.Info("[CONCURRENCY] Started with max_active_jobs=%d", w.config.MaxActiveJobs)

	// Register with master
	if err := w.register(ctx); err != nil {
		return fmt.Errorf("failed to register with master: %w", err)
	}

	// Start heartbeat goroutine
	w.wg.Add(1)
	go w.heartbeatLoop(ctx)

	// Start job polling goroutine
	w.wg.Add(1)
	go w.jobLoop(ctx)

	w.logger.Info("[COMMANDS] Command polling disabled")

	// Wait for stop signal
	<-w.stopChan
	w.logger.Info("Worker stopping...")
	w.setStatus(StatusStopped)

	// Unregister from master
	unregisterCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.unregister(unregisterCtx); err != nil {
		w.logger.Warn("Failed to unregister from master: %v", err)
	}

	// Wait for goroutines to finish with timeout
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		w.logger.Info("All goroutines stopped cleanly")
	case <-time.After(30 * time.Second):
		w.logger.Warn("Timeout waiting for goroutines, forcing exit")
	}

	w.logger.Info("Worker stopped")

	return nil
}

// Stop signals the worker to stop gracefully.
// This method is idempotent - calling it multiple times has no additional effect.
func (w *Worker) Stop() {
	w.stopOnce.Do(func() {
		w.logger.Info("Stop requested")
		close(w.stopChan)
		w.stopped.Store(true)
	})
}
