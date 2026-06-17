// Package worker provides the core worker orchestration logic.
// This file serves as the thin orchestrator that coordinates the worker modules.
package worker

import (
	"context"
	"math/rand"
	"time"

	"velox-worker-agent/pkg/logger"
)

// Start begins the worker's main loop with automatic re-registration on failure.
func (w *Worker) Start(ctx context.Context) error {
	logger.LogStartup(w.config.WorkerID, w.version, w.config.MasterURL)
	w.logger.Debug("Work Directory: %s", w.config.WorkDir)

	w.concurrencyLimiter.Start(ctx)
	w.logger.Info("[CONCURRENCY] Started with max_active_jobs=%d", w.config.MaxActiveJobs)

	// Connection state machine with automatic backoff
	backoff := registrationInitialBackoff

	for !w.IsStopped() {
		w.setConnState(ConnConnecting)
		w.connFailureCount = 0

		if err := w.register(ctx); err != nil {
			w.connFailureCount++
			w.logger.Warn("[CONNECT] Registration failed (attempt %d): %v", w.connFailureCount, err)
			w.setConnState(ConnDisconnected)

			// Exponential backoff with jitter
			jitter := time.Duration(rand.Float64() * float64(backoff) * 0.25)
			sleepDuration := backoff + jitter
			w.logger.Info("[CONNECT] Backing off for %v before retry", sleepDuration.Round(time.Millisecond))

			select {
			case <-time.After(sleepDuration):
				backoff = time.Duration(float64(backoff) * registrationBackoffMult)
				if backoff > registrationMaxBackoff {
					backoff = registrationMaxBackoff
				}
				continue
			case <-w.stopChan:
				w.logger.Info("Worker stopping during backoff")
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Registration succeeded — reset backoff
		backoff = registrationInitialBackoff
		w.setConnState(ConnReady)
		w.logger.Info("[CONNECT] Registration successful — running session")

		// Run session: start all loops, manage lifecycle
		w.runSession(ctx)

		// Session ended — either through stop or disconnect
		if w.IsStopped() {
			break
		}
		w.logger.Warn("[SESSION] Session ended — will reconnect")
	}

	w.setConnState(ConnDisconnected)
	w.logger.Info("Worker stopped")
	return nil
}

// runSession starts all communication loops and waits for shutdown or disconnect.
func (w *Worker) runSession(ctx context.Context) {
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start heartbeat goroutine
	w.wg.Add(1)
	go w.heartbeatLoop(sessionCtx)

	// Start dedicated lease renewal goroutine
	w.wg.Add(1)
	go w.leaseRenewLoop(sessionCtx)

	// Start job polling goroutine
	w.wg.Add(1)
	go w.jobLoop(sessionCtx)

	// Start command polling goroutine
	w.wg.Add(1)
	go w.commandLoop(sessionCtx)

	// Wait for stop signal or session disconnect
	select {
	case <-w.stopChan:
		w.logger.Info("Worker stopping...")
		w.setStatus(StatusStopped)
		w.setConnState(ConnDraining)
	case <-ctx.Done():
		w.logger.Warn("Parent context cancelled — draining")
		w.setConnState(ConnDraining)
	}

	// Cancel session context to stop all loops
	cancel()

	// Unregister from master
	unregisterCtx, unregisterCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer unregisterCancel()
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
}

// setConnState updates the connection state.
func (w *Worker) setConnState(state ConnectionState) {
	w.connStateMu.Lock()
	defer w.connStateMu.Unlock()
	oldState := w.connState
	w.connState = state
	w.logger.Debug("[CONN] State transition: %s -> %s", oldState, state)
}

// ConnState returns the current connection state.
func (w *Worker) ConnState() ConnectionState {
	w.connStateMu.RLock()
	defer w.connStateMu.RUnlock()
	return w.connState
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
