package worker

// worker_lifecycle.go: worker lifecycle — entrypoint, shutdown,
// session wrapper, and connection-level error classifier. The
// "main loop" of the agent (Start → runSession → receive loop →
// runSession returns) lives here; everything else (per-message
// dispatch, message decoding, artifact-ack registry) is owned by
// worker_claimloop.go + worker_artifacts.go. Registration metadata
// (Hello payload, capabilities) lives in worker_registration.go.
// Extracted from worker.go (commit <prior> → next).

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/bootstrap"
	"velox-worker-agent/pkg/logger"
)

// Start begins the worker's main loop with automatic re-registration on failure.
// Creates a fresh transport instance per session attempt (reconnect P0 fix).
//
// RW-PROD-003 §3 A6 defence-in-depth: the very first thing Start does is
// call bootstrap.HardGate(). The composition root (cmd/velox-worker-agent/main.go)
// already wired bootstrap.Run SYNCHRONOUSLY between the C++ pipeline
// construction and the executor wiring (RW-PROD-003 A5), so by the time
// Start is called in production the gate is already true. This check
// exists to catch FUTURE refactors that might reorder the composition
// root — e.g. someone calls worker.New(...).Start() directly from a
// test harness or a future --doctor entry-point. In that case the gate
// is false and Start returns with a "bootstrap_not_run" error BEFORE
// touching the transport, the registry, or the heartbeat loop. The
// worker is then correctly seen as `registered=false` from the master's
// selector because no Hello message is ever produced.
func (w *Worker) Start(ctx context.Context) error {
	if err := bootstrap.HardGate(); err != nil {
		w.logger.Error("[START_GUARD] refusing to start: %v", err)
		return fmt.Errorf("worker.Start precondition: %w", err)
	}
	logger.LogStartup(w.config.WorkerID, w.version, w.config.MasterURL)
	w.logger.Debug("Work Directory: %s", w.config.WorkerDir)

	w.concurrencyLimiter.Start(ctx)
	w.logger.Info("[CONCURRENCY] Started with max_active_jobs=%d", w.config.MaxActiveJobs)

	// PR-3.5: surface empty executor registry early. WithRegistry(empty)
	// is the supported default; operators must see this on the wire
	// before deciding the worker is broken or PR-3.6 hasn't shipped.
	if w.executorRegistry != nil && len(w.executorRegistry.Descriptors()) == 0 {
		w.logger.Warn("[STARTUP] executor registry is empty — ZERO executors will be advertised to master until scene.composite.v1 is registered (PR-3.6)")
	}

	// Connection state machine with automatic backoff
	backoff := registrationInitialBackoff

	for !w.IsStopped() {
		// P0 reconnect fix: create a fresh transport each session attempt.
		// After Close(), transports are not reusable (channels + sync.Once).
		w.transport = w.newTransport()
		if w.transport == nil {
			w.connFailureCount++
			w.logger.Warn("[CONNECT] Transport factory returned nil — backing off")
			w.setConnState(ConnDisconnected)
			telemetry.SetHealthRegistered(false)
			jitter := time.Duration(rand.Float64() * float64(connectionRetryBackoff) * 0.3)
			sleepDuration := connectionRetryBackoff + jitter
			select {
			case <-time.After(sleepDuration):
				continue
			case <-w.stopChan:
				w.logger.Info("Worker stopping during transport-factory backoff")
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		w.setConnState(ConnConnecting)
		w.connFailureCount = 0

		hello := w.buildHello()
		if err := w.transport.Connect(ctx, hello); err != nil {
			w.connFailureCount++

			w.logger.Warn("[CONNECT] Registration failed (attempt %d): %v", w.connFailureCount, err)
			w.setConnState(ConnDisconnected)
			// RW-PROD-004 A4: mirror ConnReady on every ConnDisconnected
			// transition so /health/ready drops `not_registered` once the
			// session is re-established. MarkRegistered queues an
			// UpdateReady copy-and-store under the process-global atomic.Pointer.
			telemetry.SetHealthRegistered(false)
			_ = w.transport.Close()

			// Use short fixed retry for connection-level errors (reset, refused,
			// transport unavailable) — the server may just be restarting.
			// Exponential backoff is reserved for application-level errors
			// (credential mismatch, protocol version, TLS).
			var sleepDuration time.Duration
			if isConnectionLevelError(err) {
				jitter := time.Duration(rand.Float64() * float64(connectionRetryBackoff) * 0.3)
				sleepDuration = connectionRetryBackoff + jitter
				w.logger.Info("[CONNECT] Connection-level error, retrying in %v", sleepDuration.Round(time.Millisecond))
			} else {
				jitter := time.Duration(rand.Float64() * float64(backoff) * 0.25)
				sleepDuration = backoff + jitter
				w.logger.Info("[CONNECT] Backing off for %v before retry", sleepDuration.Round(time.Millisecond))
			}

			select {
			case <-time.After(sleepDuration):
				// Only grow backoff for non-connection errors
				if !isConnectionLevelError(err) {
					backoff = time.Duration(float64(backoff) * registrationBackoffMult)
					if backoff > registrationMaxBackoff {
						backoff = registrationMaxBackoff
					}
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
		telemetry.SetHealthRegistered(true)
		// RW-PROD-004 A4: MarkRegistered(true) is already chained via the
		// legacy SetHealthRegistered setter. The explicit call here is
		// defensive: if a future refactor decouples the legacy flag from
		// the canonical ready snapshot, we still want the ready flip to
		// happen at the ConnReady instant.
		w.logger.Info("[CONNECT] Registration successful — running session")

		// Run session: start all loops, manage lifecycle
		sessionEnded := w.runSession(ctx)

		_ = w.transport.Close()

		// Session ended — either through stop or disconnect
		if w.IsStopped() {
			break
		}
		if sessionEnded {
			w.logger.Warn("[SESSION] Session ended — will reconnect")
		} else {
			w.logger.Info("[SESSION] Session ended cleanly")
		}
	}

	w.setConnState(ConnDisconnected)
	// RW-PROD-004 A4: final teardown clears both halves of the readiness
	// taxonomy. MarkRegistered(false) drops the `not_registered` reading
	// (it should already be false at this point, but the explicit call is
	// defensive against future refactors that shift the order). MarkDrainMode
	// is left as-is: drain_mode is sticky across reconnects (operators
	// want to see it stay true until the worker exits), and Stop is the
	// final exit point — clearer to drop the flag explicitly here too so a
	// fresh process after a restart starts from a clean ready slate.
	telemetry.SetHealthRegistered(false)
	telemetry.MarkDrainMode(false)
	w.logger.Info("Worker stopped")
	return nil
}

// runSession starts all communication loops and returns true if the session
// ended due to disconnect (should reconnect), false if stopped gracefully.
func (w *Worker) runSession(ctx context.Context) bool {
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	recvCh, errCh, err := w.transport.Receive(sessionCtx)
	if err != nil {
		w.logger.Error("Failed to start receive channel: %v", err)
		return false
	}

	w.wg.Add(1)
	go w.heartbeatLoop(sessionCtx)

	w.wg.Add(1)
	go w.leaseRenewLoop(sessionCtx)

	// PR-3.6 / F4: kick the resource-sampler loop under the session
	// context. Uses NewResourceSampler-registered 5s tick + 3-tick
	// emit cadence (heartbeat.resources is the only consumer of
	// Latest(); without this loop it would stay nil forever). On
	// sessionCtx cancel, the loop exits and Sample() returns any
	// partially-built snapshot would be discarded — acceptable
	// because the next session restarts sampling.
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		if err := w.sampler.Run(sessionCtx); err != nil && !errors.Is(err, context.Canceled) {
			w.logger.Warn("[SAMPLER] resource sampler loop exited: %v", err)
		}
	}()

	w.wg.Add(1)
	go w.receiveLoop(sessionCtx, recvCh)

	w.startPersistenceLoop(sessionCtx)

	sessionEnded := false
	select {
	case <-w.stopChan:
		w.logger.Info("Worker stopping...")
		w.setStatus(StatusStopped)
		w.setConnState(ConnDraining)
	case <-ctx.Done():
		w.logger.Warn("Parent context cancelled — draining")
		w.setConnState(ConnDraining)
	case streamErr, ok := <-errCh:
		if ok {
			w.logger.Warn("[SESSION] Transport error — session ended: %v", streamErr)
		} else {
			w.logger.Info("[SESSION] Transport closed cleanly")
		}
		w.setConnState(ConnDisconnected)
		// RW-PROD-004 A4 (BLOCKER round-2 fix): the third ConnDisconnected
		// site (transport-error / clean channel close) flips the readiness
		// registered flag to false so /health/ready reports reasons=[not_registered]
		// throughout the backoff-and-reconnect window. Without this hook the
		// readiness snapshot stayed "true" between sessions even though no
		// Hello+HelloAck roundtrip has been acknowledged yet.
		telemetry.SetHealthRegistered(false)
		sessionEnded = true
	}

	cancel()

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

	return sessionEnded
}

// Stop signals the worker to stop gracefully.
// This method is idempotent - calling it multiple times has no additional effect.
//
// PR-2 (fix/canonical-attempt-identity): drain pendingTasks
// on Stop so the next session starts with an empty map.
// Without this, an offer->stop cycle would leak entries across restarts and
// the next session would carry orphaned canonical (attempt_id, lease_id)
// tuples whose master-side TaskAttempts are already in a terminal state.
func (w *Worker) Stop() {
	w.stopOnce.Do(func() {
		w.logger.Info("Stop requested")
		close(w.stopChan)
		w.stopped.Store(true)
		// Drain pendingTasks on Stop. Entries here correspond to offers
		// the worker accepted but never received a LeaseGranted for
		// (worker died mid-flight, master restarted, etc). Master's lease
		// reaper handles the stranded PENDING / RUNNING TaskAttempts on
		// the master side; the local map is safe to clear.
		w.pendingTasksMu.Lock()
		clearedTask := len(w.pendingTasks)
		w.pendingTasks = make(map[string]*PendingTaskExecution)
		w.pendingTasksMu.Unlock()
		// PR-2 followup: drain activeTaskLeases on Stop so the next
		// session starts empty and any lease the master has already
		// marked TIMED_OUT cannot keep driving phantom renewals.
		w.activeTaskLeasesMu.Lock()
		clearedTaskLeases := len(w.activeTaskLeases)
		w.activeTaskLeases = make(map[string]*ActiveTaskLease)
		w.activeTaskLeasesMu.Unlock()
		if clearedTask > 0 || clearedTaskLeases > 0 {
			w.logger.Info("[STOP] Drained pending entries: task=%d task_leases=%d",
				clearedTask, clearedTaskLeases)
		}
		// Artifact Commit Protocol v1: drain the typed reply
		// dispatcher so no goroutine is left blocked on a channel
		// after Stop. Channels have buffered capacity 1 and the
		// pipeline reads them via waitForArtifactAck, which is
		// bound to the supplied ctx; ctx-canceled reads return
		// immediately. This drain is a belt-and-suspenders cleanup
		// for stragglers (e.g. dispatcher registered but the
		// pipeline never read it).
		w.drainPendingArtifactAcks()
	})
}

// isConnectionLevelError returns true when the error is a transient
// connection-level failure (reset, refused, transport unavailable).
// These typically occur when the server is restarting and will recover
// in seconds. Application-level errors (credential mismatch, protocol
// version, TLS) return false and should use exponential backoff.
func isConnectionLevelError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	lower := strings.ToLower(msg)

	connectionPatterns := []string{
		"connection refused",
		"connection reset by peer",
		"no route to host",
		"network is unreachable",
		"transport is closing",
		"broken pipe",
		"use of closed network connection",
		"i/o timeout",
	}
	for _, p := range connectionPatterns {
		if strings.Contains(lower, p) {
			return true
		}
	}

	if strings.Contains(lower, "unavailable") {
		return true
	}

	return false
}
