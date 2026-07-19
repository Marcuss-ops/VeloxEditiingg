package worker

import (
	"context"
	"time"

	"velox-worker-agent/pkg/logger"
)

// wakeHeartbeat signals the heartbeat loop to send an immediate heartbeat.
// Safe when w.heartbeatWake is nil (no-op).
func (w *Worker) wakeHeartbeat() {
	if w.heartbeatWake == nil {
		return
	}
	select {
	case w.heartbeatWake <- struct{}{}:
	default:
	}
}

// heartbeatLoop sends periodic heartbeats to the master via
// w.sendHeartbeat (defined in heartbeat_payload.go). This file owns the
// loop orchestration — ticker, status-driven rescheduling, wake signal,
// and backoff on consecutive failures — but intentionally does NOT know
// about the protobuf shape of the heartbeat; that lives in
// heartbeat_payload.go.
func (w *Worker) heartbeatLoop(ctx context.Context) {
	defer w.wg.Done()

	consecutiveErrors := 0
	maxConsecutiveErrors := 5
	currentInterval := w.getHeartbeatInterval()

	ticker := time.NewTicker(currentInterval)
	defer ticker.Stop()

	if err := w.sendHeartbeat(ctx); err != nil {
		logger.LogHeartbeatFailed(w.config.WorkerID, err, 1, maxConsecutiveErrors)
	} else {
		logger.LogHeartbeatSuccess(w.config.WorkerID, string(StatusIdle))
	}

	lastStatus := w.getStatus()

	for {
		select {
		case <-ctx.Done():
			w.logger.Debug("Heartbeat loop exiting (context done)")
			return
		case <-w.stopChan:
			w.logger.Debug("Heartbeat loop exiting (stop signal)")
			return
		case <-w.heartbeatWake:
			currentInterval = w.getHeartbeatInterval()
			ticker.Reset(currentInterval)
			if err := w.sendHeartbeat(ctx); err != nil {
				logger.LogHeartbeatFailed(w.config.WorkerID, err, consecutiveErrors+1, maxConsecutiveErrors)
			} else {
				consecutiveErrors = 0
			}
		case <-ticker.C:
			currentStatus := w.getStatus()
			if currentStatus != lastStatus {
				newInterval := w.getHeartbeatInterval()
				if newInterval != currentInterval {
					w.logger.Debug("[HEARTBEAT] Status changed %s->%s, adjusting interval %v->%v",
						lastStatus, currentStatus, currentInterval, newInterval)
					currentInterval = newInterval
					ticker.Reset(currentInterval)
				}
				lastStatus = currentStatus
			}

			err := w.sendHeartbeat(ctx)
			if err != nil {
				consecutiveErrors++
				logger.LogHeartbeatFailed(w.config.WorkerID, err, consecutiveErrors, maxConsecutiveErrors)

				if consecutiveErrors >= maxConsecutiveErrors {
					currentInterval = time.Duration(float64(currentInterval) * heartbeatBackoffMultiplier)
					if currentInterval > heartbeatMaxBackoff {
						currentInterval = heartbeatMaxBackoff
					}
					w.logger.Warn("[HEARTBEAT_BACKOFF] Applying backoff, next heartbeat in %v",
						currentInterval)
					ticker.Reset(currentInterval)
				}
			} else {
				if consecutiveErrors > 0 {
					logger.LogHeartbeatRecover(w.config.WorkerID, consecutiveErrors)
				}
				consecutiveErrors = 0

				newInterval := w.getHeartbeatInterval()
				if newInterval != currentInterval {
					currentInterval = newInterval
					ticker.Reset(currentInterval)
				}
			}
		}
	}
}

// getStatus returns the current worker status, derived from activeJobs and error state.
// Kept here (heartbeat-internal helper) — single-call site is inside the loop.
func (w *Worker) getStatus() Status {
	return w.Status()
}
