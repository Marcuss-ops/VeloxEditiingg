package worker

import (
	"context"
	"time"

	"velox-worker-agent/internal/telemetry"
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

func waitHeartbeatFloor(ctx context.Context, stop <-chan struct{}, lastSent time.Time) bool {
	if lastSent.IsZero() {
		return true
	}
	wait := heartbeatWakeMinInterval - time.Since(lastSent)
	if wait <= 0 {
		return true
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	case <-stop:
		return false
	}
}

// heartbeatLoop sends periodic heartbeats to the master via
// w.sendHeartbeat (defined in heartbeat_payload.go). This file owns the
// loop orchestration — ticker, status-driven rescheduling, wake signal,
// and backoff on consecutive failures — but intentionally does NOT know
// about the protobuf shape of the heartbeat; that lives in
// heartbeat_payload.go.
// sendHeartbeatAtFloor applies the single traffic gate shared by the initial,
// wake-triggered, and ticker-triggered heartbeat paths. The timestamp is
// recorded before Send so failed attempts are throttled too: a transport error
// may still have reached the master, and an immediate retry would otherwise
// defeat the traffic bound.
func (w *Worker) sendHeartbeatAtFloor(ctx context.Context, lastSent *time.Time) (bool, error) {
	if !waitHeartbeatFloor(ctx, w.stopChan, *lastSent) {
		return false, ctx.Err()
	}
	*lastSent = time.Now()
	return true, w.sendHeartbeat(ctx)
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	w.heartbeatLoopWithInterval(ctx, 0)
}

// heartbeatLoopWithInterval is split out so tests can exercise ticker and wake
// traffic through the same loop without waiting for the production busy
// cadence. A zero interval selects the status-based production interval.
func (w *Worker) heartbeatLoopWithInterval(ctx context.Context, forcedInterval time.Duration) {
	defer w.wg.Done()

	consecutiveErrors := 0
	maxConsecutiveErrors := 5
	currentInterval := w.getHeartbeatInterval()
	if forcedInterval > 0 {
		currentInterval = forcedInterval
	}
	lastHeartbeatSentAt := time.Time{}

	ticker := time.NewTicker(currentInterval)
	defer ticker.Stop()

	if sent, err := w.sendHeartbeatAtFloor(ctx, &lastHeartbeatSentAt); !sent {
		return
	} else if err != nil {
		logger.LogHeartbeatFailed(w.config.WorkerID, err, 1, maxConsecutiveErrors)
	} else {
		logger.LogHeartbeatSuccess(w.config.WorkerID, string(StatusIdle))
	}

	lastStatus := w.Status()

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
			if sent, err := w.sendHeartbeatAtFloor(ctx, &lastHeartbeatSentAt); !sent {
				return
			} else if err != nil {
				consecutiveErrors++
				logger.LogHeartbeatFailed(w.config.WorkerID, err, consecutiveErrors, maxConsecutiveErrors)
			} else {
				consecutiveErrors = 0
			}
		case <-ticker.C:
			// Update NIC saturation EWMA and alert state before each heartbeat.
			if w.networkAdmissionController != nil {
				w.networkAdmissionController.UpdateSaturation()
				telemetry.MarkNetworkSaturationCritical(w.networkAdmissionController.IsCritical())
			}
			currentStatus := w.Status()
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

			sent, err := w.sendHeartbeatAtFloor(ctx, &lastHeartbeatSentAt)
			if !sent {
				return
			}
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
