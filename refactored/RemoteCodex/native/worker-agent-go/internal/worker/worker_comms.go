// Package worker provides communication and heartbeat logic for the worker agent.
package worker

import (
	"context"
	"os"
	"time"

	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/logger"
)

// Heartbeat intervals based on worker status
const (
	heartbeatIntervalIdle      = 60 * time.Second // Idle: less frequent
	heartbeatIntervalBusy      = 15 * time.Second // Busy: more frequent for progress updates
	heartbeatIntervalError     = 10 * time.Second // Error: rapid recovery attempts
	heartbeatMaxBackoff        = 5 * time.Minute  // Maximum backoff interval
	heartbeatBackoffMultiplier = 2.0              // Backoff multiplier
)

// register registers the worker with the master server.
func (w *Worker) register(ctx context.Context) error {
	hostname, _ := os.Hostname()

	info := &api.WorkerInfo{
		WorkerID:   w.config.WorkerID,
		WorkerName: w.config.WorkerName,
		Capabilities: map[string]bool{
			"video_render":     true,
			"audio_processing": true,
			"image_processing": true,
		},
		Hostname:      hostname,
		IP:            "", // Could be populated from network interface
		Version:       w.version,
		CodeVersion:   w.version,
		BundleVersion: w.config.BundleVersion,
	}

	w.logger.Debug("Registering with master at %s", w.config.MasterURL)
	if err := w.apiClient.RegisterWorker(ctx, info); err != nil {
		logger.LogRegisterFailed(w.config.WorkerID, w.config.MasterURL, err)
		return err
	}

	logger.LogRegisterSuccess(w.config.WorkerID, w.config.MasterURL)

	// Log whether we received an auth token for future requests
	token := w.apiClient.AuthToken()
	if token != "" {
		w.logger.Debug("[AUTH] Auth token received during registration (length: %d)", len(token))
	} else {
		w.logger.Debug("[AUTH] No auth token received — continuing without token (tokenless requests are allowed)")
	}

	return nil
}

// unregister unregisters the worker from the master server.
func (w *Worker) unregister(ctx context.Context) error {
	w.logger.Debug("Unregistering from master...")
	return w.apiClient.UnregisterWorker(ctx, w.config.WorkerID)
}

// getHeartbeatInterval returns the appropriate heartbeat interval based on worker status.
func (w *Worker) getHeartbeatInterval() time.Duration {
	w.mu.RLock()
	defer w.mu.RUnlock()

	switch w.status {
	case StatusBusy:
		return heartbeatIntervalBusy
	case StatusError:
		return heartbeatIntervalError
	default:
		return heartbeatIntervalIdle
	}
}

// heartbeatLoop sends periodic heartbeats to the master with adaptive intervals.
func (w *Worker) heartbeatLoop(ctx context.Context) {
	defer w.wg.Done()

	consecutiveErrors := 0
	maxConsecutiveErrors := 5
	currentInterval := w.getHeartbeatInterval()

	ticker := time.NewTicker(currentInterval)
	defer ticker.Stop()

	// Send initial heartbeat immediately
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
		case <-ticker.C:
			// Check if status changed and adjust interval
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

				// Apply exponential backoff on errors
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
				// Reset on success
				if consecutiveErrors > 0 {
					logger.LogHeartbeatRecover(w.config.WorkerID, consecutiveErrors)
				}
				consecutiveErrors = 0

				// Reset to status-based interval
				newInterval := w.getHeartbeatInterval()
				if newInterval != currentInterval {
					currentInterval = newInterval
					ticker.Reset(currentInterval)
				}
			}
		}
	}
}

// getStatus returns the current worker status (thread-safe).
func (w *Worker) getStatus() Status {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.status
}

// sendHeartbeat sends a single heartbeat to the master.
func (w *Worker) sendHeartbeat(ctx context.Context) error {
	w.mu.RLock()
	status := w.status
	jobID := ""
	if w.currentJob != nil {
		jobID = w.currentJob.JobID
	}
	w.mu.RUnlock()

	recentLogs, recentErrors := w.recentLogs.Snapshot(300, 100)
	extra := map[string]interface{}{}
	if len(recentLogs) > 0 {
		extra["recent_logs"] = recentLogs
		extra["recent_logs_count"] = len(recentLogs)
	}
	if len(recentErrors) > 0 {
		extra["recent_errors"] = recentErrors
		extra["recent_errors_count"] = len(recentErrors)
	}
	extra["worker_status"] = string(status)
	extra["worker_id"] = w.config.WorkerID
	extra["worker_name"] = w.config.WorkerName
	extra["code_version"] = w.version
	extra["bundle_version"] = w.config.BundleVersion
	extra["jobs_completed"] = w.jobsCompleted.Load()
	extra["jobs_failed"] = w.jobsFailed.Load()
	if w.currentJob != nil {
		extra["current_job"] = map[string]interface{}{
			"job_id":       w.currentJob.JobID,
			"job_run_id":   w.currentJob.JobRunID,
			"run_id":       w.currentJob.JobRunID,
			"job_type":     w.currentJob.JobType,
			"priority":     w.currentJob.Priority,
			"timeout_secs": w.currentJob.TimeoutSecs,
		}
	}

	payload := &api.HeartbeatPayload{
		WorkerID:      w.config.WorkerID,
		WorkerName:    w.config.WorkerName,
		Status:        string(status),
		JobID:         jobID,
		CurrentJob:    jobID,
		CodeVersion:   w.version,
		BundleVersion: w.config.BundleVersion,
		Extra:         extra,
	}

	return w.apiClient.SendHeartbeat(ctx, payload)
}

// calculateBackoff returns the next backoff interval capped at heartbeatMaxBackoff.
func (w *Worker) calculateBackoff(current time.Duration) time.Duration {
	next := time.Duration(float64(current) * heartbeatBackoffMultiplier)
	if next > heartbeatMaxBackoff {
		return heartbeatMaxBackoff
	}
	return next
}

