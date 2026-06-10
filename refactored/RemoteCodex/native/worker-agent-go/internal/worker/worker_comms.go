// Package worker provides communication and heartbeat logic for the worker agent.
package worker

import (
	"context"
	"os"
	"time"

	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/logger"
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
		Hostname: hostname,
		IP:       "", // Could be populated from network interface
		Version:  w.version,
	}

	w.logger.Debug("Registering with master at %s", w.config.MasterURL)
	if err := w.apiClient.RegisterWorker(ctx, info); err != nil {
		logger.LogRegisterFailed(w.config.WorkerID, w.config.MasterURL, err)
		return err
	}

	logger.LogRegisterSuccess(w.config.WorkerID, w.config.MasterURL)
	return nil
}

// unregister unregisters the worker from the master server.
func (w *Worker) unregister(ctx context.Context) error {
	w.logger.Debug("Unregistering from master...")
	return w.apiClient.UnregisterWorker(ctx, w.config.WorkerID)
}

// heartbeatLoop sends periodic heartbeats to the master with exponential backoff on failures.
func (w *Worker) heartbeatLoop(ctx context.Context) {
	defer w.wg.Done()

	baseInterval := 30 * time.Second
	currentInterval := baseInterval
	consecutiveErrors := 0
	maxConsecutiveErrors := 5

	ticker := time.NewTicker(baseInterval)
	defer ticker.Stop()

	// Send initial heartbeat immediately
	if err := w.sendHeartbeat(ctx); err != nil {
		logger.LogHeartbeatFailed(w.config.WorkerID, err, 1, maxConsecutiveErrors)
	} else {
		logger.LogHeartbeatSuccess(w.config.WorkerID, string(StatusIdle))
	}

	for {
		select {
		case <-ctx.Done():
			w.logger.Debug("Heartbeat loop exiting (context done)")
			return
		case <-w.stopChan:
			w.logger.Debug("Heartbeat loop exiting (stop signal)")
			return
		case <-ticker.C:
			err := w.sendHeartbeat(ctx)
			if err != nil {
				consecutiveErrors++
				logger.LogHeartbeatFailed(w.config.WorkerID, err, consecutiveErrors, maxConsecutiveErrors)

				// Apply exponential backoff
				if consecutiveErrors >= maxConsecutiveErrors {
					currentInterval = w.calculateBackoff(currentInterval)
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
				currentInterval = baseInterval
				ticker.Reset(baseInterval)
			}
		}
	}
}

// calculateBackoff calculates the next backoff interval.
func (w *Worker) calculateBackoff(current time.Duration) time.Duration {
	next := time.Duration(float64(current) * w.heartbeatBackoff.multiplier)
	if next > w.heartbeatBackoff.maxInterval {
		next = w.heartbeatBackoff.maxInterval
	}
	return next
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
		WorkerID:   w.config.WorkerID,
		WorkerName: w.config.WorkerName,
		Status:     string(status),
		JobID:      jobID,
		CurrentJob: jobID,
		Extra:      extra,
	}

	return w.apiClient.SendHeartbeat(ctx, payload)
}

