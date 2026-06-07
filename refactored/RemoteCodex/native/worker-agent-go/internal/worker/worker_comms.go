// Package worker provides communication and heartbeat logic for the worker agent.
package worker

import (
	"context"
	"fmt"
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

	w.logger.Debug("Registering with master at %s (api_mode: %s)", w.config.MasterURL, w.config.APIMode)
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
					w.logger.Warn("[HEARTBEAT_BACKOFF] Applying backoff, next heartbeat in %v (api_mode: %s)",
						currentInterval, w.config.APIMode)
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
	extra["api_mode"] = string(w.config.APIMode)
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

// commandLoop polls for commands from the master and processes them.
func (w *Worker) commandLoop(ctx context.Context) {
	defer w.wg.Done()

	pollInterval := 10 * time.Second
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	// Periodic cleanup of seenCommands (every 10 minutes)
	cleanupTicker := time.NewTicker(10 * time.Minute)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Debug("Command loop exiting (context done)")
			return
		case <-w.stopChan:
			w.logger.Debug("Command loop exiting (stop signal)")
			return
		case <-cleanupTicker.C:
			w.cleanupSeenCommands()
		case <-ticker.C:
			commands, err := w.apiClient.GetCommands(ctx, w.config.WorkerID)
			if err != nil {
				w.logger.Warn("Failed to poll for commands: %v", err)
				continue
			}

			for _, cmd := range commands {
				// Send command to job loop for processing
				select {
				case w.commandChan <- cmd:
					w.logger.Debug("Queued command: %s", cmd.Command)
				default:
					w.logger.Warn("Command channel full, dropping command: %s", cmd.Command)
				}
			}
		}
	}
}

// processCommand handles a command from the master.
func (w *Worker) processCommand(ctx context.Context, cmd api.WorkerCommand) {
	w.logger.Info("[COMMAND] Processing command: %s (timestamp: %s)", cmd.Command, cmd.Timestamp)

	if w.markCommandSeen(cmd) {
		w.logger.Warn("[COMMAND] Duplicate command skipped: %s (timestamp: %s)", cmd.Command, cmd.Timestamp)
		if ackErr := w.apiClient.AckCommand(ctx, w.config.WorkerID, cmd.Command); ackErr != nil {
			w.logger.Warn("[COMMAND] Failed to ack duplicate command %s: %v", cmd.Command, ackErr)
		}
		_ = w.apiClient.UpdateStatus(ctx, w.config.WorkerID, "command_completed", map[string]interface{}{
			"command":   cmd.Command,
			"duplicate": true,
			"timestamp": cmd.Timestamp,
		})
		return
	}

	var err error
	var details map[string]interface{}

	switch cmd.Command {
	case "restart_worker":
		details, err = w.handleRestartCommand(ctx, cmd)
	case "update_code":
		details, err = w.handleUpdateCodeCommand(ctx, cmd)
	case "run_smoke_job":
		details, err = w.handleSmokeJobCommand(ctx, cmd)
	case "drain":
		details, err = w.handleDrainCommand(ctx, cmd)
	case "undrain":
		details, err = w.handleUndrainCommand(ctx, cmd)
	default:
		w.logger.Warn("[COMMAND] Unknown command: %s", cmd.Command)
		err = fmt.Errorf("unknown command: %s", cmd.Command)
	}

	// Acknowledge command
	ackErr := w.apiClient.AckCommand(ctx, w.config.WorkerID, cmd.Command)
	if ackErr != nil {
		w.logger.Warn("[COMMAND] Failed to ack command %s: %v", cmd.Command, ackErr)
	} else {
		w.logger.Info("[COMMAND] Acknowledged command: %s", cmd.Command)
	}

	// Report status update
	if err != nil {
		w.logger.Error("[COMMAND] Command %s failed: %v", cmd.Command, err)
		w.apiClient.UpdateStatus(ctx, w.config.WorkerID, "command_failed", map[string]interface{}{
			"command": cmd.Command,
			"error":   err.Error(),
		})
	} else {
		w.apiClient.UpdateStatus(ctx, w.config.WorkerID, "command_completed", map[string]interface{}{
			"command": cmd.Command,
			"details": details,
		})
	}
}

// handleRestartCommand handles the restart_worker command.
func (w *Worker) handleRestartCommand(ctx context.Context, cmd api.WorkerCommand) (map[string]interface{}, error) {
	w.logger.Info("[COMMAND] Restart worker command received")

	// Check if we have a current job
	w.mu.RLock()
	hasJob := w.currentJob != nil
	w.mu.RUnlock()

	if hasJob {
		w.logger.Warn("[COMMAND] Cannot restart: worker has active job")
		return nil, fmt.Errorf("cannot restart: worker has active job")
	}

	// Schedule restart after a brief delay to allow ack to be sent
	go func() {
		time.Sleep(2 * time.Second)
		w.logger.Info("[COMMAND] Executing restart...")
		w.Stop()

		// Trigger self-restart via systemd or process manager
		// This is a graceful stop; the process manager should restart the service
		if w.exitFunc != nil {
			w.exitFunc(0)
		}
	}()

	return map[string]interface{}{
		"action":   "restart_scheduled",
		"delay_ms": 2000,
	}, nil
}

// handleUpdateCodeCommand handles the update_code command.
func (w *Worker) handleUpdateCodeCommand(ctx context.Context, cmd api.WorkerCommand) (map[string]interface{}, error) {
	w.logger.Info("[COMMAND] Update code command received")

	// Extract version/URL from payload
	var downloadURL, version string
	if cmd.Payload != nil {
		if u, ok := cmd.Payload["download_url"].(string); ok {
			downloadURL = u
		}
		if v, ok := cmd.Payload["version"].(string); ok {
			version = v
		}
	}

	if downloadURL == "" {
		return nil, fmt.Errorf("update_code: missing download_url in payload")
	}

	// Check if we have a current job
	w.mu.RLock()
	hasJob := w.currentJob != nil
	w.mu.RUnlock()

	if hasJob {
		w.logger.Warn("[COMMAND] Cannot update: worker has active job")
		return nil, fmt.Errorf("cannot update: worker has active job")
	}

	// Set drain mode during update
	w.drainMode.Store(true)

	// Download and apply update
	// For now, log the action; actual implementation would download and extract
	w.logger.Info("[COMMAND] Would download update from: %s (version: %s)", downloadURL, version)

	// Schedule update restart
	go func() {
		time.Sleep(2 * time.Second)
		w.logger.Info("[COMMAND] Executing update restart...")
		w.Stop()
		if w.exitFunc != nil {
			w.exitFunc(0)
		}
	}()

	return map[string]interface{}{
		"action":        "update_scheduled",
		"version":       version,
		"download_url":  downloadURL,
		"drain_enabled": true,
	}, nil
}

// handleSmokeJobCommand executes a lightweight health-check job and reports
// success/failure back to master via command status update.
func (w *Worker) handleSmokeJobCommand(ctx context.Context, cmd api.WorkerCommand) (map[string]interface{}, error) {
	w.logger.Info("[COMMAND] Smoke job command received")

	jobType := "health_check"
	timeoutSecs := 60
	simulateError := false
	jobID := fmt.Sprintf("smoke-%d", time.Now().UTC().Unix())

	if cmd.Payload != nil {
		if v, ok := cmd.Payload["job_type"].(string); ok && v != "" {
			jobType = v
		}
		if v, ok := cmd.Payload["timeout_secs"].(float64); ok && int(v) > 0 {
			timeoutSecs = int(v)
		}
		if v, ok := cmd.Payload["simulate_error"].(bool); ok {
			simulateError = v
		}
		if v, ok := cmd.Payload["job_id"].(string); ok && v != "" {
			jobID = v
		}
	}

	if simulateError {
		return nil, fmt.Errorf("smoke job forced error (simulate_error=true)")
	}

	smokeJob := &api.Job{
		JobID:       jobID,
		JobType:     jobType,
		Priority:    0,
		TimeoutSecs: timeoutSecs,
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
	}

	output, err := w.runJobTask(ctx, smokeJob)
	if err != nil {
		return nil, fmt.Errorf("smoke job failed: %w", err)
	}

	return map[string]interface{}{
		"smoke_job_id":   smokeJob.JobID,
		"smoke_job_type": smokeJob.JobType,
		"smoke_status":   "success",
		"output":         output,
	}, nil
}

// handleDrainCommand handles the drain command.
func (w *Worker) handleDrainCommand(ctx context.Context, cmd api.WorkerCommand) (map[string]interface{}, error) {
	w.logger.Info("[COMMAND] Drain command received - entering drain mode")

	w.drainMode.Store(true)

	return map[string]interface{}{
		"action":       "drain_enabled",
		"drain_active": true,
	}, nil
}

// handleUndrainCommand handles the undrain command.
func (w *Worker) handleUndrainCommand(ctx context.Context, cmd api.WorkerCommand) (map[string]interface{}, error) {
	w.logger.Info("[COMMAND] Undrain command received - exiting drain mode")

	w.drainMode.Store(false)

	return map[string]interface{}{
		"action":       "drain_disabled",
		"drain_active": false,
	}, nil
}
