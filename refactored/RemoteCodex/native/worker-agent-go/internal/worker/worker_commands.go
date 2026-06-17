// Package worker provides command polling and lifecycle management for the worker agent.
package worker

import (
	"context"
	"fmt"
	"time"

	"velox-worker-agent/pkg/api"
)

// commandPollInterval returns the configured command polling interval.
// Falls back to 30s if the config value is not set or invalid.
func (w *Worker) commandPollInterval() time.Duration {
	secs := w.config.CommandPollIntervalSecs
	if secs <= 0 {
		secs = 30
	}
	return time.Duration(secs) * time.Second
}

// commandLoop polls for commands from the master and processes them.
// Supported commands: drain, restart_worker, update_code, reboot_host.
// The polling interval is configurable via WorkerConfig.CommandPollIntervalSecs (default: 30s).
func (w *Worker) commandLoop(ctx context.Context) {
	defer w.wg.Done()

	pollInterval := w.commandPollInterval()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	pollCount := 0
	lastSummaryLog := time.Now()
	summaryInterval := 5 * time.Minute

	w.logger.Info("[COMMANDS] Command polling started — checking every %v", pollInterval)

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("[COMMANDS] Command loop exiting (context done)")
			return
		case <-w.stopChan:
			w.logger.Info("[COMMANDS] Command loop exiting (stop signal)")
			return
		case <-ticker.C:
			if w.IsStopped() {
				continue
			}

			pollCount++

			commands, err := w.apiClient.GetCommands(ctx, w.config.WorkerID)
			if err != nil {
				w.logger.Debug("[COMMANDS] Poll attempt %d failed: %v", pollCount, err)
				continue
			}

			for _, cmd := range commands {
				// Deduplicate commands using the in-memory seenCommands map
				if w.markCommandSeen(cmd) {
					w.logger.Debug("[COMMANDS] Skipping duplicate command: %s (timestamp: %s)", cmd.Command, cmd.Timestamp)
					continue
				}

				w.logger.Info("[COMMANDS] Processing command: %s (timestamp: %s)", cmd.Command, cmd.Timestamp)
				w.processCommand(ctx, cmd)
			}

			if time.Since(lastSummaryLog) >= summaryInterval {
				w.logger.Info("[COMMANDS] Status: alive — %d polls sent, no pending commands (next check in %v)",
					pollCount, pollInterval)
				lastSummaryLog = time.Now()
			}
		}
	}
}

// processCommand processes a single command from the master.
// It handles the command, acknowledges it, and logs the outcome.
func (w *Worker) processCommand(ctx context.Context, cmd api.WorkerCommand) {
	var resultErr error

	switch cmd.Command {
	case "drain":
		w.drainMode.Store(true)
		w.logger.Info("[COMMANDS] Drain mode enabled — no new jobs will be accepted")

	case "restart_worker":
		w.logger.Info("[COMMANDS] Restart requested — enabling drain mode, will restart after current jobs complete")
		w.drainMode.Store(true)
		// Actual restart is handled externally by Docker/systemd.
		// The drain ensures no new jobs are claimed, and the supervisor
		// (Docker restart policy / systemd) will restart the process.

	case "update_code":
		version := ""
		if cmd.Payload != nil {
			if v, ok := cmd.Payload["version"].(string); ok {
				version = v
			}
		}
		w.logger.Info("[COMMANDS] Update requested — version=%s (download will be handled by supervisor)", version)
		// The actual binary update is handled by the external update mechanism
		// (install-worker.sh / Docker image update / bundle download).
		// This ack tells the master the command was received successfully.

	case "reboot_host":
		w.logger.Info("[COMMANDS] Host reboot requested — draining jobs first")
		w.drainMode.Store(true)
		// Host reboot is handled by an external script/playbook.

	case "cancel_job":
		jobID := ""
		if cmd.Payload != nil {
			if j, ok := cmd.Payload["job_id"].(string); ok {
				jobID = j
			}
		}
		if jobID == "" {
			w.logger.Warn("[COMMANDS] cancel_job command missing job_id in payload")
			resultErr = fmt.Errorf("cancel_job: missing job_id")
		} else {
			w.logger.Info("[COMMANDS] Cancel requested for job %s", jobID)
			if !w.cancelJob(jobID) {
				w.logger.Warn("[COMMANDS] Job %s not found on this worker — may be running elsewhere or already finished", jobID)
			}
		}

	default:
		w.logger.Warn("[COMMANDS] Unknown command type: %s", cmd.Command)
		resultErr = fmt.Errorf("unknown command: %s", cmd.Command)
	}

	// Ack the command by command_id so the master knows it was received (even on error).
	if cmd.CommandID != "" {
		if ackErr := w.apiClient.AckCommandByID(ctx, w.config.WorkerID, cmd.CommandID); ackErr != nil {
			w.logger.Warn("[COMMANDS] Failed to ack command %s (id=%s): %v", cmd.Command, cmd.CommandID, ackErr)
		} else {
			w.logger.Debug("[COMMANDS] Acknowledged command: %s (id=%s)", cmd.Command, cmd.CommandID)
		}
	} else {
		// Legacy fallback: ack by type
		if ackErr := w.apiClient.AckCommand(ctx, w.config.WorkerID, cmd.Command); ackErr != nil {
			w.logger.Warn("[COMMANDS] Failed to ack command %s: %v", cmd.Command, ackErr)
		} else {
			w.logger.Debug("[COMMANDS] Acknowledged command (legacy): %s", cmd.Command)
		}
	}

	if resultErr != nil {
		w.logger.Error("[COMMANDS] Command %s processing error: %v", cmd.Command, resultErr)
	}
}
