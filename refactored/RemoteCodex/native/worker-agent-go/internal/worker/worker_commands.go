// Package worker provides command processing logic for the worker agent.
// Command polling is now handled by the ControlTransport; the receiveLoop in
// worker.go routes Command messages to processCommand. This file retains
// the command processing and deduplication logic.
package worker

import (
	"context"
	"fmt"
	"time"

	"velox-shared/controltransport"
	"velox-worker-agent/pkg/api"
)

// processCommand processes a single command from the master received via
// the transport receive channel. It handles the command, acknowledges it
// via transport.Send(), and logs the outcome.
func (w *Worker) processCommand(ctx context.Context, cmd api.WorkerCommand) {
	var resultErr error

	// Deduplicate commands using the in-memory seenCommands map
	if w.markCommandSeen(cmd) {
		w.logger.Debug("[COMMANDS] Skipping duplicate command: %s (id: %s)", cmd.Command, cmd.CommandID)
		return
	}

	w.logger.Info("[COMMANDS] Processing command: %s (id: %s)", cmd.Command, cmd.CommandID)

	switch cmd.Command {
	case "drain":
		w.drainMode.Store(true)
		w.logger.Info("[COMMANDS] Drain mode enabled — no new jobs will be accepted")

	case "restart_worker":
		w.logger.Info("[COMMANDS] Restart requested — enabling drain mode, will restart after current jobs complete")
		w.drainMode.Store(true)

	case "update_code":
		version := ""
		if cmd.Payload != nil {
			if v, ok := cmd.Payload["version"].(string); ok {
				version = v
			}
		}
		w.logger.Info("[COMMANDS] Update requested — version=%s (download will be handled by supervisor)", version)

	case "reboot_host":
		w.logger.Info("[COMMANDS] Host reboot requested — draining jobs first")
		w.drainMode.Store(true)

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

	// Ack the command via transport so the master knows it was received.
	ackPayload := map[string]interface{}{
		"command":    cmd.Command,
		"command_id": cmd.CommandID,
	}
	if resultErr != nil {
		ackPayload["error"] = resultErr.Error()
	}

	ackMsg := controltransport.NewMessageWithPayload(
		controltransport.MsgCommandAck,
		w.config.WorkerID,
		w.config.ProtocolVersion,
		ackPayload,
	)
	// Use a background context with timeout for the ack
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer ackCancel()

	if ackErr := w.transport.Send(ackCtx, ackMsg); ackErr != nil {
		w.logger.Warn("[COMMANDS] Failed to ack command %s (id=%s): %v", cmd.Command, cmd.CommandID, ackErr)
	} else {
		w.logger.Debug("[COMMANDS] Acknowledged command: %s (id=%s)", cmd.Command, cmd.CommandID)
	}

	if resultErr != nil {
		w.logger.Error("[COMMANDS] Command %s processing error: %v", cmd.Command, resultErr)
	}
}
