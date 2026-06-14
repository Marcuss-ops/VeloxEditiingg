package workers

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// SendCommandHandler handles POST /worker/send_command
func (h *WorkerUpdateHandler) SendCommandHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		workerID := c.Query("worker_id")
		command := c.Query("command")

		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id required"})
			return
		}

		allowedCommands := map[string]bool{
			"restart_worker": true,
			"update_code":    true,
			"reboot_host":    true,
			"run_smoke_job":  true,
			"cancel_job":     true,
		}
		if !allowedCommands[command] {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Invalid command. Allowed: restart_worker, update_code, reboot_host, run_smoke_job, cancel_job",
			})
			return
		}

		// Check if worker is revoked
		if h.reg.IsRevoked(workerID) {
			c.JSON(http.StatusForbidden, gin.H{"error": "Worker is revoked"})
			return
		}

		// Push command
		params := map[string]interface{}{}
		if command == "update_code" {
			params["version"] = h.codeVersion
			h.updateMgr.RequestUpdate(workerID, h.codeVersion)
		}
		if command == "run_smoke_job" {
			params = buildSmokeJobPayload(workerID)
		}
		h.cmdMgr.PushCommand(workerID, command, params)

		log.Printf("[COMMAND] Command '%s' queued for worker %s", command, workerID[:min(16, len(workerID))]+"...")

		c.JSON(http.StatusOK, gin.H{
			"status":    "queued",
			"worker_id": workerID,
			"command":   command,
		})
	}
}

// SendCommandBulkHandler handles POST /workers/send_command_bulk
func (h *WorkerUpdateHandler) SendCommandBulkHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerIDs      []string               `json:"worker_ids"`
			Workers        []string               `json:"workers"`
			Command        string                 `json:"command"`
			ExcludeRevoked bool                   `json:"exclude_revoked"`
			Payload        map[string]interface{} `json:"payload"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}

		// Accept both worker_ids and workers
		workerIDs := body.WorkerIDs
		if len(workerIDs) == 0 {
			workerIDs = body.Workers
		}

		if len(workerIDs) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_ids required (non-empty list)"})
			return
		}

		if body.Command == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "command required"})
			return
		}

		allowedCommands := map[string]bool{
			"restart_worker": true,
			"update_code":    true,
			"reboot_host":    true,
			"run_smoke_job":  true,
			"cancel_job":     true,
		}
		if !allowedCommands[body.Command] {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Invalid command. Allowed: restart_worker, update_code, reboot_host, run_smoke_job, cancel_job",
			})
			return
		}

		excludeRevoked := body.ExcludeRevoked
		if !excludeRevoked {
			excludeRevoked = true // Default to true
		}

		// Calculate target artifact SHA256 for update_code
		targetArtifactSHA := ""
		if body.Command == "update_code" {
			targetArtifactSHA = h.computeBundleSHA256()
		}

		queued := []string{}
		skipped := []string{}
		invalid := []string{}

		for _, wid := range workerIDs {
			if wid == "" {
				invalid = append(invalid, wid)
				continue
			}

			// Check if revoked
			if excludeRevoked && h.reg.IsRevoked(wid) {
				skipped = append(skipped, wid)
				continue
			}

			// Push command
			params := map[string]interface{}{}
			if body.Command == "update_code" {
				params["version"] = h.codeVersion
				params["target_artifact_sha256"] = targetArtifactSHA
				h.updateMgr.RequestUpdate(wid, h.codeVersion)
			} else if body.Command == "run_smoke_job" {
				if len(body.Payload) > 0 {
					params = body.Payload
				} else {
					params = buildSmokeJobPayload(wid)
				}
			}
			h.cmdMgr.PushCommand(wid, body.Command, params)
			queued = append(queued, wid)
		}

		log.Printf("[COMMAND] Bulk command '%s': queued=%d skipped=%d invalid=%d",
			body.Command, len(queued), len(skipped), len(invalid))

		c.JSON(http.StatusOK, gin.H{
			"status":             "queued",
			"command":            body.Command,
			"queued_count":       len(queued),
			"queued_worker_ids":  queued,
			"skipped_worker_ids": skipped,
			"invalid_worker_ids": invalid,
		})
	}
}

func (h *WorkerUpdateHandler) eligibleWorkers(ctx context.Context, excludeLocal bool) []string {
	if h.reg == nil {
		return []string{}
	}

	allWorkers := h.reg.List(ctx)
	eligible := make([]string, 0, len(allWorkers))
	for _, info := range allWorkers {
		if h.reg.IsRevoked(info.WorkerID) {
			continue
		}
		if info.Drain {
			continue
		}
		if excludeLocal {
			name := info.WorkerName
			if len(name) > 12 && name[:12] == "Local-Worker" {
				continue
			}
		}
		eligible = append(eligible, info.WorkerID)
	}
	return eligible
}

func buildSmokeJobPayload(workerID string) map[string]interface{} {
	now := time.Now().UTC()
	jobID := fmt.Sprintf("smoke-%s-%d", workerID, now.Unix())
	return map[string]interface{}{
		"job_id":          jobID,
		"job_type":        "health_check",
		"priority":        0,
		"timeout_secs":    60,
		"created_at":      now.Format(time.RFC3339),
		"expect_callback": true,
	}
}
