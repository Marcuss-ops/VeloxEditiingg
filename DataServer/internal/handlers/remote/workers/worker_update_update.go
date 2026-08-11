package workers

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// FullUpdateLinuxHandler handles POST /workers/full_update_linux
func (h *WorkerUpdateHandler) FullUpdateLinuxHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		excludeLocal, dryRun := h.readUpdateAllOptions(c)
		eligible := h.eligibleWorkers(c.Request.Context(), excludeLocal)
		target := h.latestBundleTarget()

		if len(eligible) == 0 {
			c.JSON(http.StatusOK, gin.H{
				"status":  "no_workers",
				"queued":  0,
				"message": "No eligible workers",
			})
			return
		}

		maintenanceID := uuid.New().String()
		commandsQueued, queuedWorkers, failedWorkers := h.queueBundleUpdateForWorkers(eligible, target, dryRun, maintenanceID)

		log.Printf("[UPDATE] Full update Linux: %d workers, %d commands, maintenance_id=%s",
			len(eligible), commandsQueued, maintenanceID)

		c.JSON(http.StatusOK, gin.H{
			"status":          "queued",
			"maintenance_id":  maintenanceID,
			"queued":          len(queuedWorkers),
			"total_eligible":  len(eligible),
			"commands_queued": commandsQueued,
			"target_version":  target.Version,
			"target_hash":     target.Hash,
			"target_filename": target.Filename,
			"worker_ids":      eligible,
			"updated_workers": queuedWorkers,
			"updated_count":   len(queuedWorkers),
			"failed_workers":  failedWorkers,
		})
	}
}

// UpdateAllHandler handles POST /workers/update_all
func (h *WorkerUpdateHandler) UpdateAllHandler() gin.HandlerFunc {
	return h.FullUpdateLinuxHandler()
}

// UpdateAllLatestBundleHandler handles POST /workers/update_all_latest_bundle
func (h *WorkerUpdateHandler) UpdateAllLatestBundleHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		excludeLocal, dryRun := h.readUpdateAllOptions(c)
		eligible := h.eligibleWorkers(c.Request.Context(), excludeLocal)
		target := h.latestBundleTarget()

		if len(eligible) == 0 {
			c.JSON(http.StatusOK, gin.H{
				"status":  "no_workers",
				"queued":  0,
				"message": "No eligible workers",
			})
			return
		}

		maintenanceID := uuid.New().String()
		commandsQueued, queuedWorkers, failedWorkers := h.queueBundleUpdateForWorkers(eligible, target, dryRun, maintenanceID)

		log.Printf("[UPDATE] Latest bundle update queued: workers=%d maintenance_id=%s version=%s hash=%s",
			len(eligible), maintenanceID, target.Version, target.Hash)

		c.JSON(http.StatusOK, gin.H{
			"status":          "queued",
			"maintenance_id":  maintenanceID,
			"queued":          len(queuedWorkers),
			"total_eligible":  len(eligible),
			"commands_queued": commandsQueued,
			"target_version":  target.Version,
			"target_hash":     target.Hash,
			"target_filename": target.Filename,
			"worker_ids":      eligible,
			"updated_workers": queuedWorkers,
			"updated_count":   len(queuedWorkers),
			"failed_workers":  failedWorkers,
		})
	}
}

// RestartAllHandler handles POST /workers/restart_all
func (h *WorkerUpdateHandler) RestartAllHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		excludeLocal, _ := h.readUpdateAllOptions(c)
		eligible := h.eligibleWorkers(c.Request.Context(), excludeLocal)

		if len(eligible) == 0 {
			c.JSON(http.StatusOK, gin.H{
				"status":  "no_workers",
				"queued":  0,
				"message": "No eligible workers",
			})
			return
		}

		queuedWorkers := make([]string, 0, len(eligible))
		failedWorkers := make([]string, 0)
		for _, wid := range eligible {
			if _, err := h.cmdMgr.PushCommandWithError(wid, "restart_worker", nil); err != nil {
				failedWorkers = append(failedWorkers, wid)
				continue
			}
			queuedWorkers = append(queuedWorkers, wid)
		}

		log.Printf("[UPDATE] Restart all queued for %d workers", len(eligible))

		c.JSON(http.StatusOK, gin.H{
			"status":            "queued",
			"queued":            len(queuedWorkers),
			"worker_ids":        eligible,
			"restarted_workers": queuedWorkers,
			"restarted_count":   len(queuedWorkers),
			"failed_workers":    failedWorkers,
			"command":           "restart_worker",
		})
	}
}

// RolloutUpdateHandler handles POST /workers/rollout_update
func (h *WorkerUpdateHandler) RolloutUpdateHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			BatchSize     int     `json:"batch_size"`
			CanaryPercent float64 `json:"canary_percent"`
			ExcludeLocal  bool    `json:"exclude_local"`
			RolloutID     string  `json:"rollout_id"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}

		batchSize := body.BatchSize
		if batchSize <= 0 {
			batchSize = 10
		}

		canaryPercent := body.CanaryPercent
		if canaryPercent <= 0 {
			canaryPercent = 1.0
		}

		rolloutID := body.RolloutID
		if rolloutID == "" {
			rolloutID = uuid.New().String()
		}

		ctx := c.Request.Context()
		allWorkers := h.reg.List(ctx)

		eligible := []string{}
		for _, info := range allWorkers {
			if h.reg.IsRevoked(info.WorkerID.String()) {
				continue
			}
			if info.Drain {
				continue
			}
			if body.ExcludeLocal {
				name := info.WorkerName
				if len(name) > 12 && name[:12] == "Local-Worker" {
					continue
				}
			}
			eligible = append(eligible, info.WorkerID.String())
		}

		if len(eligible) == 0 {
			c.JSON(http.StatusOK, gin.H{
				"status":     "no_workers",
				"message":    "No eligible workers for rollout",
				"rollout_id": rolloutID,
			})
			return
		}

		totalWorkers := len(eligible)
		canaryCount := int(float64(totalWorkers) * canaryPercent / 100.0)
		if canaryCount < 1 {
			canaryCount = 1
		}

		canaryWorkers := eligible[:canaryCount]
		remainingWorkers := eligible[canaryCount:]

		batches := [][]string{}
		for i := 0; i < len(remainingWorkers); i += batchSize {
			end := i + batchSize
			if end > len(remainingWorkers) {
				end = len(remainingWorkers)
			}
			batches = append(batches, remainingWorkers[i:end])
		}

		targetArtifactSHA := h.computeBundleSHA256()
		target := h.latestBundleTarget()
		queuedCanary := make([]string, 0, len(canaryWorkers))
		failedCanary := make([]string, 0)
		for _, wid := range canaryWorkers {
			failed := false
			if _, err := h.cmdMgr.PushCommandWithError(wid, "update_code", map[string]interface{}{
				"version":                target.Version,
				"bundle_version":         target.Version,
				"bundle_hash":            target.Hash,
				"target_artifact_sha256": target.Hash,
			}); err != nil {
				failed = true
			}
			if _, err := h.cmdMgr.PushCommandWithError(wid, "restart_worker", nil); err != nil {
				failed = true
			}
			if _, err := h.cmdMgr.PushCommandWithError(wid, "run_smoke_job", buildSmokeJobPayload(wid)); err != nil {
				failed = true
			}
			if failed {
				failedCanary = append(failedCanary, wid)
			} else {
				queuedCanary = append(queuedCanary, wid)
			}
			// Phase 4.4: in-memory UpdateManager mirror removed
		}

		log.Printf("[UPDATE] Rollout update started (rollout_id=%s)", rolloutID)
		log.Printf("   Total eligible: %d, Canary: %d, Batches: %d", totalWorkers, len(canaryWorkers), len(batches))

		c.JSON(http.StatusOK, gin.H{
			"status":                "queued",
			"rollout_id":            rolloutID,
			"target_version":        target.Version,
			"target_hash":           targetArtifactSHA,
			"canary_workers":        queuedCanary,
			"failed_canary_workers": failedCanary,
			"batches":               batches,
			"total_workers":         totalWorkers,
			"batch_size":            batchSize,
			"canary_percent":        canaryPercent,
		})
	}
}
