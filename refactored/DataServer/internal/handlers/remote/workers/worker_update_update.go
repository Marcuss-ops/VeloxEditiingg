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
		commandsQueued := h.queueBundleUpdateForWorkers(eligible, target, dryRun, maintenanceID)

		log.Printf("[UPDATE] Full update Linux: %d workers, %d commands, maintenance_id=%s",
			len(eligible), commandsQueued, maintenanceID)

		c.JSON(http.StatusOK, gin.H{
			"status":          "queued",
			"maintenance_id":  maintenanceID,
			"queued":          len(eligible),
			"total_eligible":  len(eligible),
			"commands_queued": commandsQueued,
			"target_version":  target.Version,
			"target_hash":     target.Hash,
			"target_filename": target.Filename,
			"worker_ids":      eligible,
			"updated_workers": eligible,
			"updated_count":   len(eligible),
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
		commandsQueued := h.queueBundleUpdateForWorkers(eligible, target, dryRun, maintenanceID)

		log.Printf("[UPDATE] Latest bundle update queued: workers=%d maintenance_id=%s version=%s hash=%s",
			len(eligible), maintenanceID, target.Version, target.Hash)

		c.JSON(http.StatusOK, gin.H{
			"status":          "queued",
			"maintenance_id":  maintenanceID,
			"queued":          len(eligible),
			"total_eligible":  len(eligible),
			"commands_queued": commandsQueued,
			"target_version":  target.Version,
			"target_hash":     target.Hash,
			"target_filename": target.Filename,
			"worker_ids":      eligible,
			"updated_workers": eligible,
			"updated_count":   len(eligible),
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

		for _, wid := range eligible {
			h.cmdMgr.PushCommand(wid, "restart_worker", nil)
		}

		log.Printf("[UPDATE] Restart all queued for %d workers", len(eligible))

		c.JSON(http.StatusOK, gin.H{
			"status":            "queued",
			"queued":            len(eligible),
			"worker_ids":        eligible,
			"restarted_workers": eligible,
			"restarted_count":   len(eligible),
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
			if h.reg.IsRevoked(info.WorkerID) {
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
			eligible = append(eligible, info.WorkerID)
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
		for _, wid := range canaryWorkers {
			h.cmdMgr.PushCommand(wid, "update_code", map[string]interface{}{
				"version":                target.Version,
				"bundle_version":         target.Version,
				"bundle_hash":            target.Hash,
				"target_artifact_sha256": target.Hash,
			})
			h.cmdMgr.PushCommand(wid, "restart_worker", nil)
			h.cmdMgr.PushCommand(wid, "run_smoke_job", buildSmokeJobPayload(wid))
			h.updateMgr.RequestUpdate(wid, target.Version)
		}

		log.Printf("[UPDATE] Rollout update started (rollout_id=%s)", rolloutID)
		log.Printf("   Total eligible: %d, Canary: %d, Batches: %d", totalWorkers, len(canaryWorkers), len(batches))

		c.JSON(http.StatusOK, gin.H{
			"status":         "queued",
			"rollout_id":     rolloutID,
			"target_version": target.Version,
			"target_hash":    targetArtifactSHA,
			"canary_workers": canaryWorkers,
			"batches":        batches,
			"total_workers":  totalWorkers,
			"batch_size":     batchSize,
			"canary_percent": canaryPercent,
		})
	}
}

// UpdateStateHandler handles POST /worker/update_state
func (h *WorkerUpdateHandler) UpdateStateHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID       string                 `json:"worker_id"`
			State          string                 `json:"state"`
			ArtifactSHA256 string                 `json:"artifact_sha256"`
			Version        string                 `json:"version"`
			Error          string                 `json:"error"`
			UpdateInfo     map[string]interface{} `json:"update_info"`
			NumeroEntita   int                    `json:"numero_entita"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}

		if body.WorkerID == "" || body.State == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id and state required"})
			return
		}
		if !h.authorizeWorkerRequest(c, body.WorkerID) {
			return
		}

		if h.reg.IsRevoked(body.WorkerID) {
			c.Status(http.StatusNoContent)
			return
		}

		ctx := c.Request.Context()
		worker := h.reg.GetWorker(ctx, body.WorkerID)
		workerName := body.WorkerID[:min(16, len(body.WorkerID))] + "..."
		if worker != nil && worker.WorkerName != "" {
			workerName = worker.WorkerName
		}

		targetArtifactSHA := h.computeBundleSHA256()

		switch body.State {
		case "UPDATE_DOWNLOADED":
			artifactPreview := "N/A"
			if body.ArtifactSHA256 != "" {
				if len(body.ArtifactSHA256) > 16 {
					artifactPreview = body.ArtifactSHA256[:16] + "..."
				} else {
					artifactPreview = body.ArtifactSHA256
				}
			}
			log.Printf("[UPDATE] Worker %s: UPDATE_DOWNLOADED - zip downloaded, hash=%s", workerName, artifactPreview)

		case "UPDATE_APPLIED":
			log.Printf("[OK] Worker %s: UPDATE_APPLIED - symlink updated, waiting for restart", workerName)
			if body.UpdateInfo != nil {
				log.Printf("   [INFO] Dirs updated: %v, Files updated: %v",
					body.UpdateInfo["dirs_updated"], body.UpdateInfo["files_updated"])
			}

		case "WORKER_ONLINE":
			isAligned := body.ArtifactSHA256 != "" && body.ArtifactSHA256 == targetArtifactSHA
			if isAligned {
				log.Printf("")
				log.Printf("[OK] ========================================")
				log.Printf("[UPDATE] Worker %s UPDATED AND ONLINE!", workerName)
				log.Printf("[OK] ========================================")
				log.Printf("   [INFO] Artifact: %s...", body.ArtifactSHA256[:min(16, len(body.ArtifactSHA256))])
				log.Printf("   [OK] Aligned: YES")
				log.Printf("")
				h.updateMgr.ClearUpdate(body.WorkerID)
			} else {
				log.Printf("Worker %s online with different artifact (not yet updated)", workerName)
			}

		case "UPDATE_FAILED":
			log.Printf("[ERROR] Worker %s: UPDATE_FAILED - %s", workerName, body.Error)
		}

		c.JSON(http.StatusOK, gin.H{
			"status":    "ok",
			"worker_id": body.WorkerID,
			"state":     body.State,
		})
	}
}

// UpdateAckHandler handles POST /worker/update_ack
func (h *WorkerUpdateHandler) UpdateAckHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID     string `json:"worker_id"`
			LocalVersion string `json:"local_version"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}
		if body.WorkerID != "" && !h.authorizeWorkerRequest(c, body.WorkerID) {
			return
		}

		if body.WorkerID != "" && body.LocalVersion != "" {
			h.updateMgr.AckUpdate(body.WorkerID, body.LocalVersion)

			ctx := c.Request.Context()
			worker := h.reg.GetWorker(ctx, body.WorkerID)
			workerName := body.WorkerID[:min(16, len(body.WorkerID))] + "..."
			if worker != nil && worker.WorkerName != "" {
				workerName = worker.WorkerName
			}
			log.Printf("[UPDATE] Worker %s: Legacy ACK received (version: %s)", workerName, body.LocalVersion)
		}

		c.JSON(http.StatusOK, gin.H{
			"status":    "ack",
			"worker_id": body.WorkerID,
			"version":   body.LocalVersion,
		})
	}
}

// GetUpdateStatusHandler handles GET /workers/update_status
func (h *WorkerUpdateHandler) GetUpdateStatusHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		allWorkers := h.reg.List(ctx)

		status := make(map[string]interface{})
		targetArtifactSHA := h.computeBundleSHA256()

		for _, info := range allWorkers {
			pending := h.updateMgr.GetPendingUpdate(info.WorkerID)
			if pending != nil {
				status[info.WorkerID] = map[string]interface{}{
					"worker_name":            info.WorkerName,
					"target_version":         pending.Version,
					"target_artifact_sha256": targetArtifactSHA,
					"requested_at":           pending.RequestedAt,
					"ack":                    pending.Ack,
					"ack_version":            pending.AckVersion,
				}
			}
		}

		c.JSON(http.StatusOK, gin.H{
			"target_version":         h.codeVersion,
			"target_artifact_sha256": targetArtifactSHA,
			"updates":                status,
		})
	}
}
