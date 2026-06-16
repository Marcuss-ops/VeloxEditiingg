package workers

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

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
