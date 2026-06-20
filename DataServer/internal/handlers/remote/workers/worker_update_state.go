package workers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"velox-server/internal/logging"
)

var workerUpdateLog = logging.NewLogger("workers.update_handler")

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
			workerUpdateLog.InfoWithMsg(logging.CodeWorkerUpdateDownloaded, "UPDATE_DOWNLOADED - zip downloaded", map[string]interface{}{"worker": workerName, "artifact_sha": artifactPreview})

		case "UPDATE_APPLIED":
			workerUpdateLog.InfoWithMsg(logging.CodeWorkerUpdateApplied, "UPDATE_APPLIED - symlink updated, waiting for restart", map[string]interface{}{"worker": workerName})
			if body.UpdateInfo != nil {
				workerUpdateLog.InfoWithMsg(logging.CodeWorkerUpdateFinalized, "Dirs/files updated", map[string]interface{}{"dirs": body.UpdateInfo["dirs_updated"], "files": body.UpdateInfo["files_updated"]})
			}
		case "WORKER_ONLINE":
			isAligned := body.ArtifactSHA256 != "" && body.ArtifactSHA256 == targetArtifactSHA
			if isAligned {
				workerUpdateLog.InfoWithMsg(logging.CodeWorkerOnlineAligned, "UPDATED AND ONLINE", map[string]interface{}{"worker": workerName, "artifact_sha": body.ArtifactSHA256[:min(16, len(body.ArtifactSHA256))], "aligned": true})
				// Phase 4.4: ClearUpdate removed — alignment is reflected by
				// the worker_commands row for `update_code` being acked.
			} else {
				workerUpdateLog.InfoWithMsg(logging.CodeWorkerOnlineMisaligned, "online with different artifact (not yet updated)", map[string]interface{}{"worker": workerName, "aligned": false})
			}

		case "UPDATE_FAILED":
			workerUpdateLog.ErrorWithMsg(logging.CodeWorkerUpdateFailed, "UPDATE_FAILED", map[string]interface{}{"worker": workerName, "err": body.Error})
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
			// Phase 4.4: AckUpdate removal. The legacy POST /worker/update_ack
			// endpoint remains in place for downgrade paths but is a no-op
			// for the in-memory mirror; the canonical record is now in
			// worker_commands (acked via AckCommandByID from gRPC CommandAck).
			ctx := c.Request.Context()
			worker := h.reg.GetWorker(ctx, body.WorkerID)
			workerName := body.WorkerID[:min(16, len(body.WorkerID))] + "..."
			if worker != nil && worker.WorkerName != "" {
				workerName = worker.WorkerName
			}
			workerUpdateLog.InfoWithMsg(logging.CodeWorkerUpdateAck, "Legacy ACK received (no-op for in-memory mirror)", map[string]interface{}{"worker": workerName, "local_version": body.LocalVersion})
		}

		c.JSON(http.StatusOK, gin.H{
			"status":    "ack",
			"worker_id": body.WorkerID,
			"version":   body.LocalVersion,
		})
	}
}

// GetUpdateStatusHandler handles GET /workers/update_status
// Phase 4.4: derives status from worker_commands instead of the
// in-memory UpdateManager. Pending update_code rows surface here with
// status="pending" or "delivered".
func (h *WorkerUpdateHandler) GetUpdateStatusHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		allWorkers := h.reg.List(ctx)

		status := make(map[string]interface{})
		targetArtifactSHA := h.computeBundleSHA256()

		for _, info := range allWorkers {
			if info.WorkerID == "" {
				continue
			}
			pendingCmds := h.cmdMgr.GetPendingCommands(info.WorkerID)
			hasUpdate := false
			var updateVersion string
			for _, pc := range pendingCmds {
				if pc.Command == "update_code" {
					hasUpdate = true
					if v, ok := pc.Params["version"].(string); ok {
						updateVersion = v
					}
					break
				}
			}
			if hasUpdate {
				status[info.WorkerID] = map[string]interface{}{
					"worker_name":            info.WorkerName,
					"target_version":         updateVersion,
					"target_artifact_sha256": targetArtifactSHA,
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
