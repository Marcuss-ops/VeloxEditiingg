package lifecycle

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// HeartbeatHandler handles worker heartbeats with token authorization.
func (h *Handler) HeartbeatHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID        string                 `json:"worker_id"`
			WorkerName      string                 `json:"worker_name"`
			Status          string                 `json:"status"`
			CurrentJob      string                 `json:"current_job"`
			CodeVersion     string                 `json:"code_version"`
			BundleVersion   string                 `json:"bundle_version"`
			BundleHash      string                 `json:"bundle_hash"`
			ProtocolVersion string                 `json:"protocol_version"`
			EngineVersion   string                 `json:"engine_version"`
			Capabilities    map[string]interface{} `json:"capabilities"`
			Extra           map[string]interface{} `json:"extra"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			log.Printf("workers/heartbeat: failed to bind JSON: %v", err)
		}
		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "missing worker_id"})
			return
		}
		if !h.authorizeWorkerRequest(c, body.WorkerID) {
			return
		}

		status := body.Status
		if status == "" {
			status = "online"
		}

		extra := body.Extra
		if extra == nil {
			extra = make(map[string]interface{})
		}
		if body.CodeVersion != "" {
			extra["code_version"] = body.CodeVersion
		}
		if body.BundleVersion != "" {
			extra["bundle_version"] = body.BundleVersion
		}
		if body.BundleHash != "" {
			extra["bundle_hash"] = body.BundleHash
		}
		if body.ProtocolVersion != "" {
			extra["protocol_version"] = body.ProtocolVersion
		}
		if body.EngineVersion != "" {
			extra["engine_version"] = body.EngineVersion
		}
		if body.Capabilities != nil {
			extra["capabilities"] = body.Capabilities
		}

		if err := h.reg.Heartbeat(c.Request.Context(), body.WorkerID, body.WorkerName, status, body.CurrentJob, extra); err != nil {
			log.Printf("workers/heartbeat: heartbeat failed for %s: %v", body.WorkerID, err)
		}
		c.JSON(http.StatusOK, gin.H{"ok": true})
	}
}

func (h *Handler) UpdateStatusHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID string                 `json:"worker_id"`
			Status   string                 `json:"status"`
			Details  map[string]interface{} `json:"details"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "invalid JSON body"})
			return
		}
		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "worker_id required"})
			return
		}
		if !h.authorizeWorkerRequest(c, body.WorkerID) {
			return
		}

		log.Printf("[UPDATE] Worker status update: worker=%s status=%s details=%v", body.WorkerID, body.Status, body.Details)

		existing := h.reg.GetWorker(c.Request.Context(), body.WorkerID)
		recentLogs := []string{}
		recentErrors := []string{}
		if existing != nil {
			recentLogs = append(recentLogs, existing.RecentLogs...)
			recentErrors = append(recentErrors, existing.RecentErrors...)
		}
		line := fmt.Sprintf("[%s] status=%s details=%v", time.Now().UTC().Format(time.RFC3339), body.Status, body.Details)
		recentLogs = append(recentLogs, line)
		if len(recentLogs) > 300 {
			recentLogs = recentLogs[len(recentLogs)-300:]
		}
		if body.Status == "command_failed" {
			recentErrors = append(recentErrors, line)
			if len(recentErrors) > 120 {
				recentErrors = recentErrors[len(recentErrors)-120:]
			}
		}
		_ = h.reg.UpdateWorker(c.Request.Context(), body.WorkerID, map[string]interface{}{
			"recent_logs":   recentLogs,
			"recent_errors": recentErrors,
		})

		c.JSON(http.StatusOK, gin.H{"success": true, "message": "status updated"})
	}
}
