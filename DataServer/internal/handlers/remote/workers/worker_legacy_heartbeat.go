package workers

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	workersreg "velox-server/internal/workers"
)

// HeartbeatBody matches Python: worker_id, worker_name, status, recent_logs, recent_errors, readiness, etc.
func Heartbeat(reg *workersreg.Registry) gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID      string                 `json:"worker_id"`
			WorkerName    string                 `json:"worker_name"`
			Status        string                 `json:"status"`
			CurrentJob    string                 `json:"current_job"`
			CodeVersion   string                 `json:"code_version"`
			BundleVersion string                 `json:"bundle_version"`
			Extra         map[string]interface{} `json:"extra"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			log.Printf("workers/heartbeat: failed to bind JSON: %v", err)
		}
		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "missing worker_id"})
			return
		}
		if body.Status == "" {
			body.Status = "online"
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

		if err := reg.Heartbeat(c.Request.Context(), body.WorkerID, body.WorkerName, body.Status, body.CurrentJob, extra); err != nil {
			log.Printf("workers/heartbeat: heartbeat failed for %s: %v", body.WorkerID, err)
		}
		c.JSON(http.StatusOK, gin.H{"ok": true})
	}
}
