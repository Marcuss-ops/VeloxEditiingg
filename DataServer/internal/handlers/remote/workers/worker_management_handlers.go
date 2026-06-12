package workers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	workersreg "velox-server/internal/workers"
)

// RenameWorker handles POST /worker/rename
func RenameWorker(reg *workersreg.Registry) gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID string `json:"worker_id"`
			NewName  string `json:"new_name"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "worker_id required"})
			return
		}
		if body.NewName == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "new_name required"})
			return
		}

		// Get current worker info
		workerInfo := reg.GetWorker(c.Request.Context(), body.WorkerID)
		if workerInfo == nil {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "worker not found"})
			return
		}

		oldName := workerInfo.WorkerName
		// Update worker name via heartbeat
		_ = reg.Heartbeat(c.Request.Context(), body.WorkerID, body.NewName, workerInfo.Status, workerInfo.CurrentJob, nil)

		c.JSON(http.StatusOK, gin.H{
			"status":    "ok",
			"worker_id": body.WorkerID,
			"old_name":  oldName,
			"new_name":  body.NewName,
		})
	}
}

// SetWorkerGroup handles POST /worker/set_group
func SetWorkerGroup(reg *workersreg.Registry) gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID    string `json:"worker_id"`
			WorkerGroup string `json:"worker_group"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "worker_id required"})
			return
		}

		// Get current worker info
		workerInfo := reg.GetWorker(c.Request.Context(), body.WorkerID)
		if workerInfo == nil {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "worker not found"})
			return
		}

		// Update worker group via heartbeat with extra
		extra := map[string]interface{}{
			"worker_group": body.WorkerGroup,
		}
		_ = reg.Heartbeat(c.Request.Context(), body.WorkerID, workerInfo.WorkerName, workerInfo.Status, workerInfo.CurrentJob, extra)

		c.JSON(http.StatusOK, gin.H{
			"status":       "ok",
			"worker_id":    body.WorkerID,
			"worker_group": body.WorkerGroup,
		})
	}
}

// ReportWorkerError handles POST /worker/report_error
func ReportWorkerError(reg *workersreg.Registry) gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID  string `json:"worker_id"`
			Error     string `json:"error"`
			JobID     string `json:"job_id"`
			Timestamp string `json:"timestamp"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "worker_id required"})
			return
		}

		// Log the error (in production, this would go to a logger or error tracking system)
		// For now, we just acknowledge receipt

		c.JSON(http.StatusOK, gin.H{
			"ok":        true,
			"worker_id": body.WorkerID,
			"error":     body.Error,
			"job_id":    body.JobID,
		})
	}
}
