package jobs

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	"velox-server/internal/queue"
	"velox-server/internal/workers"
)

func GetJob(cfg *config.Config, q *queue.Queue, reg *workers.Registry) gin.HandlerFunc {
	return func(c *gin.Context) {
		workerID := strings.TrimSpace(c.Query("worker_id"))
		workerName := strings.TrimSpace(c.Query("worker_name"))
		drain := c.Query("drain") == "true"
		schedulable := c.Query("schedulable") != "false" // default true

		if workerID == "" {
			c.JSON(http.StatusOK, gin.H{"job": nil, "reason": "missing worker_id"})
			return
		}
		clientIP := c.ClientIP()

		// Single-worker mode
		if cfg.ForceSingleWorker != "" && cfg.ForceSingleWorker != "0" && !strings.EqualFold(cfg.ForceSingleWorker, "false") {
			allowed := workerID == cfg.ForceSingleWorker || clientIP == cfg.ForceSingleWorker
			if !allowed {
				c.JSON(http.StatusOK, gin.H{"job": nil, "reason": "Single-worker mode active (" + cfg.ForceSingleWorker + ")"})
				return
			}
		}

		// Allowlist
		allowedWorkers := strings.TrimSpace(cfg.AllowedWorkers)
		if allowedWorkers != "" && !strings.EqualFold(allowedWorkers, "*") && !strings.EqualFold(allowedWorkers, "ALL") {
			parts := strings.Split(allowedWorkers, ",")
			allowlistOK := false
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p == "" {
					continue
				}
				if clientIP == p || workerID == p || workerName == p {
					allowlistOK = true
					break
				}
			}
			if !allowlistOK && cfg.AllowlistAllowRegistered && reg.IsRegistered(c.Request.Context(), workerID) {
				allowlistOK = true
			}
			if !allowlistOK {
				c.JSON(http.StatusOK, gin.H{"job": nil, "reason": "Worker not allowed"})
				return
			}
		}

		// Check worker drain/schedulable status from registry
		if !drain {
			workerInfo := reg.GetWorker(c.Request.Context(), workerID)
			if workerInfo != nil {
				if workerInfo.Drain {
					c.JSON(http.StatusOK, gin.H{"job": nil, "reason": "Worker draining"})
					return
				}
				if !schedulable && !workerInfo.Schedulable {
					c.JSON(http.StatusOK, gin.H{"job": nil, "reason": "Worker not schedulable"})
					return
				}
			}
		}

		jobID, err := q.GetNextJobID(c.Request.Context())
		if err != nil || jobID == "" {
			c.JSON(http.StatusOK, gin.H{"job": nil})
			return
		}
		payload, _ := q.GetJobPayload(c.Request.Context(), jobID)
		if payload == nil {
			payload = make(map[string]interface{})
		}
		payload["job_id"] = jobID
		payload["id"] = jobID
		if err := q.LeaseJob(c.Request.Context(), jobID, workerID); err != nil {
			c.JSON(http.StatusOK, gin.H{"job": nil, "reason": "lease failed"})
			return
		}
		_ = reg.Heartbeat(c.Request.Context(), workerID, workerName, "busy", jobID, nil)
		c.JSON(http.StatusOK, gin.H{"job": payload})
	}
}

func StartJob(c *gin.Context) {
	var body map[string]interface{}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func CompleteJob(q *queue.Queue, reg *workers.Registry) gin.HandlerFunc {
	return func(c *gin.Context) {
		var body map[string]interface{}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
			return
		}
		jobID, _ := body["job_id"].(string)
		if jobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "missing job_id"})
			return
		}
		if err := q.CompleteJob(c.Request.Context(), jobID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}
		workerID, _ := body["worker_id"].(string)
		if workerID != "" {
			_ = reg.Heartbeat(c.Request.Context(), workerID, "", "online", "", nil)
		}
		c.JSON(http.StatusOK, gin.H{"ok": true})
	}
}

func FailJob(cfg *config.Config, q *queue.Queue, reg *workers.Registry) gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			JobID    string `json:"job_id"`
			WorkerID string `json:"worker_id"`
			Error    string `json:"error"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if body.JobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "missing job_id"})
			return
		}
		attempt, _ := q.GetJobAttempt(c.Request.Context(), body.JobID)
		maxAttempts := cfg.MaxJobAttempts
		if maxAttempts <= 0 {
			maxAttempts = 3
		}
		requeue := attempt < maxAttempts
		if err := q.FailJob(c.Request.Context(), body.JobID, body.Error, requeue); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if body.WorkerID != "" {
			_ = reg.Heartbeat(c.Request.Context(), body.WorkerID, "", "online", "", nil)
		}
		c.JSON(http.StatusOK, gin.H{"ok": true})
	}
}
