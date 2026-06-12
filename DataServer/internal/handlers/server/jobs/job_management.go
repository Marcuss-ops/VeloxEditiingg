package jobs

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/queue"
)

func (h *JobSubmissionHandler) BulkDeleteJobsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			JobIDs []string `json:"job_ids"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if len(body.JobIDs) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "job_ids must be a non-empty list"})
			return
		}

		removed := []string{}
		for _, jobID := range body.JobIDs {
			if err := h.fileQ.DeleteJob(c.Request.Context(), jobID); err == nil {
				removed = append(removed, jobID)
			}
		}

		c.JSON(http.StatusOK, gin.H{
			"status":        "ok",
			"removed_count": len(removed),
			"removed_ids":   removed,
		})
	}
}

func (h *JobSubmissionHandler) RetryJobHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Param("id")

		job, err := h.fileQ.GetJob(c.Request.Context(), jobID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "Job not found"})
			return
		}

		if job.Status != queue.StatusError && job.Status != queue.StatusFailed {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Can only retry ERROR or FAILED jobs"})
			return
		}

		jobs, _ := h.fileQ.GetAllJobs(c.Request.Context())
		if j, exists := jobs[jobID]; exists {
			j.Status = queue.StatusPending
			j.LastError = ""
			j.ErrorMessage = ""
			j.AssignedTo = ""
			j.ClaimedBy = ""
			now := time.Now().Unix()
			j.UpdatedAt = now
			j.History = append(j.History, queue.JobHistoryEntry{
				Status:    "PENDING",
				Timestamp: time.Now().UTC().Format(time.RFC3339),
				Message:   "Job manually retried",
			})
			jobs[jobID] = j
		}

		c.JSON(http.StatusOK, gin.H{
			"status":  "PENDING",
			"job_id":  jobID,
			"message": "Job queued for retry",
		})
	}
}

func (h *JobSubmissionHandler) GetJobsDashboardHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		limit := 50
		if l := c.Query("limit"); l != "" {
			if n, err := strconv.Atoi(l); err == nil && n >= 1 && n <= 200 {
				limit = n
			}
		}

		stats, err := h.fileQ.Stats(c.Request.Context())
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		jobs, _ := h.fileQ.GetAllJobs(c.Request.Context())

		recent := map[string][]map[string]interface{}{
			"PENDING":    {},
			"PROCESSING": {},
			"COMPLETED":  {},
			"ERROR":      {},
		}

		keepFields := []string{"job_id", "video_name", "status", "created_at", "updated_at",
			"completed_at", "started_at", "processing_at", "assigned_at",
			"assigned_to", "assigned_worker_ip", "error", "error_message",
			"last_error", "drive_url", "remote_status", "last_drive_upload_result"}

		for _, job := range jobs {
			status := string(job.Status)
			if status == "FAILED" {
				status = "ERROR"
			}

			if arr, ok := recent[status]; ok && len(arr) < limit {
				trimmed := map[string]interface{}{
					"job_id":     job.JobID,
					"video_name": job.VideoName,
					"status":     status,
					"created_at": job.CreatedAt,
					"updated_at": job.UpdatedAt,
				}
				for _, f := range keepFields {
					switch f {
					case "assigned_to":
						trimmed[f] = job.AssignedTo
					case "last_error":
						trimmed[f] = job.LastError
					case "drive_url":
						trimmed[f] = job.DriveURL
					}
				}
				if trimmed["video_name"] == "" && job.SlotData != nil {
					if vn, ok := job.SlotData["video_name"].(string); ok {
						trimmed["video_name"] = vn
					}
				}
				recent[status] = append(arr, trimmed)
			}
		}

		c.JSON(http.StatusOK, gin.H{
			"counts":    stats,
			"recent":    recent,
			"timestamp": time.Now().Unix(),
		})
	}
}
