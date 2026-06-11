package workers

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	"velox-server/internal/queue"
)

// CompleteJobEnhanced handles job completion with pending upload validation
// Matches Python /complete_job behavior:
// - Validates video upload via pending_uploads
// - Returns 409 if video not received
// - Triggers YouTube/Drive upload workflow
func CompleteJobEnhanced(cfg *config.Config, fileQ *queue.FileQueue) gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Query("job_id")
		workerID := c.Query("worker_id")
		videoUploaded := c.Query("video_uploaded") == "true"
		resultPath := c.Query("result_path")
		jobRunID := c.Query("job_run_id")

		if jobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "job_id required"})
			return
		}

		// Parse upload_info from body if present
		var uploadInfo map[string]interface{}
		if c.Request.Body != nil {
			var body struct {
				UploadInfo map[string]interface{} `json:"upload_info"`
			}
			if err := c.ShouldBindJSON(&body); err == nil {
				uploadInfo = body.UploadInfo
			}
		}

		// Check pending uploads
		pending := globalUploadManager.GetPendingUpload(jobID)

		// Get current job as map for flexible field access
		job, err := fileQ.GetJobAsMap(c.Request.Context(), jobID)
		if err != nil || job == nil {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": fmt.Sprintf("Job %s non trovato", jobID)})
			return
		}
		if jobRunID == "" {
			if v, ok := job["job_run_id"].(string); ok && strings.TrimSpace(v) != "" {
				jobRunID = strings.TrimSpace(v)
			} else if v, ok := job["run_id"].(string); ok && strings.TrimSpace(v) != "" {
				jobRunID = strings.TrimSpace(v)
			}
		}

		// Check allow_complete_without_video flag
		allowCompleteWithoutVideo := false
		if v, ok := job["allow_complete_without_video"].(bool); ok {
			allowCompleteWithoutVideo = v
		}

		// Validate video upload
		if !videoUploaded && pending == nil && !allowCompleteWithoutVideo {
			c.JSON(http.StatusConflict, gin.H{
				"ok":    false,
				"error": "Video non ricevuto dal master (video_uploaded=false e pending_uploads mancante).",
			})
			return
		}

		if !videoUploaded && pending == nil && allowCompleteWithoutVideo {
			log.Printf("[WARN] complete_job senza video accettato per job %s (workflow script/voiceover)", jobID)
		}

		// Check idempotency on job_run_id
		completedRuns := []string{}
		if v, ok := job["completed_job_run_ids"].([]interface{}); ok {
			for _, r := range v {
				if rs, ok := r.(string); ok {
					completedRuns = append(completedRuns, rs)
				}
			}
		}
		if jobRunID != "" {
			for _, r := range completedRuns {
				if r == jobRunID {
					c.JSON(http.StatusOK, gin.H{
						"success":          true,
						"job_id":           jobID,
						"idempotent":       true,
						"upload_scheduled": false,
					})
					return
				}
			}
		}

		// Update job
		now := time.Now().UTC().Format(time.RFC3339)
		updates := map[string]interface{}{
			"status":         "COMPLETED",
			"completed_at":   now,
			"assigned_to":    workerID,
			"video_uploaded": videoUploaded,
		}
		if resultPath != "" {
			updates["result_path"] = resultPath
		}
		if uploadInfo != nil {
			updates["upload_info"] = uploadInfo
		}

		// Update history - convert from job map
		history := []queue.JobHistoryEntry{}
		if v, ok := job["history"].([]interface{}); ok {
			for _, h := range v {
				if hm, ok := h.(map[string]interface{}); ok {
					entry := queue.JobHistoryEntry{}
					if s, ok := hm["status"].(string); ok {
						entry.Status = s
					}
					if t, ok := hm["timestamp"]; ok {
						entry.Timestamp = t
					}
					if s, ok := hm["worker_id"].(string); ok {
						entry.WorkerID = s
					}
					if s, ok := hm["message"].(string); ok {
						entry.Message = s
					}
					history = append(history, entry)
				}
			}
		}
		// Add new history entry
		history = append(history, queue.JobHistoryEntry{
			Status:    "COMPLETED",
			Timestamp: now,
			WorkerID:  workerID,
			Message:   "Job completed by worker",
		})
		updates["history"] = history

		// Update completed_job_run_ids
		if jobRunID != "" {
			completedRuns = append(completedRuns, jobRunID)
			// Limit to last 50
			if len(completedRuns) > 50 {
				completedRuns = completedRuns[len(completedRuns)-50:]
			}
			updates["completed_job_run_ids"] = completedRuns
		}

		if err := fileQ.UpdateJobFields(c.Request.Context(), jobID, updates); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}

		// Check if we should schedule YouTube/Drive upload
		uploadScheduled := false
		if pending != nil {
			log.Printf("[JOB] complete_job: trovato file in pending per job %s", jobID)
			ytGroup, _ := pending.UploadInfo["youtube_group"].(string)
			if ytGroup != "" {
				log.Printf("[UPLOAD] Upload schedulato: verrà tentato YouTube (youtube_group=%s)", ytGroup)
				uploadScheduled = true
			}
		}

		c.JSON(http.StatusOK, gin.H{
			"success":          true,
			"job_id":           jobID,
			"upload_scheduled": uploadScheduled,
		})
	}
}
