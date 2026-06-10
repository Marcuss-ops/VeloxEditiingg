package workers

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	"velox-server/internal/queue"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// Global upload manager instance
var globalUploadManager = NewUploadManager()

// PendingUpload tracks a video waiting to be uploaded
type PendingUpload struct {
	VideoPath  string                 `json:"video_path"`
	WorkerID   string                 `json:"worker_id"`
	JobRunID   string                 `json:"job_run_id"`
	UploadInfo map[string]interface{} `json:"upload_info"`
	ReceivedAt time.Time              `json:"received_at"`
}

// UploadManager tracks pending uploads
type UploadManager struct {
	mu    sync.RWMutex
	files map[string]*PendingUpload // job_id -> pending upload
}

// NewUploadManager creates a new upload manager
func NewUploadManager() *UploadManager {
	return &UploadManager{
		files: make(map[string]*PendingUpload),
	}
}

// AddPendingUpload adds a pending upload
func (um *UploadManager) AddPendingUpload(jobID string, upload *PendingUpload) {
	um.mu.Lock()
	defer um.mu.Unlock()
	um.files[jobID] = upload
}

// GetPendingUpload gets a pending upload
func (um *UploadManager) GetPendingUpload(jobID string) *PendingUpload {
	um.mu.RLock()
	defer um.mu.RUnlock()
	return um.files[jobID]
}

// RemovePendingUpload removes a pending upload
func (um *UploadManager) RemovePendingUpload(jobID string) {
	um.mu.Lock()
	defer um.mu.Unlock()
	delete(um.files, jobID)
}

// HeartbeatBody matches Python: worker_id, worker_name, status, recent_logs, recent_errors, readiness, etc.
func Heartbeat(reg *workersreg.Registry) gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID   string                 `json:"worker_id"`
			WorkerName string                 `json:"worker_name"`
			Status     string                 `json:"status"`
			CurrentJob string                 `json:"current_job"`
			Extra      map[string]interface{} `json:"extra"`
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
		if err := reg.Heartbeat(c.Request.Context(), body.WorkerID, body.WorkerName, body.Status, body.CurrentJob, body.Extra); err != nil {
			log.Printf("workers/heartbeat: heartbeat failed for %s: %v", body.WorkerID, err)
		}
		c.JSON(http.StatusOK, gin.H{"ok": true})
	}
}

// WorkersList same response shape as Python GET /workers
func WorkersList(reg *workersreg.Registry, workersRepo store.WorkersRepository) gin.HandlerFunc {
	return func(c *gin.Context) {
		if workersRepo != nil {
			if dbWorkers, err := workersRepo.ListWorkers(); err == nil && len(dbWorkers) > 0 {
				c.JSON(http.StatusOK, gin.H{"workers": dbWorkers})
				return
			}
		}
		list := reg.List(c.Request.Context())
		c.JSON(http.StatusOK, gin.H{"workers": list})
	}
}

// WorkersStatus returns same shape as Python GET /workers_status for installer/dashboard
func WorkersStatus(reg *workersreg.Registry, q *queue.Queue) gin.HandlerFunc {
	const heartbeatTimeoutSec = 900 // 15 min like Python
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		list := reg.List(ctx)
		now := time.Now().UTC()
		var workersList []gin.H
		activeCount := 0
		for _, w := range list {
			var since float64
			if w.LastHB != "" {
				if t, err := time.Parse(time.RFC3339, w.LastHB); err == nil {
					since = now.Sub(t.UTC()).Seconds()
				}
			}
			active := since < heartbeatTimeoutSec
			if active {
				activeCount++
			}
			workersList = append(workersList, gin.H{
				"worker_id":            w.WorkerID,
				"worker_name":          w.WorkerName,
				"display_name":         w.WorkerName,
				"name":                 w.WorkerName,
				"status":               w.Status,
				"last_heartbeat":       w.LastHB,
				"time_since_heartbeat": since,
				"active":               active,
				"current_job":          w.CurrentJob,
			})
		}
		pending, _ := q.ReadyCount(ctx)
		processing, _ := q.LeasedCount(ctx)
		c.JSON(http.StatusOK, gin.H{
			"workers":         workersList,
			"active_workers":  activeCount,
			"total_workers":   len(workersList),
			"pending_jobs":    pending,
			"processing_jobs": processing,
			"completed_jobs":  0,
			"error_jobs":      0,
			"total_jobs":      pending + processing,
		})
	}
}

// slugify converts a string to a safe filename
func slugify(s string) string {
	// Trim whitespace
	s = strings.TrimSpace(s)
	// Replace multiple spaces with single space
	re := regexp.MustCompile(`\s+`)
	s = re.ReplaceAllString(s, " ")
	// Remove non-alphanumeric chars except dash, underscore, dot, space
	re2 := regexp.MustCompile(`[^\w\-. ]`)
	s = re2.ReplaceAllString(s, "")
	// Replace spaces with underscores
	s = strings.ReplaceAll(s, " ", "_")
	// Limit length
	if len(s) > 120 {
		s = s[:120]
	}
	if s == "" {
		return "video"
	}
	return s
}

// UploadCompletedVideo handles video file upload from workers
// Enhanced version matching Python implementation:
// - Accepts upload_info JSON form field
// - Marks job as COMPLETED in file queue
// - Tracks in pending_uploads for YouTube/Drive upload workflow
// - Supports video naming with video_name from job spec
func UploadCompletedVideo(cfg *config.Config, fileQ *queue.FileQueue) gin.HandlerFunc {
	videosDir := cfg.VideosDir
	if videosDir == "" {
		videosDir = "./completed_videos"
	}

	return func(c *gin.Context) {
		log.Printf("🔔 RICEVUTA richiesta /upload_completed_video")

		// Ensure directory exists
		if err := os.MkdirAll(videosDir, 0755); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "failed to create videos directory"})
			return
		}

		// Get form data
		jobID := c.PostForm("job_id")
		workerID := c.PostForm("worker_id")
		uploadInfoStr := c.PostForm("upload_info")
		jobRunID := c.PostForm("job_run_id")

		if jobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "job_id required"})
			return
		}

		log.Printf("📥 Ricezione video da worker %s per job %s", workerID, jobID)

		// Parse upload_info if present
		var uploadInfo map[string]interface{}
		if uploadInfoStr != "" {
			if err := json.Unmarshal([]byte(uploadInfoStr), &uploadInfo); err != nil {
				log.Printf("⚠️ Impossibile parsare upload_info: %v", err)
			}
		}

		// Get job from queue to extract video_name and other metadata
		jobData, _ := fileQ.GetJobAsMap(c.Request.Context(), jobID)

		// Build canonical upload info from job spec (source of truth)
		canonicalUploadInfo := make(map[string]interface{})
		if uploadInfo != nil {
			// Copy existing upload_info
			for k, v := range uploadInfo {
				canonicalUploadInfo[k] = v
			}
		}
		if jobData != nil {
			// Override with job spec values
			if v, ok := jobData["youtube_group"]; ok && canonicalUploadInfo["youtube_group"] == nil {
				canonicalUploadInfo["youtube_group"] = v
			}
			if v, ok := jobData["video_name"]; ok && canonicalUploadInfo["video_name"] == nil {
				canonicalUploadInfo["video_name"] = v
			}
			if v, ok := jobData["output_video_id"]; ok && canonicalUploadInfo["output_video_id"] == nil {
				canonicalUploadInfo["output_video_id"] = v
			}
			if v, ok := jobData["output_video_mapping"]; ok && canonicalUploadInfo["output_video_mapping"] == nil {
				canonicalUploadInfo["output_video_mapping"] = v
			}
			if v, ok := jobData["voiceover_channel_mapping"]; ok && canonicalUploadInfo["voiceover_channel_mapping"] == nil {
				canonicalUploadInfo["voiceover_channel_mapping"] = v
			}
			if jobRunID == "" {
				if v, ok := jobData["job_run_id"].(string); ok && strings.TrimSpace(v) != "" {
					jobRunID = strings.TrimSpace(v)
				} else if v, ok := jobData["run_id"].(string); ok && strings.TrimSpace(v) != "" {
					jobRunID = strings.TrimSpace(v)
				}
			}
		}

		// Set max multipart memory to 10MB (larger files stream to disk)
		c.Request.ParseMultipartForm(10 << 20) // 10 MB

		// Set max multipart memory to 10MB (larger files stream to disk)
		c.Request.ParseMultipartForm(10 << 20) // 10 MB

		// Get the file
		file, header, err := c.Request.FormFile("video_file")
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "video_file required"})
			return
		}
		defer file.Close()

		// Extract extension
		ext := filepath.Ext(header.Filename)
		if ext == "" {
			ext = ".mp4"
		}

		// Build sensible filename using video_name if available
		videoName := "video"
		if v, ok := canonicalUploadInfo["video_name"].(string); ok && v != "" {
			videoName = v
		} else if jobData != nil {
			if v, ok := jobData["video_name"].(string); ok && v != "" {
				videoName = v
			}
		}
		niceName := slugify(videoName)

		outputVideoID := jobID
		if v, ok := canonicalUploadInfo["output_video_id"].(string); ok && v != "" {
			outputVideoID = v
		} else if jobData != nil {
			if v, ok := jobData["output_video_id"].(string); ok && v != "" {
				outputVideoID = v
			}
		}

		// Build filename: nice_name_outputVideoID_jobRunID.ext
		var videoFilename string
		if jobRunID != "" {
			videoFilename = fmt.Sprintf("%s_%s_%s%s", niceName, outputVideoID, jobRunID, ext)
		} else {
			videoFilename = fmt.Sprintf("%s_%s%s", niceName, outputVideoID, ext)
		}

		// Create temp file first (atomic write)
		tempName := fmt.Sprintf(".tmp_%s_%d%s", jobID, time.Now().Unix(), ext)
		tempPath := filepath.Join(videosDir, tempName)

		out, err := os.Create(tempPath)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "failed to create temp file"})
			return
		}

		// Copy file content
		written, err := io.Copy(out, file)
		if err != nil {
			out.Close()
			os.Remove(tempPath)
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "failed to save video"})
			return
		}
		out.Close()

		// Rename to final name (atomic)
		finalPath := filepath.Join(videosDir, videoFilename)
		if err := os.Rename(tempPath, finalPath); err != nil {
			os.Remove(tempPath)
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "failed to save video"})
			return
		}

		fileSize := written / (1024 * 1024) // MB
		log.Printf("✅ Video salvato: %s (%d MB)", finalPath, fileSize)

		// Mark job as COMPLETED in file queue
		if fileQ != nil {
			now := time.Now().UTC().Format(time.RFC3339)
			updates := map[string]interface{}{
				"status":             "COMPLETED",
				"completed_at":       now,
				"completed_by":       workerID,
				"video_uploaded":     true,
				"master_video_path":  finalPath,
				"result_path_worker": finalPath,
				"job_run_id":         jobRunID,
			}
			if err := fileQ.UpdateJobFields(c.Request.Context(), jobID, updates); err != nil {
				log.Printf("⚠️ Impossibile marcare COMPLETED il job %s: %v", jobID, err)
			} else {
				log.Printf("✅ Job %s marcato COMPLETED via upload_completed_video", jobID)
			}
		}

		// Add to pending uploads for YouTube/Drive workflow
		globalUploadManager.AddPendingUpload(jobID, &PendingUpload{
			VideoPath:  finalPath,
			WorkerID:   workerID,
			JobRunID:   jobRunID,
			UploadInfo: canonicalUploadInfo,
			ReceivedAt: time.Now(),
		})
		log.Printf("💾 Info upload salvate in pending_uploads per job %s", jobID)
		if ytGroup, ok := canonicalUploadInfo["youtube_group"].(string); ok && ytGroup != "" {
			log.Printf("   YouTube group: %s", ytGroup)
		}
		if vidName, ok := canonicalUploadInfo["video_name"].(string); ok && vidName != "" {
			log.Printf("   Video name: %s", vidName)
		}

		c.JSON(http.StatusOK, gin.H{
			"success":     true,
			"message":     fmt.Sprintf("Video ricevuto e salvato: %s", videoFilename),
			"job_id":      jobID,
			"video_path":  finalPath,
			"upload_info": canonicalUploadInfo,
		})
	}
}

// GetUploadManager returns the global upload manager
func GetUploadManager() *UploadManager {
	return globalUploadManager
}

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
			log.Printf("⚠️ complete_job senza video accettato per job %s (workflow script/voiceover)", jobID)
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
			log.Printf("📤 complete_job: trovato file in pending per job %s", jobID)
			ytGroup, _ := pending.UploadInfo["youtube_group"].(string)
			if ytGroup != "" {
				log.Printf("📺 Upload schedulato: verrà tentato YouTube (youtube_group=%s)", ytGroup)
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
