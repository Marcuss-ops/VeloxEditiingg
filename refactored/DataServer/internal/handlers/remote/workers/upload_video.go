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
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	ytservice "velox-server/internal/integrations/youtube"
	"velox-server/internal/queue"
)

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
func UploadCompletedVideo(cfg *config.Config, fileQ *queue.FileQueue, youtubeService *ytservice.Service) gin.HandlerFunc {
	videosDir := cfg.VideosDir
	if videosDir == "" {
		videosDir = "./completed_videos"
	}

	return func(c *gin.Context) {
		log.Printf("[RECV] RICEVUTA richiesta /upload_completed_video")

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

		log.Printf("[RECV] Ricezione video da worker %s per job %s", workerID, jobID)

		// Parse upload_info if present
		var uploadInfo map[string]interface{}
		if uploadInfoStr != "" {
			if err := json.Unmarshal([]byte(uploadInfoStr), &uploadInfo); err != nil {
				log.Printf("[WARN] Impossibile parsare upload_info: %v", err)
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
		absFinalPath, absErr := filepath.Abs(finalPath)
		if absErr != nil {
			absFinalPath = finalPath
		}

		fileSize := written / (1024 * 1024) // MB
		log.Printf("[OK] Video salvato: %s (%d MB)", finalPath, fileSize)

		// Mark job as COMPLETED in file queue
		if fileQ != nil {
			now := time.Now().UTC().Format(time.RFC3339)
			updates := map[string]interface{}{
				"status":             "COMPLETED",
				"completed_at":       now,
				"completed_by":       workerID,
				"video_uploaded":     true,
				"master_video_path":  absFinalPath,
				"result_path_worker": absFinalPath,
				"job_run_id":         jobRunID,
			}
			if err := fileQ.UpdateJobFields(c.Request.Context(), jobID, updates); err != nil {
				log.Printf("[WARN] Impossibile marcare COMPLETED il job %s: %v (will be set by SubmitResult)", jobID, err)
			} else {
				log.Printf("[OK] Job %s marcato COMPLETED via upload_completed_video", jobID)
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
		log.Printf("[UPLOAD] Info upload salvate in pending_uploads per job %s", jobID)
		if ytGroup, ok := canonicalUploadInfo["youtube_group"].(string); ok && ytGroup != "" {
			log.Printf("   YouTube group: %s", ytGroup)
			maybeAutoUploadYouTube(fileQ, youtubeService, jobID, canonicalUploadInfo, finalPath)
		}
		if vidName, ok := canonicalUploadInfo["video_name"].(string); ok && vidName != "" {
			log.Printf("   Video name: %s", vidName)
		}

		c.JSON(http.StatusOK, gin.H{
			"success":     true,
			"message":     fmt.Sprintf("Video ricevuto e salvato: %s", videoFilename),
			"job_id":      jobID,
			"video_path":  absFinalPath,
			"upload_info": canonicalUploadInfo,
		})
	}
}

// WorkerAssetHandler serves master-staged media assets to remote workers.
type WorkerAssetHandler struct {
	dataDir string
}

func NewWorkerAssetHandler(cfg *config.Config) *WorkerAssetHandler {
	dataDir := ""
	if cfg != nil {
		dataDir = strings.TrimSpace(cfg.DataDir)
		if dataDir == "" {
			dataDir = strings.TrimSpace(cfg.Runtime.DataDir)
		}
	}
	return &WorkerAssetHandler{dataDir: dataDir}
}

func (h *WorkerAssetHandler) ServeVoiceoverAsset() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h == nil || strings.TrimSpace(h.dataDir) == "" {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "asset storage unavailable"})
			return
		}

		jobID := strings.TrimSpace(c.Param("job_id"))
		filename := strings.TrimSpace(c.Param("filename"))
		if jobID == "" || filename == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "job_id and filename required"})
			return
		}

		filename = filepath.Base(filename)
		if filename == "." || filename == string(filepath.Separator) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid filename"})
			return
		}

		filePath := filepath.Join(h.dataDir, "worker_downloads", "script_assets", jobID, filename)
		if !strings.HasPrefix(filepath.Clean(filePath), filepath.Clean(filepath.Join(h.dataDir, "worker_downloads", "script_assets"))) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid asset path"})
			return
		}

		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "asset not found"})
			return
		}

		c.File(filePath)
	}
}
