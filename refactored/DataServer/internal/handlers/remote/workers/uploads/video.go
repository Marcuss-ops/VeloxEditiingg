package uploads

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/queue"
	"velox-server/internal/store"
)

// UploadCompletedVideo handles video file upload from workers.
// POST /api/v1/video/upload-completed
func UploadCompletedVideo(cfg *config.Config, fileQ *queue.FileQueue, youtubeService YouTubeAutoUploader, driveService DriveAutoUploader) gin.HandlerFunc {
	videosDir := cfg.VideosDir
	if videosDir == "" {
		videosDir = "./completed_videos"
	}

	return func(c *gin.Context) {
		// Parse multipart form
		file, header, err := c.Request.FormFile("video")
		if err != nil {
			file, header, err = c.Request.FormFile("video_file")
		}
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "Video file is required"})
			return
		}
		defer file.Close()

		jobID := c.PostForm("job_id")
		workerID := c.PostForm("worker_id")

		if jobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "job_id is required"})
			return
		}

		// Ensure videos directory exists
		if err := os.MkdirAll(videosDir, 0755); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "failed to create videos directory"})
			return
		}

		// Generate unique filename
		ext := filepath.Ext(header.Filename)
		if ext == "" {
			ext = ".mp4"
		}
		safeName := slugify(jobID) + "_" + fmt.Sprintf("%d", time.Now().Unix()) + ext
		videoPath := filepath.Join(videosDir, safeName)

		// Save uploaded file
		out, err := os.Create(videoPath)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "Failed to save uploaded file"})
			return
		}

		hasher := sha256.New()
		writer := io.MultiWriter(out, hasher)
		size, err := io.Copy(writer, file)
		out.Close()

		if err != nil {
			os.Remove(videoPath)
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "Failed to write video file"})
			return
		}

		sha256Hash := hex.EncodeToString(hasher.Sum(nil))

		// Update job with video info
		ctx := c.Request.Context()
		now := time.Now().UTC().Format(time.RFC3339)

		uploadInfo := map[string]interface{}{
			"video_path":         videoPath,
			"video_size":         size,
			"video_sha256":       sha256Hash,
			"video_filename":     safeName,
			"worker_id":          workerID,
			"uploaded_at":        now,
			"master_video_path":  videoPath,
			"result_path_worker": videoPath,
		}

		updateFields := map[string]interface{}{
			"status":                "COMPLETED",
			"completed_at":          now,
			"result_path":           videoPath,
			"result_path_worker":    videoPath,
			"master_video_path":     videoPath,
			"upload_info":           uploadInfo,
			"video_sha256":          sha256Hash,
			"youtube_upload_status": "pending", // DEPRECATED: kept for backward compat
		}

		if workerID != "" {
			updateFields["worker_id"] = workerID
		}

		if err := fileQ.UpdateJobFields(ctx, jobID, updateFields); err != nil {
			log.Printf("[UPLOAD] Failed to update job %s: %v", jobID, err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"ok":    false,
				"error": "Failed to update job status",
			})
			return
		}

		// Create artifact record via the artifacts table (PR4: normalized artifact tracking)
		dbStore := fileQ.GetDBStore()
		if dbStore != nil {
			artifact := &store.Artifact{
				JobID:           jobID,
				Type:            "video",
				StorageProvider: "local",
				LocalPath:       videoPath,
				SHA256:          sha256Hash,
				SizeBytes:       size,
				Status:          "completed",
				CreatedAt:       now,
			}
			if err := dbStore.InsertArtifact(artifact); err != nil {
				log.Printf("[UPLOAD] Failed to insert artifact for %s: %v", jobID, err)
			}
		}

		// Query delivery targets (PR4: targets resolved at enqueue time)
		deliveryTargets := getDeliveryTargets(dbStore, jobID)

		// Trigger YouTube auto-upload (async, best-effort) with resolved targets
		maybeAutoUploadYouTube(fileQ, youtubeService, jobID, uploadInfo, videoPath, deliveryTargets)

		// Trigger Drive auto-upload (async, best-effort) with resolved targets
		maybeAutoUploadDrive(fileQ, driveService, cfg.DataDir, jobID, uploadInfo, videoPath, deliveryTargets)

		log.Printf("[UPLOAD] Video upload completed: job=%s worker=%s size=%d sha256=%s",
			jobID, workerID, size, sha256Hash[:min(16, len(sha256Hash))]+"...")

		c.JSON(http.StatusOK, gin.H{
			"ok":         true,
			"job_id":     jobID,
			"video_path": videoPath,
			"size":       size,
			"sha256":     sha256Hash,
			"video_id":   safeName,
		})
	}
}

// getDeliveryTargets queries delivery_targets for a job (PR4).
// Falls back gracefully if the store is nil or table doesn't exist yet.
func getDeliveryTargets(dbStore *store.SQLiteStore, jobID string) []store.DeliveryTarget {
	if dbStore == nil {
		return nil
	}
	targets, err := dbStore.GetDeliveryTargetsByJob(jobID)
	if err != nil {
		log.Printf("[UPLOAD] Failed to query delivery targets for %s: %v", jobID, err)
		return nil
	}
	return targets
}
