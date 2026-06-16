package uploads

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

func slugify(s string) string {
	reg := regexp.MustCompile(`[^a-zA-Z0-9]+`)
	return strings.ToLower(reg.ReplaceAllString(s, "-"))
}

// UploadCompletedVideo handles video file upload from workers.
// POST /api/v1/video/upload-completed
func UploadCompletedVideo(cfg *config.Config, fileQ *queue.FileQueue, youtubeService *ytservice.Service) gin.HandlerFunc {
	videosDir := cfg.VideosDir
	if videosDir == "" {
		videosDir = "./completed_videos"
	}

	return func(c *gin.Context) {
		// Parse multipart form
		file, header, err := c.Request.FormFile("video")
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
			"video_path":      videoPath,
			"video_size":      size,
			"video_sha256":    sha256Hash,
			"video_filename":  safeName,
			"worker_id":       workerID,
			"uploaded_at":     now,
			"master_video_path": videoPath,
			"result_path_worker": videoPath,
		}

		updateFields := map[string]interface{}{
			"status":                     "COMPLETED",
			"completed_at":               now,
			"result_path":                videoPath,
			"result_path_worker":         videoPath,
			"master_video_path":          videoPath,
			"upload_info":                uploadInfo,
			"video_sha256":               sha256Hash,
			"youtube_upload_status":      "pending",
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

		// Create artifact record
		artifact := map[string]interface{}{
			"type":       "video",
			"path":       videoPath,
			"size":       size,
			"sha256":     sha256Hash,
			"filename":   safeName,
			"created_at": now,
		}
		if artifactBytes, err := json.Marshal(artifact); err == nil {
			_ = fileQ.UpdateJobFields(ctx, jobID, map[string]interface{}{
				"artifacts": string(artifactBytes),
			})
		} else {
			log.Printf("[UPLOAD] Failed to marshal artifact for %s: %v", jobID, err)
		}

		// Trigger YouTube auto-upload (async, best-effort)
		maybeAutoUploadYouTube(fileQ, youtubeService, jobID, uploadInfo, videoPath)

		log.Printf("[UPLOAD] Video upload completed: job=%s worker=%s size=%d sha256=%s",
			jobID, workerID, size, sha256Hash[:min(16, len(sha256Hash))]+"...")

		c.JSON(http.StatusOK, gin.H{
			"ok":           true,
			"job_id":       jobID,
			"video_path":   videoPath,
			"size":         size,
			"sha256":       sha256Hash,
			"video_id":     safeName,
			"youtube_url":  "",
		})
	}
}

// maybeAutoUploadYouTube checks job metadata and if conditions are met,
// triggers a YouTube auto-upload for the completed video.
func maybeAutoUploadYouTube(fileQ *queue.FileQueue, youtubeService *ytservice.Service, jobID string, uploadInfo map[string]interface{}, videoPath string) {
	if fileQ == nil || youtubeService == nil || strings.TrimSpace(jobID) == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	job, err := fileQ.GetJobAsMap(ctx, jobID)
	if err != nil || job == nil {
		log.Printf("[UPLOAD] YouTube auto-upload skipped for %s: job not found (%v)", jobID, err)
		return
	}

	if shouldSkipYouTubeUpload(job) {
		return
	}

	groupName := firstNonEmptyString(uploadInfo,
		asString(uploadInfo["youtube_group"]),
		asString(job["youtube_group"]),
		asStringFromSlot(job, "youtube_group"),
	)
	if groupName == "" {
		return
	}

	language := firstNonEmptyString(uploadInfo,
		asString(uploadInfo["audio_language_for_srt"]),
		asString(uploadInfo["target_language"]),
		asString(uploadInfo["language"]),
		asString(uploadInfo["voice_language"]),
		asString(uploadInfo["lang"]),
		asString(job["audio_language_for_srt"]),
		asString(job["target_language"]),
		asString(job["language"]),
		asString(job["voice_language"]),
		asString(job["lang"]),
		asStringFromSlot(job, "audio_language_for_srt"),
		asStringFromSlot(job, "target_language"),
		asStringFromSlot(job, "language"),
		asStringFromSlot(job, "voice_language"),
		asStringFromSlot(job, "lang"),
	)
	if language == "" {
		language = "en"
	}

	if strings.TrimSpace(videoPath) == "" {
		videoPath = firstNonEmptyString(uploadInfo,
			asString(uploadInfo["master_video_path"]),
			asString(uploadInfo["result_path_worker"]),
			asString(uploadInfo["result_path"]),
			asString(job["master_video_path"]),
			asString(job["result_path_worker"]),
			asString(job["result_path"]),
		)
	}
	if strings.TrimSpace(videoPath) == "" {
		log.Printf("[UPLOAD] YouTube auto-upload skipped for %s: missing video path", jobID)
		return
	}
	if _, statErr := os.Stat(videoPath); statErr != nil {
		log.Printf("[UPLOAD] YouTube auto-upload skipped for %s: video not found at %s (%v)", jobID, videoPath, statErr)
		return
	}

	title := firstNonEmptyString(uploadInfo,
		asString(uploadInfo["video_name"]),
		asString(uploadInfo["title"]),
		asString(job["video_name"]),
		asString(job["title"]),
	)
	if title == "" {
		title = jobID
	}

	description := firstNonEmptyString(uploadInfo,
		asString(uploadInfo["script_text"]),
		asString(uploadInfo["source_text"]),
		asString(job["script_text"]),
		asString(job["source_text"]),
	)

	privacy := firstNonEmptyString(uploadInfo,
		asString(uploadInfo["privacy_status"]),
		asString(uploadInfo["privacy"]),
		asString(job["privacy_status"]),
		asString(job["privacy"]),
	)
	if privacy == "" {
		privacy = "private"
	}

	tags := mergeStringSlices(
		asStringSlice(uploadInfo["tags"]),
		asStringSlice(job["tags"]),
		asStringSliceFromSlot(job, "tags"),
	)

	jobRunID := firstNonEmptyString(uploadInfo,
		asString(uploadInfo["job_run_id"]),
		asString(uploadInfo["run_id"]),
		asString(job["job_run_id"]),
		asString(job["run_id"]),
	)

	now := time.Now().UTC().Format(time.RFC3339)
	_ = fileQ.UpdateJobFields(ctx, jobID, map[string]interface{}{
		"youtube_upload_status": "scheduled",
		"youtube_upload_at":     now,
	})

	go func() {
		uploadCtx, uploadCancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer uploadCancel()

		channel, chErr := youtubeService.ResolveChannelByLanguage(groupName, language)
		if chErr != nil {
			_ = fileQ.UpdateJobFields(uploadCtx, jobID, map[string]interface{}{
				"youtube_upload_status": "failed",
				"last_youtube_upload_result": map[string]interface{}{
					"success":     false,
					"error":       chErr.Error(),
					"group":       groupName,
					"language":    language,
					"uploaded_at": time.Now().UTC().Format(time.RFC3339),
				},
			})
			log.Printf("[UPLOAD] YouTube auto-upload failed for %s: resolve channel: %v", jobID, chErr)
			return
		}

		health, healthErr := youtubeService.HealthCheck(uploadCtx, channel.ID)
		if healthErr != nil {
			health = map[string]interface{}{
				"ok":    false,
				"error": healthErr.Error(),
			}
		}
		if ok, _ := health["ok"].(bool); !ok {
			errMsg, _ := health["error"].(string)
			if errMsg == "" {
				errMsg = "YouTube channel authentication is not ready"
			}
			_ = fileQ.UpdateJobFields(uploadCtx, jobID, map[string]interface{}{
				"youtube_upload_status": "needs_reauth",
				"last_youtube_upload_result": map[string]interface{}{
					"success":      false,
					"error":        errMsg,
					"group":        groupName,
					"language":     language,
					"channel_id":   channel.ID,
					"channel_name": channel.Name,
					"job_run_id":   jobRunID,
					"uploaded_at":  time.Now().UTC().Format(time.RFC3339),
				},
			})
			log.Printf("[UPLOAD] YouTube auto-upload deferred for %s: %s", jobID, errMsg)
			return
		}

		result, uploadErr := youtubeService.UploadVideo(uploadCtx, channel.ID, videoPath, ytservice.UploadConfig{
			Title:         title,
			Description:   description,
			Tags:          tags,
			PrivacyStatus: privacy,
			ChannelID:     channel.ID,
			ChannelName:   channel.Name,
		})
		if uploadErr != nil {
			_ = fileQ.UpdateJobFields(uploadCtx, jobID, map[string]interface{}{
				"youtube_upload_status": "failed",
				"last_youtube_upload_result": map[string]interface{}{
					"success":      false,
					"error":        uploadErr.Error(),
					"group":        groupName,
					"language":     language,
					"channel_id":   channel.ID,
					"channel_name": channel.Name,
					"job_run_id":   jobRunID,
					"uploaded_at":  time.Now().UTC().Format(time.RFC3339),
				},
			})
			log.Printf("[UPLOAD] YouTube auto-upload failed for %s: %v", jobID, uploadErr)
			return
		}

		update := map[string]interface{}{
			"youtube_upload_status":    "completed",
			"youtube_url":              result.YouTubeURL,
			"youtube_video_id":         result.VideoID,
			"youtube_channel_id":       channel.ID,
			"youtube_channel_name":     channel.Name,
			"youtube_channel_language": channel.Language,
			"last_youtube_upload_result": map[string]interface{}{
				"success":      true,
				"youtube_url":  result.YouTubeURL,
				"video_id":     result.VideoID,
				"channel_id":   channel.ID,
				"channel_name": channel.Name,
				"group":        groupName,
				"language":     language,
				"job_run_id":   jobRunID,
				"uploaded_at":  time.Now().UTC().Format(time.RFC3339),
				"privacy":      privacy,
			},
		}
		if err := fileQ.UpdateJobFields(uploadCtx, jobID, update); err != nil {
			log.Printf("[UPLOAD] YouTube auto-upload persisted with warning for %s: %v", jobID, err)
		}
		log.Printf("[UPLOAD] YouTube auto-upload completed for %s -> %s", jobID, result.YouTubeURL)
	}()
}

func shouldSkipYouTubeUpload(job map[string]interface{}) bool {
	if job == nil {
		return true
	}
	if strings.TrimSpace(asString(job["youtube_url"])) != "" {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(asString(job["youtube_upload_status"])), "scheduled") {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(asString(job["youtube_upload_status"])), "uploading") {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(asString(job["youtube_upload_status"])), "completed") {
		return true
	}
	if result, ok := job["last_youtube_upload_result"].(map[string]interface{}); ok {
		if success, _ := result["success"].(bool); success {
			return true
		}
	}
	return false
}

func firstNonEmptyString(_ map[string]interface{}, values ...string) string {
	for _, v := range values {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}

func asString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func asStringFromSlot(job map[string]interface{}, key string) string {
	if slot, ok := job["slot_data"].(map[string]interface{}); ok {
		return asString(slot[key])
	}
	return ""
}

func asStringSlice(v interface{}) []string {
	switch val := v.(type) {
	case []string:
		return append([]string(nil), val...)
	case []interface{}:
		out := make([]string, 0, len(val))
		for _, item := range val {
			if s := strings.TrimSpace(asString(item)); s != "" {
				out = append(out, s)
			}
		}
		return out
	case string:
		parts := strings.Split(val, ",")
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			if s := strings.TrimSpace(part); s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func asStringSliceFromSlot(job map[string]interface{}, key string) []string {
	if slot, ok := job["slot_data"].(map[string]interface{}); ok {
		return asStringSlice(slot[key])
	}
	return nil
}

func mergeStringSlices(lists ...[]string) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for _, list := range lists {
		for _, item := range list {
			s := strings.TrimSpace(item)
			if s == "" {
				continue
			}
			if _, ok := seen[s]; ok {
				continue
			}
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}
