package videos

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-server/internal/integrations/youtube"

	"github.com/gin-gonic/gin"
)

func parseTags(tagsStr string) []string {
	if tagsStr == "" {
		return []string{}
	}
	return strings.Split(tagsStr, ",")
}

// UploadVideo uploads a video to YouTube
// POST /api/v1/youtube/upload
func (h *Handler) UploadVideo(c *gin.Context) {
	// Parse multipart form
	file, header, err := c.Request.FormFile("video")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "Video file is required"})
		return
	}
	defer file.Close()

	// Get form values
	channelID := c.PostForm("channel_id")
	title := c.PostForm("title")
	description := c.PostForm("description")
	tags := c.PostForm("tags")
	privacy := c.DefaultPostForm("privacy", "private")

	if channelID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "channel_id is required"})
		return
	}

	// Save uploaded file to temp
	tempDir := os.TempDir()
	tempFile := filepath.Join(tempDir, fmt.Sprintf("youtube_upload_%d_%s", time.Now().UnixNano(), header.Filename))

	if err := c.SaveUploadedFile(header, tempFile); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "Failed to save uploaded file"})
		return
	}
	defer os.Remove(tempFile)

	// Prepare upload config
	config := youtube.UploadConfig{
		Title:         title,
		Description:   description,
		Tags:          parseTags(tags),
		PrivacyStatus: privacy,
		ChannelID:     channelID,
	}

	// Upload to YouTube
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Minute)
	defer cancel()

	result, err := h.svc.UploadVideo(ctx, channelID, tempFile, config)
	if err != nil {
		log.Printf("[ERROR] Upload failed: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"ok":    false,
			"error": err.Error(),
		})
		return
	}

	// Clear cache after successful upload
	h.clearCache()

	c.JSON(http.StatusOK, gin.H{
		"ok":     true,
		"result": result,
	})
}

// UploadVideoFromPath uploads a video from a local path
// POST /api/v1/youtube/upload-path
func (h *Handler) UploadVideoFromPath(c *gin.Context) {
	var req struct {
		FilePath      string   `json:"file_path" binding:"required"`
		ChannelID     string   `json:"channel_id" binding:"required"`
		Title         string   `json:"title"`
		Description   string   `json:"description"`
		Tags          []string `json:"tags"`
		Privacy       string   `json:"privacy"`
		ThumbnailPath string   `json:"thumbnail_path"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
		return
	}

	// Check if file exists
	if _, err := os.Stat(req.FilePath); os.IsNotExist(err) {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "File not found"})
		return
	}

	// Prepare upload config
	privacy := req.Privacy
	if privacy == "" {
		privacy = "private"
	}

	config := youtube.UploadConfig{
		Title:         req.Title,
		Description:   req.Description,
		Tags:          req.Tags,
		PrivacyStatus: privacy,
		ChannelID:     req.ChannelID,
		ThumbnailPath: req.ThumbnailPath,
	}

	// Upload to YouTube
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Minute)
	defer cancel()

	result, err := h.svc.UploadVideo(ctx, req.ChannelID, req.FilePath, config)
	if err != nil {
		log.Printf("[ERROR] Upload failed: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"ok":    false,
			"error": err.Error(),
		})
		return
	}

	// Clear cache after successful upload
	h.clearCache()

	c.JSON(http.StatusOK, gin.H{
		"ok":     true,
		"result": result,
	})
}

// BatchUpload uploads multiple videos
// POST /api/v1/youtube/batch-upload
func (h *Handler) BatchUpload(c *gin.Context) {
	var req struct {
		Videos []struct {
			FilePath    string   `json:"file_path" binding:"required"`
			ChannelID   string   `json:"channel_id" binding:"required"`
			Title       string   `json:"title"`
			Description string   `json:"description"`
			Tags        []string `json:"tags"`
			Privacy     string   `json:"privacy"`
		} `json:"videos" binding:"required"`
		MinDelaySeconds int `json:"min_delay_seconds"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
		return
	}

	minDelay := 15 * time.Second
	if req.MinDelaySeconds > 0 {
		minDelay = time.Duration(req.MinDelaySeconds) * time.Second
	}

	results := make([]gin.H, 0, len(req.Videos))
	log.Printf("[UPLOAD] YouTube batch upload started: total=%d min_delay=%s", len(req.Videos), minDelay)

	for i, video := range req.Videos {
		startedAt := time.Now()
		log.Printf("[UPLOAD] Batch upload item %d/%d started: channel=%s file=%s title=%q", i+1, len(req.Videos), video.ChannelID, filepath.Base(video.FilePath), video.Title)

		// Check if file exists
		if _, err := os.Stat(video.FilePath); os.IsNotExist(err) {
			results = append(results, gin.H{
				"index": i,
				"ok":    false,
				"error": "File not found",
			})
			log.Printf("[WARN] Batch upload item %d/%d skipped: file not found (%s)", i+1, len(req.Videos), video.FilePath)
			if i < len(req.Videos)-1 {
				log.Printf("[WAIT] Waiting %s before next upload", minDelay)
				if err := sleepWithContext(c.Request.Context(), minDelay); err != nil {
					c.JSON(http.StatusRequestTimeout, gin.H{"ok": false, "error": err.Error()})
					return
				}
			}
			continue
		}

		privacy := video.Privacy
		if privacy == "" {
			privacy = "private"
		}

		config := youtube.UploadConfig{
			Title:         video.Title,
			Description:   video.Description,
			Tags:          video.Tags,
			PrivacyStatus: privacy,
			ChannelID:     video.ChannelID,
		}

		ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Minute)
		result, err := h.svc.UploadVideo(ctx, video.ChannelID, video.FilePath, config)
		cancel()

		if err != nil {
			results = append(results, gin.H{
				"index": i,
				"ok":    false,
				"error": err.Error(),
			})
			log.Printf("[ERROR] Batch upload item %d/%d failed after %s: %v", i+1, len(req.Videos), time.Since(startedAt).Round(time.Second), err)
		} else {
			results = append(results, gin.H{
				"index":  i,
				"ok":     true,
				"result": result,
			})
			log.Printf("[OK] Batch upload item %d/%d completed in %s: video_id=%s", i+1, len(req.Videos), time.Since(startedAt).Round(time.Second), result.VideoID)
		}

		if i < len(req.Videos)-1 {
			log.Printf("[WAIT] Waiting %s before next upload", minDelay)
			if err := sleepWithContext(c.Request.Context(), minDelay); err != nil {
				c.JSON(http.StatusRequestTimeout, gin.H{"ok": false, "error": err.Error()})
				return
			}
		}
	}

	// Clear cache if at least one upload in the batch succeeded
	hasSuccess := false
	for _, res := range results {
		if ok, exists := res["ok"].(bool); exists && ok {
			hasSuccess = true
			break
		}
	}
	if hasSuccess {
		h.clearCache()
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":                true,
		"results":           results,
		"total":             len(req.Videos),
		"min_delay_seconds": int(minDelay / time.Second),
	})
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
