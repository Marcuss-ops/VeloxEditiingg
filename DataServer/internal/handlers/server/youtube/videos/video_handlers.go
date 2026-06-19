package videos

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"velox-server/internal/integrations/youtube"

	"github.com/gin-gonic/gin"
)

// SetThumbnail sets the thumbnail for a video
// POST /api/v1/youtube/videos/:video_id/thumbnail
func (h *Handler) SetThumbnail(c *gin.Context) {
	videoID := c.Param("video_id")

	var req struct {
		ChannelID     string `json:"channel_id" binding:"required"`
		ThumbnailPath string `json:"thumbnail_path" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	result, err := h.svc.SetThumbnail(ctx, req.ChannelID, videoID, req.ThumbnailPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":       true,
		"video_id": videoID,
		"result":   result,
	})
}

// UpdateMetadata updates video metadata
// POST /api/v1/youtube/videos/:video_id/metadata
func (h *Handler) UpdateMetadata(c *gin.Context) {
	videoID := c.Param("video_id")

	var req struct {
		ChannelID   string   `json:"channel_id" binding:"required"`
		Title       string   `json:"title"`
		Description string   `json:"description"`
		Tags        []string `json:"tags"`
		Privacy     string   `json:"privacy"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
		return
	}

	config := youtube.UploadConfig{
		Title:         req.Title,
		Description:   req.Description,
		Tags:          req.Tags,
		PrivacyStatus: req.Privacy,
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	err := h.svc.UpdateVideoMetadata(ctx, req.ChannelID, videoID, config)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":       true,
		"video_id": videoID,
		"message":  "Metadata updated successfully",
	})
}

// PublishVideo changes video privacy to public or unlisted
// POST /api/v1/youtube/videos/:video_id/publish
func (h *Handler) PublishVideo(c *gin.Context) {
	videoID := c.Param("video_id")

	var req struct {
		ChannelID string `json:"channel_id" binding:"required"`
		Privacy   string `json:"privacy"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
		return
	}

	privacy := req.Privacy
	if privacy == "" {
		privacy = "public"
	}

	config := youtube.UploadConfig{
		PrivacyStatus: privacy,
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	err := h.svc.UpdateVideoMetadata(ctx, req.ChannelID, videoID, config)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":       true,
		"video_id": videoID,
		"privacy":  privacy,
		"message":  fmt.Sprintf("Video published as %s", privacy),
	})
}

// DeleteVideo deletes a video
// DELETE /api/v1/youtube/videos/:video_id
func (h *Handler) DeleteVideo(c *gin.Context) {
	videoID := c.Param("video_id")

	channelID := c.Query("channel_id")
	if channelID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "channel_id query parameter is required"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	err := h.svc.DeleteVideo(ctx, channelID, videoID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	h.clearCache()

	c.JSON(http.StatusOK, gin.H{
		"ok":       true,
		"video_id": videoID,
		"message":  "Video deleted successfully",
	})
}
