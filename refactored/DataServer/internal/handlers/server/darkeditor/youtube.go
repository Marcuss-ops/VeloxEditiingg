package darkeditor

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/integrations/youtube"
)

// YouTubeIntegrationConfig holds configuration for YouTube integration
type YouTubeIntegrationConfig struct {
	// YouTubeService is the existing YouTube service
	YouTubeService *youtube.Service
	// TempDir for temporary files
	TempDir string
}

// YouTubeIntegrationHandler handles YouTube-related operations for Dark Editor
type YouTubeIntegrationHandler struct {
	cfg *YouTubeIntegrationConfig
}

// SetThumbnail sets a thumbnail for a YouTube video
func (h *YouTubeIntegrationHandler) SetThumbnail(c *gin.Context) {
	var req SetThumbnailRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	// Check if YouTube service is available
	if h.cfg.YouTubeService == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "YouTube service not configured",
		})
		return
	}

	// Get the thumbnail file path
	thumbnailPath := filepath.Join(h.cfg.TempDir, req.Filename)
	if _, err := os.Stat(thumbnailPath); os.IsNotExist(err) {
		c.JSON(http.StatusNotFound, gin.H{"error": "Thumbnail file not found"})
		return
	}

	// Determine channel to use
	channelID := req.ChannelID
	if channelID == "" {
		// Get first available channel
		channels := h.cfg.YouTubeService.GetAuthChannels()
		if len(channels) == 0 {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": "No YouTube channels configured",
			})
			return
		}
		channelID = channels[0].ID
	}

	// Set the thumbnail
	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
	defer cancel()

	result, err := h.cfg.YouTubeService.SetThumbnail(ctx, channelID, req.VideoID, thumbnailPath)
	if err != nil {
		log.Printf("❌ Failed to set YouTube thumbnail: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to set thumbnail: %v", err),
		})
		return
	}

	log.Printf("✅ YouTube thumbnail set for video %s", req.VideoID)

	c.JSON(http.StatusOK, SetThumbnailResponse{
		Success:      true,
		VideoID:      req.VideoID,
		VideoURL:     fmt.Sprintf("https://www.youtube.com/watch?v=%s", req.VideoID),
		Message:      "Thumbnail uploaded successfully",
		ThumbnailURL: result,
	})
}

// GetChannels returns available YouTube channels
func (h *YouTubeIntegrationHandler) GetChannels(c *gin.Context) {
	if h.cfg.YouTubeService == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "YouTube service not configured",
		})
		return
	}

	channels := h.cfg.YouTubeService.GetAuthChannels()

	channelInfos := make([]ChannelInfo, 0, len(channels))
	for _, ch := range channels {
		channelInfos = append(channelInfos, ChannelInfo{
			ID:        ch.ID,
			Title:     ch.Title,
			Thumbnail: ch.Thumbnail,
		})
	}

	c.JSON(http.StatusOK, GetChannelsResponse{
		Channels: channelInfos,
	})
}

// ValidateChannel validates a YouTube channel's OAuth token
func (h *YouTubeIntegrationHandler) ValidateChannel(c *gin.Context) {
	channelID := c.Param("channel_id")

	if h.cfg.YouTubeService == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "YouTube service not configured",
		})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	result, err := h.cfg.YouTubeService.ValidateToken(ctx, channelID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, result)
}

// UploadThumbnailDirect handles direct thumbnail upload (file + video_id in same request)
func (h *YouTubeIntegrationHandler) UploadThumbnailDirect(c *gin.Context) {
	// Get video ID from form
	videoID := c.PostForm("video_id")
	if videoID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "video_id is required"})
		return
	}

	// Get channel ID (optional)
	channelID := c.PostForm("channel_id")

	// Get the uploaded file
	file, header, err := c.Request.FormFile("thumbnail")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No thumbnail file provided"})
		return
	}
	defer file.Close()

	// Validate file type
	contentType := header.Header.Get("Content-Type")
	if !strings.HasPrefix(contentType, "image/") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "File must be an image"})
		return
	}

	// Save to temp file
	tempFilename := fmt.Sprintf("yt_thumb_%d_%s", time.Now().Unix(), header.Filename)
	tempPath := filepath.Join(h.cfg.TempDir, tempFilename)

	// Create temp file
	outFile, err := os.Create(tempPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create temp file"})
		return
	}
	defer outFile.Close()

	// Copy file content
	if _, err := outFile.ReadFrom(file); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save file"})
		return
	}

	// Check if YouTube service is available
	if h.cfg.YouTubeService == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "YouTube service not configured",
		})
		return
	}

	// Determine channel to use
	if channelID == "" {
		channels := h.cfg.YouTubeService.GetAuthChannels()
		if len(channels) == 0 {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": "No YouTube channels configured",
			})
			return
		}
		channelID = channels[0].ID
	}

	// Set the thumbnail
	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
	defer cancel()

	result, err := h.cfg.YouTubeService.SetThumbnail(ctx, channelID, videoID, tempPath)
	if err != nil {
		log.Printf("❌ Failed to set YouTube thumbnail: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to set thumbnail: %v", err),
		})
		return
	}

	// Clean up temp file
	os.Remove(tempPath)

	log.Printf("✅ YouTube thumbnail uploaded for video %s", videoID)

	c.JSON(http.StatusOK, gin.H{
		"success":       true,
		"video_id":      videoID,
		"video_url":     fmt.Sprintf("https://www.youtube.com/watch?v=%s", videoID),
		"message":       "Thumbnail uploaded successfully",
		"thumbnail_url": result,
	})
}

// GetVideoInfo returns information about a YouTube video
func (h *YouTubeIntegrationHandler) GetVideoInfo(c *gin.Context) {
	videoID := c.Param("video_id")

	if h.cfg.YouTubeService == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "YouTube service not configured",
		})
		return
	}

	// Get first available channel for API access
	channels := h.cfg.YouTubeService.GetAuthChannels()
	if len(channels) == 0 {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "No YouTube channels configured",
		})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	service, err := h.cfg.YouTubeService.GetYouTubeService(ctx, channels[0].ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to get YouTube service: %v", err),
		})
		return
	}

	// Get video details
	response, err := service.Videos.List([]string{"snippet", "status", "statistics"}).Id(videoID).Do()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": fmt.Sprintf("Failed to get video info: %v", err),
		})
		return
	}

	if len(response.Items) == 0 {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "Video not found",
		})
		return
	}

	video := response.Items[0]

	c.JSON(http.StatusOK, gin.H{
		"id":             video.Id,
		"title":          video.Snippet.Title,
		"description":    video.Snippet.Description,
		"channel_id":     video.Snippet.ChannelId,
		"channel_title":  video.Snippet.ChannelTitle,
		"published_at":   video.Snippet.PublishedAt,
		"thumbnail":      video.Snippet.Thumbnails,
		"privacy_status": video.Status.PrivacyStatus,
		"view_count":     video.Statistics.ViewCount,
		"like_count":     video.Statistics.LikeCount,
		"comment_count":  video.Statistics.CommentCount,
		"video_url":      fmt.Sprintf("https://www.youtube.com/watch?v=%s", video.Id),
	})
}

// StartOAuthFlow initiates the OAuth flow for adding a new YouTube channel
func (h *YouTubeIntegrationHandler) StartOAuthFlow(c *gin.Context) {
	channelName := c.Query("name")
	if channelName == "" {
		channelName = fmt.Sprintf("channel_%d", time.Now().Unix())
	}

	if h.cfg.YouTubeService == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "YouTube service not configured",
		})
		return
	}

	authURL := h.cfg.YouTubeService.GetOAuthStartURL(channelName)
	if authURL == "" {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "OAuth not configured",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"auth_url": authURL,
		"message":  "Visit the URL to authorize the channel",
	})
}

// HealthCheck returns the health status of YouTube integration
func (h *YouTubeIntegrationHandler) HealthCheck(c *gin.Context) {
	if h.cfg.YouTubeService == nil {
		c.JSON(http.StatusOK, gin.H{
			"status":  "unavailable",
			"message": "YouTube service not configured",
		})
		return
	}

	channels := h.cfg.YouTubeService.GetAuthChannels()

	c.JSON(http.StatusOK, gin.H{
		"status":           "ok",
		"channels_count":   len(channels),
		"oauth_configured": true,
	})
}

// SetThumbnailV1 handles the route POST /api/v1/youtube/videos/:video_id/thumbnail
func (h *YouTubeIntegrationHandler) SetThumbnailV1(c *gin.Context) {
	videoID := c.Param("video_id")
	var req struct {
		ChannelID     string `json:"channel_id"`
		ThumbnailPath string `json:"thumbnail_path" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	if h.cfg.YouTubeService == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "YouTube service not configured"})
		return
	}

	thumbnailPath := filepath.Join(h.cfg.TempDir, req.ThumbnailPath)
	if _, err := os.Stat(thumbnailPath); os.IsNotExist(err) {
		c.JSON(http.StatusNotFound, gin.H{"error": "Thumbnail file not found"})
		return
	}

	channelID := req.ChannelID
	if channelID == "" {
		channels := h.cfg.YouTubeService.GetAuthChannels()
		if len(channels) == 0 {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "No YouTube channels configured"})
			return
		}
		channelID = channels[0].ID
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
	defer cancel()

	result, err := h.cfg.YouTubeService.SetThumbnail(ctx, channelID, videoID, thumbnailPath)
	if err != nil {
		log.Printf("❌ Failed to set YouTube thumbnail: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to set thumbnail: %v", err)})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":       true,
		"video_id": videoID,
		"result":   result,
	})
}

