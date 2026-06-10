package youtube

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// GetHealth checks YouTube API health
// GET /api/v1/youtube/credentials/health
func (h *YouTubeHandlers) GetHealth(c *gin.Context) {
	channelID := c.Query("channel_id")

	if channelID == "" {
		// Return general status
		channels := h.service.GetChannels()
		c.JSON(http.StatusOK, gin.H{
			"ok":       true,
			"channels": len(channels),
			"message":  "YouTube service is running",
		})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	defer cancel()

	health, err := h.service.HealthCheck(ctx, channelID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, health)
}

// ValidateToken validates a channel's OAuth token
// GET /api/v1/youtube/credentials/validate/:id
func (h *YouTubeHandlers) ValidateToken(c *gin.Context) {
	channelID := c.Param("id")

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	result, err := h.service.ValidateToken(ctx, channelID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

// RevokeToken revokes a channel's OAuth token
// DELETE /api/v1/youtube/credentials/revoke/:id
func (h *YouTubeHandlers) RevokeToken(c *gin.Context) {
	channelID := c.Param("id")

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	err := h.service.RevokeToken(ctx, channelID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":      true,
		"message": fmt.Sprintf("Channel '%s' token revoked and removed", channelID),
	})
}

// RefreshToken forces a token refresh for a channel
// POST /api/v1/youtube/credentials/refresh/:id
func (h *YouTubeHandlers) RefreshToken(c *gin.Context) {
	channelID := c.Param("id")

	channel := h.service.GetChannel(channelID)
	if channel == nil {
		c.JSON(http.StatusNotFound, gin.H{
			"ok":    false,
			"error": "Channel not found",
		})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	// Force validation which will trigger refresh if needed
	result, err := h.service.ValidateToken(ctx, channelID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

// GetQuota returns quota information
// GET /api/v1/youtube/credentials/quota
func (h *YouTubeHandlers) GetQuota(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	quota := h.service.GetQuotaUsage(ctx)

	c.JSON(http.StatusOK, gin.H{
		"ok":    true,
		"quota": quota,
	})
}
