package youtube

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type AccountInfo struct {
	ID           string `json:"id"`
	Title        string `json:"title"`
	Name         string `json:"name"`
	Thumbnail    string `json:"thumbnail"`
	Language     string `json:"language"`
	TokenPresent bool   `json:"token_present"`
	Email        string `json:"email,omitempty"`
}

func (ym *YouTubeManager) ListAccountsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		if ym.service == nil {
			c.JSON(http.StatusOK, gin.H{"ok": true, "accounts": []AccountInfo{}, "count": 0})
			return
		}

		channels := ym.service.GetAuthChannels()
		accounts := make([]AccountInfo, 0, len(channels))

		for _, ch := range channels {
			accounts = append(accounts, AccountInfo{
				ID:           ch.ID,
				Title:        ch.Title,
				Name:         ch.Name,
				Thumbnail:    ch.Thumbnail,
				Language:     ch.Language,
				TokenPresent: ch.AccessToken != "" || ch.RefreshToken != "",
				Email:        ch.Email,
			})
		}

		c.JSON(http.StatusOK, gin.H{
			"ok":       true,
			"accounts": accounts,
			"count":    len(accounts),
		})
	}
}

func (ym *YouTubeManager) GetAccountHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		channelID := c.Param("id")

		if ym.service == nil {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "Service not initialized"})
			return
		}

		ch := ym.service.GetAuthChannel(channelID)
		if ch == nil {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "Account not found"})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"ok": true,
			"account": AccountInfo{
				ID:           ch.ID,
				Title:        ch.Title,
				Name:         ch.Name,
				Thumbnail:    ch.Thumbnail,
				Language:     ch.Language,
				TokenPresent: ch.AccessToken != "" || ch.RefreshToken != "",
				Email:        ch.Email,
			},
		})
	}
}

func (ym *YouTubeManager) RefreshAccountHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		channelID := c.Param("id")

		if ym.service == nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "Service not initialized"})
			return
		}

		ch := ym.service.GetAuthChannel(channelID)
		if ch == nil {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "Account not found"})
			return
		}

		ctx := c.Request.Context()
		result, err := ym.service.ValidateOAuthAccessToken(ctx, channelID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"ok":    false,
				"error": fmt.Sprintf("Failed to refresh token: %v", err),
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"ok":        true,
			"refreshed": result["refreshed"],
			"valid":     result["valid"],
			"message":   "Token refreshed",
		})
	}
}

func (h *YouTubeHandlers) GetHealth(c *gin.Context) {
	channelID := c.Query("channel_id")

	if channelID == "" {
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

func (h *YouTubeHandlers) ValidateToken(c *gin.Context) {
	channelID := c.Param("id")

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	result, err := h.service.ValidateOAuthAccessToken(ctx, channelID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

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

	result, err := h.service.ValidateOAuthAccessToken(ctx, channelID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, result)
}

func (h *YouTubeHandlers) GetQuota(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()

	quota := h.service.GetQuotaUsage(ctx)

	c.JSON(http.StatusOK, gin.H{
		"ok":    true,
		"quota": quota,
	})
}
