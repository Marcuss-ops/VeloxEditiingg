package youtube

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

// AccountInfo represents an OAuth account for the frontend.
type AccountInfo struct {
	ID           string `json:"id"`
	Title        string `json:"title"`
	Name         string `json:"name"`
	Thumbnail    string `json:"thumbnail"`
	Language     string `json:"language"`
	TokenPresent bool   `json:"token_present"`
	Email        string `json:"email,omitempty"`
}

// ListAccountsHandler returns all YouTube OAuth accounts.
// GET /api/youtube/accounts
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
			"ok":      true,
			"accounts": accounts,
			"count":   len(accounts),
		})
	}
}

// GetAccountHandler returns a single YouTube OAuth account.
// GET /api/youtube/accounts/:id
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

// RefreshAccountHandler refreshes the OAuth token for a YouTube account.
// POST /api/youtube/accounts/:id/refresh
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
		result, err := ym.service.ValidateToken(ctx, channelID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"ok":    false,
				"error": fmt.Sprintf("Failed to refresh token: %v", err),
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"ok":      true,
			"refreshed": result["refreshed"],
			"valid":   result["valid"],
			"message": "Token refreshed",
		})
	}
}
