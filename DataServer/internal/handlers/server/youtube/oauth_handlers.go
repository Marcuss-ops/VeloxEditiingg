package youtube

import (
	"context"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// StartOAuth initiates OAuth flow
// GET /api/v1/youtube/oauth/start
func (h *YouTubeHandlers) StartOAuth(c *gin.Context) {
	channelName := c.Query("channel_name")
	if channelName == "" {
		channelName = uuid.New().String()[:8]
	}

	authURL := h.service.GetOAuthStartURL(channelName)
	if authURL == "" {
		c.JSON(http.StatusInternalServerError, gin.H{
			"ok":    false,
			"error": "OAuth not configured",
		})
		return
	}

	// Store state for verification
	c.SetCookie("youtube_oauth_channel", channelName, 600, "/", "", false, true)

	c.JSON(http.StatusOK, gin.H{
		"ok":       true,
		"auth_url": authURL,
		"channel":  channelName,
	})
}

// OAuthCallback handles OAuth callback
// GET /api/v1/youtube/oauth/callback
func (h *YouTubeHandlers) OAuthCallback(c *gin.Context) {
	code := c.Query("code")
	state := c.Query("state")
	errorParam := c.Query("error")

	if errorParam != "" {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": errorParam})
		return
	}

	// Get channel name from cookie
	channelName, err := c.Cookie("youtube_oauth_channel")
	if err != nil {
		channelName = strings.TrimPrefix(state, "youtube_")
		channelName = strings.Split(channelName, "_")[0]
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	channel, err := h.service.HandleOAuthCallback(ctx, code, channelName)
	if err != nil {
		log.Printf("[ERROR] OAuth callback failed: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	// Clear cookie
	c.SetCookie("youtube_oauth_channel", "", -1, "/", "", false, true)

	// Return HTML that closes the popup
	c.Header("Content-Type", "text/html")
	c.String(http.StatusOK, `
<!DOCTYPE html>
<html>
<head><title>Authentication Successful</title></head>
<body>
<h2>[OK] Authentication Successful!</h2>
<p>Channel: %s</p>
<p>You can close this window.</p>
<script>
if (window.opener) {
    window.opener.postMessage({type: 'youtube_auth_success', channel: '%s'}, '*');
    window.close();
}
</script>
</body>
</html>
`, channel.Title, channel.ID)
}
