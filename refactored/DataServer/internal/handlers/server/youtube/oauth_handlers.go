package youtube

import (
	"context"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"velox-server/internal/config"
)

func resolveYouTubeRedirectURL(c *gin.Context) string {
	if masterURL := strings.TrimSpace(config.GetMasterURL()); masterURL != "" {
		return strings.TrimRight(masterURL, "/") + "/api/v1/youtube/oauth/callback"
	}

	if c == nil || c.Request == nil {
		return ""
	}

	scheme := "http"
	if proto := strings.TrimSpace(c.GetHeader("X-Forwarded-Proto")); proto != "" {
		scheme = proto
	} else if c.Request.TLS != nil {
		scheme = "https"
	}

	host := strings.TrimSpace(c.GetHeader("X-Forwarded-Host"))
	if host == "" {
		host = strings.TrimSpace(c.Request.Host)
	}
	if host == "" {
		return ""
	}

	if isRawIPHost(host) && !isLoopbackHost(host) {
		return ""
	}

	return scheme + "://" + host + "/api/v1/youtube/oauth/callback"
}

func isRawIPHost(host string) bool {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	return net.ParseIP(host) != nil
}

func isLoopbackHost(host string) bool {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	switch strings.ToLower(host) {
	case "localhost", "127.0.0.1", "::1":
		return true
	default:
		return false
	}
}

// StartOAuth initiates OAuth flow
// GET /api/v1/youtube/oauth/start
func (h *YouTubeHandlers) StartOAuth(c *gin.Context) {
	channelName := c.Query("channel_name")
	if channelName == "" {
		channelName = uuid.New().String()[:8]
	}

	redirectURL := resolveYouTubeRedirectURL(c)
	if redirectURL == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"ok":    false,
			"error": "OAuth redirect URL not configured for this host",
		})
		return
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
	if redirectURL != "" {
		c.SetCookie("youtube_oauth_redirect", redirectURL, 600, "/", "", false, true)
	}

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
	redirectURL, _ := c.Cookie("youtube_oauth_redirect")
	if redirectURL == "" {
		redirectURL = resolveYouTubeRedirectURL(c)
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
	c.SetCookie("youtube_oauth_redirect", "", -1, "/", "", false, true)

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
