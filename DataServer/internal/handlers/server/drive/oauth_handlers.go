package drive

import (
	"context"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	integrationsDrive "velox-server/internal/integrations/drive"
)

func sanitizeDriveTokenName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "drive_manual"
	}
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "drive_manual"
	}
	return out
}

// DriveOAuthStartHandler starts the Google Drive OAuth flow.
// GET /api/drive/oauth/start
func (h *DriveHandlers) DriveOAuthStartHandler(c *gin.Context) {
	svc := h.svc.DriveService()
	if svc == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "drive service not configured"})
		return
	}

	tokenName := sanitizeDriveTokenName(c.Query("token_name"))
	state := tokenName
	if state == "" {
		state = "drive_manual"
	}

	authURL := integrationsDrive.GetAuthURL(svc.GetOAuthConfig(), state)
	c.JSON(http.StatusOK, gin.H{
		"ok":         true,
		"auth_url":   authURL,
		"token_name": tokenName,
		"state":      state,
	})
}

// DriveOAuthCallback stores the Drive token returned by Google.
// GET /api/drive/oauth/callback
func (h *DriveHandlers) DriveOAuthCallbackHandler(c *gin.Context) {
	svc := h.svc.DriveService()
	if svc == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "drive service not configured"})
		return
	}

	code := strings.TrimSpace(c.Query("code"))
	state := sanitizeDriveTokenName(c.Query("state"))
	errorParam := strings.TrimSpace(c.Query("error"))
	if errorParam != "" {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": errorParam})
		return
	}
	if code == "" {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "missing code"})
		return
	}
	if state == "" {
		state = "drive_manual"
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 45*time.Second)
	defer cancel()

	token, err := integrationsDrive.ExchangeCode(ctx, svc.GetOAuthConfig(), code)
	if err != nil {
		log.Printf("[DRIVE] OAuth callback failed: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	if token == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "empty token response"})
		return
	}

	if err := svc.GetTokenManager().SaveToken(state, token); err != nil {
		log.Printf("[DRIVE] Failed to persist token %s: %v", state, err)
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	svc.SetToken(token)

	c.Header("Content-Type", "text/html")
	c.String(http.StatusOK, `
<!DOCTYPE html>
<html>
<head><title>Drive Authentication Successful</title></head>
<body>
<h2>[OK] Drive Authentication Successful</h2>
<p>Token name: %s</p>
<p>You can close this window.</p>
</body>
</html>
	`, state)
}
