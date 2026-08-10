package api

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	workersreg "velox-server/internal/workers"
)

func AdminAuthMiddleware(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Loopback bypass is an explicit development-only opt-in. SSH
		// forwards arrive as loopback on the master and must still present
		// the admin bearer token.
		if cfg != nil && cfg.Runtime.AllowLoopbackAdminAuthDev && workersreg.IsLocalRequestIP(c.ClientIP()) {
			c.Next()
			return
		}

		// Browser-based requests are never allowed to reach the admin
		// token path: the Origin header is a reliable indicator of a
		// cross-origin browser request. Reject it before wasting cycles
		// on token comparison and to ensure admin credentials never need
		// to live in the browser.
		if c.GetHeader("Origin") != "" {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": "direct browser access forbidden",
			})
			return
		}

		expected := strings.TrimSpace(cfg.Auth.AdminToken)
		if expected == "" {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": "admin token required for remote access",
			})
			return
		}

		token := workersreg.ExtractBearerToken(c.GetHeader("Authorization"), "", "")
		if token == "" || subtle.ConstantTimeCompare([]byte(token), []byte(expected)) != 1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "invalid admin token",
			})
			return
		}

		c.Next()
	}
}

// WorkerOrAdminAuthMiddleware protects worker data-plane endpoints. Workers
// authenticate with their short-lived command token, while operators use the
// admin token. This is separate from AdminAuthMiddleware so worker tokens do
// not grant access to unrelated admin routes.
func WorkerOrAdminAuthMiddleware(cfg *config.Config, tokenMgr *workersreg.TokenManager) gin.HandlerFunc {
	admin := ""
	if cfg != nil {
		admin = strings.TrimSpace(cfg.Auth.AdminToken)
	}
	return func(c *gin.Context) {
		if c.GetHeader("Origin") != "" {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "direct browser access forbidden"})
			return
		}
		token := workersreg.ExtractBearerToken(c.GetHeader("Authorization"), c.GetHeader("X-Worker-Token"), "")
		if token != "" && admin != "" && subtle.ConstantTimeCompare([]byte(token), []byte(admin)) == 1 {
			// Admin operators may submit a report for any worker during
			// remediation. Mark this explicit exception so handlers do not
			// confuse it with an unbound worker token.
			c.Set("authenticated_admin", true)
			c.Next()
			return
		}
		if token != "" && tokenMgr != nil {
			if workerID, ok := tokenMgr.ValidateWorkerCommandToken(token); ok {
				c.Set("authenticated_worker_id", workerID)
				c.Next()
				return
			}
		}
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid worker or admin token"})
	}
}
