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
