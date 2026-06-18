package api

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/queue"
	workersreg "velox-server/internal/workers"
)

func AdminAuthMiddleware(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		if workersreg.IsLocalRequestIP(c.ClientIP()) {
			c.Next()
			return
		}

		// Allow read-only dashboard routes without an admin token.
		// The workers/ansible UI is meant to stay live on public instances,
		// but write operations must still remain protected.
		if c.Request.Method == http.MethodGet && IsPublicReadOnlyRoute(c.Request.URL.Path) {
			c.Next()
			return
		}

		expected := strings.TrimSpace(cfg.Auth.AdminToken)
		if expected == "" {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": "admin token required for remote access",
			})
			return
		}

		token := workersreg.ExtractBearerToken(
			c.GetHeader("Authorization"),
			c.GetHeader("X-Admin-Token"),
			c.Query("token"),
		)
		if token == "" || subtle.ConstantTimeCompare([]byte(token), []byte(expected)) != 1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": "invalid admin token",
			})
			return
		}

		c.Next()
	}
}

func IsPublicReadOnlyRoute(path string) bool {
	if path == "" {
		return false
	}

	publicPrefixes := []string{
		"/api/v1/jobs",
		"/api/v1/workers",
		"/api/v1/dashboard/summary",
		"/api/v1/dashboard/realtime",
		"/api/v1/dashboard/health",
		"/api/v1/youtube",
		"/api/v1/analytics",
		"/api/v1/groups",
		"/api/v1/channels",
		"/api/v1/drive-links",
		"/api/v1/drive",
		"/api/v1/master",
		"/api/v1/ansible",
		"/api/v1/admin/ansible",
		"/api/v1/endpoints-status",
		"/api/v1/services",
		"/api/v1/bundle",
		"/api/v1/queue",
		"/api/v1/stats",
		"/api/v1/calendar",
		"/api/v1/livestream",
		"/api/bundle",
	}

	for _, prefix := range publicPrefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}

	// Legacy compat paths used by the workers dashboard SPA.
	if path == "/jobs" || path == "/workers" || path == "/workers_status" || path == "/api/workers_status" || path == "/api/v1/workers_status" {
		return true
	}

	return false
}

func workerStatusCounts(ctx context.Context, fileQ *queue.FileQueue) (pending, processing, completed, errorCount, total int64) {
	if fileQ != nil {
		if stats, err := fileQ.Stats(ctx); err == nil {
			pending = stats["pending"]
			processing = stats["processing"]
			completed = stats["completed"]
			errorCount = stats["error"]
			total = stats["total"]
			return
		}
	}
	return
}

