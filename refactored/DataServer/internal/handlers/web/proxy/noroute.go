package proxy

import (
	"log"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/gin-gonic/gin"
)

// CutoverMetrics tracks request handling metrics
type CutoverMetrics struct {
	GoHandledRequests int64 // Requests handled natively by Go
	BlockedRequests   int64 // Requests blocked (404)
}

var (
	cutoverMetrics CutoverMetrics
)

// GetCutoverMetrics returns current metrics (thread-safe)
func GetCutoverMetrics() CutoverMetrics {
	return CutoverMetrics{
		GoHandledRequests: atomic.LoadInt64(&cutoverMetrics.GoHandledRequests),
		BlockedRequests:   atomic.LoadInt64(&cutoverMetrics.BlockedRequests),
	}
}

// CutoverMetricsHandler returns a Gin handler that exposes metrics
func CutoverMetricsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		metrics := GetCutoverMetrics()
		c.JSON(http.StatusOK, gin.H{
			"go_only_mode": true,
			"metrics": gin.H{
				"go_handled_requests": metrics.GoHandledRequests,
				"blocked_requests":    metrics.BlockedRequests,
			},
			"note": "Python backends removed - system runs in permanent Go-only mode",
		})
	}
}

// NoRouteHandler handles all unmatched routes.
// Python backends have been completely removed.
// All /api/* requests are handled by Go or return 404.
func NoRouteHandler(serveSPA gin.HandlerFunc, landing gin.HandlerFunc, darkEditorProxy gin.HandlerFunc) gin.HandlerFunc {
	log.Printf("🔒 Go-only mode: Python proxy disabled - all routes handled natively")

	return func(c *gin.Context) {
		path := c.Request.URL.Path
		if path == "" {
			path = "/"
		}

		// Legacy worker compatibility shim:
		// Old workers may still poll command endpoints even after the route cutover.
		// Respond with an empty command list instead of surfacing a noisy 404.
		if c.Request.Method == http.MethodGet &&
			(path == "/api/workers/commands" || path == "/api/v1/workers/commands" || path == "/workers/commands") {
			atomic.AddInt64(&cutoverMetrics.GoHandledRequests, 1)
			c.JSON(http.StatusOK, gin.H{
				"success": true,
				"data":    []interface{}{},
			})
			return
		}
		if c.Request.Method == http.MethodPost &&
			(path == "/api/workers/commands/ack" || path == "/api/v1/workers/commands/ack" || path == "/workers/commands/ack") {
			atomic.AddInt64(&cutoverMetrics.GoHandledRequests, 1)
			c.JSON(http.StatusOK, gin.H{
				"success": true,
			})
			return
		}

		if path == "/favicon.ico" || path == "/dark_editor_v2/favicon.ico" {
			c.Data(http.StatusOK, "image/svg+xml; charset=utf-8", []byte(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop offset="0%" stop-color="#ef4444"/><stop offset="100%" stop-color="#f97316"/></linearGradient></defs><rect width="64" height="64" rx="16" fill="#0f172a"/><circle cx="32" cy="32" r="18" fill="url(#g)"/><path d="M25 27h14l-7 14z" fill="#fff"/></svg>`))
			return
		}

		// CRITICAL: API routes MUST return JSON 404, never HTML
		// This prevents frontend from receiving HTML when calling non-existent API endpoints
		if strings.HasPrefix(path, "/api/") {
			atomic.AddInt64(&cutoverMetrics.BlockedRequests, 1)
			log.Printf("❌ API 404: %s %s (route not found in Go server)", c.Request.Method, path)
			c.JSON(http.StatusNotFound, gin.H{
				"error":  "API route not found",
				"path":   path,
				"method": c.Request.Method,
				"hint":   "This API endpoint does not exist. Check /api/v1/ for available endpoints.",
			})
			return
		}

		// Dark Editor UI fallback (ensures /dark_editor_v2 is always proxied)
		if (c.Request.Method == "GET" || c.Request.Method == "HEAD") && darkEditorProxy != nil {
			if strings.HasPrefix(path, "/dark_editor_v2") &&
				!strings.HasPrefix(path, "/dark_editor_v2/api") &&
				!strings.HasPrefix(path, "/dark_editor_v2/temp") &&
				!strings.HasPrefix(path, "/dark_editor_v2/projects") {
				darkEditorProxy(c)
				return
			}
		}

		// GET/HEAD requests for root or SPA routes: serve SPA or landing page
		if (c.Request.Method == "GET" || c.Request.Method == "HEAD") && serveSPA != nil {
			serveSPA(c)
			// Check if SPA signaled that file was not found
			if spaNotFound, exists := c.Get("spa_file_not_found"); exists && spaNotFound.(bool) {
				atomic.AddInt64(&cutoverMetrics.BlockedRequests, 1)
				c.JSON(http.StatusNotFound, gin.H{
					"error": "resource not found",
					"path":  path,
				})
				return
			}
			atomic.AddInt64(&cutoverMetrics.GoHandledRequests, 1)
			return
		}

		// Landing page for root
		if (path == "/" || path == "") && landing != nil && (c.Request.Method == "GET" || c.Request.Method == "HEAD") {
			atomic.AddInt64(&cutoverMetrics.GoHandledRequests, 1)
			landing(c)
			return
		}

		// All other unmatched routes: return 404
		atomic.AddInt64(&cutoverMetrics.BlockedRequests, 1)
		// Skip logging noisy 404s (e.g. workers calling wrong heartbeat path)
		if path != "/workers/heartbeat" {
			log.Printf("❌ 404: %s %s (no Python fallback)", c.Request.Method, path)
		}
		c.JSON(http.StatusNotFound, gin.H{
			"error":  "route not found",
			"path":   path,
			"method": c.Request.Method,
			"hint":   "This route does not exist in the Go server. Check API documentation.",
		})
	}
}
