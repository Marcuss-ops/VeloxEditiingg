package proxy

import (
	"log"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/gin-gonic/gin"
)

var (
	goHandledRequests int64
	blockedRequests   int64
)

// NoRouteHandler handles all unmatched routes.
// All /api/* requests are handled natively or return 404.
func NoRouteHandler(serveSPA gin.HandlerFunc, landing gin.HandlerFunc, darkEditorProxy gin.HandlerFunc) gin.HandlerFunc {

	return func(c *gin.Context) {
		path := c.Request.URL.Path
		if path == "" {
			path = "/"
		}

		if path == "/favicon.ico" || path == "/dark_editor_v2/favicon.ico" {
			c.Data(http.StatusOK, "image/svg+xml; charset=utf-8", []byte(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop offset="0%" stop-color="#ef4444"/><stop offset="100%" stop-color="#f97316"/></linearGradient></defs><rect width="64" height="64" rx="16" fill="#0f172a"/><circle cx="32" cy="32" r="18" fill="url(#g)"/><path d="M25 27h14l-7 14z" fill="#fff"/></svg>`))
			return
		}

		// CRITICAL: API routes MUST return JSON 404, never HTML
		// This prevents frontend from receiving HTML when calling non-existent API endpoints
		if strings.HasPrefix(path, "/api/") {
			atomic.AddInt64(&blockedRequests, 1)
			log.Printf("[ERROR] API 404: %s %s (route not found in Go server)", c.Request.Method, path)
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

		// GET/HEAD requests: try SPA first, fall back to landing page for root
		if c.Request.Method == "GET" || c.Request.Method == "HEAD" {
			isRoot := path == "/" || path == ""

			// Try SPA handler if available
			if serveSPA != nil {
				serveSPA(c)
				// If the SPA wrote a response body, we're done
				if c.Writer.Size() > 0 {
					if !c.IsAborted() {
						// Check if SPA signaled that file was not found
						if spaNotFound, exists := c.Get("spa_file_not_found"); exists && spaNotFound.(bool) {
							atomic.AddInt64(&blockedRequests, 1)
							c.JSON(http.StatusNotFound, gin.H{
								"error": "resource not found",
								"path":  path,
							})
							return
						}
						atomic.AddInt64(&goHandledRequests, 1)
					}
					return
				}
				// SPA wrote nothing — fall through to landing page for root
			}

			// Fallback: landing page for root path
			if isRoot && landing != nil {
				atomic.AddInt64(&goHandledRequests, 1)
				landing(c)
				return
			}
		}

		// All other unmatched routes: return 404
		atomic.AddInt64(&blockedRequests, 1)
		log.Printf("[ERROR] 404: %s %s", c.Request.Method, path)
		c.JSON(http.StatusNotFound, gin.H{
			"error":  "route not found",
			"path":   path,
			"method": c.Request.Method,
			"hint":   "This route does not exist in the Go server. Check API documentation.",
		})
	}
}
