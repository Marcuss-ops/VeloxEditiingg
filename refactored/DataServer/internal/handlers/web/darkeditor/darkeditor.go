package darkeditor

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
)

// DarkEditorConfig holds configuration for the dark editor
type DarkEditorConfig struct {
	StaticDir   string // Path to dark_editor/static directory
	TempDir     string // Path to dark_editor/temp directory
	ProjectsDir string // Path to dark_editor/projects directory
	ProxyURL    string // URL to proxy to (e.g., http://localhost:3000 for Next.js)
}

// DarkEditorHandler serves the Dark Editor by proxying to Next.js
func DarkEditorHandler(cfg *DarkEditorConfig) gin.HandlerFunc {
	localIndexPath := ""
	var localIndexBytes []byte
	if cfg != nil && cfg.StaticDir != "" {
		localIndexPath = filepath.Join(cfg.StaticDir, "index.html")
		localIndexBytes, _ = os.ReadFile(localIndexPath)
	}

	serveLocalFallback := func(c *gin.Context) {
		if len(localIndexBytes) == 0 && localIndexPath != "" {
			if bytes, err := os.ReadFile(localIndexPath); err == nil && len(bytes) > 0 {
				localIndexBytes = bytes
			}
		}
		if len(localIndexBytes) > 0 {
			c.Data(http.StatusOK, "text/html; charset=utf-8", localIndexBytes)
			return
		}
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "Dark Editor not configured. Set VELOX_DARK_EDITOR_PROXY_URL or provide VELOX_DARK_EDITOR_DIR.",
		})
	}

	// If ProxyURL is set, use reverse proxy
	if cfg.ProxyURL != "" {
		targetURL, err := url.Parse(cfg.ProxyURL)
		if err != nil {
			panic("Invalid Dark Editor ProxyURL: " + err.Error())
		}
		proxy := httputil.NewSingleHostReverseProxy(targetURL)
		proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, _ error) {
			if len(localIndexBytes) > 0 {
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(localIndexBytes)
				return
			}
			if localIndexPath != "" {
				http.ServeFile(w, r, localIndexPath)
				return
			}
			http.Error(w, "Dark Editor upstream unavailable", http.StatusBadGateway)
		}

		return func(c *gin.Context) {
			// Skip API and file-serving paths - they are handled by Go handlers
			fullPath := c.Request.URL.Path
			if (strings.HasPrefix(fullPath, "/dark_editor_v2/api") && !strings.HasPrefix(fullPath, "/dark_editor_v2/api/v1/youtube")) ||
				strings.HasPrefix(fullPath, "/dark_editor_v2/temp") ||
				strings.HasPrefix(fullPath, "/dark_editor_v2/projects") {
				c.Next()
				return
			}

			// Keep the original path so Next.js basePath (/dark_editor_v2) continues to work.

			// Update headers for proper proxy forwarding
			// Force the upstream to send plain bytes so the gateway controls compression.
			c.Request.Header.Set("Accept-Encoding", "identity")
			c.Request.Header.Set("X-Forwarded-Host", c.Request.Host)
			if c.Request.Header.Get("X-Forwarded-Proto") == "" {
				if c.Request.TLS != nil {
					c.Request.Header.Set("X-Forwarded-Proto", "https")
				} else {
					c.Request.Header.Set("X-Forwarded-Proto", "http")
				}
			}
			c.Request.Header.Set("X-Forwarded-For", c.ClientIP())

			proxy.ServeHTTP(c.Writer, c.Request)
		}
	}

	// Fallback: serve local dark editor HTML if available, otherwise return service unavailable.
	return func(c *gin.Context) {
		serveLocalFallback(c)
	}
}

// DarkEditorProxyMiddleware proxies all /dark_editor_v2/* UI routes to Next.js
// while leaving /dark_editor_v2/api/* to Go handlers.
func DarkEditorProxyMiddleware(cfg *DarkEditorConfig) gin.HandlerFunc {
	handler := DarkEditorHandler(cfg)
	return func(c *gin.Context) {
		if cfg == nil || cfg.ProxyURL == "" {
			c.Next()
			return
		}
		if c.Request.Method != http.MethodGet && c.Request.Method != http.MethodHead {
			c.Next()
			return
		}
		path := c.Request.URL.Path
		if path == "/dark_editor_v2" || strings.HasPrefix(path, "/dark_editor_v2/") {
			if (strings.HasPrefix(path, "/dark_editor_v2/api") && !strings.HasPrefix(path, "/dark_editor_v2/api/v1/youtube")) ||
				strings.HasPrefix(path, "/dark_editor_v2/temp") ||
				strings.HasPrefix(path, "/dark_editor_v2/projects") {
				c.Next()
				return
			}
			handler(c)
			c.Abort()
			return
		}
		c.Next()
	}
}

// RegisterRoutes registers all dark editor routes
func RegisterRoutes(r *gin.Engine, cfg *DarkEditorConfig) {
	handler := DarkEditorHandler(cfg)

	// API routes (/dark_editor_v2/api/*) are registered separately in main.go.
	// We always register the UI entry points so the editor works even if the proxy is down.
	r.GET("/dark_editor", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/dark_editor_v2")
	})
	r.GET("/dark_editor_v2", handler)
	r.GET("/dark_editor_v2/", handler)

	// Only register proxy middleware if ProxyURL is configured.
	// This handles nested UI routes and assets when Next.js is available.
	if cfg != nil && cfg.ProxyURL != "" {
		r.Use(DarkEditorProxyMiddleware(cfg))
		r.GET("/_next/*path", handler)
	}
}
