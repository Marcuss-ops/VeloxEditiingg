package spa

import (
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"

	"velox-server/internal/config"

	"github.com/gin-gonic/gin"
)

// ServeSPA serves the SPA from cfg.SPADir: static files if present, else index.html (history fallback).
//
// Index.html is read once at registration time. If the dist directory is
// removed/uninstalled at runtime, fall back to 503 with an explanatory hint
// rather than spamming open()-error logs per request.
func ServeSPA(cfg *config.Config) gin.HandlerFunc {
	root := strings.TrimRight(cfg.SPADir, string(os.PathSeparator))
	indexPath := filepath.Join(root, "index.html")
	indexBytes, err := os.ReadFile(indexPath)
	indexMissing := err != nil || len(indexBytes) == 0
	if indexMissing {
		indexBytes = nil
	}

	return func(c *gin.Context) {
		if c.Request.Method != http.MethodGet && c.Request.Method != http.MethodHead {
			c.Next()
			return
		}
		p := c.Request.URL.Path
		if p == "" || p[0] != '/' {
			p = "/"
		}
		// Avoid path traversal
		clean := path.Clean(p)
		if strings.Contains(clean, "..") {
			c.AbortWithStatus(http.StatusBadRequest)
			return
		}
		// Only serve index.html for root path, not for all missing paths
		if clean == "/" {
			clean = "/index.html"
		}
		fpath := filepath.Join(root, filepath.FromSlash(clean))
		fi, statErr := os.Stat(fpath)
		if statErr == nil && fi.Mode().IsRegular() {
			c.File(fpath)
			c.Abort()
			return
		}
		// Bonus check: never call c.File() on a missing index.html
		// below — that path used to log "no such file or directory" per
		// request when the dist was uninstalled.
		if indexMissing {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{
				"ok":    false,
				"error": "SPA dist unavailable",
				"hint":  "SPA dist not bundled. Set VELOX_SPA_DIR (or run `npm run build` in frontend_standalone/web) and restart to enable the UI. API routes are unaffected.",
			})
			return
		}
		// SPA history fallback: serve index.html for any path that didn't match a static file,
		// so client-side routes like /youtube/upload, /youtube_manager, /dashboard work.
		// Paths with a file extension (e.g. .js, .css) that weren't found stay as 404.
		base := filepath.Base(clean)
		if strings.Contains(base, ".") && base != "index.html" {
			c.Set("spa_file_not_found", true)
			c.Next()
			return
		}
		c.Data(http.StatusOK, "text/html; charset=utf-8", indexBytes)
		c.Abort()
		return
	}
}
