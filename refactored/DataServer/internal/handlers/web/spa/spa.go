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

// ServeSPA serves the SPA from cfg.Frontend.SPADir: static files if present, else index.html (history fallback).
// Safe to call only when cfg.Frontend.SPADir != "" and the dir exists.
// Returns false in context if file not found, so NoRoute can proxy to Python.
func ServeSPA(cfg *config.Config) gin.HandlerFunc {
	root := strings.TrimRight(cfg.Frontend.SPADir, string(os.PathSeparator))
	indexPath := filepath.Join(root, "index.html")
	indexBytes, _ := os.ReadFile(indexPath) // cache index.html for fallback

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
		fi, err := os.Stat(fpath)
		if err == nil && fi.Mode().IsRegular() {
			c.File(fpath)
			c.Abort()
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
		// SPA fallback: serve index.html for client-side routes
		if len(indexBytes) > 0 {
			c.Data(http.StatusOK, "text/html; charset=utf-8", indexBytes)
		} else {
			c.File(indexPath)
		}
		c.Abort()
		return
	}
}
