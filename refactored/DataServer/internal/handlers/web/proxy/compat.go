package proxy

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
)

// EndpointsStatus returns minimal JSON for GET /api/endpoints-status (Creator Studio UI).
func EndpointsStatus(c *gin.Context) {
	now := time.Now().UTC().Format(time.RFC3339)
	c.JSON(http.StatusOK, gin.H{
		"ok":             true,
		"base_url":       "http://127.0.0.1:8000",
		"base_url_local": "http://127.0.0.1:8000",
		"endpoints": []gin.H{
			{"name": "Health", "method": "GET", "path": "/api/health", "status": "ok", "status_code": 200, "message": "OK"},
			{"name": "Endpoints status", "method": "GET", "path": "/api/endpoints-status", "status": "ok", "status_code": 200, "message": "OK"},
			{"name": "Code version", "method": "GET", "path": "/api/master/code-version", "status": "ok", "status_code": 200, "message": "OK"},
			{"name": "Services availability", "method": "GET", "path": "/api/services/availability", "status": "ok", "status_code": 200, "message": "OK"},
		},
		"summary":   gin.H{"total": 4, "ok": 4, "error": 0},
		"timestamp": now,
	})
}

// ServicesAvailability returns minimal JSON for GET /api/services/availability.
func ServicesAvailability(c *gin.Context) {
	now := time.Now().UTC().Format(time.RFC3339)
	c.JSON(http.StatusOK, gin.H{
		"ok":     true,
		"source": "velox-server",
		"services": gin.H{
			"master": gin.H{"available": true, "message": "Go gateway attivo"},
		},
		"timestamp": now,
	})
}

// ServicesEnsureStarted is a no-op for POST /api/services/ensure_started.
func ServicesEnsureStarted(c *gin.Context) {
	now := time.Now().UTC().Format(time.RFC3339)
	c.JSON(http.StatusOK, gin.H{"ok": true, "timestamp": now})
}

// MasterCodeVersion returns minimal JSON for GET /api/master/code-version.
func MasterCodeVersion(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		now := time.Now().UTC().Format(time.RFC3339)
		c.JSON(http.StatusOK, gin.H{
			"ok":             true,
			"server":         "velox-server",
			"version_number": cfg.VersionNumber,
			"timestamp":      now,
		})
	}
}

// Root handles GET / so the UI doesn't get 500 when the request is proxied to a backend that may fail (e.g. template error).
func Root(c *gin.Context) {
	c.Redirect(http.StatusFound, "/api/health")
}

// LandingPage returns a handler that serves a minimal HTML when SPA is not built.
func LandingPage(cfg *config.Config) gin.HandlerFunc {
	html := `<!DOCTYPE html>
<html lang="it">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Velox</title></head>
<body style="font-family:system-ui;max-width:40rem;margin:2rem auto;padding:1rem;">
<h1>Velox</h1>
<p>Entry point unico: <strong>:8000</strong></p>
<ul>
<li><a href="/api/health">API Health</a></li>
</ul>
<p>Per servire la SPA React su <code>/</code>: build <code>frontend_standalone/web</code> (<code>npm run build</code>) e imposta <code>VELOX_SPA_DIR</code> al path di <code>dist/</code>.</p>
</body>
</html>`
	return func(c *gin.Context) {
		c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(html))
	}
}
