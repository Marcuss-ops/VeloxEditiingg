package frontend

import (
	"log"
	"os"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/handlers/web/proxy"
	"velox-server/internal/handlers/web/spa"
)

// Module provides SPA and static file serving.
type Module struct {
	cfg             *config.Config
	spaDistDir      string
	spaAssetsDir    string
	serveSPAHandler gin.HandlerFunc
}

// New creates a new frontend module.
func New(cfg *config.Config) *Module {
	return &Module{
		cfg: cfg,
	}
}

// Name returns the module identifier.
func (m *Module) Name() string {
	return "frontend"
}

// RegisterRoutes registers frontend endpoints.
func (m *Module) RegisterRoutes(r *gin.Engine) {
	// Determine SPA directories
	m.spaAssetsDir = "frontend_standalone/web/dist/assets"
	if m.cfg.SPADir != "" {
		m.spaAssetsDir = m.cfg.SPADir + "/assets"
		m.spaDistDir = m.cfg.SPADir
	} else {
		m.spaDistDir = "frontend_standalone/web/dist"
	}

	// Static files
	r.Static("/creator_studio_app/dist/assets", m.spaAssetsDir)
	r.Static("/assets", m.spaAssetsDir)
	r.StaticFile("/creator_studio_app/dist/favicon.svg", m.spaDistDir+"/favicon.svg")

	// Ansible guide
	r.GET("/ansible_computers/guide", func(c *gin.Context) {
		c.File(m.spaDistDir + "/ansible-hosts-guide.html")
	})

	// Favicons
	r.GET("/favicon.ico", func(c *gin.Context) {
		c.Data(200, "image/svg+xml; charset=utf-8", []byte(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop offset="0%" stop-color="#ef4444"/><stop offset="100%" stop-color="#f97316"/></linearGradient></defs><rect width="64" height="64" rx="16" fill="#0f172a"/><circle cx="32" cy="32" r="18" fill="url(#g)"/><path d="M25 27h14l-7 14z" fill="#fff"/></svg>`))
	})
	r.GET("/dark_editor_v2/favicon.ico", func(c *gin.Context) {
		c.Data(200, "image/svg+xml; charset=utf-8", []byte(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop offset="0%" stop-color="#ef4444"/><stop offset="100%" stop-color="#f97316"/></linearGradient></defs><rect width="64" height="64" rx="16" fill="#0f172a"/><circle cx="32" cy="32" r="18" fill="url(#g)"/><path d="M25 27h14l-7 14z" fill="#fff"/></svg>`))
	})

	// SPA handler — use SPADir from env or fall back to the default relative path
	spaDir := m.cfg.SPADir
	if spaDir == "" {
		spaDir = "frontend_standalone/web/dist"
	}
	if _, err := os.Stat(spaDir); err == nil {
		// Clone cfg so we can override SPADir without mutating the original
		spaCfg := *m.cfg
		spaCfg.SPADir = spaDir
		m.serveSPAHandler = spa.ServeSPA(&spaCfg)
	}
	if m.serveSPAHandler == nil {
		m.serveSPAHandler = func(c *gin.Context) {
			c.Next()
		}
	}

	// NoRoute handler (SPA fallback + landing page)
	landing := proxy.LandingPage(m.cfg)
	r.NoRoute(proxy.NoRouteHandler(m.serveSPAHandler, landing, nil))

	log.Printf("[FRONTEND] Static files registered (SPA: %s)", m.spaDistDir)
}
