package frontend

import (
	"log"
	"os"
	"path/filepath"

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
//
// Velox-core can run API-only (no SPA bundled). In that case VELOX_SPA_DIR
// is unset and frontend_standalone/dist is shipped as a separate repository
// of its own. The module degrades gracefully: API routes stay live, the
// NoRoute handler serves a landing page that explains how to install the
// external frontend, and a warning is logged once at boot.
func (m *Module) RegisterRoutes(r *gin.Engine) {
	// Determine SPA directories. We never silently fall back to a non-existent
	// bundled path: if VELOX_SPA_DIR is unset AND none of the candidate paths
	// resolve to a real directory, m.spaDistDir stays empty and the module
	// registers an API-only landing page instead of a fake SPA.
	candidates := []string{}
	if m.cfg.SPADir != "" {
		candidates = append(candidates, m.cfg.SPADir)
	}
	candidates = append(candidates,
		"../frontend_standalone/web/dist",
		"frontend_standalone/web/dist",
	)
	m.spaDistDir = ""
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		abs, err := filepath.Abs(candidate)
		if err != nil {
			continue
		}
		fi, err := os.Stat(abs)
		if err != nil || !fi.IsDir() {
			continue
		}
		// Require a non-empty index.html under the candidate — otherwise we
		// would later call c.File() on a missing path and Gin would log an
		// "open ... no such file" error per request.
		indexPath := filepath.Join(abs, "index.html")
		if _, err := os.Stat(indexPath); err != nil {
			continue
		}
		m.spaDistDir = abs
		break
	}

	if m.spaDistDir == "" {
		// SPA not bundled; ship API-only and explain how to install it.
		log.Printf("[FRONTEND] WARNING: no SPA bundle found. Set VELOX_SPA_DIR to a directory that contains index.html (e.g. checkout frontend-velox and run `npm run build`, or `export VELOX_SPA_DIR=$PWD/frontend_standalone/web/build`). The Go server keeps serving API routes; UI routes land on the placeholder page until the env is set.")
		landing := proxy.LandingPage(m.cfg)
		r.NoRoute(proxy.NoRouteHandler(nil, landing, nil))
		// Still register the favicons so browser fetches don't 404 noisily.
		m.registerFavicons(r)
		return
	}

	m.spaAssetsDir = filepath.Join(m.spaDistDir, "assets")

	// Static files
	r.Static("/creator_studio_app/dist/assets", m.spaAssetsDir)
	r.Static("/assets", m.spaAssetsDir)
	r.StaticFile("/creator_studio_app/dist/favicon.svg", m.spaDistDir+"/favicon.svg")

	// Ansible guide (optional — only if the file is shipped in the dist)
	guidePath := filepath.Join(m.spaDistDir, "ansible-hosts-guide.html")
	if _, err := os.Stat(guidePath); err == nil {
		r.GET("/ansible_computers/guide", func(c *gin.Context) {
			c.File(guidePath)
		})
	}

	m.registerFavicons(r)

	// SPA handler — clone cfg so we can override SPADir without mutating the original.
	spaCfg := *m.cfg
	spaCfg.SPADir = m.spaDistDir
	m.serveSPAHandler = spa.ServeSPA(&spaCfg)

	// Explicit suite entrypoints so direct hits to /youtube-suite never fall
	// through to the JSON 404 handler when the SPA is available.
	r.GET("/youtube-suite", m.serveSPAHandler)
	r.GET("/youtube-suite/", m.serveSPAHandler)

	// NoRoute handler (SPA fallback + landing page)
	landing := proxy.LandingPage(m.cfg)
	r.NoRoute(proxy.NoRouteHandler(m.serveSPAHandler, landing, nil))

	log.Printf("[FRONTEND] SPA dist served from %s", m.spaDistDir)
}

func (m *Module) registerFavicons(r *gin.Engine) {
	faviconSVG := []byte(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop offset="0%" stop-color="#ef4444"/><stop offset="100%" stop-color="#f97316"/></linearGradient></defs><rect width="64" height="64" rx="16" fill="#0f172a"/><circle cx="32" cy="32" r="18" fill="url(#g)"/><path d="M25 27h14l-7 14z" fill="#fff"/></svg>`)
	r.GET("/favicon.ico", func(c *gin.Context) {
		c.Data(200, "image/svg+xml; charset=utf-8", faviconSVG)
	})
	r.GET("/dark_editor_v2/favicon.ico", func(c *gin.Context) {
		c.Data(200, "image/svg+xml; charset=utf-8", faviconSVG)
	})
}
