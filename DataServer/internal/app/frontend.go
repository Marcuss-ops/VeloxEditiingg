package app

import (
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	"velox-server/internal/handlers/web/proxy"
	"velox-server/internal/handlers/web/spa"
)

// FrontendModule provides SPA and static file serving.
type FrontendModule struct {
	cfg             *config.Config
	spaDistDir      string
	spaAssetsDir    string
	serveSPAHandler gin.HandlerFunc
}

// NewFrontendModule creates a new frontend module.
func NewFrontendModule(cfg *config.Config) *FrontendModule {
	return &FrontendModule{
		cfg: cfg,
	}
}

// Name returns the module identifier.
func (m *FrontendModule) Name() string {
	return "frontend"
}

// RegisterRoutes registers frontend endpoints.
func (m *FrontendModule) RegisterRoutes(r *gin.Engine) {
	// Determine SPA directories
	m.spaAssetsDir = "frontend_standalone/web/dist/assets"
	if m.cfg.Frontend.SPADir != "" {
		m.spaAssetsDir = m.cfg.Frontend.SPADir + "/assets"
		m.spaDistDir = m.cfg.Frontend.SPADir
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
	spaDir := m.cfg.Frontend.SPADir
	if spaDir == "" {
		spaDir = "frontend_standalone/web/dist"
	}
	if _, err := os.Stat(spaDir); err == nil {
		// Clone cfg so we can override SPADir without mutating the original
		spaCfg := *m.cfg
		spaCfg.Frontend.SPADir = spaDir
		m.serveSPAHandler = spa.ServeSPA(&spaCfg)
	}
	if m.serveSPAHandler == nil {
		m.serveSPAHandler = func(c *gin.Context) {
			c.Next()
		}
	}

	// NoRoute handler (SPA fallback + landing page + dark editor proxy)
	landing := proxy.LandingPage(m.cfg)

	// Dark Editor reverse proxy: /dark_editor_v2/* → DARK_EDITOR_URL (default http://localhost:3001).
	// The URL is read once at startup so dev/CI/prod can point to a different host/port
	// without recompiling. Routing is already gated to /dark_editor_v2/* in proxy.NoRouteHandler,
	// but the proxy itself still rewrites Location headers and forwards X-Forwarded-* below.
	var darkEditorProxy gin.HandlerFunc
	darkEditorURL := os.Getenv("DARK_EDITOR_URL")
	if darkEditorURL == "" {
		darkEditorURL = "http://localhost:3001"
	}
	target, err := url.Parse(darkEditorURL)
	if err != nil {
		log.Printf("[FRONTEND] Dark Editor proxy disabled (invalid DARK_EDITOR_URL=%q): %v", darkEditorURL, err)
	} else {
		rp := httputil.NewSingleHostReverseProxy(target)
		originalDirector := rp.Director
		rp.Director = func(req *http.Request) {
			originalDirector(req)
			scheme := "http"
			if req.TLS != nil || strings.EqualFold(req.Header.Get("X-Forwarded-Proto"), "https") {
				scheme = "https"
			}
			req.Header.Set("X-Forwarded-Host", req.Host)
			req.Header.Set("X-Forwarded-Proto", scheme)
			if clientIP, _, err := net.SplitHostPort(req.RemoteAddr); err == nil {
				req.Header.Set("X-Forwarded-For", clientIP)
			} else {
				req.Header.Set("X-Forwarded-For", req.RemoteAddr)
			}
		}
		rp.ModifyResponse = func(resp *http.Response) error {
			if loc := resp.Header.Get("Location"); loc != "" {
				if rl, err := url.Parse(loc); err == nil && rl.IsAbs() && rl.Host == target.Host {
					rl.Scheme = ""
					rl.Host = ""
					resp.Header.Set("Location", rl.String())
				}
			}
			return nil
		}
		darkEditorProxy = func(c *gin.Context) {
			// Defense in depth: even though noroute.go gates on this prefix,
			// guard here so the env-var-driven proxy can't become a general
			// open proxy if a future refactor drops the upstream filter.
			if !strings.HasPrefix(c.Request.URL.Path, "/dark_editor_v2") {
				c.Next()
				return
			}
			rp.ServeHTTP(c.Writer, c.Request)
		}
		log.Printf("[FRONTEND] Dark Editor proxy enabled → %s://%s", target.Scheme, target.Host)
	}

	r.NoRoute(proxy.NoRouteHandler(m.serveSPAHandler, landing, darkEditorProxy))

	log.Printf("[FRONTEND] Static files registered (SPA: %s, DarkEditor proxy: %v)", m.spaDistDir, darkEditorProxy != nil)
}
