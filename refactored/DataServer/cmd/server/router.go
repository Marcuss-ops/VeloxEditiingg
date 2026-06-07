package main

import (
	"log"
	"os"
	"path/filepath"
	"strings"

	"velox-server/internal/config"
	"velox-server/internal/handlers/server/analytics"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/handlers/server/drive"

	workers "velox-server/internal/handlers/remote/workers"
	"velox-server/internal/handlers/server/youtube"
	"velox-server/internal/handlers/web/proxy"
	"velox-server/internal/handlers/web/spa"
	integrations_drive "velox-server/internal/integrations/drive"
	integrationsYoutube "velox-server/internal/integrations/youtube"

	"github.com/gin-gonic/gin"
)

// corsMiddleware returns a CORS middleware for development mode.
// Allows requests from localhost:3000 (Next.js dev server).
func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if strings.HasPrefix(origin, "http://localhost:3000") ||
			strings.HasPrefix(origin, "http://127.0.0.1:3000") ||
			strings.HasPrefix(origin, "http://localhost:3001") ||
			strings.HasPrefix(origin, "http://127.0.0.1:3001") {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, PATCH")
			c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With, Accept, Origin")
			c.Header("Access-Control-Allow-Credentials", "true")
			c.Header("Access-Control-Max-Age", "86400")
		}
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	}
}

func newRouter(cfg *config.Config, deps *serverDeps) *gin.Engine {
	// Engine: in release mode skip request logging to avoid log flood.
	var r *gin.Engine
	if os.Getenv("GIN_MODE") == "release" {
		gin.SetMode(gin.ReleaseMode)
		r = gin.New()
		r.Use(gin.Recovery())
	} else {
		r = gin.Default()
	}

	configureTrustedProxies(r)

	// Rewrite dark_editor_v2 API calls to native API v1 routes (excluding proxy routes)
	r.Use(func(c *gin.Context) {
		if strings.HasPrefix(c.Request.URL.Path, "/dark_editor_v2/api/v1/") && !strings.HasPrefix(c.Request.URL.Path, "/dark_editor_v2/api/v1/youtube") {
			c.Request.URL.Path = strings.Replace(c.Request.URL.Path, "/dark_editor_v2/api/v1/", "/api/v1/", 1)
		}
		c.Next()
	})

	r.Use(corsMiddleware())
	r.Use(requestIDMiddleware())
	r.Use(accessLogMiddleware())
	r.Use(gzipMiddleware())
	r.Use(addGzipHeaders())

	// Fix for wrong asset paths in bundled JS: redirect /creator_studio_app/dist/assets/ to /assets/
	// This fixes CSS/JS loading issues in pages like /ansible_computers
	// Serve static files from the SPA assets directory
	// Use relative path from working directory to avoid hardcoded absolute paths
	spaAssetsDir := "frontend_standalone/web/dist/assets"
	var spaDistDir string
	if cfg.SPADir != "" {
		spaAssetsDir = cfg.SPADir + "/assets"
		spaDistDir = cfg.SPADir
	} else {
		spaDistDir = "frontend_standalone/web/dist"
	}
	r.Static("/creator_studio_app/dist/assets", spaAssetsDir)
	r.Static("/assets", spaAssetsDir)
	// Serve root-level SPA assets (favicon, etc.) individually to avoid Gin route conflicts
	r.StaticFile("/creator_studio_app/dist/favicon.svg", spaDistDir+"/favicon.svg")
	r.GET("/favicon.ico", func(c *gin.Context) {
		c.Data(200, "image/svg+xml; charset=utf-8", []byte(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop offset="0%" stop-color="#ef4444"/><stop offset="100%" stop-color="#f97316"/></linearGradient></defs><rect width="64" height="64" rx="16" fill="#0f172a"/><circle cx="32" cy="32" r="18" fill="url(#g)"/><path d="M25 27h14l-7 14z" fill="#fff"/></svg>`))
	})
	r.GET("/dark_editor_v2/favicon.ico", func(c *gin.Context) {
		c.Data(200, "image/svg+xml; charset=utf-8", []byte(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop offset="0%" stop-color="#ef4444"/><stop offset="100%" stop-color="#f97316"/></linearGradient></defs><rect width="64" height="64" rx="16" fill="#0f172a"/><circle cx="32" cy="32" r="18" fill="url(#g)"/><path d="M25 27h14l-7 14z" fill="#fff"/></svg>`))
	})

	registerDiagnosticsRoutes(r, cfg, deps)
	registerHealthAndExplorerRoutes(r)
	registerCutoverMetricsRoute(r)
	registerAPIV1Routes(r, cfg, deps)
	registerNativeV1Routes(r, deps)

	registerDriveRoutes(r, cfg, deps)
	registerYouTubeRoutes(r, cfg, deps)

	registerWorkerLifecycleRoutes(r, deps)

	initCachesAndManagers(r, cfg, deps)
	serveSPAHandler := registerStaticAndSPA(r, cfg)

	// Dark Editor API + proxy.
	registerDarkEditor(r, cfg, deps)

	landing := proxy.LandingPage(cfg)
	r.NoRoute(proxy.NoRouteHandler(serveSPAHandler, landing, depsDarkEditorProxyHandler(cfg, deps.paths.dataDir, r)))

	return r
}

func registerHealthAndExplorerRoutes(r *gin.Engine) {
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "healthy"})
	})
	r.GET("/api/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "healthy"})
	})
}

func registerCutoverMetricsRoute(r *gin.Engine) {
	r.GET("/metrics", func(c *gin.Context) {
		c.String(200, "# metrics stub")
	})
}

func registerAPIV1Routes(r *gin.Engine, cfg *config.Config, deps *serverDeps) {
	// Register V1 API routes with all dependencies
	api.RegisterV1Routes(r, cfg, deps.fileQ, deps.redisQ, deps.reg, deps.jobAPI, deps.jobSubmitHandler, deps.workersRepo, deps.sqliteStore, deps.workerUpdateHandler, deps.workerLifecycle, deps.ansibleHandlers)
	if deps.workerUpdateHandler != nil {
		r.GET("/bundle_manifest.json", deps.workerUpdateHandler.GetBundleManifestHandler())
		r.GET("/api/worker/bundle", deps.workerUpdateHandler.GetBundleDownloadHandler())
		r.HEAD("/api/worker/bundle", deps.workerUpdateHandler.GetBundleDownloadHandler())
		r.GET("/api/worker/v2/manifest", deps.workerUpdateHandler.GetManifestV2Handler())
		r.GET("/api/worker/v2/chunk/:chunkName", deps.workerUpdateHandler.GetChunkV2Handler())
	}

	// Register Ansible routes (admin only)
	if deps.ansibleHandlers != nil {
		v1Admin := r.Group("/api/v1/admin")
		v1Admin.Use(adminAuthMiddleware(cfg))
		v1Admin.GET("/ansible/computers/summary", deps.ansibleHandlers.AnsibleComputersSummaryHandler)
		v1Admin.GET("/ansible/computers/list", deps.ansibleHandlers.AnsibleComputersListHandler)
		v1Admin.POST("/ansible/computers", deps.ansibleHandlers.AnsibleComputersSaveHandler)
		v1Admin.DELETE("/ansible/computers/:id", deps.ansibleHandlers.AnsibleComputersDeleteHandler)
		v1Admin.GET("/ansible/computers/logs/:id", deps.ansibleHandlers.AnsibleComputersLogsHandler)
	}
}

func registerNativeV1Routes(r *gin.Engine, deps *serverDeps) {
	// Register native V1 routes (Go-native implementation without Python)
	var ytService *integrationsYoutube.Service
	if deps.youtubeHandlers != nil {
		ytService = deps.youtubeHandlers.GetService()
	}
	api.RegisterV1NativeRoutes(r, deps.streamsQ, deps.redisQ, deps.reg, deps.ansibleHandlers, ytService, deps.paths.dataDir)
}

func registerDriveRoutes(r *gin.Engine, cfg *config.Config, deps *serverDeps) {
	// Initialize Drive handlers
	driveHandlers, err := drive.NewDriveHandlers(&integrations_drive.ServiceConfig{
		ClientID:     cfg.DriveClientID,
		ClientSecret: cfg.DriveClientSecret,
		RedirectURI:  cfg.DriveRedirectURI,
		TokensDir:    cfg.DriveTokensDir,
	})
	if err != nil {
		log.Printf("⚠️  Drive handlers init failed: %v", err)
		return
	}

	// Register Drive API routes
	drive.RegisterDriveRoutes(r, driveHandlers)
	log.Printf("✅ Drive API routes registered at /api/drive/*")
}

func registerYouTubeRoutes(r *gin.Engine, cfg *config.Config, deps *serverDeps) {
	// Initialize YouTube service
	youtubeService, err := integrationsYoutube.NewService(&integrationsYoutube.ServiceConfig{
		TokensDir:          cfg.YouTubeTokensDir,
		YoutubePostingPath: cfg.YouTubePostingPath,
		CredentialsDir:     cfg.YouTubeCredentialsDir,
		DataDir:            deps.paths.dataDir,
		NVIDIAAPIKey:       cfg.NVIDIAAPIKey,
		NVIDIATextURL:      cfg.NVIDIATextURL,
	})
	if err != nil {
		log.Printf("⚠️ YouTube service init failed: %v", err)
		deps.youtubeHandlers = nil
		deps.youtubeManager = nil
		return
	}

	// Connect QuotaManager to SQLiteStore for tracking
	if deps.sqliteStore != nil {
		youtubeService.GetQuotaManager().SetStore(deps.sqliteStore)
	}

	// ==========================================
	// Unify Storage: create a single Storage instance shared by
	// YouTubeHandlers (upload groups) and YouTubeManager (manager/competitor groups).
	// This ensures both systems read/write from the same ChannelsSaved.json,
	// with GroupType="upload" vs "manager" to distinguish group types.
	// ==========================================
	var youtubeStorage *integrationsYoutube.Storage
	if deps.paths.dataDir != "" {
		storage, storageErr := integrationsYoutube.NewStorage(deps.paths.dataDir)
		if storageErr != nil {
			log.Printf("⚠️ YouTube Storage init failed: %v", storageErr)
		} else {
			youtubeStorage = storage

			// Phase 2: Migrate legacy groups.json into unified Storage
			oldGroupsPath := filepath.Join(deps.paths.dataDir, "youtube", "groups.json")
			if _, statErr := os.Stat(oldGroupsPath); statErr == nil {
				youtubeChannelsPath := filepath.Join(deps.paths.dataDir, "youtube", "channels", "channels.json")
				migrated, migErr := storage.MigrateFromGroupsJSON(oldGroupsPath, youtubeChannelsPath)
				if migErr != nil {
					log.Printf("⚠️ Upload groups migration check: %v", migErr)
				} else if migrated > 0 {
					log.Printf("✅ Migrated %d upload groups into unified Storage", migrated)
				}
			}
		}
	}

	// Initialize YouTube handlers with shared Storage
	deps.youtubeHandlers, err = youtube.NewYouTubeHandlers(&integrationsYoutube.ServiceConfig{
		TokensDir:          cfg.YouTubeTokensDir,
		YoutubePostingPath: cfg.YouTubePostingPath,
		CredentialsDir:     cfg.YouTubeCredentialsDir,
		DataDir:            deps.paths.dataDir,
		NVIDIAAPIKey:       cfg.NVIDIAAPIKey,
		NVIDIATextURL:      cfg.NVIDIATextURL,
	}, youtubeStorage, deps.sqliteStore)
	if err != nil {
		log.Printf("⚠️ YouTube handlers init failed: %v", err)
		deps.youtubeHandlers = nil
		// Don't return — manager may still work
	}

	if deps.youtubeHandlers != nil {
		// Register all YouTube API routes
		// Use a sub-group with auth for sensitive operations
		ytGroup := r.Group("/api/v1/youtube")
		ytGroup.Use(adminAuthMiddleware(cfg))
		youtube.RegisterYouTubeRoutes(ytGroup, deps.youtubeHandlers)

		// Public OAuth callback (MUST remain outside auth group)
		r.GET("/youtube_channels/oauth/callback", deps.youtubeHandlers.OAuthCallback)
		r.GET("/api/v1/youtube/oauth/callback", deps.youtubeHandlers.OAuthCallback)

		log.Printf("✅ YouTube API routes registered at /api/v1/youtube/* (protected by Admin Token)")
	}

	// Create YouTube Manager (for /api/youtube/manager/* routes — competitor tracking)
	// Uses the same shared Storage for unified group data
	if deps.paths.dataDir != "" {
		apiKey := cfg.YouTubeAPIKey
		fallbackURL := cfg.RemoteFallbackURL
		deps.youtubeManager = youtube.NewYouTubeManager(deps.paths.dataDir, apiKey, fallbackURL, youtubeStorage, youtubeService)
		youtube.YouTubeRoutes(r, cfg, deps.youtubeManager)
		if apiKey != "" {
			log.Printf("✅ YouTube Manager routes registered at /api/youtube/manager/* (full mode)")
		} else {
			log.Printf("✅ YouTube Manager routes registered at /api/youtube/manager/* (groups only, no API key)")
		}
	}
}

func registerWorkerLifecycleRoutes(r *gin.Engine, deps *serverDeps) {
	if deps == nil {
		return
	}

	// Compatibility aliases for worker agents that still call /workers/* or /api/workers/*.
	if deps.reg != nil {
		heartbeat := workers.Heartbeat(deps.reg)
		r.POST("/workers/heartbeat", heartbeat)
		r.POST("/api/workers/heartbeat", heartbeat)
	}

	if deps.workerLifecycle != nil {
		// Allow both legacy and new worker agents to register and poll commands.
		r.POST("/workers/register", deps.workerLifecycle.RegisterCompatHandler())
		r.POST("/api/workers/register", deps.workerLifecycle.RegisterCompatHandler())
		r.POST("/api/v1/workers/register", deps.workerLifecycle.RegisterCompatHandler())
		r.POST("/workers/unregister", deps.workerLifecycle.UnregisterCompatHandler())
		r.POST("/api/workers/unregister", deps.workerLifecycle.UnregisterCompatHandler())
		r.POST("/api/v1/workers/unregister", deps.workerLifecycle.UnregisterCompatHandler())
		r.GET("/workers/commands", deps.workerLifecycle.GetCommandsCompatHandler())
		r.GET("/api/workers/commands", deps.workerLifecycle.GetCommandsCompatHandler())
		r.GET("/api/v1/workers/commands", deps.workerLifecycle.GetCommandsCompatHandler())
		r.POST("/workers/commands", deps.workerLifecycle.GetCommandsCompatHandler())
		r.POST("/api/workers/commands", deps.workerLifecycle.GetCommandsCompatHandler())
		r.POST("/api/v1/workers/commands", deps.workerLifecycle.GetCommandsCompatHandler())
		r.POST("/workers/commands/ack", deps.workerLifecycle.AckCommandCompatHandler())
		r.POST("/api/workers/commands/ack", deps.workerLifecycle.AckCommandCompatHandler())
		r.POST("/api/v1/workers/commands/ack", deps.workerLifecycle.AckCommandCompatHandler())
		r.POST("/workers/status", deps.workerLifecycle.UpdateStatusCompatHandler())
		r.POST("/api/workers/status", deps.workerLifecycle.UpdateStatusCompatHandler())
		r.POST("/api/v1/workers/status", deps.workerLifecycle.UpdateStatusCompatHandler())
	}

	if deps.jobAPI != nil {
		r.POST("/api/jobs/get", deps.jobAPI.GetJobCompatHandler())
		r.POST("/jobs/get", deps.jobAPI.GetJobCompatHandler())
		r.POST("/api/jobs/result", deps.jobAPI.SubmitResultCompatHandler())
		r.POST("/jobs/result", deps.jobAPI.SubmitResultCompatHandler())
	}
}

func initCachesAndManagers(r *gin.Engine, cfg *config.Config, deps *serverDeps) {
	// Initialize legacy analytics cache with SQLite store for migration and compatibility
	analytics.InitAnalyticsCache(deps.paths.dataDir, deps.sqliteStore)
}

func registerStaticAndSPA(r *gin.Engine, cfg *config.Config) gin.HandlerFunc {
	// Check if SPA directory is configured and exists
	if cfg.SPADir != "" {
		if _, err := os.Stat(cfg.SPADir); err == nil {
			// SPA directory exists, use real ServeSPA
			return spa.ServeSPA(cfg)
		}
	}
	// No SPA configured, return noop handler
	return func(c *gin.Context) {
		c.Next()
	}
}
