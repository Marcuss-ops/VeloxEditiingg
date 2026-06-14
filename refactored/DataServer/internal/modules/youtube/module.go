package youtube

import (
	"context"
	"log"

	"github.com/gin-gonic/gin"
	"velox-server/internal/app"
	"velox-server/internal/config"
	ytHandlers "velox-server/internal/handlers/server/youtube"
	integrationsYoutube "velox-server/internal/integrations/youtube"
	"velox-server/internal/store"
)

// Module provides YouTube integration endpoints.
type Module struct {
	app.BaseModule
	cfg            *config.Config
	dataDir        string
	sqliteStore    *store.SQLiteStore
	adminAuth      gin.HandlerFunc
	youtubeService *integrationsYoutube.Service
	youtubeStorage *integrationsYoutube.Storage
	handlers       *ytHandlers.YouTubeHandlers
	manager        *ytHandlers.YouTubeManager
}

func New(cfg *config.Config, dataDir string, sqliteStore *store.SQLiteStore, adminAuth gin.HandlerFunc) *Module {
	return &Module{
		cfg:         cfg,
		dataDir:     dataDir,
		sqliteStore: sqliteStore,
		adminAuth:   adminAuth,
	}
}

func (m *Module) Name() string {
	return "youtube"
}

func (m *Module) Handlers() *ytHandlers.YouTubeHandlers {
	return m.handlers
}

func (m *Module) Manager() *ytHandlers.YouTubeManager {
	return m.manager
}

func (m *Module) Service() *integrationsYoutube.Service {
	return m.youtubeService
}

func (m *Module) RegisterRoutes(r *gin.Engine) {
	youtubeService, err := integrationsYoutube.NewService(&integrationsYoutube.ServiceConfig{
		TokensDir:          m.cfg.YouTubeTokensDir,
		YoutubePostingPath: m.cfg.YouTubePostingPath,
		CredentialsDir:     m.cfg.YouTubeCredentialsDir,
		DataDir:            m.dataDir,
		NVIDIAAPIKey:       m.cfg.NVIDIAAPIKey,
		NVIDIATextURL:      m.cfg.NVIDIATextURL,
	}, m.sqliteStore)
	if err != nil {
		log.Printf("[YOUTUBE] Service init failed: %v", err)
		return
	}
	m.youtubeService = youtubeService

	if m.sqliteStore != nil {
		youtubeService.GetQuotaManager().SetStore(m.sqliteStore)
	}

	if m.dataDir != "" {
		storage, storageErr := integrationsYoutube.NewStorage(m.dataDir, m.sqliteStore)
		if storageErr != nil {
			log.Printf("[YOUTUBE] Storage init failed: %v", storageErr)
		} else {
			m.youtubeStorage = storage
		}
	}

	handlers, err := ytHandlers.NewYouTubeHandlers(youtubeService, m.youtubeStorage, m.sqliteStore)
	if err != nil {
		log.Printf("[YOUTUBE] Handlers init failed: %v", err)
		return
	}
	m.handlers = handlers

	ytGroup := r.Group("/api/v1/youtube")
	if m.adminAuth != nil {
		ytGroup.Use(m.adminAuth)
	}
	ytHandlers.RegisterYouTubeRoutes(ytGroup, m.handlers)

	r.GET("/youtube_channels/oauth/callback", m.handlers.OAuthCallback)
	r.GET("/api/v1/youtube/oauth/callback", m.handlers.OAuthCallback)

	log.Printf("[YOUTUBE] API routes registered at /api/v1/youtube/*")

	if m.dataDir != "" {
		apiKey := m.cfg.YouTubeAPIKey
		fallbackURL := m.cfg.RemoteFallbackURL
		m.manager = ytHandlers.NewYouTubeManager(m.dataDir, apiKey, fallbackURL, m.youtubeStorage, youtubeService)
		ytHandlers.YouTubeRoutes(r, m.cfg, m.manager)
		if apiKey != "" {
			log.Printf("[YOUTUBE] Manager routes registered at /api/youtube/manager/* (full mode)")
		} else {
			log.Printf("[YOUTUBE] Manager routes registered at /api/youtube/manager/* (groups only)")
		}
	}
}

func (m *Module) Start(ctx context.Context) error {
	log.Printf("[YOUTUBE] Module started")
	return nil
}

func (m *Module) Stop(ctx context.Context) error {
	log.Printf("[YOUTUBE] Module stopped")
	return nil
}
