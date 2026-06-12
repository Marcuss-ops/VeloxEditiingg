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
	youtubeService *integrationsYoutube.Service
	youtubeStorage *integrationsYoutube.Storage
	handlers       *ytHandlers.YouTubeHandlers
	manager        *ytHandlers.YouTubeManager
}

// New creates a new YouTube module.
func New(cfg *config.Config, dataDir string, sqliteStore *store.SQLiteStore) *Module {
	return &Module{
		cfg:         cfg,
		dataDir:     dataDir,
		sqliteStore: sqliteStore,
	}
}

// Name returns the module identifier.
func (m *Module) Name() string {
	return "youtube"
}

// Handlers returns the YouTube handlers (for use by other modules).
func (m *Module) Handlers() *ytHandlers.YouTubeHandlers {
	return m.handlers
}

// Manager returns the YouTube manager (for use by other modules).
func (m *Module) Manager() *ytHandlers.YouTubeManager {
	return m.manager
}

// Service returns the YouTube service (for use by other modules).
func (m *Module) Service() *integrationsYoutube.Service {
	return m.youtubeService
}

// RegisterRoutes registers YouTube endpoints.
func (m *Module) RegisterRoutes(r *gin.Engine) {
	// Initialize YouTube service
	youtubeService, err := integrationsYoutube.NewService(&integrationsYoutube.ServiceConfig{
		TokensDir:          m.cfg.YouTubeTokensDir,
		YoutubePostingPath: m.cfg.YouTubePostingPath,
		CredentialsDir:     m.cfg.YouTubeCredentialsDir,
		DataDir:            m.dataDir,
		NVIDIAAPIKey:       m.cfg.NVIDIAAPIKey,
		NVIDIATextURL:      m.cfg.NVIDIATextURL,
	})
	if err != nil {
		log.Printf("[YOUTUBE] Service init failed: %v", err)
		return
	}
	m.youtubeService = youtubeService

	// Connect QuotaManager to SQLiteStore for tracking
	if m.sqliteStore != nil {
		youtubeService.GetQuotaManager().SetStore(m.sqliteStore)
	}

	// Create shared Storage
	if m.dataDir != "" {
		storage, storageErr := integrationsYoutube.NewStorage(m.dataDir)
		if storageErr != nil {
			log.Printf("[YOUTUBE] Storage init failed: %v", storageErr)
		} else {
			m.youtubeStorage = storage
		}
	}

	// Initialize YouTube handlers
	handlers, err := ytHandlers.NewYouTubeHandlers(&integrationsYoutube.ServiceConfig{
		TokensDir:          m.cfg.YouTubeTokensDir,
		YoutubePostingPath: m.cfg.YouTubePostingPath,
		CredentialsDir:     m.cfg.YouTubeCredentialsDir,
		DataDir:            m.dataDir,
		NVIDIAAPIKey:       m.cfg.NVIDIAAPIKey,
		NVIDIATextURL:      m.cfg.NVIDIATextURL,
	}, m.youtubeStorage, m.sqliteStore)
	if err != nil {
		log.Printf("[YOUTUBE] Handlers init failed: %v", err)
		return
	}
	m.handlers = handlers

	// Register YouTube API routes
	ytGroup := r.Group("/api/v1/youtube")
	// Note: admin auth middleware should be applied here
	ytHandlers.RegisterYouTubeRoutes(ytGroup, m.handlers)

	// Public OAuth callback
	r.GET("/youtube_channels/oauth/callback", m.handlers.OAuthCallback)
	r.GET("/api/v1/youtube/oauth/callback", m.handlers.OAuthCallback)

	log.Printf("[YOUTUBE] API routes registered at /api/v1/youtube/*")

	// Create YouTube Manager for competitor tracking
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

// Start initializes the module.
func (m *Module) Start(ctx context.Context) error {
	log.Printf("[YOUTUBE] Module started")
	return nil
}

// Stop gracefully shuts down the module.
func (m *Module) Stop(ctx context.Context) error {
	log.Printf("[YOUTUBE] Module stopped")
	return nil
}
