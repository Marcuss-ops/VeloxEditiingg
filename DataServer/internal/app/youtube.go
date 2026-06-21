package app

import (
	"fmt"
	"log"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	ytHandlers "velox-server/internal/handlers/server/youtube"
	integrationsYoutube "velox-server/internal/integrations/youtube"
	"velox-server/internal/store"
)

// YouTubeModule provides YouTube integration endpoints.
//
// PR15.1: the integration *integrationsYoutube.Service and
// *integrationsYoutube.Storage are built EAGERLY in the constructor so
// bootstrap can register the YouTube delivery provider and other deps.
// RegisterRoutes only mounts HTTP handlers — no more lazy-init that hid a
// real bug where deps.blobStore/delivery calls found a nil Service.
type YouTubeModule struct {
	cfg            *config.Config
	dataDir        string
	sqliteStore    *store.SQLiteStore
	youtubeService *integrationsYoutube.Service
	youtubeStorage *integrationsYoutube.Storage
	handlers       *ytHandlers.YouTubeHandlers
	manager        *ytHandlers.YouTubeManager
}

// NewYouTubeModule creates a new fully-initialized YouTube module.
//
// The YouTube integration service and storage are constructed here, NOT
// inside RegisterRoutes. This is the lifecycle bug fix at the core of
// PR15.1: callers (bootstrap, delivery providers) can read Service()
// before any routes are registered.
//
// In test mode, passing sqliteStore == nil or cfg == nil keeps YouTube
// accessors nil to preserve the nil-safe contract used by
// youtube_test.go.
func NewYouTubeModule(cfg *config.Config, dataDir string, sqliteStore *store.SQLiteStore) (*YouTubeModule, error) {
	m := &YouTubeModule{
		cfg:         cfg,
		dataDir:     dataDir,
		sqliteStore: sqliteStore,
	}
	if err := m.buildService(); err != nil {
		return nil, err
	}
	return m, nil
}

// buildService constructs the integration Service and Storage eagerly.
// In test mode (cfg == nil or sqliteStore == nil) it stays a no-op so
// Service()/Storage()/Handlers()/Manager() all return nil — matching the
// contract the youtube_test.go nil-accessor tests rely on.
func (m *YouTubeModule) buildService() error {
	if m == nil || m.cfg == nil || m.sqliteStore == nil {
		return nil
	}
	ytSvc, err := integrationsYoutube.NewService(&integrationsYoutube.ServiceConfig{
		TokensDir:          m.cfg.YouTube.TokensDir,
		YoutubePostingPath: m.cfg.YouTube.PostingPath,
		CredentialsDir:     m.cfg.YouTube.CredentialsDir,
		DataDir:            m.dataDir,
		NVIDIAAPIKey:       m.cfg.NVIDIA.APIKey,
		NVIDIATextURL:      m.cfg.NVIDIA.TextURL,
	}, m.sqliteStore)
	if err != nil {
		return fmt.Errorf("youtube service: %w", err)
	}
	m.youtubeService = ytSvc
	m.youtubeService.GetQuotaManager().SetStore(m.sqliteStore)
	m.youtubeService.GetQuotaManager().SetAnalyticsRepo(store.NewSQLiteYouTubeAnalyticsRepository(m.sqliteStore))

	if m.dataDir != "" {
		storage, storageErr := integrationsYoutube.NewStorage(m.dataDir, m.sqliteStore)
		if storageErr != nil {
			return fmt.Errorf("youtube storage: %w", storageErr)
		}
		m.youtubeStorage = storage
	}
	return nil
}

func (m *YouTubeModule) Name() string {
	return "youtube"
}

func (m *YouTubeModule) Handlers() *ytHandlers.YouTubeHandlers {
	return m.handlers
}

func (m *YouTubeModule) Manager() *ytHandlers.YouTubeManager {
	return m.manager
}

func (m *YouTubeModule) Service() *integrationsYoutube.Service {
	return m.youtubeService
}

// Storage returns the lazily-constructed YouTube integration storage.
// May be nil when dataDir was empty at construction time.
func (m *YouTubeModule) Storage() *integrationsYoutube.Storage {
	return m.youtubeStorage
}

// RegisterRoutes registers YouTube HTTP endpoints.
//
// PR15.1: RegisterRoutes no longer creates services. It only constructs
// HTTP handlers from the already-built service and mounts routes.
func (m *YouTubeModule) RegisterRoutes(r *gin.Engine) {
	if m.youtubeService == nil {
		log.Printf("[YOUTUBE] Skipping route registration - service not initialized (test mode)")
		return
	}

	handlers, err := ytHandlers.NewYouTubeHandlers(m.youtubeService, m.youtubeStorage)
	if err != nil {
		log.Printf("[YOUTUBE] Handlers init failed: %v", err)
		return
	}
	m.handlers = handlers

	ytGroup := r.Group("/api/v1/youtube")
	ytHandlers.RegisterYouTubeRoutes(ytGroup, m.handlers)

	r.GET("/api/v1/youtube/oauth/callback", m.handlers.OAuthCallback)

	log.Printf("[YOUTUBE] API routes registered at /api/v1/youtube/*")

	if m.youtubeStorage != nil {
		apiKey := m.cfg.YouTube.APIKey
		m.manager = ytHandlers.NewYouTubeManager(m.dataDir, apiKey, m.youtubeStorage, m.youtubeService)
		ytHandlers.YouTubeRoutes(r, m.cfg, m.manager)
		if apiKey != "" {
			log.Printf("[YOUTUBE] Manager routes registered at /api/youtube/manager/* (full mode)")
		} else {
			log.Printf("[YOUTUBE] Manager routes registered at /api/youtube/manager/* (groups only)")
		}
	}
}
