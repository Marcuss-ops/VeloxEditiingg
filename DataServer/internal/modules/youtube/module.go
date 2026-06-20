package youtube

import (
	"log"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	ytHandlers "velox-server/internal/handlers/server/youtube"
	integrationsYoutube "velox-server/internal/integrations/youtube"
	"velox-server/internal/store"
)

// Module provides YouTube integration endpoints.
type Module struct {
	cfg            *config.Config
	dataDir        string
	sqliteStore    *store.SQLiteStore
	youtubeService *integrationsYoutube.Service
	youtubeStorage *integrationsYoutube.Storage
	handlers       *ytHandlers.YouTubeHandlers
	manager        *ytHandlers.YouTubeManager
}

func New(cfg *config.Config, dataDir string, sqliteStore *store.SQLiteStore) *Module {
	return &Module{
		cfg:         cfg,
		dataDir:     dataDir,
		sqliteStore: sqliteStore,
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
		TokensDir:          m.cfg.YouTube.TokensDir,
		YoutubePostingPath: m.cfg.YouTube.PostingPath,
		CredentialsDir:     m.cfg.YouTube.CredentialsDir,
		DataDir:            m.dataDir,
		NVIDIAAPIKey:       m.cfg.NVIDIA.APIKey,
		NVIDIATextURL:      m.cfg.NVIDIA.TextURL,
	}, m.sqliteStore)
	if err != nil {
		log.Printf("[YOUTUBE] Service init failed: %v", err)
		return
	}
	m.youtubeService = youtubeService

	if m.sqliteStore != nil {
		youtubeService.GetQuotaManager().SetStore(m.sqliteStore)
		youtubeService.GetQuotaManager().SetDB(m.sqliteStore.DB())
	}

	if m.dataDir != "" {
		storage, storageErr := integrationsYoutube.NewStorage(m.dataDir, m.sqliteStore)
		if storageErr != nil {
			log.Printf("[YOUTUBE] Storage init failed: %v", storageErr)
		} else {
			m.youtubeStorage = storage
		}
	}

	handlers, err := ytHandlers.NewYouTubeHandlers(youtubeService, m.youtubeStorage)
	if err != nil {
		log.Printf("[YOUTUBE] Handlers init failed: %v", err)
		return
	}
	m.handlers = handlers

	ytGroup := r.Group("/api/v1/youtube")
	ytHandlers.RegisterYouTubeRoutes(ytGroup, m.handlers)

	r.GET("/api/v1/youtube/oauth/callback", m.handlers.OAuthCallback)

	log.Printf("[YOUTUBE] API routes registered at /api/v1/youtube/*")

	if m.dataDir != "" {
		apiKey := m.cfg.YouTube.APIKey
		m.manager = ytHandlers.NewYouTubeManager(m.dataDir, apiKey, m.youtubeStorage, youtubeService)
		ytHandlers.YouTubeRoutes(r, m.cfg, m.manager)
		if apiKey != "" {
			log.Printf("[YOUTUBE] Manager routes registered at /api/youtube/manager/* (full mode)")
		} else {
			log.Printf("[YOUTUBE] Manager routes registered at /api/youtube/manager/* (groups only)")
		}
	}
}
