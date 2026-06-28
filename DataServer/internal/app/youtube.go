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
// PR15.1: the integration *integrationsYoutube.Service is built
// EAGERLY in the constructor so bootstrap can register the YouTube
// delivery provider and other deps. RegisterRoutes only mounts HTTP
// handlers — no more lazy-init that hid a real bug where
// deps.blobStore/delivery calls found a nil Service.
//
// PR-YT-REPO: the previous *integrationsYoutube.Storage field is
// removed entirely. The Storage facade (and its variadic NewStorage,
// in-memory mode + late SetStore) was destroyed when YouTubeStore +
// StorageStore collapsed into the canonical youtube.Repository; the
// Storage methods promoted into Service as Service.AddChannel etc.
// Handlers (and the YouTubeManager) now take *Service only.
type YouTubeModule struct {
	cfg            *config.Config
	dataDir        string
	sqliteStore    *store.SQLiteStore
	youtubeService *integrationsYoutube.Service
	handlers       *ytHandlers.YouTubeHandlers
	manager        *ytHandlers.YouTubeManager
}

// NewYouTubeModule creates a new fully-initialized YouTube module.
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

// buildService constructs the integration Service eagerly. In test
// mode (cfg == nil or sqliteStore == nil) it stays a no-op so
// Service()/Handlers()/Manager() all return nil — matching the
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
	m.youtubeService.GetQuotaManager().SetDB(m.sqliteStore.DB())
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

// RegisterRoutes registers YouTube HTTP endpoints.
//
// PR-YT-REPO: handlers + manager are constructed from the already-built
// service. The legacy (service, storage) / (dataDir, apiKey, storage,
// service) signatures are gone — the Storage facade no longer exists.
// dataDir is still required by NewYouTubeManager because the underpsinx
// `services/youtube.New` needs it for NewCache + NewFeedCache initialisation;
// the dataDir is the same workspace dir the integration Service was
// constructed with.
func (m *YouTubeModule) RegisterRoutes(r *gin.Engine) {
	if m.youtubeService == nil {
		log.Printf("[YOUTUBE] Skipping route registration - service not initialized (test mode)")
		return
	}

	handlers, err := ytHandlers.NewYouTubeHandlers(m.youtubeService)
	if err != nil {
		log.Printf("[YOUTUBE] Handlers init failed: %v", err)
		return
	}
	m.handlers = handlers

	ytGroup := r.Group("/api/v1/youtube")
	ytHandlers.RegisterYouTubeRoutes(ytGroup, m.handlers)

	r.GET("/api/v1/youtube/oauth/callback", m.handlers.OAuthCallback)

	log.Printf("[YOUTUBE] API routes registered at /api/v1/youtube/*")

	apiKey := m.cfg.YouTube.APIKey
	m.manager = ytHandlers.NewYouTubeManager(m.dataDir, apiKey, m.youtubeService)
	ytHandlers.YouTubeRoutes(r, m.cfg, m.manager)
	if apiKey != "" {
		log.Printf("[YOUTUBE] Manager routes registered at /api/youtube/manager/* (full mode)")
	} else {
		log.Printf("[YOUTUBE] Manager routes registered at /api/youtube/manager/* (groups only)")
	}
}
