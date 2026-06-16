package youtube

import (
	"log"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/handlers/server/audit"
	ytHandlers "velox-server/internal/handlers/server/youtube"
	integrationsYoutube "velox-server/internal/integrations/youtube"
	"velox-server/internal/secrets/aesgcm"
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
	// Resolve the OAuth secret cipher FIRST, BEFORE NewService. The cipher must be
	// available to the constructor because loadOAuthChannelsFromSQLite (the boot
	// hydrator) reads through it on the same call. The previous "wire cipher
	// after construction" pattern produced the failure mode called out in the
	// re-analysis: the boot hydrator exited early with `oauth cipher nil`, the
	// operator saw YouTube OAuth routes registered but every channel came up
	// credential-less after a restart.
	//
	// requireIfMissing=true is the contract: a server without
	// VELOX_YT_OAUTH_TOKEN_KEY cannot meaningfully persist OAuth credentials
	// (SQLite-only contract, S6), so the only safe boot is to refuse. The
	// entire YouTube route surface is gated on this. Operators see a clear
	// startup failure rather than a later runtime surprise.
	cipher, err := aesgcm.LoadFromEnv(true)
	if err != nil {
		log.Printf("[YOUTUBE][FAIL] OAuth secret cipher unavailable: %v — VELOX_YT_OAUTH_TOKEN_KEY (or _FILE variant) must be configured. YouTube routes will NOT be registered.", err)
		return
	}
	if cipher == nil {
		log.Printf("[YOUTUBE][FAIL] OAuth secret cipher nil: VELOX_YT_OAUTH_TOKEN_KEY resolved but did not produce a key — refusing to start YouTube routes.")
		return
	}
	log.Printf("[OK] YouTube OAuth secret cipher initialised (key_version=%d)", cipher.KeyVersion())

	youtubeService, err := integrationsYoutube.NewService(&integrationsYoutube.ServiceConfig{
		TokensDir:          m.cfg.YouTubeTokensDir,
		YoutubePostingPath: m.cfg.YouTubePostingPath,
		CredentialsDir:     m.cfg.YouTubeCredentialsDir,
		DataDir:            m.dataDir,
		NVIDIAAPIKey:       m.cfg.NVIDIAAPIKey,
		NVIDIATextURL:      m.cfg.NVIDIATextURL,
	}, m.sqliteStore, cipher)
	if err != nil {
		log.Printf("[YOUTUBE] Service init failed: %v", err)
		return
	}
	m.youtubeService = youtubeService

	if m.sqliteStore != nil {
		youtubeService.GetQuotaManager().SetStore(m.sqliteStore)
	}

	// BackfillOAuthTokensFromJSON is no longer called from boot. The runtime
	// path now rehydrates exclusively from youtube_oauth_tokens via
	// loadOAuthChannelsFromSQLite (the S6 contract). The
	// BackfillOAuthTokensFromJSON helper is retained as a one-shot
	// primitive for the planned `velox migrate youtube-oauth-json` admin
	// command and for legacy data recovery; nothing runs it automatically.

	if m.dataDir != "" {
		storage, storageErr := integrationsYoutube.NewStorage(m.dataDir, m.sqliteStore)
		if storageErr != nil {
			log.Printf("[YOUTUBE] Storage init failed: %v", storageErr)
		} else {
			m.youtubeStorage = storage
		}
	}

	// Register the /api/v1/audit/persistence endpoint. Lives here because the
	// YouTube Storage is the only component that records safety-guard state and
	// knows the live channel/group counts; the audit handler reads both.
	if m.youtubeStorage != nil || m.sqliteStore != nil {
		auditHandler := audit.NewPersistenceHandler(m.cfg, m.sqliteStore, m.youtubeStorage)
		r.GET("/api/v1/audit/persistence", auditHandler.Handle)
		log.Printf("[YOUTUBE] Audit endpoint registered at /api/v1/audit/persistence")
	}

	handlers, err := ytHandlers.NewYouTubeHandlers(youtubeService, m.youtubeStorage)
	if err != nil {
		log.Printf("[YOUTUBE] Handlers init failed: %v", err)
		return
	}
	m.handlers = handlers

	ytGroup := r.Group("/api/v1/youtube")
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
