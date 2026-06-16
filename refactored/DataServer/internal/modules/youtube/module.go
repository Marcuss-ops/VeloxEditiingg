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
	"context"
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

	// Resolve the OAuth secret cipher used by HandleOAuthCallback and the
	// OAuth auto-refresh path. requireIfMissing=true is enforced here:
	// step S6 of the migration plan removes JSON dual-write paths, so a
	// server without VELOX_YT_OAUTH_TOKEN_KEY cannot meaningfully persist
	// OAuth credentials, and the only safe boot is to refuse. The previous
	// "warn-and-continue" mode produced the dual-write drift the verdict
	// called out (json + sqlite could diverge on ValidateToken).
	//
	// We surface the missing-key error as a registration failure so the
	// /api/v1/youtube/* routes are NOT registered when the cipher is
	// missing. Operators see a clear startup failure rather than a
	// later runtime surprise (Service.HandleOAuthCallback returns "cipher
	// not configured" only when a real OAuth flow is attempted).
	cipher, err := aesgcm.LoadFromEnv(true)
	if err != nil {
		log.Printf("[YOUTUBE][FAIL] OAuth secret cipher unavailable: %v — VELOX_YT_OAUTH_TOKEN_KEY (or _FILE variant) must be configured. YouTube OAuth routes will NOT be registered; SQLite-only flow cannot be reached without at-rest encryption.", err)
	} else if cipher == nil {
		log.Printf("[YOUTUBE][FAIL] OAuth secret cipher nil: VELOX_YT_OAUTH_TOKEN_KEY resolved but did not produce a key — refusing to start OAuth routes.")
	} else {
		youtubeService.SetOAuthSecretCipher(cipher)
		_, _ = youtubeService.BackfillOAuthTokensFromJSON(context.Background())
		log.Printf("[OK] YouTube OAuth secret cipher initialised (key_version=%d)", cipher.KeyVersion())
	}

	if m.dataDir != "" {
		storage, storageErr := integrationsYoutube.NewStorage(m.dataDir, m.sqliteStore)
		if storageErr != nil {
			log.Printf("[YOUTUBE] Storage init failed: %v", storageErr)
		} else {
			m.youtubeStorage = storage
		}
	}

	// Fail-closed gate: if the AES cipher did not resolve above, do NOT
	// register any YouTube route that would touch OAuth secrets. Otherwise
	// HandleOAuthCallback would refuse every connect attempt with a runtime
	// 500 ("cipher not configured") which would look indistinguishable
	// from a normal OAuth error and tempt operators into "maybe try again"
	// loops. Either the server boots with a usable cipher or it does not
	// expose the OAuth surface.
	if cipher == nil || err != nil {
		log.Printf("[YOUTUBE][FAIL] YouTube OAuth routes disabled until VELOX_YT_OAUTH_TOKEN_KEY is configured.")
		return
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
