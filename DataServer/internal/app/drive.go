package app

import (
	"log"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	driveHandlers "velox-server/internal/handlers/server/drive"
	integrationsDrive "velox-server/internal/integrations/drive"
	"velox-server/internal/store"
)

// DriveModule provides Google Drive integration endpoints.
type DriveModule struct {
	cfg         *config.Config
	sqliteStore *store.SQLiteStore

	// Lazy-initialized on RegisterRoutes so the constructor doesn't pay the
	// cost when Drive isn't wired up.
	service  *integrationsDrive.Service
	handlers *driveHandlers.DriveHandlers
}

// NewDriveModule creates a new Drive module.
func NewDriveModule(cfg *config.Config) *DriveModule {
	m := &DriveModule{
		cfg: cfg,
	}
	_ = m.init()
	return m
}

// Name returns the module identifier.
func (m *DriveModule) Name() string {
	return "drive"
}

// Service exposes the lazy-initialized Drive service for downstream
// consumers (bootstrap threads it through RegisterV1Routes). Returns nil
// when credentials are absent or init hasn't run.
func (m *DriveModule) Service() *integrationsDrive.Service {
	if m == nil {
		return nil
	}
	return m.service
}

// Handlers returns the Drive handlers (for use by other modules).
func (m *DriveModule) Handlers() *driveHandlers.DriveHandlers {
	return m.handlers
}

// WithSQLiteStore wires the SQLite backend into the Drive module so that
// drive-link writes (cache / master folder upserts / list reads) actually
// persist. Without this call the handler receives a nil store and all writes
// silently no-op, leaving operators under the assumption that "the folder
// exists in SQLite" when it doesn't. Safe to call once after NewDriveModule.
func (m *DriveModule) WithSQLiteStore(s *store.SQLiteStore) {
	if m == nil || s == nil {
		return
	}
	m.sqliteStore = s
	if m.handlers != nil {
		log.Printf("[DRIVE] SQLite store wired (driveModules will persist writes)")
	}
}

// RegisterRoutes registers Drive endpoints.
func (m *DriveModule) RegisterRoutes(r *gin.Engine) {
	if err := m.init(); err != nil {
		log.Printf("[DRIVE] Init failed: %v", err)
		return
	}
	if m.handlers == nil {
		log.Printf("[DRIVE] Handlers unavailable after init")
		return
	}
	driveHandlers.RegisterDriveRoutes(r, m.handlers)
	log.Printf("[DRIVE] API routes registered at /api/drive/*")
}

func (m *DriveModule) init() error {
	if m == nil || m.cfg == nil {
		return nil
	}
	if m.service != nil && m.handlers != nil {
		return nil
	}

	// Initialize Drive service
	if m.service == nil && m.cfg.Drive.ClientID != "" && m.cfg.Drive.ClientSecret != "" {
		svc, err := integrationsDrive.NewService(&integrationsDrive.ServiceConfig{
			ClientID:     m.cfg.Drive.ClientID,
			ClientSecret: m.cfg.Drive.ClientSecret,
			RedirectURI:  m.cfg.Drive.RedirectURI,
			TokensDir:    m.cfg.Drive.TokensDir,
		})
		if err != nil {
			return err
		}
		m.service = svc
	}

	if m.service != nil {
		if err := m.service.LoadFirstToken(); err != nil {
			log.Printf("[DRIVE] No initial token loaded: %v", err)
		}
	}

	// Initialize Drive handlers
	if m.handlers != nil {
		return nil
	}
	handlers, err := driveHandlers.NewDriveHandlers(&integrationsDrive.ServiceConfig{
		ClientID:     m.cfg.Drive.ClientID,
		ClientSecret: m.cfg.Drive.ClientSecret,
		RedirectURI:  m.cfg.Drive.RedirectURI,
		TokensDir:    m.cfg.Drive.TokensDir,
	}, m.service, m.sqliteStore)
	if err != nil {
		return err
	}
	m.handlers = handlers
	return nil
}
