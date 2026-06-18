package drive

import (
	"log"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	driveHandlers "velox-server/internal/handlers/server/drive"
	integrationsDrive "velox-server/internal/integrations/drive"
	"velox-server/internal/store"
)

// Module provides Google Drive integration endpoints.
type Module struct {
	cfg         *config.Config
	sqliteStore *store.SQLiteStore

	// Lazy-initialized on RegisterRoutes so the constructor doesn't pay the
	// cost when Drive isn't wired up.
	service  *integrationsDrive.Service
	handlers *driveHandlers.DriveHandlers
}

// New creates a new Drive module.
func New(cfg *config.Config) *Module {
	m := &Module{
		cfg: cfg,
	}
	_ = m.init()
	return m
}

// Name returns the module identifier.
func (m *Module) Name() string {
	return "drive"
}

// Service exposes the lazy-initialized Drive service for downstream
// consumers (bootstrap threads it through RegisterV1Routes). Returns nil
// when credentials are absent or init hasn't run.
func (m *Module) Service() *integrationsDrive.Service {
	if m == nil {
		return nil
	}
	return m.service
}

// Handlers returns the Drive handlers (for use by other modules).
func (m *Module) Handlers() *driveHandlers.DriveHandlers {
	return m.handlers
}

// WithSQLiteStore wires the SQLite backend into the Drive module so that
// drive-link writes (cache / master folder upserts / list reads) actually
// persist. Without this call the handler receives a nil store and all writes
// silently no-op, leaving operators under the assumption that "the folder
// exists in SQLite" when it doesn't. Safe to call once after `drive.New(cfg)`.
func (m *Module) WithSQLiteStore(s *store.SQLiteStore) {
	if m == nil || s == nil {
		return
	}
	m.sqliteStore = s
	if m.handlers != nil {
		// Re-init the link cache now that we have a real store. The wrapper
		// inside handlers uses driveLinksStore global internally, but if the
		// adapter splits up after PR9 this hook is the right place to relay.
		log.Printf("[DRIVE] SQLite store wired (driveModules will persist writes)")
	}
}

// RegisterRoutes registers Drive endpoints.
func (m *Module) RegisterRoutes(r *gin.Engine) {
	if err := m.init(); err != nil {
		log.Printf("[DRIVE] Init failed: %v", err)
		return
	}
	if m.handlers == nil {
		log.Printf("[DRIVE] Handlers unavailable after init")
		return
	}
	// Relay the SQLite store through to the handler so drive-link +
	// master-folder CRUD persists. DriveModules were previously constructed
	// without a SQLiteStore reference (m.sqliteStore was zero-value), which
	// meant all write paths ran through the legacy JSON backup only.
	// Bootstrap calls WithSQLiteStore() right before RegisterRoutes so that
	// is the canonical path; we double-up here for any caller that goes
	// through this method directly.
	if m.sqliteStore != nil {
		m.handlers.SetSQLiteStore(m.sqliteStore)
	}
	driveHandlers.RegisterDriveRoutes(r, m.handlers)
	log.Printf("[DRIVE] API routes registered at /api/drive/*")
}

func (m *Module) init() error {
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
	if m.sqliteStore != nil {
		m.handlers.SetSQLiteStore(m.sqliteStore)
	}
	return nil
}
