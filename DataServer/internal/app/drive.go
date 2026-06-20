package app

import (
	"fmt"
	"log"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	driveHandlers "velox-server/internal/handlers/server/drive"
	integrationsDrive "velox-server/internal/integrations/drive"
	"velox-server/internal/store"
)

// DriveModule provides Google Drive integration endpoints.
//
// PR15.1: Drive is constructed fully in NewDriveModule(cfg, sqliteStore).
// No more WithSQLiteStore setter, no more double-init() in RegisterRoutes.
// RegisterRoutes only mounts HTTP routes.
type DriveModule struct {
	cfg         *config.Config
	sqliteStore *store.SQLiteStore

	service  *integrationsDrive.Service
	handlers *driveHandlers.DriveHandlers
}

// NewDriveModule creates a fully-initialized Drive module.
//
// The SQLite store is passed directly (no setter). All initialization
// runs synchronously here so a service instance is available before any
// route is registered. Returns an error rather than swallowing failures
// silently — the original constructor dropped init errors on the floor.
func NewDriveModule(cfg *config.Config, sqliteStore *store.SQLiteStore) (*DriveModule, error) {
	m := &DriveModule{
		cfg:         cfg,
		sqliteStore: sqliteStore,
	}
	if err := m.init(); err != nil {
		return nil, fmt.Errorf("drive module init: %w", err)
	}
	return m, nil
}

// Name returns the module identifier.
func (m *DriveModule) Name() string {
	return "drive"
}

// Service exposes the Drive service for downstream consumers (bootstrap
// threads it through RegisterV1Routes / delivery providers). Returns nil
// when credentials are absent or init did not run.
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

// RegisterRoutes registers Drive endpoints.
//
// PR15.1: RegisterRoutes no longer calls init() — the module is fully
// initialized. It only mounts HTTP routes. The SetSQLiteStore call is
// still safe (idempotent) because handlers.SetSQLiteStore was already
// called once during init().
func (m *DriveModule) RegisterRoutes(r *gin.Engine) {
	if m == nil || m.handlers == nil {
		log.Printf("[DRIVE] Handlers unavailable")
		return
	}
	if m.sqliteStore != nil {
		m.handlers.SetSQLiteStore(m.sqliteStore)
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
	// Single source of truth for SQLite wiring on Drive handlers:
	//   - init() sets it once so Handlers() returns a fully-wired handler.
	//   - RegisterRoutes calls SetSQLiteStore again as a no-op/idempotent
	//     guard for callers that go through RegisterRoutes directly.
	// Nil-safe (SetSQLiteStore must accept nil and no-op).
	if m.sqliteStore != nil {
		m.handlers.SetSQLiteStore(m.sqliteStore)
	}
	return nil
}
