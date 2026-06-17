package drive

import (
	"log"
	"strings"

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
func New(cfg *config.Config, sqliteStore *store.SQLiteStore) *Module {
	m := &Module{
		cfg:         cfg,
		sqliteStore: sqliteStore,
	}
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

	// Defensive guard: Drive is an optional integration. If credentials
	// are absent, don't construct handlers — RegisterRoutes will skip them.
	// This avoids NewDriveHandlers validating empty config and panicking.
	if strings.TrimSpace(m.cfg.Drive.ClientID) == "" || strings.TrimSpace(m.cfg.Drive.ClientSecret) == "" {
		log.Printf("[DRIVE] credentials missing; Drive module will run in disabled mode (no handlers, no routes)")
		return nil
	}

	// Initialize Drive service. The cfg.Drive sub-struct is the canonical
	// source post-PR0a refactor; legacy cfg.DriveClientID/Secret accesses
	// were retired along with the flat Config field aliases.
	if m.service == nil {
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
