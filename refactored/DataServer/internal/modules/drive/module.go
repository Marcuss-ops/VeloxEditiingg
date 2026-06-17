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
	handlers    *driveHandlers.DriveHandlers
}

// New creates a new Drive module.
func New(cfg *config.Config, sqliteStore *store.SQLiteStore) *Module {
	return &Module{
		cfg:         cfg,
		sqliteStore: sqliteStore,
	}
}

// Name returns the module identifier.
func (m *Module) Name() string {
	return "drive"
}

// Handlers returns the Drive handlers (for use by other modules).
func (m *Module) Handlers() *driveHandlers.DriveHandlers {
	return m.handlers
}

// RegisterRoutes registers Drive endpoints.
func (m *Module) RegisterRoutes(r *gin.Engine) {
	// Initialize Drive service
	var driveSvc *integrationsDrive.Service
	if m.cfg.Drive.ClientID != "" && m.cfg.Drive.ClientSecret != "" {
		svc, err := integrationsDrive.NewService(&integrationsDrive.ServiceConfig{
			ClientID:     m.cfg.Drive.ClientID,
			ClientSecret: m.cfg.Drive.ClientSecret,
			RedirectURI:  m.cfg.Drive.RedirectURI,
			TokensDir:    m.cfg.Drive.TokensDir,
		})
		if err != nil {
			log.Printf("[DRIVE] Service init failed: %v", err)
		} else {
			driveSvc = svc
		}
	}

	// Initialize Drive handlers (pass existing store, no secondary connection)
	handlers, err := driveHandlers.NewDriveHandlers(&integrationsDrive.ServiceConfig{
		ClientID:     m.cfg.Drive.ClientID,
		ClientSecret: m.cfg.Drive.ClientSecret,
		RedirectURI:  m.cfg.Drive.RedirectURI,
		TokensDir:    m.cfg.Drive.TokensDir,
	}, driveSvc, m.sqliteStore)
	if err != nil {
		log.Printf("[DRIVE] Handlers init failed: %v", err)
		return
	}
	m.handlers = handlers

	// Register Drive API routes
	driveHandlers.RegisterDriveRoutes(r, m.handlers)
	log.Printf("[DRIVE] API routes registered at /api/drive/*")
}


