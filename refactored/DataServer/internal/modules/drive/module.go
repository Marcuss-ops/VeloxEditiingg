package drive

import (
	"log"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	driveHandlers "velox-server/internal/handlers/server/drive"
	integrationsDrive "velox-server/internal/integrations/drive"
)

// Module provides Google Drive integration endpoints.
type Module struct {
	cfg      *config.Config
	handlers *driveHandlers.DriveHandlers
}

// New creates a new Drive module.
func New(cfg *config.Config) *Module {
	return &Module{
		cfg: cfg,
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
	if m.cfg.DriveClientID != "" && m.cfg.DriveClientSecret != "" {
		svc, err := integrationsDrive.NewService(&integrationsDrive.ServiceConfig{
			ClientID:     m.cfg.DriveClientID,
			ClientSecret: m.cfg.DriveClientSecret,
			RedirectURI:  m.cfg.DriveRedirectURI,
			TokensDir:    m.cfg.DriveTokensDir,
		})
		if err != nil {
			log.Printf("[DRIVE] Service init failed: %v", err)
		} else {
			driveSvc = svc
		}
	}

	// Initialize Drive handlers
	handlers, err := driveHandlers.NewDriveHandlers(&integrationsDrive.ServiceConfig{
		ClientID:     m.cfg.DriveClientID,
		ClientSecret: m.cfg.DriveClientSecret,
		RedirectURI:  m.cfg.DriveRedirectURI,
		TokensDir:    m.cfg.DriveTokensDir,
	}, driveSvc)
	if err != nil {
		log.Printf("[DRIVE] Handlers init failed: %v", err)
		return
	}
	m.handlers = handlers

	// Register Drive API routes
	driveHandlers.RegisterDriveRoutes(r, m.handlers)
	log.Printf("[DRIVE] API routes registered at /api/drive/*")
}
