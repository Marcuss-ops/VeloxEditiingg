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
	service  *integrationsDrive.Service
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

// Handlers returns the Drive handlers (for use by other modules).
func (m *Module) Handlers() *driveHandlers.DriveHandlers {
	return m.handlers
}

// Service returns the underlying Drive API service.
func (m *Module) Service() *integrationsDrive.Service {
	return m.service
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

	// Initialize Drive service
	if m.service == nil && m.cfg.DriveClientID != "" && m.cfg.DriveClientSecret != "" {
		svc, err := integrationsDrive.NewService(&integrationsDrive.ServiceConfig{
			ClientID:     m.cfg.DriveClientID,
			ClientSecret: m.cfg.DriveClientSecret,
			RedirectURI:  m.cfg.DriveRedirectURI,
			TokensDir:    m.cfg.DriveTokensDir,
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
		ClientID:     m.cfg.DriveClientID,
		ClientSecret: m.cfg.DriveClientSecret,
		RedirectURI:  m.cfg.DriveRedirectURI,
		TokensDir:    m.cfg.DriveTokensDir,
	}, m.service)
	if err != nil {
		return err
	}
	m.handlers = handlers
	return nil
}
