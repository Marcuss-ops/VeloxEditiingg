package darkeditor

import (
	"github.com/gin-gonic/gin"
	"velox-server/internal/store"
)

// Config holds configuration for the dark editor handlers
type Config struct {
	TempDir       string // Directory for temporary files
	ProjectsDir   string // Directory for projects (fallback)
	LogDir        string // Directory for log files
	NVIDIAAPIKey  string // NVIDIA API key for AI generation
	MaxUploadSize int64  // Max upload size in bytes
	MaxLogSize    int64  // Max log file size in bytes (default: 10MB)
}

// Handler holds the dark editor handlers
type Handler struct {
	cfg    *Config
	store  store.ProjectStore // Optional PostgreSQL store (nil = file-based)
	logger *Logger            // Persistent logger
}

// NewHandler creates a new dark editor handler
func NewHandler(cfg *Config) *Handler {
	if cfg.MaxUploadSize == 0 {
		cfg.MaxUploadSize = 50 * 1024 * 1024 // 50MB default
	}
	if cfg.MaxLogSize == 0 {
		cfg.MaxLogSize = 10 * 1024 * 1024 // 10MB default
	}

	h := &Handler{cfg: cfg}

	// Initialize logger if LogDir is configured
	if cfg.LogDir != "" {
		logger, err := NewLogger(cfg.LogDir, cfg.MaxLogSize)
		if err == nil {
			h.logger = logger
		}
	}

	return h
}

// NewHandlerWithStore creates a new dark editor handler with PostgreSQL store
func NewHandlerWithStore(cfg *Config, projectStore store.ProjectStore) *Handler {
	h := NewHandler(cfg)
	h.store = projectStore
	return h
}

// SetStore sets the project store (for late initialization)
func (h *Handler) SetStore(s store.ProjectStore) {
	h.store = s
}

// SetLogger sets the logger (for late initialization)
func (h *Handler) SetLogger(l *Logger) {
	h.logger = l
}

// GetLogger returns the logger
func (h *Handler) GetLogger() *Logger {
	return h.logger
}

// IntegrationHandlers holds the integration handlers for Dark Editor
type IntegrationHandlers struct {
	BackgroundRemoval *BackgroundRemovalHandler
	YouTube           *YouTubeIntegrationHandler
	Drive             *DriveIntegrationHandler
}

// RegisterAPIRoutes registers all Dark Editor API routes
func RegisterAPIRoutes(r *gin.Engine, h *Handler) {
	RegisterAPIRoutesWithIntegrations(r, h, nil)
}
