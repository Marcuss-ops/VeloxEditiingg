package lifecycle

import (
	"velox-server/internal/config"
	workersreg "velox-server/internal/workers"
)

// Handler manages worker lifecycle operations (registration, heartbeat, commands, control).
type Handler struct {
	cfg           *config.Config
	reg           *workersreg.Registry
	cmdMgr        *workersreg.CommandManager
	updateMgr     *workersreg.UpdateManager
	tokenMgr      *workersreg.TokenManager
	codeVersion   string
	versionNumber string
}

// NewHandler creates a new lifecycle Handler.
func NewHandler(cfg *config.Config, reg *workersreg.Registry, dataDir string) *Handler {
	return &Handler{
		cfg:           cfg,
		reg:           reg,
		cmdMgr:        workersreg.NewCommandManager(),
		updateMgr:     workersreg.NewUpdateManager(),
		tokenMgr:      workersreg.NewTokenManager(),
		codeVersion:   cfg.Workers.CodeVersion,
		versionNumber: cfg.Workers.VersionNumber,
	}
}

// GetCommandManager returns the command manager.
func (h *Handler) GetCommandManager() *workersreg.CommandManager {
	return h.cmdMgr
}

// GetUpdateManager returns the update manager.
func (h *Handler) GetUpdateManager() *workersreg.UpdateManager {
	return h.updateMgr
}

// GetTokenManager returns the token manager.
func (h *Handler) GetTokenManager() *workersreg.TokenManager {
	return h.tokenMgr
}

// Config returns the runtime config.
func (h *Handler) Config() *config.Config {
	return h.cfg
}
