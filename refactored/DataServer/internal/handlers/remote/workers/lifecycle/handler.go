package lifecycle

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// Handler manages worker lifecycle operations (registration, heartbeat, commands, control).
type Handler struct {
	cfg           *config.Config
	reg           *workersreg.Registry
	store         *store.SQLiteStore
	cmdMgr        *workersreg.CommandManager
	updateMgr     *workersreg.UpdateManager
	tokenMgr      *workersreg.TokenManager
	codeVersion   string
	versionNumber string
}

// NewHandler creates a new lifecycle Handler with SQLite-backed managers.
func NewHandler(cfg *config.Config, reg *workersreg.Registry, dbStore *store.SQLiteStore, dataDir string) *Handler {
	return &Handler{
		cfg:           cfg,
		reg:           reg,
		store:         dbStore,
		cmdMgr:        workersreg.NewCommandManager(dbStore),
		updateMgr:     workersreg.NewUpdateManager(),
		tokenMgr:      workersreg.NewTokenManager(dbStore),
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

func (h *Handler) authorizeWorkerRequest(c *gin.Context, workerID string) bool {
	token := workersreg.ExtractBearerToken(
		c.GetHeader("Authorization"),
		c.GetHeader("X-Admin-Token"),
		c.Query("token"),
	)
	if !workersreg.AuthorizeWorkerToken(h.tokenMgr, token, workerID, c.ClientIP()) {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid worker token"})
		return false
	}
	return true
}
