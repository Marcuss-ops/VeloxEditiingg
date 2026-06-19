package lifecycle

import (
	"velox-server/internal/config"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// Handler manages worker lifecycle operations (registration, heartbeat,
// commands, control). The fields below are consumed by the sibling files
// in this package (commands.go: RequestUpdateHandler — h.cmdMgr,
// h.codeVersion; control.go: RestartWorkerHandler/RevokeWorkerHandler/
// DrainWorkerHandler/GetWorkerDetailsHandler/CleanupStaleWorkersHandler/
// ListRevokedWorkersHandler — h.cmdMgr, h.reg, h.tokenMgr) and by router.go
// + internal/modules/workers via the GetTokenManager getter.
//
// Phase 5 hygiene pass (dead references after Phase 4.4 UpdateManager removal):
//   - field `store`        — never read (was set from dbStore only to be passed
//                            into workersreg.NewCommandManager / NewTokenManager
//                            which already takes dbStore).
//   - field `versionNumber` — assigned, never read.
//   - param  `dataDir`     — never referenced in the body.
//   - method `GetCommandManager` — no external callers anywhere in DataServer;
//                              sister files in the package access h.cmdMgr
//                              directly.
type Handler struct {
	cfg         *config.Config
	reg         *workersreg.Registry
	cmdMgr      *workersreg.CommandManager
	tokenMgr    *workersreg.TokenManager
	codeVersion string
}

// NewHandler creates a new lifecycle Handler with SQLite-backed managers.
func NewHandler(cfg *config.Config, reg *workersreg.Registry, dbStore *store.SQLiteStore) *Handler {
	return &Handler{
		cfg:         cfg,
		reg:         reg,
		cmdMgr:      workersreg.NewCommandManager(dbStore),
		tokenMgr:    workersreg.NewTokenManager(dbStore),
		codeVersion: cfg.Workers.CodeVersion,
	}
}

// GetTokenManager returns the token manager used by the HTTP-based worker
// auth middleware. Do NOT remove without also fixing router.go and
// internal/modules/workers — both access it directly.
func (h *Handler) GetTokenManager() *workersreg.TokenManager {
	return h.tokenMgr
}

// Config returns the runtime config.
func (h *Handler) Config() *config.Config {
	return h.cfg
}


