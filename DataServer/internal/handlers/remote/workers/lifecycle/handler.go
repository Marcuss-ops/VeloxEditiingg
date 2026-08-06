package lifecycle

import (
	"log"
	"strings"

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
// + internal/app.WorkersModule via the GetTokenManager getter.
//
// Phase 5 hygiene pass (dead references after Phase 4.4 UpdateManager removal):
//   - field `store`        — never read (was set from dbStore only to be passed
//     into workersreg.NewCommandManager / NewTokenManager
//     which already takes dbStore).
//   - field `versionNumber` — assigned, never read.
//   - param  `dataDir`     — never referenced in the body.
//   - method `GetCommandManager` — no external callers anywhere in DataServer;
//     sister files in the package access h.cmdMgr
//     directly.
type Handler struct {
	cfg         *config.Config
	reg         *workersreg.Registry
	cmdMgr      *workersreg.CommandManager
	tokenMgr    *workersreg.TokenManager
	dbStore     *store.SQLiteStore // credential persistence + session store
	codeVersion string
}

// NewHandler creates a new lifecycle Handler with SQLite-backed managers.
func NewHandler(cfg *config.Config, reg *workersreg.Registry, dbStore *store.SQLiteStore) *Handler {
	return &Handler{
		cfg:         cfg,
		reg:         reg,
		cmdMgr:      workersreg.NewCommandManager(dbStore),
		tokenMgr:    workersreg.NewTokenManager(dbStore),
		dbStore:     dbStore,
		codeVersion: cfg.Workers.CodeVersion,
	}
}

// GetTokenManager returns the token manager used by the HTTP-based worker
// auth middleware. Do NOT remove without also fixing router.go and
// internal/app.WorkersModule — both access it directly.
func (h *Handler) GetTokenManager() *workersreg.TokenManager {
	return h.tokenMgr
}

// Config returns the runtime config.
func (h *Handler) Config() *config.Config {
	return h.cfg
}

// IsWorkerAllowed reports whether the supplied workerID is on the master-side
// VELOX_ALLOWED_WORKERS allowlist. Used by RegisterV2Handler to reject
// unknown workers with HTTP 403 BEFORE the gRPC stream handshake.
//
// Operator-visible rejection at the HTTP layer: a worker not in the
// allowlist gets an immediate 403 (and no session token), not a transient
// 200 + token followed by a gRPC PermissionDenied later. The gRPC path
// remains authoritative — both paths MUST agree on the allowlist decision;
// they differ only in the status-code surface (HTTP 403 vs gRPC
// PermissionDenied).
//
// The lookup logic mirrors grpcserver/allowlistAuthorizer::IsAllowed byte-
// for-byte (including the `*` wildcard semantics) so drift between the
// HTTP and gRPC paths is impossible at the byte level. A future refactor
// could move both behind a shared internal/auth/workerauthz package;
// until then the duplication is intentional and tested.
//
// Edge cases:
//   - empty workerID                       → denied (always)
//   - allowlist CSV empty OR "*" + production    → denied (bootstrap should have fail-fast blocked this)
//   - allowlist CSV empty OR "*" + dev (AllowInsecureDev=true) → allowed with a one-time warn
//   - allowlist CSV non-empty AND non-`*`        → worker MUST exact-match (whitespace trimmed)
func (h *Handler) IsWorkerAllowed(workerID string) bool {
	if h == nil || h.cfg == nil {
		return false
	}
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return false
	}
	csv := strings.TrimSpace(strings.Join(h.cfg.Workers.AllowedWorkerIDs, ","))
	// Mirror grpcserver/allowlistAuthorizer::IsAllowed: an empty CSV
	// OR a CSV of literal "*" are both treated as "no allowlist"
	// (the dev-bypass surface). Bootstrap rejects "*" via
	// ValidateProductionWorkers so the only configuration that
	// reaches this branch with "*" is dev (or an operator who
	// hand-crafted a config).
	if csv == "" || csv == "*" {
		if h.cfg.Runtime.GRPCAllowInsecureDev {
			log.Printf("[WORKERS][REGISTER] VELOX_ALLOWED_WORKERS is empty/\"*\" in dev mode — allowing %q (matches gRPC handler behaviour)", workerID)
			return true
		}
		log.Printf("[WORKERS][REGISTER] VELOX_ALLOWED_WORKERS is empty/\"*\" in production mode — denying worker %q", workerID)
		return false
	}
	for _, id := range strings.Split(csv, ",") {
		if strings.TrimSpace(id) == workerID {
			return true
		}
	}
	return false
}
