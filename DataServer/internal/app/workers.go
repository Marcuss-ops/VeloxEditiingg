package app

import (
	"log"
	"strings"

	"github.com/gin-gonic/gin"

	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/config"
	workersapi "velox-server/internal/handlers/remote/workers"
	"velox-server/internal/handlers/remote/workers/assets"
	"velox-server/internal/handlers/remote/workers/lifecycle"
	validationhandlers "velox-server/internal/handlers/remote/workers/validation"
	"velox-server/internal/handlers/server/api"
	driveintegration "velox-server/internal/integrations/drive"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// WorkersModule provides worker management endpoints.
//
// Step 1/15 of the fleet-operator rollout adds `adminWorkersHandler`
// (the canonical WorkerCard surface under /api/v1/admin/workers).
// Step 6/15 of the fleet-operator rollout adds
// `adminWorkersMutationsHandler` (POST /api/v1/admin/workers/{id}/
// /{drain,resume,quarantine}). Both handlers read from the SAME
// registry and live under the SAME adminAuth-gated URL group; the
// mutations handler additionally depends on the FleetController
// (injected via SetMutationsHandler after buildFleet is wired in
// cmd/server/bootstrap_composition.go). Keeping them co-resident
// avoids duplicating the route table or the registry wiring
// boilerplate.
type WorkersModule struct {
	reg                                  *workersreg.Registry
	tokenMgr                             *workersreg.TokenManager
	adminAuth                            gin.HandlerFunc
	workerLifecycle                      *lifecycle.Handler
	workerUpdateHandler                  *workersapi.WorkerUpdateHandler
	workerAssetHandler                   *assets.Handler
	workersHandler                       *api.WorkersHandler
	adminWorkersHandler                  *api.AdminWorkersHandler
	adminWorkersMutationsHandler         *api.AdminWorkersMutationsHandler
	metricsHandler                       *api.MetricsHandler
	sessionsHandler                      *api.SessionsHandler
	eventsHandler                        *api.EventsHandler
	adminWorkersHealthHandler            *api.AdminWorkersHealthHandler
	adminWorkersSmokeHandler             *api.AdminWorkersSmokeHandler
	adminWorkersMetricsAggregatorHandler *api.AdminWorkersMetricsAggregatorHandler
	adminWorkersSSHCheckHandler          *api.AdminWorkersSSHCheckHandler
	adminWorkersAlertsHandler            *api.AdminWorkersAlertsHandler
	validationHandler                    *validationhandlers.Handler
	protectedAssetsHandler               *api.ProtectedAssetsHandler
	protectedAssetsAuth                  gin.HandlerFunc
}

// NewWorkersModule creates a new workers module.
func NewWorkersModule(cfg *config.Config, reg *workersreg.Registry, lifecycle *lifecycle.Handler, updateHandler *workersapi.WorkerUpdateHandler, adminAuth gin.HandlerFunc, assetSvc *voiceoverassets.AssetService, blobStore store.BlobStore, driveSvcs ...*driveintegration.Service) *WorkersModule {
	var tokenMgr *workersreg.TokenManager
	if lifecycle != nil {
		tokenMgr = lifecycle.GetTokenManager()
	}
	var driveSvc *driveintegration.Service
	if len(driveSvcs) > 0 {
		driveSvc = driveSvcs[0]
	}
	return &WorkersModule{
		reg:                 reg,
		tokenMgr:            tokenMgr,
		workerLifecycle:     lifecycle,
		workerUpdateHandler: updateHandler,
		adminAuth:           adminAuth,
		workerAssetHandler:  assets.NewHandler(cfg, tokenMgr, assetSvc, blobStore, driveSvc),
		workersHandler:      api.NewWorkersHandler(reg),
		adminWorkersHandler: api.NewAdminWorkersHandler(reg),
		protectedAssetsAuth: api.WorkerOrAdminAuthMiddleware(cfg, tokenMgr),
	}
}

// IssueAssetPickupToken issues a worker-session token for the canonical
// /api/v1/agent/assets/:asset_id endpoint. The composition root uses this
// only to build the production Level-D smoke pickup URL; asset bytes still
// come from AssetService + BlobStore and the endpoint remains authenticated.
func (m *WorkersModule) IssueAssetPickupToken(principal string) string {
	if m == nil || m.tokenMgr == nil || strings.TrimSpace(principal) == "" {
		return ""
	}
	return m.tokenMgr.GenerateToken(strings.TrimSpace(principal))
}

// SetProtectedAssetsHandler wires the master lookahead snapshot consumed by
// worker cache cleaners. The route is intentionally worker-token protected,
// not admin-only, because every worker polls it during normal operation.
func (m *WorkersModule) SetProtectedAssetsHandler(h *api.ProtectedAssetsHandler) {
	if m == nil {
		return
	}
	m.protectedAssetsHandler = h
}

// Registry exposes the underlying worker registry so the composition
// root (cmd/server/bootstrap_composition.go) can pass it to
// dependent handlers built in another file (notably the Step 6/15
// AdminWorkersMutationsHandler, which needs both the Registry
// and the FleetController's ControllerPublisher seam). Read-only
// accessor — callers MUST NOT mutate the registry directly.
func (m *WorkersModule) Registry() *workersreg.Registry { return m.reg }

// SetDeploymentReader wires the read-only deployment ledger into the admin
// worker cards. Mutations remain owned by FleetController.
func (m *WorkersModule) SetDeploymentReader(reader api.DeploymentReader) {
	if m != nil && m.adminWorkersHandler != nil {
		m.adminWorkersHandler.SetDeploymentReader(reader)
	}
}

// SetOperationLedgerReader wires the read-only fleet_operations audit
// ledger into the admin worker cards (WorkerOperationState.Error — the
// failure reason of the last update/rollback). Mutations remain owned by
// FleetController.
func (m *WorkersModule) SetOperationLedgerReader(reader api.OperationLedgerReader) {
	if m != nil && m.adminWorkersHandler != nil {
		m.adminWorkersHandler.SetOperationLedgerReader(reader)
	}
}

// SetMutationsHandler wires the Step 6/15 admin mutations handler
// (POST drain/resume/quarantine). Idempotent — safe to call before
// RegisterRoutes; passing nil disables the POST routes so a
// misconfigured FleetController wire-up keeps the surface silent
// rather than 503-on-every-request.
//
// Composition order: buildFleet (which constructs FleetController) is
// called AFTER buildModules (which constructs WorkersModule), so the
// composition root injects the handler here AFTER buildFleet returns.
func (m *WorkersModule) SetMutationsHandler(h *api.AdminWorkersMutationsHandler) {
	m.adminWorkersMutationsHandler = h
}

// SetMetricsHandler wires the per-worker metrics read endpoint
// (GET /api/v1/workers/:worker_id/metrics). Idempotent; safe to
// call before RegisterRoutes. Passing nil disables the route.
func (m *WorkersModule) SetMetricsHandler(h *api.MetricsHandler) { m.metricsHandler = h }

// SetSessionsHandler wires the per-worker sessions read endpoint
// (GET /api/v1/workers/:worker_id/sessions). Idempotent; safe to
// call before RegisterRoutes. Passing nil disables the route.
func (m *WorkersModule) SetSessionsHandler(h *api.SessionsHandler) { m.sessionsHandler = h }

// SetEventsHandler wires the per-worker events read endpoint
// (GET /api/v1/workers/:worker_id/events). Idempotent; safe to
// call before RegisterRoutes. Passing nil disables the route.
func (m *WorkersModule) SetEventsHandler(h *api.EventsHandler) { m.eventsHandler = h }

// SetHealthHandler wires the Step 10/15 admin workers 4-level
// health probe handler (GET /api/v1/admin/workers/{id}/health
// with ?level=A|B|C|D, or absent returns aggregated envelope
// over the 4 levels). Composition order: buildFleet (which
// wires the FleetController and registry) returns BEFORE
// buildModules returns moduleDeps.Workers; the composition
// root injects the handler here AFTER buildFleet finishes,
// matching the SetMutationsHandler / SetMetricsHandler / Set
// -SessionsHandler / SetEventsHandler pattern.
//
// Idempotent — safe to call before RegisterRoutes; passing
// nil disables the route so a misconfigured bootstrap does
// not 503-on-every-request (the adminWorkersHealthHandler
// nil-guard inside RegisterRoutes handles the skip).
func (m *WorkersModule) SetHealthHandler(h *api.AdminWorkersHealthHandler) {
	m.adminWorkersHealthHandler = h
}

// SetSmokeHandler wires the Step 12/15 admin workers smoke
// handler (POST /api/v1/admin/workers/{id}/smoke — publishes
// an OperationKindSmoke to the FleetController queue for
// async execution by LevelDSmokeExecutor).
//
// Idempotent — safe to call before RegisterRoutes; passing
// nil disables the route so a misconfigured bootstrap does
// not 503-on-every-request (the adminWorkersSmokeHandler
// nil-guard inside RegisterRoutes handles the skip).
func (m *WorkersModule) SetSmokeHandler(h *api.AdminWorkersSmokeHandler) {
	m.adminWorkersSmokeHandler = h
}

// SetMetricsAggregatorHandler wires the Step 13/15 admin workers
// telemetry endpoint (GET /api/v1/admin/workers/:worker_id/metrics —
// per-worker snapshot; the fleet-wide aggregate serves from
// GET /api/v1/fleet/metrics, the canonical Phase 6 fleet namespace).
// Both read the persisted 13-metric snapshot written every 5 minutes
// by the metrics-snapshot-supervisor in
// cmd/server/bootstrap_composition.go.
//
// Idempotent — safe to call before RegisterRoutes; passing nil
// disables the routes so a misconfigured bootstrap (no SQLite
// store) does not 503-on-every-request.
func (m *WorkersModule) SetMetricsAggregatorHandler(h *api.AdminWorkersMetricsAggregatorHandler) {
	m.adminWorkersMetricsAggregatorHandler = h
}

// SetAlertsHandler wires the Step 16/15 admin workers structured
// alerting endpoints: per-worker alerts at
// GET /api/v1/admin/workers/:worker_id/alerts, plus the fleet-wide
// ledger at GET /api/v1/fleet/alerts/active + GET /api/v1/fleet/
// alerts/recent (the canonical Phase 6 fleet namespace; the legacy
// /api/v1/admin/alerts aliases were removed). All read from the
// alert_events table (migration 107) populated every 5 minutes by
// the alerts-supervisor in cmd/server/bootstrap_composition.go.
//
// Idempotent — safe to call before RegisterRoutes; passing nil
// disables the routes so a misconfigured bootstrap (no SQLite
// store) does not 503-on-every-request.
// SetSSHCheckHandler wires the Step 17/15 fleet-operator SSH
// connectivity diagnostic (GET /api/v1/admin/workers/ssh-check).
// Idempotent — safe to call before RegisterRoutes; passing nil
// disables the route (the nil-guard inside RegisterRoutes handles
// the skip).
func (m *WorkersModule) SetSSHCheckHandler(h *api.AdminWorkersSSHCheckHandler) {
	m.adminWorkersSSHCheckHandler = h
}

func (m *WorkersModule) SetAlertsHandler(h *api.AdminWorkersAlertsHandler) {
	m.adminWorkersAlertsHandler = h
}

// SetValidationHandler wires the persistent worker-validation endpoints.
// The handler is injected at the composition root so no route can silently
// fall back to an in-memory or permissive validation state.
func (m *WorkersModule) SetValidationHandler(h *validationhandlers.Handler) {
	if m != nil {
		m.validationHandler = h
	}
}

func (m *WorkersModule) Name() string {
	return "workers"
}

func (m *WorkersModule) RegisterRoutes(r *gin.Engine) {
	// ── Canonical /api/v1/agent namespace (Phase 6 API-surface
	//    unification) ────────────────────────────────────────────
	// Worker-AUTHENTICATED traffic only: register, cache snapshot,
	// asset download. These routes authenticate with the worker session
	// token (or the worker credential on register); they are NOT
	// operator surfaces. The legacy pre-canonical paths were removed
	// once the usage counter showed zero sustained traffic.
	if m.workerLifecycle != nil {
		r.POST("/api/v1/agent/register", m.workerLifecycle.RegisterV2Handler())
	}
	if m.validationHandler != nil {
		r.POST("/api/v1/agent/validation", m.protectedAssetsAuth, m.validationHandler.HandleValidationReport())
	}
	if m.workerAssetHandler != nil {
		r.GET("/api/v1/agent/assets/:asset_id", m.workerAssetHandler.ServeAsset())
	}
	if m.workersHandler != nil && m.protectedAssetsHandler != nil {
		r.GET("/api/v1/agent/cache/protected-assets", m.protectedAssetsAuth, m.protectedAssetsHandler.Snapshot())
	}

	// Legacy bundle/update HTTP routes were retired. Bundle generation,
	// manifest/chunk serving, force rebuild, fleet-wide bundle updates, and
	// worker-requested updates are intentionally not mounted here; callers
	// must use the canonical admin worker update operation and the current
	// worker control protocol. Keeping the handlers available for internal
	// migration/tests does not expose a legacy HTTP surface.

	// PR 4 — canonical worker read-model endpoints.
	// The protected-assets snapshot is consumed by workers with their worker
	// session token, so it must not be nested under the admin-only group.
	// The /api/v1/workers diagnostic surface (list/get + per-worker read
	// endpoints) is still consumed by the operator runbook and
	// scripts/cert/master_state.sh; it is counted as surface=legacy by the
	// route-usage middleware until those consumers migrate to
	// /api/v1/admin/workers.
	if m.workersHandler != nil {
		adminWorkers := r.Group("/api/v1/workers")
		if m.adminAuth != nil {
			adminWorkers.Use(m.adminAuth)
		}
		adminWorkers.GET("", m.workersHandler.ListWorkers())
		adminWorkers.GET("/:worker_id", m.workersHandler.GetWorker())
		// Per-worker metrics / sessions / events read endpoints
		// (RW-PROD-005). Each is registered only when the
		// corresponding handler was wired via the Set* setters so
		// a no-store configuration (tests, partial bootstrap)
		// does not register routes that would 503 every request.
		if m.metricsHandler != nil {
			adminWorkers.GET("/:worker_id/metrics", m.metricsHandler.ListWorkerMetrics())
		}
		if m.sessionsHandler != nil {
			adminWorkers.GET("/:worker_id/sessions", m.sessionsHandler.ListWorkerSessions())
		}
		if m.eventsHandler != nil {
			adminWorkers.GET("/:worker_id/events", m.eventsHandler.ListWorkerEvents())
		}
	}

	// Step 1/15 — Fleet operator canonical WorkerCard endpoints.
	// Distinct URL from /api/v1/workers (allowlist/diagnostic surface);
	// these are gated explicitly by adminAuth (VELOX_ADMIN_TOKEN)
	// so the operator dashboard never bypasses auth. The nil-guard
	// mirrors the diagnostic surface's pattern: a misconfigured
	// bootstrap that passes a nil handler keeps the route
	// un-mounted rather than mounting a 503-on-every-request dead
	// route. Same registry as the diagnostic surface — the only
	// difference is shape (WorkerCard vs WorkerResponse) and auth.
	if m.adminWorkersHandler != nil {
		adminWorkers := r.Group("/api/v1/admin/workers")
		if m.adminAuth != nil {
			adminWorkers.Use(m.adminAuth)
		}
		adminWorkers.GET("", m.adminWorkersHandler.ListAdminWorkers())
		adminWorkers.GET("/:worker_id", m.adminWorkersHandler.GetAdminWorker())
		// Step 6/15 — admin worker mutations (drain / resume /
		// quarantine / update). Mounted inside the same adminAuth-gated
		// group as the read endpoints so the operator dashboard's
		// canonical auth surface stays single-source-of-truth.
		// Nil-tolerant via the adminWorkersMutationsHandler nil
		// guard (the composition root may pass nil when the
		// FleetController is unavailable — silent skip rather than
		// a 503-on-every-request dead route).
		if m.adminWorkersMutationsHandler != nil {
			adminWorkers.POST("/:worker_id/drain", m.adminWorkersMutationsHandler.DrainWorker())
			adminWorkers.POST("/:worker_id/resume", m.adminWorkersMutationsHandler.ResumeWorker())
			adminWorkers.POST("/:worker_id/quarantine", m.adminWorkersMutationsHandler.QuarantineWorker())
			adminWorkers.POST("/:worker_id/update", m.adminWorkersMutationsHandler.UpdateWorker())
			adminWorkers.POST("/:worker_id/config", m.adminWorkersMutationsHandler.ConfigWorker())
		}
		// Legacy /worker/* control actions migrated into the canonical
		// admin namespace (Phase 6 API-surface unification): revoke /
		// unrevoke / restart now identify the worker via the :worker_id
		// path param instead of the legacy JSON body. Drain is served
		// exclusively by the canonical mutation handler above; the old
		// /worker/drain route is gone.
		if m.workerLifecycle != nil {
			adminWorkers.POST("/:worker_id/revoke", m.workerLifecycle.RevokeWorkerHandler())
			adminWorkers.POST("/:worker_id/unrevoke", m.workerLifecycle.UnrevokeWorkerHandler())
			adminWorkers.POST("/:worker_id/restart", m.workerLifecycle.RestartWorkerHandler())
		}
		// Legacy GET /worker/revoked migrated to the canonical admin
		// namespace: GET /api/v1/admin/workers/revoked (list of revoked
		// worker IDs).
		if m.workerLifecycle != nil {
			adminWorkers.GET("/revoked", m.workerLifecycle.ListRevokedWorkersHandler())
		}
		// Step 10/15 — 4-level health probe endpoint
		// (GET /api/v1/admin/workers/:worker_id/health?level=A|B|C|D).
		// Mounted in the same adminAuth-gated group as the
		// mutations routes so the operator dashboard's canonical
		// auth surface stays single-source-of-truth. Nil-tolerant
		// via the adminWorkersHealthHandler nil guard — silent
		// skip rather than 503-on-every-request when the
		// FleetController / registry isn't wired.
		if m.adminWorkersHealthHandler != nil {
			adminWorkers.GET("/:worker_id/health", m.adminWorkersHealthHandler.GetWorkerHealth())
		}
		// Step 12/15 — on-demand Level D smoke endpoint
		// (POST /api/v1/admin/workers/:worker_id/smoke).
		// Mounted in the same adminAuth-gated group as the
		// mutations routes so the operator dashboard's canonical
		// auth surface stays single-source-of-truth. Nil-tolerant
		// via the adminWorkersSmokeHandler nil guard — silent
		// skip rather than 503-on-every-request when the
		// FleetController isn't wired. Real smoke execution is
		// driven async by LevelDSmokeExecutor via the FleetController
		// tick goroutine (Step 7+).
		if m.adminWorkersSmokeHandler != nil {
			adminWorkers.POST("/:worker_id/smoke", m.adminWorkersSmokeHandler.TriggerSmoke())
		}
		// Step 13/15 — fleet telemetry (per-worker endpoint only):
		//   GET /api/v1/admin/workers/:worker_id/metrics
		//     → latest snapshot for one worker (404 when no
		//       snapshot yet for the worker; the scheduler
		//       writes one within 5 min of bootstrap).
		// Serves the persisted worker_metrics_snapshots table
		// (migration 105); the dashboard renders a staleness
		// indicator via the snapshotted_at field rather than
		// computing on every read. Nil-tolerant via the
		// adminWorkersMetricsAggregatorHandler nil guard. The
		// fleet-wide snapshot lives at GET /api/v1/fleet/metrics
		// (the canonical fleet namespace); the legacy
		// /api/v1/admin/workers/metrics route was removed.
		if m.adminWorkersMetricsAggregatorHandler != nil {
			adminWorkers.GET("/:worker_id/metrics", m.adminWorkersMetricsAggregatorHandler.GetWorkerMetrics())
		}
		// Step 16/15 — fleet operator structured alerting
		// (12-rule catalog persisted to alert_events). Mounted
		// inside the same adminAuth-gated group so the operator
		// dashboard's canonical auth surface stays
		// single-source-of-truth. Nil-tolerant via the
		// adminWorkersAlertsHandler nil guard.
		if m.adminWorkersAlertsHandler != nil {
			adminWorkers.GET("/:worker_id/alerts", m.adminWorkersAlertsHandler.ListWorkerAlerts())
		}
		// Step 17/15 — fleet-operator SSH connectivity diagnostic.
		// GET /api/v1/admin/workers/ssh-check — one row per worker in
		// the canonical WorkerNodeRegistry (ssh / hostkey / sudo -n).
		// Mounted in the same adminAuth-gated group as the other
		// read endpoints. Nil-tolerant via the sshCheckHandler nil
		// guard (silent skip when the registry isn't wired).
		if m.adminWorkersSSHCheckHandler != nil {
			adminWorkers.GET("/ssh-check", m.adminWorkersSSHCheckHandler.RunSSHCheck())
		}
	}
	// ── Canonical /api/v1/fleet namespace (Phase 6) ───────────────
	// Aggregated fleet surfaces: the fleet-wide metrics snapshot and the
	// fleet-wide alert ledger. These are NOT per-worker and NOT agent
	// traffic — they feed dashboards, so they live under /api/v1/fleet
	// (adminAuth-gated like the operator surface). The legacy
	// /api/v1/admin/workers/metrics + /api/v1/admin/alerts aliases were
	// removed once the usage counter showed zero sustained traffic.
	fleetGroup := r.Group("/api/v1/fleet")
	if m.adminAuth != nil {
		fleetGroup.Use(m.adminAuth)
	}
	if m.adminWorkersMetricsAggregatorHandler != nil {
		fleetGroup.GET("/metrics", m.adminWorkersMetricsAggregatorHandler.ListFleetMetrics())
	}
	if m.adminWorkersAlertsHandler != nil {
		fleetGroup.GET("/alerts/active", m.adminWorkersAlertsHandler.ListFleetActiveAlerts())
		fleetGroup.GET("/alerts/recent", m.adminWorkersAlertsHandler.ListRecentAlerts())
	}
	if m.validationHandler != nil {
		// Keep validation independent from the WorkerCard handler so a
		// partial bootstrap cannot silently omit a persistence-backed route.
		adminValidation := r.Group("/api/v1/admin/workers")
		if m.adminAuth != nil {
			adminValidation.Use(m.adminAuth)
		}
		adminValidation.GET("/:worker_id/validation", m.validationHandler.GetWorkerValidationHandler())
		fleetGroup.GET("/validations", m.validationHandler.GetAllValidationsHandler())
	}

	log.Printf("[WORKERS] Routes registered")
}
