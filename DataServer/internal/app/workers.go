package app

import (
	"log"

	"github.com/gin-gonic/gin"

	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/config"
	workersapi "velox-server/internal/handlers/remote/workers"
	"velox-server/internal/handlers/remote/workers/assets"
	"velox-server/internal/handlers/remote/workers/lifecycle"
	"velox-server/internal/handlers/server/api"
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
	reg                          *workersreg.Registry
	adminAuth                    gin.HandlerFunc
	workerLifecycle              *lifecycle.Handler
	workerUpdateHandler          *workersapi.WorkerUpdateHandler
	workerAssetHandler           *assets.Handler
	workersHandler               *api.WorkersHandler
	adminWorkersHandler          *api.AdminWorkersHandler
	adminWorkersMutationsHandler *api.AdminWorkersMutationsHandler
	metricsHandler               *api.MetricsHandler
	sessionsHandler              *api.SessionsHandler
	eventsHandler                *api.EventsHandler
	adminWorkersHealthHandler    *api.AdminWorkersHealthHandler
	adminWorkersSmokeHandler     *api.AdminWorkersSmokeHandler
}

// NewWorkersModule creates a new workers module.
func NewWorkersModule(cfg *config.Config, reg *workersreg.Registry, lifecycle *lifecycle.Handler, updateHandler *workersapi.WorkerUpdateHandler, adminAuth gin.HandlerFunc, assetSvc *voiceoverassets.AssetService, blobStore store.BlobStore) *WorkersModule {
	var tokenMgr *workersreg.TokenManager
	if lifecycle != nil {
		tokenMgr = lifecycle.GetTokenManager()
	}
	return &WorkersModule{
		reg:                 reg,
		workerLifecycle:     lifecycle,
		workerUpdateHandler: updateHandler,
		adminAuth:           adminAuth,
		workerAssetHandler:  assets.NewHandler(cfg, tokenMgr, assetSvc, blobStore),
		workersHandler:      api.NewWorkersHandler(reg),
		adminWorkersHandler: api.NewAdminWorkersHandler(reg),
	}
}

// Registry exposes the underlying worker registry so the composition
// root (cmd/server/bootstrap_composition.go) can pass it to
// dependent handlers built in another file (notably the Step 6/15
// AdminWorkersMutationsHandler, which needs both the Registry
// and the FleetController's ControllerPublisher seam). Read-only
// accessor — callers MUST NOT mutate the registry directly.
func (m *WorkersModule) Registry() *workersreg.Registry { return m.reg }

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

func (m *WorkersModule) Name() string {
	return "workers"
}

func (m *WorkersModule) RegisterRoutes(r *gin.Engine) {
	if m.workerLifecycle != nil {
		r.POST("/api/v1/workers/register", m.workerLifecycle.RegisterV2Handler())
		workerAdmin := r.Group("/worker")
		if m.adminAuth != nil {
			workerAdmin.Use(m.adminAuth)
		}
		workerAdmin.POST("/revoke", m.workerLifecycle.RevokeWorkerHandler())
		workerAdmin.POST("/unrevoke", m.workerLifecycle.UnrevokeWorkerHandler())
		workerAdmin.GET("/revoked", m.workerLifecycle.ListRevokedWorkersHandler())
		workerAdmin.POST("/drain", m.workerLifecycle.DrainWorkerHandler())
		workerAdmin.POST("/restart", m.workerLifecycle.RestartWorkerHandler())
		workerAdmin.POST("/request_update", m.workerLifecycle.RequestUpdateHandler())
	}

	if m.workerUpdateHandler != nil {
		r.POST("/bundle/manifest/generate", m.workerUpdateHandler.GenerateManifestV2Handler())
		// Canonical v2 bundle routes.
		r.GET("/api/worker/v2/manifest", m.workerUpdateHandler.GetManifestV2Handler())
		r.GET("/api/worker/v2/chunk/:chunkName", m.workerUpdateHandler.GetChunkV2Handler())
	}

	if m.workerAssetHandler != nil {
		r.GET("/api/v1/worker-assets/:asset_id", m.workerAssetHandler.ServeAsset())
	}

	// PR 4 — canonical worker read-model endpoints.
	// Protected by admin auth (local IP + read-only GET bypass for dashboards).
	if m.workersHandler != nil {
		v1Workers := r.Group("/api/v1/workers")
		if m.adminAuth != nil {
			v1Workers.Use(m.adminAuth)
		}
		v1Workers.GET("", m.workersHandler.ListWorkers())
		v1Workers.GET("/:worker_id", m.workersHandler.GetWorker())
		// Per-worker metrics / sessions / events read endpoints
		// (RW-PROD-005). Each is registered only when the
		// corresponding handler was wired via the Set* setters so
		// a no-store configuration (tests, partial bootstrap)
		// does not register routes that would 503 every request.
		if m.metricsHandler != nil {
			v1Workers.GET("/:worker_id/metrics", m.metricsHandler.ListWorkerMetrics())
		}
		if m.sessionsHandler != nil {
			v1Workers.GET("/:worker_id/sessions", m.sessionsHandler.ListWorkerSessions())
		}
		if m.eventsHandler != nil {
			v1Workers.GET("/:worker_id/events", m.eventsHandler.ListWorkerEvents())
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
		// quarantine). Mounted inside the same adminAuth-gated
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
	}

	log.Printf("[WORKERS] Routes registered")
}
