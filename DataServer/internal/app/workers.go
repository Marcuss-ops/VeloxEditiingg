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
type WorkersModule struct {
	reg                 *workersreg.Registry
	adminAuth           gin.HandlerFunc
	workerLifecycle     *lifecycle.Handler
	workerUpdateHandler *workersapi.WorkerUpdateHandler
	workerAssetHandler  *assets.Handler
	workersHandler      *api.WorkersHandler
	metricsHandler      *api.MetricsHandler
	sessionsHandler     *api.SessionsHandler
	eventsHandler       *api.EventsHandler
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
	}
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

	log.Printf("[WORKERS] Routes registered")
}
