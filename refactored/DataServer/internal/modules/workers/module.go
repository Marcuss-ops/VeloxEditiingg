package workers

import (
	"log"

	"github.com/gin-gonic/gin"
	"velox-server/internal/app"
	"velox-server/internal/config"
	workersapi "velox-server/internal/handlers/remote/workers"
	workersreg "velox-server/internal/workers"
)

// Module provides worker management endpoints.
type Module struct {
	app.BaseModule
	reg                 *workersreg.Registry
	adminAuth           gin.HandlerFunc
	workerLifecycle     *workersapi.WorkerLifecycle
	workerUpdateHandler *workersapi.WorkerUpdateHandler
}

func New(_ *config.Config, reg *workersreg.Registry, lifecycle *workersapi.WorkerLifecycle, updateHandler *workersapi.WorkerUpdateHandler, adminAuth gin.HandlerFunc) *Module {
	return &Module{
		reg:                 reg,
		workerLifecycle:     lifecycle,
		workerUpdateHandler: updateHandler,
		adminAuth:           adminAuth,
	}
}

func (m *Module) Name() string {
	return "workers"
}

func (m *Module) RegisterRoutes(r *gin.Engine) {
	if m.reg != nil {
		r.POST("/api/workers/heartbeat", workersapi.Heartbeat(m.reg))
	}

	if m.workerLifecycle != nil {
		r.POST("/api/workers/register", m.workerLifecycle.RegisterHandler())
		r.POST("/api/workers/unregister", m.workerLifecycle.UnregisterHandler())
		r.GET("/api/workers/commands", m.workerLifecycle.GetCommandsHandler())
		r.POST("/api/workers/commands", m.workerLifecycle.GetCommandsHandler())
		r.POST("/api/workers/commands/ack", m.workerLifecycle.AckCommandHandler())
		r.POST("/api/workers/status", m.workerLifecycle.UpdateStatusHandler())
	}

	if m.workerLifecycle != nil {
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
		r.GET("/bundle_manifest.json", m.workerUpdateHandler.GetBundleManifestHandler())
		r.POST("/bundle/manifest/generate", m.workerUpdateHandler.GenerateManifestV2Handler())
		r.GET("/api/worker/bundle", m.workerUpdateHandler.GetBundleDownloadHandler())
		r.HEAD("/api/worker/bundle", m.workerUpdateHandler.GetBundleDownloadHandler())
		r.GET("/api/worker/v2/manifest", m.workerUpdateHandler.GetManifestV2Handler())
		r.GET("/api/worker/v2/chunk/:chunkName", m.workerUpdateHandler.GetChunkV2Handler())
	}

	log.Printf("[WORKERS] Routes registered")
}


