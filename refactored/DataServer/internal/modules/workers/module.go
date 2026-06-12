package workers

import (
	"context"
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
	cfg               *config.Config
	reg               *workersreg.Registry
	workerLifecycle   *workersapi.WorkerLifecycle
	workerUpdateHandler *workersapi.WorkerUpdateHandler
}

// New creates a new workers module.
func New(cfg *config.Config, reg *workersreg.Registry, lifecycle *workersapi.WorkerLifecycle, updateHandler *workersapi.WorkerUpdateHandler) *Module {
	return &Module{
		cfg:               cfg,
		reg:               reg,
		workerLifecycle:   lifecycle,
		workerUpdateHandler: updateHandler,
	}
}

// Name returns the module identifier.
func (m *Module) Name() string {
	return "workers"
}

// RegisterRoutes registers worker management endpoints.
func (m *Module) RegisterRoutes(r *gin.Engine) {
	// Worker lifecycle routes (worker-token authenticated)
	if m.workerLifecycle != nil {
		r.POST("/api/workers/register", m.workerLifecycle.RegisterCompatHandler())
		r.POST("/api/workers/unregister", m.workerLifecycle.UnregisterCompatHandler())
		r.GET("/api/workers/commands", m.workerLifecycle.GetCommandsCompatHandler())
		r.POST("/api/workers/commands", m.workerLifecycle.GetCommandsCompatHandler())
		r.POST("/api/workers/commands/ack", m.workerLifecycle.AckCommandCompatHandler())
		r.POST("/api/workers/status", m.workerLifecycle.UpdateStatusCompatHandler())

		// Worker-poller endpoints
		r.GET("/worker/command", m.workerLifecycle.WorkerCommandHandler())
		r.POST("/worker/command", m.workerLifecycle.WorkerCommandHandler())
		r.POST("/worker/command_ack", m.workerLifecycle.WorkerCommandAckHandler())
	}

	// Worker management routes (admin protected)
	if m.workerLifecycle != nil {
		workerAdmin := r.Group("/worker")
		// Note: admin auth middleware should be applied here
		workerAdmin.POST("/revoke", m.workerLifecycle.RevokeWorkerHandler())
		workerAdmin.POST("/unrevoke", m.workerLifecycle.UnrevokeWorkerHandler())
		workerAdmin.POST("/drain", m.workerLifecycle.DrainWorkerHandler())
		workerAdmin.POST("/restart", m.workerLifecycle.RestartWorkerHandler())
		workerAdmin.POST("/request_update", m.workerLifecycle.RequestUpdateHandler())
	}

	// Worker bundle endpoints
	if m.workerUpdateHandler != nil {
		r.GET("/bundle_manifest.json", m.workerUpdateHandler.GetBundleManifestHandler())
		r.GET("/api/worker/bundle", m.workerUpdateHandler.GetBundleDownloadHandler())
		r.HEAD("/api/worker/bundle", m.workerUpdateHandler.GetBundleDownloadHandler())
		r.GET("/api/worker/v2/manifest", m.workerUpdateHandler.GetManifestV2Handler())
		r.GET("/api/worker/v2/chunk/:chunkName", m.workerUpdateHandler.GetChunkV2Handler())
	}

	log.Printf("[WORKERS MODULE] Routes registered")
}

// Start initializes the module.
func (m *Module) Start(ctx context.Context) error {
	log.Printf("[WORKERS MODULE] Started")
	return nil
}

// Stop gracefully shuts down the module.
func (m *Module) Stop(ctx context.Context) error {
	log.Printf("[WORKERS MODULE] Stopped")
	return nil
}
