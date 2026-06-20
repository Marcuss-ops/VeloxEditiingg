package app

import (
	"log"

	"github.com/gin-gonic/gin"

	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/config"
	workersapi "velox-server/internal/handlers/remote/workers"
	"velox-server/internal/handlers/remote/workers/assets"
	"velox-server/internal/handlers/remote/workers/lifecycle"
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
	}
}

func (m *WorkersModule) Name() string {
	return "workers"
}

func (m *WorkersModule) RegisterRoutes(r *gin.Engine) {
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
		r.POST("/bundle/manifest/generate", m.workerUpdateHandler.GenerateManifestV2Handler())
		// Canonical v2 bundle routes.
		r.GET("/api/worker/v2/manifest", m.workerUpdateHandler.GetManifestV2Handler())
		r.GET("/api/worker/v2/chunk/:chunkName", m.workerUpdateHandler.GetChunkV2Handler())
	}

	if m.workerAssetHandler != nil {
		r.GET("/api/v1/worker-assets/:asset_id", m.workerAssetHandler.ServeAsset())
	}

	log.Printf("[WORKERS] Routes registered")
}
