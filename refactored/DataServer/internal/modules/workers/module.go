package workers

import (
	"log"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	workersapi "velox-server/internal/handlers/remote/workers"
	"velox-server/internal/handlers/remote/workers/assets"
	"velox-server/internal/handlers/remote/workers/lifecycle"
	workersreg "velox-server/internal/workers"
)

// Module provides worker management endpoints.
type Module struct {
	reg                 *workersreg.Registry
	adminAuth           gin.HandlerFunc
	workerLifecycle     *lifecycle.Handler
	workerUpdateHandler *workersapi.WorkerUpdateHandler
	workerAssetHandler  *assets.Handler
}

func New(cfg *config.Config, reg *workersreg.Registry, lifecycle *lifecycle.Handler, updateHandler *workersapi.WorkerUpdateHandler, adminAuth gin.HandlerFunc) *Module {
	var tokenMgr *workersreg.TokenManager
	if lifecycle != nil {
		tokenMgr = lifecycle.GetTokenManager()
	}
	return &Module{
		reg:                 reg,
		workerLifecycle:     lifecycle,
		workerUpdateHandler: updateHandler,
		adminAuth:           adminAuth,
		workerAssetHandler:  assets.NewHandler(cfg, tokenMgr),
	}
}

func (m *Module) Name() string {
	return "workers"
}

func (m *Module) RegisterRoutes(r *gin.Engine) {
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

	if m.workerAssetHandler != nil {
		r.GET("/api/v1/worker-assets/:asset_id", m.workerAssetHandler.ServeAsset())
		r.GET("/api/worker/assets/voiceover/:job_id/:filename", m.workerAssetHandler.ServeVoiceoverAsset())
		r.GET("/api/worker/assets/scene-image/:job_id/:filename", m.workerAssetHandler.ServeSceneImageAsset())
	}

	log.Printf("[WORKERS] Routes registered")
}
