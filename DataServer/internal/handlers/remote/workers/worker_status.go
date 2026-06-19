package workers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// WorkersList response shape for GET /workers
func WorkersList(reg *workersreg.Registry, workersRepo store.WorkersRepository, updateHandler ...*WorkerUpdateHandler) gin.HandlerFunc {
	return func(c *gin.Context) {
		master := workerStatusMetadata(firstUpdateHandler(updateHandler))
		if workersRepo != nil {
			if dbWorkers, err := workersRepo.ListWorkers(); err == nil && len(dbWorkers) > 0 {
				c.JSON(http.StatusOK, gin.H{"workers": dbWorkers, "master": master})
				return
			}
		}
		list := reg.List(c.Request.Context())
		c.JSON(http.StatusOK, gin.H{"workers": list, "master": master})
	}
}

func firstUpdateHandler(updateHandler []*WorkerUpdateHandler) *WorkerUpdateHandler {
	if len(updateHandler) == 0 || updateHandler[0] == nil {
		return nil
	}
	return updateHandler[0]
}

func WorkerStatusMetadata(h *WorkerUpdateHandler) gin.H {
	return workerStatusMetadata(h)
}

func workerStatusMetadata(h *WorkerUpdateHandler) gin.H {
	version := ""
	codeVersion := ""
	if h != nil {
		version = h.cfg.Workers.VersionNumber
		codeVersion = h.codeVersion
		if codeVersion == "" {
			codeVersion = h.cfg.Workers.VersionNumber
		}
	}
	if codeVersion == "" {
		codeVersion = version
	}
	bundleHash := ""
	if h != nil {
		bundleHash = h.ComputeBundleSHA256()
		if bundleHash == "" {
			bundleHash = computeBundleHashFromManifest(h.bundleDir)
		}
	}
	return gin.H{
		"bundle_version":   version,
		"bundle_hash":      bundleHash,
		"code_version":     codeVersion,
		"protocol_version": workersreg.DefaultWorkerProtocolVersion,
		"engine_version":   codeVersion,
	}
}
