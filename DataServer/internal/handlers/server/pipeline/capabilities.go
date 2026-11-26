package pipeline

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/workers"
	"velox-shared/contract"
	"velox-shared/controltransport"
)

// Capabilities exposes the worker-backed runtime features needed by M2M
// submitters. It uses the same live registry snapshot as task placement and
// is protected by the jobs.submit M2M middleware in RegisterRoutes.
func (h *Handlers) Capabilities() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h == nil || h.workerRegistry == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "worker_capabilities_unavailable"})
			return
		}
		certificationAvailable := false
		for _, worker := range h.workerRegistry.List(c.Request.Context()) {
			if hasVideoSourceCertification(worker) {
				certificationAvailable = true
			}
		}
		capabilities := []string{}
		if certificationAvailable {
			capabilities = append(capabilities, controltransport.CapabilityVideoSourceCertificationV1)
		}
		c.JSON(http.StatusOK, gin.H{"capabilities": capabilities})
	}
}

func hasVideoSourceCertification(worker workers.Worker) bool {
	return worker.SessionActive &&
		strings.EqualFold(worker.ConnectionStatus, workers.StatusConnected) &&
		worker.ExecutorRegistrySnapshot().Has(contract.VideoSourceCertificationExecutorID, contract.VideoSourceCertificationVersion)
}
