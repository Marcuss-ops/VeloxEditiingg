package pipeline

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/workers"
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
		available := false
		for _, worker := range h.workerRegistry.List(c.Request.Context()) {
			if hasStockWindowPreparation(worker) {
				available = true
				break
			}
		}
		capabilities := []string{}
		if available {
			capabilities = append(capabilities, "stock_window_preparation")
		}
		c.JSON(http.StatusOK, gin.H{"capabilities": capabilities})
	}
}

func hasStockWindowPreparation(worker workers.Worker) bool {
	return worker.SessionActive &&
		strings.EqualFold(worker.ConnectionStatus, workers.StatusConnected) &&
		worker.ExecutorRegistrySnapshot().Has("video.prepare.window.v1", 1)
}
