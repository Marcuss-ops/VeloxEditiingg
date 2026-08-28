// Package api — Admin worker capacity endpoint.
//
// GET /api/v1/admin/workers/:worker_id/capacity — reads from the Master SQL
// database (worker_resource_samples, task_attempt_metrics, capacity_benchmark_runs)
// and returns a single canonical capacity report. No Prometheus dependency.
//
// The report joins:
//   - worker_resource_samples → host resource peaks/floors over a configurable window
//   - task_attempt_metrics   → per-job capacity facts (scratch, publish, page faults)
//   - capacity_benchmark_runs → latest benchmark sweet spot + validation
//
// Purpose: a single JSON document that answers "does this worker handle 3, 5, or 8 jobs?"
// with historical evidence from the database, not live Prometheus queries.

package api

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
)

// WorkerCapacityReportDeps holds the read-only store dependency.
type WorkerCapacityReportDeps struct {
	Store *store.SQLiteStore
}

// AdminWorkerCapacityHandler serves GET /api/v1/admin/workers/:worker_id/capacity.
type AdminWorkerCapacityHandler struct {
	deps WorkerCapacityReportDeps
}

// NewAdminWorkerCapacityHandler creates a new handler.
func NewAdminWorkerCapacityHandler(deps WorkerCapacityReportDeps) *AdminWorkerCapacityHandler {
	return &AdminWorkerCapacityHandler{deps: deps}
}

// GetWorkerCapacity returns the capacity report for one worker.
func (h *AdminWorkerCapacityHandler) GetWorkerCapacity() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.deps.Store == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "store not available"})
			return
		}
		workerID := strings.TrimSpace(c.Param("worker_id"))
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id path parameter is required"})
			return
		}

		report, err := h.deps.Store.GetWorkerCapacityReport(c.Request.Context(), workerID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "capacity report query failed: " + err.Error()})
			return
		}
		if report == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "no capacity data found for worker"})
			return
		}
		c.JSON(http.StatusOK, report)
	}
}
