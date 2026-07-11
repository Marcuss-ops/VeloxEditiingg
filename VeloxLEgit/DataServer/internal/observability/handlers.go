// Package observability / handlers.go
//
// HTTP handlers for the Velox observability REST API.
// Follows the existing Gin HandlerFunc pattern used across the DataServer.
// All handlers delegate to the Service for business logic and are
// read-only — no mutations, no side effects.
package observability

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

// Handlers groups the HTTP handler methods for the observability API.
type Handlers struct {
	svc *Service
}

// NewHandlers creates a new Handlers instance backed by the given service.
func NewHandlers(svc *Service) *Handlers {
	if svc == nil {
		panic("observability: NewHandlers requires a non-nil Service")
	}
	return &Handlers{svc: svc}
}

// OverviewHandler returns the aggregate system health snapshot.
//
//	GET /api/observability/overview
func (h *Handlers) OverviewHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		result, err := h.svc.Overview(c.Request.Context())
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":   "overview_failed",
				"message": err.Error(),
			})
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

// JobDetailHandler returns the detailed waterfall for a single job.
//
//	GET /api/observability/jobs/:job_id
func (h *Handlers) JobDetailHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Param("job_id")
		if jobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "missing_job_id",
				"message": "job_id path parameter is required",
			})
			return
		}

		result, err := h.svc.SummarizeJob(c.Request.Context(), jobID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{
				"error":   "job_not_found",
				"message": err.Error(),
			})
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

// WorkersHandler returns the per-worker performance list.
//
//	GET /api/observability/workers
func (h *Handlers) WorkersHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		result, err := h.svc.ListWorkers(c.Request.Context())
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":   "workers_failed",
				"message": err.Error(),
			})
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

// PhaseTrendsHandler returns phase timing trends, optionally filtered.
//
//	GET /api/observability/phases/trends?phase=engine.segment_build&executor=scene.composite.v1
func (h *Handlers) PhaseTrendsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		phase := c.Query("phase")
		executor := c.Query("executor")

		if phase == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "missing_phase",
				"message": "phase query parameter is required (e.g. ?phase=engine.segment_build)",
			})
			return
		}

		result, err := h.svc.PhaseTrends(c.Request.Context(), phase, executor)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":   "phase_trends_failed",
				"message": err.Error(),
			})
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

// RegressionsHandler compares task_attempt_metrics between two git SHAs.
//
//	GET /api/observability/regressions?before=SHA1&after=SHA2&threshold=5
func (h *Handlers) RegressionsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		before := c.Query("before")
		after := c.Query("after")

		if before == "" || after == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "missing_params",
				"message": "before and after query params are required (git SHAs)",
			})
			return
		}

		threshold := 5.0 // default 5% threshold
		if t := c.Query("threshold"); t != "" {
			parsed, err := strconv.ParseFloat(t, 64)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{
					"error":   "invalid_threshold",
					"message": "threshold must be a number (e.g. 5)",
				})
				return
			}
			threshold = parsed
		}

		result, err := h.svc.CompareVersions(c.Request.Context(), before, after, threshold)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":   "regressions_failed",
				"message": err.Error(),
			})
			return
		}
		c.JSON(http.StatusOK, result)
	}
}
