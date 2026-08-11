// Package observability / handlers.go
//
// HTTP handlers for the Velox observability REST API.
// Follows the existing Gin HandlerFunc pattern used across the DataServer.
// All handlers delegate to the Service for business logic and are
// read-only — no mutations, no side effects.
package observability

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

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

// JobInspectHandler is the canonical operator projection used by fleetctl.
// It includes the job aggregate, execution metrics, cache rollup, timeline,
// artifacts and delivery state in one bounded response.
func (h *Handlers) JobInspectHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Param("job_id")
		if jobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "missing_job_id"})
			return
		}
		result, err := h.svc.InspectJob(c.Request.Context(), jobID)
		if err != nil {
			status := http.StatusInternalServerError
			if len(err.Error()) >= 5 && err.Error()[len(err.Error())-5:] == "found" {
				status = http.StatusNotFound
			}
			c.JSON(status, gin.H{"error": "job_inspect_failed", "message": err.Error()})
			return
		}
		c.JSON(http.StatusOK, result)
	}
}

// JobMetricsHandler returns the execution and cache portions of inspection,
// keeping a small response for dashboards and shell consumers.
func (h *Handlers) JobMetricsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Param("job_id")
		result, err := h.svc.InspectJob(c.Request.Context(), jobID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "job_not_found", "message": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"job_id": jobID, "job": result.Job, "execution": result.Execution})
	}
}

// JobEventsHandler exposes the same timeline used by job watch without
// requiring callers to parse the full inspection response.
func (h *Handlers) JobEventsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Param("job_id")
		result, err := h.svc.InspectJob(c.Request.Context(), jobID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "job_not_found", "message": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"job_id": jobID, "status": result.Job.Status, "events": result.Events})
	}
}

// JobCancelHandler is the canonical operator-only cancellation surface for
// direct jobs that do not have a pipeline_runs parent.
//
//	POST /api/v1/admin/jobs/:job_id/cancel
func (h *Handlers) JobCancelHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := strings.TrimSpace(c.Param("job_id"))
		if jobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "missing_job_id"})
			return
		}
		var body struct {
			Reason string `json:"reason"`
		}
		if c.Request.Body != nil {
			if err := json.NewDecoder(c.Request.Body).Decode(&body); err != nil && err != io.EOF {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_json", "message": err.Error()})
				return
			}
		}
		if err := h.svc.CancelJob(c.Request.Context(), jobID, strings.TrimSpace(body.Reason)); err != nil {
			status := http.StatusInternalServerError
			if strings.Contains(err.Error(), "not found") {
				status = http.StatusNotFound
			}
			c.JSON(status, gin.H{"error": "job_cancel_failed", "message": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"ok": true, "job_id": jobID, "status": "CANCELLED"})
	}
}

// JobAuditHandler returns the append-only audit history for a job or task.
func (h *Handlers) JobAuditHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("job_id")
		if id == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "missing_job_id"})
			return
		}
		events, err := h.svc.ListAudit(c.Request.Context(), id, 1000)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "audit_failed", "message": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"job_id": id, "events": events})
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

// ProductionDoctorHandler returns a fail-closed fleet/security/readiness
// report. The HTTP response remains 200 so callers can inspect every check;
// the `healthy` field is the contract used by fleetctl.
func (h *Handlers) ProductionDoctorHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		result, err := h.svc.ProductionDoctor(c.Request.Context())
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "doctor_failed", "message": err.Error()})
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
