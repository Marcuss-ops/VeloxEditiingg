// Package api — PR 4 canonical worker endpoint handlers.
//
// Two endpoints:
//
//	GET /api/v1/workers           — list all workers with computed status
//	GET /api/v1/workers/:worker_id — get a single worker by ID
//
// Both read from the workers.Registry (the operational read model), NOT
// from the gRPC session map. The session map is a transient connection
// artefact; the Registry is the SQLite-backed authoritative source.
//
// Security: the response DTO deliberately excludes secret, credential_hash,
// TLS file paths, tokens, and raw IP addresses that could leak internal
// topology. See WorkerResponse for the whitelist of exposed fields.
//
// File layout in this package:
//
//	workers_handler.go            — this file: HTTP entry points only.
//	workers_dto.go                — DTO shapes + canonical enum constants.
//	workers_mapper.go             — conversion / sanitization / parsing
//	                                helpers (security-critical surface).
//	workers_handler_types.go      — redistributive shell (kept for git
//	                                history; content moved to dto.go).
//	workers_handler_filters.go    — redistributive shell (kept for git
//	                                history; content moved to mapper.go).
//	workers_handler_current_task.go — LoadCurrentTask adapter (separate
//	                                concern; not part of the dto/mapper
//	                                split).
//	api_v1.go                     — AdminAuthMiddleware + public route
//	                                allow-list.
package api

import (
	"net/http"
	"sort"

	"github.com/gin-gonic/gin"
	workersreg "velox-server/internal/workers"
)

// WorkersHandler holds the dependency on the worker registry.
type WorkersHandler struct {
	reg *workersreg.Registry
}

// NewWorkersHandler creates a WorkersHandler wired to the Registry read model.
func NewWorkersHandler(reg *workersreg.Registry) *WorkersHandler {
	return &WorkersHandler{reg: reg}
}

// ListWorkers returns GET /api/v1/workers — a sanitized JSON array of all
// registered workers with computed status.
func (h *WorkersHandler) ListWorkers() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.reg == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "worker registry not available"})
			return
		}
		list := h.reg.List(c.Request.Context())
		resp := WorkersListResponse{
			Workers: make([]WorkerResponse, 0, len(list)),
		}
		for _, w := range list {
			resp.Workers = append(resp.Workers, sanitizeWorker(w))
		}
		// Stable order: sort by worker_id so dashboards don't flicker.
		sort.Slice(resp.Workers, func(i, j int) bool {
			return resp.Workers[i].WorkerID < resp.Workers[j].WorkerID
		})
		c.JSON(http.StatusOK, resp)
	}
}

// GetWorker returns GET /api/v1/workers/:worker_id — a single sanitized
// worker or 404.
func (h *WorkersHandler) GetWorker() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.reg == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "worker registry not available"})
			return
		}
		workerID := c.Param("worker_id")
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id is required"})
			return
		}
		info := h.reg.GetWorker(c.Request.Context(), workerID)
		if info == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "worker not found"})
			return
		}
		c.JSON(http.StatusOK, sanitizeWorker(*info))
	}
}
