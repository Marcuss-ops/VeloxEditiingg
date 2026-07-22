// Package instaedit exposes the InstaEdit BFF route group on the
// Velox master. Every endpoint in this group is protected by the
// instaeditauth JWT middleware, which verifies signature, issuer,
// audience, expiry, and required scopes.
//
// The routes mounted here are the canonical surface the InstaEdit
// BFF (internal/veloxclient) calls. Handlers scope every read to
// the workspace_id carried in the signed JWT and stamp the
// workspace_id on jobs created through this surface.
package instaedit

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"velox-server/internal/instaeditauth"
)

// Scope constants used by the JWT-protected route group.
const (
	ScopeJobsRead    = "velox:jobs:read"
	ScopeJobsWrite   = "velox:jobs:write"
	ScopeWorkersRead = "velox:workers:read"
	ScopeAssetsRead  = "velox:assets:read"
)

// HandlerDeps carries the dependencies required by the InstaEdit BFF
// handlers. All fields are required for the route group to be mounted;
// the composition root skips the group when the verifier is nil.
type HandlerDeps struct {
	Verifier *instaeditauth.Verifier
	Service  *Service
}

// Handler holds the dependencies for the InstaEdit BFF endpoints.
type Handler struct {
	deps HandlerDeps
}

// NewHandler creates a Handler wired to the given dependencies.
func NewHandler(deps HandlerDeps) *Handler {
	return &Handler{deps: deps}
}

// RegisterRoutes mounts the /api/v1/instaedit/* routes on the given
// engine. All routes require a valid InstaEdit JWT and the
// appropriate scope.
func (h *Handler) RegisterRoutes(r *gin.Engine) {
	g := r.Group("/api/v1/instaedit")

	jobs := g.Group("/jobs")
	{
		jobs.GET("", instaeditauth.Middleware(h.deps.Verifier, []string{ScopeJobsRead}), h.listJobs())
		jobs.POST("", instaeditauth.Middleware(h.deps.Verifier, []string{ScopeJobsWrite}), h.createJob())
		jobs.GET("/:id", instaeditauth.Middleware(h.deps.Verifier, []string{ScopeJobsRead}), h.getJob())
		jobs.POST("/:id/cancel", instaeditauth.Middleware(h.deps.Verifier, []string{ScopeJobsWrite}), h.cancelJob())
		jobs.GET("/:id/deliveries", instaeditauth.Middleware(h.deps.Verifier, []string{ScopeJobsRead}), h.listJobDeliveries())
	}

	workers := g.Group("/workers")
	{
		workers.GET("", instaeditauth.Middleware(h.deps.Verifier, []string{ScopeWorkersRead}), h.listWorkers())
		workers.GET("/:id", instaeditauth.Middleware(h.deps.Verifier, []string{ScopeWorkersRead}), h.getWorker())
	}

	assets := g.Group("/assets")
	{
		assets.GET("/:id", instaeditauth.Middleware(h.deps.Verifier, []string{ScopeAssetsRead}), h.getAsset())
	}
}

// claimsFromContext is a small helper that extracts the verified JWT
// claims. Handlers should treat a nil return as an unexpected error
// because the middleware aborts the request when verification fails.
func (h *Handler) claimsFromContext(c *gin.Context) *instaeditauth.Claims {
	return instaeditauth.FromContext(c)
}

func (h *Handler) listJobs() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		limit := 100
		if l := c.Query("limit"); l != "" {
			if n, err := parseLimit(l); err == nil {
				limit = n
			}
		}
		jobs, err := h.deps.Service.ListJobs(c.Request.Context(), claims.WorkspaceID, limit)
		if err != nil {
			writeServiceError(c, err)
			return
		}
		c.JSON(http.StatusOK, listJobsResponse{Jobs: jobs})
	}
}

func (h *Handler) createJob() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		var req createJobRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON: " + err.Error()})
			return
		}

		dsts := make([]CreateDestinationCmd, 0, len(req.DeliveryPlan.Destinations))
		for _, d := range req.DeliveryPlan.Destinations {
			dsts = append(dsts, CreateDestinationCmd{
				ExternalDestinationID: d.ExternalDestinationID,
				Metadata:              d.Metadata,
			})
		}

		job, err := h.deps.Service.CreateJob(c.Request.Context(), CreateJobCmd{
			WorkspaceID:  claims.WorkspaceID,
			ProjectID:    req.ProjectID,
			RenderSpec:   req.RenderSpec,
			Destinations: dsts,
		})
		if err != nil {
			writeServiceError(c, err)
			return
		}
		c.JSON(http.StatusCreated, job)
	}
}

func (h *Handler) getJob() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		detail, err := h.deps.Service.GetJob(c.Request.Context(), claims.WorkspaceID, c.Param("id"))
		if err != nil {
			writeServiceError(c, err)
			return
		}
		c.JSON(http.StatusOK, detail)
	}
}

func (h *Handler) cancelJob() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		if err := h.deps.Service.CancelJob(c.Request.Context(), claims.WorkspaceID, c.Param("id")); err != nil {
			writeServiceError(c, err)
			return
		}
		c.Status(http.StatusNoContent)
	}
}

func (h *Handler) listJobDeliveries() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		deliveries, err := h.deps.Service.GetJobDeliveries(c.Request.Context(), claims.WorkspaceID, c.Param("id"))
		if err != nil {
			writeServiceError(c, err)
			return
		}
		c.JSON(http.StatusOK, listDeliveriesResponse{Deliveries: deliveries})
	}
}

func (h *Handler) listWorkers() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		workers, err := h.deps.Service.ListWorkers(c.Request.Context(), claims.WorkspaceID)
		if err != nil {
			writeServiceError(c, err)
			return
		}
		c.JSON(http.StatusOK, listWorkersResponse{Workers: workers})
	}
}

func (h *Handler) getWorker() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		worker, err := h.deps.Service.GetWorker(c.Request.Context(), claims.WorkspaceID, c.Param("id"))
		if err != nil {
			writeServiceError(c, err)
			return
		}
		c.JSON(http.StatusOK, worker)
	}
}

func (h *Handler) getAsset() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		asset, err := h.deps.Service.GetAsset(c.Request.Context(), claims.WorkspaceID, c.Param("id"))
		if err != nil {
			writeServiceError(c, err)
			return
		}
		c.JSON(http.StatusOK, asset)
	}
}

// writeServiceError maps domain errors from the service to HTTP
// status codes. Any non-domain error is treated as an internal error.
func writeServiceError(c *gin.Context, err error) {
	switch {
	case err == nil:
		return
	case errors.Is(err, ErrNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
	case errors.Is(err, ErrBadRequest):
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	case errors.Is(err, ErrInvalidPayload), errors.Is(err, ErrDestinationUnknown), errors.Is(err, ErrDestinationDisabled):
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": err.Error()})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	}
}

func parseLimit(s string) (int, error) {
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid limit")
	}
	if n > 500 {
		n = 500
	}
	return n, nil
}
