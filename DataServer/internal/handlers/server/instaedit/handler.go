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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/creatorflow"
	"velox-server/internal/instaeditauth"
)

// Scope constants used by the JWT-protected route group.
const (
	// Keep the route-local names for readability, but source their wire
	// values from the canonical scope vocabulary. The InstaEdit BFF signs
	// these exact values; duplicating the strings here previously allowed
	// the two repositories to drift and made every BFF request fail with
	// 403 insufficient scope.
	ScopeJobsRead    = instaeditauth.ScopeJobsRead
	ScopeJobsWrite   = instaeditauth.ScopeJobsWrite
	ScopeWorkersRead = instaeditauth.ScopeWorkersRead
	ScopeAssetsRead  = instaeditauth.ScopeAssetsRead
)

// HandlerDeps carries the dependencies required by the InstaEdit BFF
// handlers. All fields are required for the route group to be mounted;
// the composition root skips the group when the verifier is nil.
type HandlerDeps struct {
	Verifier      *instaeditauth.Verifier
	Service       *Service
	WebhookSecret string
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

	// Editor access is intentionally project-scoped. The signed project_id
	// claim must match the route parameter; this group has no catalog/list
	// endpoint and therefore cannot enumerate global groups or channels.
	// The accepted bridge contract is one-way: InstaEdit owns application
	// projects, groups, channels and permissions; Velox owns only editor state
	// and rendering for the opaque project handle.
	editor := g.Group("/editor/projects/:project_id")
	{
		editor.GET("", instaeditauth.MiddlewareWithProject(h.deps.Verifier, []string{instaeditauth.ScopeEditorRead}, "read_editor_context", "project_id"), h.editorContext())
		editor.PUT("/document", instaeditauth.MiddlewareWithProject(h.deps.Verifier, []string{instaeditauth.ScopeEditorWrite}, "write_editor_document", "project_id"), h.editorDocument())
	}

}

func (h *Handler) editorContext() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		c.JSON(http.StatusOK, gin.H{"project_id": claims.ProjectID, "workspace_id": claims.WorkspaceID})
	}
}

func (h *Handler) editorDocument() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		// The project-scoped bridge deliberately acknowledges only the
		// authenticated context. Canvas persistence remains owned by the
		// editor runtime and no groups/channels are accepted or returned.
		c.JSON(http.StatusOK, gin.H{"project_id": claims.ProjectID, "workspace_id": claims.WorkspaceID, "accepted": true})
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
		decoder := json.NewDecoder(c.Request.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON: " + err.Error()})
			return
		}
		if req.ContractVersion != "velox.job.v1" {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error_code": "UNSUPPORTED_CONTRACT_VERSION", "message": "unsupported contract_version"})
			return
		}
		if strings.TrimSpace(req.IdempotencyKey) == "" {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error_code": "INVALID_IDEMPOTENCY_KEY", "message": "idempotency_key is required"})
			return
		}
		if !req.RenderOnly && len(req.DeliveryPlan.Destinations) == 0 && len(req.Publications) == 0 {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error_code": "INVALID_DELIVERY_PLAN", "message": "delivery_plan.destinations or publications is required unless render_only=true"})
			return
		}

		dsts := make([]CreateDestinationCmd, 0, len(req.DeliveryPlan.Destinations))
		for _, d := range req.DeliveryPlan.Destinations {
			dsts = append(dsts, CreateDestinationCmd{
				ExternalDestinationID: d.ExternalDestinationID,
				PublicationID:         d.PublicationID,
				Metadata:              d.Metadata,
			})
		}

		job, err := h.deps.Service.CreateJob(c.Request.Context(), CreateJobCmd{
			WorkspaceID:     claims.WorkspaceID,
			ProjectID:       req.ProjectID,
			ContractVersion: req.ContractVersion,
			IdempotencyKey:  req.IdempotencyKey,
			RenderSpec:      req.RenderSpec,
			Destinations:    dsts,
			PublishAt:       req.PublishAt,
			Target:          req.Target,
			Publications:    req.Publications,
			RenderOnly:      req.RenderOnly,
		})
		if err != nil {
			writeServiceError(c, err)
			return
		}
		c.JSON(http.StatusAccepted, job)
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
	case errors.Is(err, creatorflow.ErrIdempotencyKeyReused):
		c.JSON(http.StatusConflict, gin.H{"error_code": "IDEMPOTENCY_CONFLICT", "message": "idempotency key was already used with a different payload"})
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
