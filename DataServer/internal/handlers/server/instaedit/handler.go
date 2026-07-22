// Package instaedit exposes the InstaEdit BFF route group on the
// Velox master. Every endpoint in this group is protected by the
// instaeditauth JWT middleware, which verifies signature, issuer,
// audience, expiry, and required scopes.
//
// The routes mounted here are the canonical surface the InstaEdit
// BFF (internal/veloxclient) calls. Handlers are currently stubs
// returning 501 while the workspace-scoped implementations are
// being added; the JWT gate is fully functional and rejects
// unauthenticated or unauthorized requests.
package instaedit

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"

	"velox-server/internal/instaeditauth"
)

// Scope constants used by the JWT-protected route group.
const (
	ScopeJobsRead   = "velox:jobs:read"
	ScopeJobsWrite  = "velox:jobs:write"
	ScopeWorkersRead = "velox:workers:read"
	ScopeAssetsRead = "velox:assets:read"
)

// Handler holds the dependencies for the InstaEdit BFF endpoints.
// Currently it has no repository dependencies because the handlers
// are stubs; future implementations will add the read/write deps
// here and wire them in the composition root.
type Handler struct {
	verifier *instaeditauth.Verifier
}

// NewHandler creates a Handler wired to the given verifier.
func NewHandler(verifier *instaeditauth.Verifier) *Handler {
	return &Handler{verifier: verifier}
}

// RegisterRoutes mounts the /api/v1/instaedit/* routes on the given
// engine. All routes require a valid InstaEdit JWT and the
// appropriate scope.
func (h *Handler) RegisterRoutes(r *gin.Engine) {
	g := r.Group("/api/v1/instaedit")

	jobs := g.Group("/jobs")
	{
		jobs.GET("", instaeditauth.Middleware(h.verifier, []string{ScopeJobsRead}), h.listJobs())
		jobs.POST("", instaeditauth.Middleware(h.verifier, []string{ScopeJobsWrite}), h.createJob())
		jobs.GET("/:id", instaeditauth.Middleware(h.verifier, []string{ScopeJobsRead}), h.getJob())
		jobs.POST("/:id/cancel", instaeditauth.Middleware(h.verifier, []string{ScopeJobsWrite}), h.cancelJob())
		jobs.GET("/:id/deliveries", instaeditauth.Middleware(h.verifier, []string{ScopeJobsRead}), h.listJobDeliveries())
	}

	workers := g.Group("/workers")
	{
		workers.GET("", instaeditauth.Middleware(h.verifier, []string{ScopeWorkersRead}), h.listWorkers())
		workers.GET("/:id", instaeditauth.Middleware(h.verifier, []string{ScopeWorkersRead}), h.getWorker())
	}

	assets := g.Group("/assets")
	{
		assets.GET("/:id", instaeditauth.Middleware(h.verifier, []string{ScopeAssetsRead}), h.getAsset())
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
		log.Printf("[instaedit] listJobs workspace=%d (not implemented)", claims.WorkspaceID)
		notImplemented(c)
	}
}

func (h *Handler) createJob() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		log.Printf("[instaedit] createJob workspace=%d (not implemented)", claims.WorkspaceID)
		notImplemented(c)
	}
}

func (h *Handler) getJob() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		log.Printf("[instaedit] getJob id=%s workspace=%d (not implemented)", c.Param("id"), claims.WorkspaceID)
		notImplemented(c)
	}
}

func (h *Handler) cancelJob() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		log.Printf("[instaedit] cancelJob id=%s workspace=%d (not implemented)", c.Param("id"), claims.WorkspaceID)
		notImplemented(c)
	}
}

func (h *Handler) listJobDeliveries() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		log.Printf("[instaedit] listJobDeliveries id=%s workspace=%d (not implemented)", c.Param("id"), claims.WorkspaceID)
		notImplemented(c)
	}
}

func (h *Handler) listWorkers() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		log.Printf("[instaedit] listWorkers workspace=%d (not implemented)", claims.WorkspaceID)
		notImplemented(c)
	}
}

func (h *Handler) getWorker() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		log.Printf("[instaedit] getWorker id=%s workspace=%d (not implemented)", c.Param("id"), claims.WorkspaceID)
		notImplemented(c)
	}
}

func (h *Handler) getAsset() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		log.Printf("[instaedit] getAsset id=%s workspace=%d (not implemented)", c.Param("id"), claims.WorkspaceID)
		notImplemented(c)
	}
}

func notImplemented(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{
		"error":   "not implemented",
		"message": "InstaEdit BFF handler is a stub; workspace-scoped implementation is pending.",
	})
}
