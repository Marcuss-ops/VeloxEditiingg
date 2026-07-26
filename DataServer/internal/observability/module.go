// Package observability / module.go
//
// ObservabilityModule implements the app.Module interface for registering
// the observability REST API routes on the Gin engine. Wire this module
// into the app.Registry during bootstrap to expose the endpoints.
package observability

import (
	"github.com/gin-gonic/gin"
)

// ObservabilityModule registers the observability REST API routes.
// Implements the app.Module interface.
type ObservabilityModule struct {
	handlers *Handlers
	auth     gin.HandlerFunc
}

// NewModule creates a new ObservabilityModule backed by the given service.
func NewModule(svc *Service, auth ...gin.HandlerFunc) *ObservabilityModule {
	var adminAuth gin.HandlerFunc
	if len(auth) > 0 {
		adminAuth = auth[0]
	}
	return &ObservabilityModule{
		handlers: NewHandlers(svc),
		auth:     adminAuth,
	}
}

// Name returns the module identifier.
func (m *ObservabilityModule) Name() string {
	return "observability"
}

// RegisterRoutes registers all observability REST API endpoints.
//
//	GET /api/observability/overview        — system health snapshot
//	GET /api/observability/jobs/:job_id    — job detail with phase waterfall
//	GET /api/observability/workers         — per-worker performance list
//	GET /api/observability/phases/trends   — phase timing trends
func (m *ObservabilityModule) RegisterRoutes(r *gin.Engine) {
	v1 := r.Group("/api/observability")
	if m.auth != nil {
		v1.Use(m.auth)
	}
	v1.GET("/overview", m.handlers.OverviewHandler())
	v1.GET("/jobs/:job_id", m.handlers.JobDetailHandler())
	v1.GET("/workers", m.handlers.WorkersHandler())
	v1.GET("/phases/trends", m.handlers.PhaseTrendsHandler())
	v1.GET("/regressions", m.handlers.RegressionsHandler())
}
