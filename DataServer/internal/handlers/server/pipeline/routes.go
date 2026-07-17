// Package pipeline: routes.go carries h.RegisterRoutes, the single Gin
// mount surface for all pipeline-installed HTTP endpoints. Keeping the
// mount surface isolated from the rest of the package means route
// additions/removals show up as a single-file diff in this file and
// the audit/ownership of the routing table stays in one place.
//
// Routes mounted here:
//
//   - POST   /api/script-simple                                       → h.ScriptSimple()
//   - POST   /api/script-multiple                                     → h.ScriptBatch()
//   - group  /api/remote/pipeline/   (adminAuth-guarded when non-nil)
//   - POST   /generate                                              → h.Generate()
//   - GET    /status/:trace_id                                      → h.Status()
//   - DELETE /cancel/:trace_id                                      → h.Cancel()
//   - ungrouped /api/v1/pipeline-runs family (canonical versioned API):
//   - POST /api/v1/pipeline-runs                       → h.CreatePipelineRun()
//   - GET  /api/v1/pipeline-runs/:id                   → h.PipelineRunStatus()
//   - POST /api/v1/pipeline-runs/:id/cancel            → h.CancelPipelineRun()
//   - POST /api/v1/pipeline-runs/:id/retry             → h.RetryPipelineRun()
//   - GET  /api/v1/pipeline-runs/:id/timeline          → h.PipelineRunTimeline()
//   - GET  /api/v1/pipeline-runs/:id/artifacts         → h.PipelineRunArtifacts()
//   - GET  /api/v1/pipeline-runs/:id/deliveries        → h.PipelineRunDeliveries()
//
// adminAuth is the gin.HandlerFunc applied to the
// /api/remote/pipeline/* group when non-nil; pass nil to disable
// authentication on the operator routes — the trusted-network / test
// form. The canonical CLI form (cmd/server/router.go) always passes
// a non-nil adminAuth handler.
package pipeline

import (
	"github.com/gin-gonic/gin"
)

// RegisterRoutes mounts all pipeline endpoints on the given engine.
//
//	adminAuth — when non-nil, applied to the operator routes
//	             (generate/status/cancel). Pass nil for the trusted
//	             network or test mounts.
func (h *Handlers) RegisterRoutes(r *gin.Engine, adminAuth gin.HandlerFunc) {
	r.POST("/api/script-simple", h.ScriptSimple())
	r.POST("/api/script-multiple", h.ScriptBatch())

	remote := r.Group("/api/remote/pipeline")
	if adminAuth != nil {
		remote.Use(adminAuth)
	}
	remote.POST("/generate", h.Generate())
	remote.GET("/status/:trace_id", h.Status())
	remote.DELETE("/cancel/:trace_id", h.Cancel())

	// Canonical, versioned pipeline-runs API. The POST creates a
	// durable pipeline_run before the remote call; the GET returns the
	// aggregated status projection. The :id param accepts either the
	// pipeline_run id (run_...) or the request_id (req_...) for
	// backwards compatibility with clients that only stored the request_id.
	r.POST("/api/v1/pipeline-runs", h.CreatePipelineRun())
	r.GET("/api/v1/pipeline-runs/:id", h.PipelineRunStatus())
	r.POST("/api/v1/pipeline-runs/:id/cancel", h.CancelPipelineRun())
	r.POST("/api/v1/pipeline-runs/:id/retry", h.RetryPipelineRun())
	r.GET("/api/v1/pipeline-runs/:id/timeline", h.PipelineRunTimeline())
	r.GET("/api/v1/pipeline-runs/:id/artifacts", h.PipelineRunArtifacts())
	r.GET("/api/v1/pipeline-runs/:id/deliveries", h.PipelineRunDeliveries())
}
