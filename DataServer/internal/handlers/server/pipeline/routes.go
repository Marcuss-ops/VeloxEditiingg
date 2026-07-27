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
//   - group /api/v1/creator/       (adminAuth-guarded when non-nil)
//   - POST /jobs                                                    → h.CreatorPush()
//   - group /api/v1/jobs/         (adminAuth-guarded when non-nil)
//   - POST /                                                    → h.SubmitJob()
//   - ungrouped /api/v1/pipeline-runs family (canonical versioned API):
//   - POST /api/v1/pipeline-runs                       → h.CreatePipelineRun()
//   - GET  /api/v1/pipeline-runs/:id                   → h.PipelineRunStatus()
//   - POST /api/v1/pipeline-runs/:id/cancel            → h.CancelPipelineRun()
//   - POST /api/v1/pipeline-runs/:id/retry             → h.RetryPipelineRun()
//   - GET  /api/v1/pipeline-runs/:id/timeline          → h.PipelineRunTimeline()
//   - GET  /api/v1/pipeline-runs/:id/artifacts         → h.PipelineRunArtifacts()
//   - GET  /api/v1/pipeline-runs/:id/deliveries        → h.PipelineRunDeliveries()
//
// adminAuth is the gin.HandlerFunc applied to the machine-to-machine route
// groups when non-nil. The canonical CLI form (cmd/server/router.go) always
// passes a non-nil adminAuth handler.
package pipeline

import (
	"github.com/gin-gonic/gin"
)

// RegisterRoutes mounts all pipeline endpoints on the given engine.
//
// adminAuth — when non-nil, applied to creator push + remote/pipeline
//             + canonical pipeline-runs groups (and to AdminAuth-
//             surfaced admin endpoints via the composition root).
// m2mJobsAuth — when non-nil, applied EXCLUSIVELY to /api/v1/jobs
//             (the simplified M2M intake). When nil
//             m2mJobsAuth falls back to adminAuth so older test mounts
//             continue to work — production wiring MUST supply a real
//             M2M middleware (see cmd/server/router.go::registerPipelineRoutes).
func (h *Handlers) RegisterRoutes(r *gin.Engine, adminAuth, m2mJobsAuth gin.HandlerFunc) {
	r.POST("/api/script-simple", h.ScriptSimple())
	r.POST("/api/script-multiple", h.ScriptBatch())

	remote := r.Group("/api/remote/pipeline")
	if adminAuth != nil {
		remote.Use(adminAuth)
	}
	remote.POST("/generate", h.Generate())
	remote.GET("/status/:trace_id", h.Status())
	remote.DELETE("/cancel/:trace_id", h.Cancel())

	creator := r.Group("/api/v1/creator")
	if adminAuth != nil {
		creator.Use(adminAuth)
	}
	creator.POST("/jobs", h.CreatorPush())
	creator.POST("/assets", h.CreatorAssetUpload())

	// Simplified job submission for external M2M automation. The
	// handler reads m2m_client_id from context (set by the M2M
	// middleware) and stamps creator_forwardings.external_client_id.
	// Falls back to adminAuth when m2mJobsAuth is nil so unit-test
	// mounts retain the legacy "ops-only via VELOX_ADMIN_TOKEN"
	// shape; production wiring in cmd/server/router.go always passes
	// a real m2mJobsAuth.
	jobs := r.Group("/api/v1/jobs")
	if m2mJobsAuth == nil {
		// Fail-closed: a misconfigured deploy that forgot to wire
		// the M2M middleware silently regresses to adminAuth, which
		// is the very surface this commit retires. Loud boot
		// failure here closes the regression. Log+Fatal matches the
		// existing wiring pattern in cmd/server/router.go for nil
		// Resolver. Test mounts that want the legacy adminAuth
		// behaviour on this path should wrap adminAuth in a
		// middleware that ALSO satisfies M2MAuth contracts (e.g.
		// the in-package m2mJobsAuthFake shim).
		panic("pipeline.RegisterRoutes: /api/v1/jobs requires m2mJobsAuth; adminAuth fallback is forbidden by P1 spec (use m2mJobsAuthFake or pass a wrapped middleware for test mounts)")
	}
	jobs.Use(m2mJobsAuth)
	jobs.POST("", h.SubmitJob())

	// Canonical, versioned pipeline-runs API. The POST creates a
	// durable pipeline_run before the remote call; the GET returns the
	// aggregated status projection. The :id param accepts either the
	// pipeline_run id (run_...) or the request_id (req_...) for
	// backwards compatibility with clients that only stored the request_id.
	pipelineRuns := r.Group("/api/v1/pipeline-runs")
	if adminAuth != nil {
		pipelineRuns.Use(adminAuth)
	}
	pipelineRuns.POST("", h.CreatePipelineRun())
	pipelineRuns.GET("/:id", h.PipelineRunStatus())
	pipelineRuns.POST("/:id/cancel", h.CancelPipelineRun())
	pipelineRuns.POST("/:id/retry", h.RetryPipelineRun())
	pipelineRuns.GET("/:id/timeline", h.PipelineRunTimeline())
	pipelineRuns.GET("/:id/artifacts", h.PipelineRunArtifacts())
	pipelineRuns.GET("/:id/deliveries", h.PipelineRunDeliveries())
}
