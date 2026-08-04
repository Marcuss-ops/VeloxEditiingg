// Package pipeline: routes.go carries h.RegisterRoutes, the single Gin
// mount surface for all pipeline-installed HTTP endpoints.
package pipeline

import (
	"github.com/gin-gonic/gin"
)

// RegisterRoutes mounts all pipeline endpoints on the given engine.
// m2mJobsAuth protects both job submission and publishing-target discovery:
// they are two steps of the same trusted machine-to-machine workflow.
func (h *Handlers) RegisterRoutes(r *gin.Engine, adminAuth, m2mJobsAuth gin.HandlerFunc) {
	creator := r.Group("/api/v1/creator")
	if adminAuth != nil {
		creator.Use(adminAuth)
	}
	creator.POST("/jobs", h.CreatorPush())
	creator.POST("/assets", h.CreatorAssetUpload())

	if m2mJobsAuth == nil {
		panic("pipeline.RegisterRoutes: M2M publishing/job routes require m2mJobsAuth")
	}

	// Social target discovery. The handler calls InstaEdit, synchronizes the
	// local delivery_destinations registry and returns destination_id values
	// ready for POST /api/v1/jobs.
	publishing := r.Group("/api/v1/publishing")
	publishing.Use(m2mJobsAuth)
	publishing.POST("/targets", h.ListPublishingTargets())
	publishing.POST("/catalog", h.ListPublishingCatalog())

	// Simplified job submission for external M2M automation.
	jobs := r.Group("/api/v1/jobs")
	jobs.Use(m2mJobsAuth)
	jobs.POST("", h.SubmitJob())
	jobs.POST("/batch", h.SubmitJobBatch())
	jobs.POST("/validate", h.ValidateJob())
	jobs.POST("/estimate", h.EstimateJob())
	jobs.GET("/:id", h.GetSubmittedJob())

	publications := r.Group("/api/v1/publications")
	publications.Use(m2mJobsAuth)
	publications.POST("/preview", h.PreviewPublication())

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
