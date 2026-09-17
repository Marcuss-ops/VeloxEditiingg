// Package pipeline: routes.go carries h.RegisterRoutes, the single Gin
// mount surface for all pipeline-installed HTTP endpoints.
package pipeline

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func retiredCatalogHandler(c *gin.Context) {
	c.JSON(http.StatusGone, gin.H{"ok": false, "error": "editor_catalog_removed", "owner": "instaedit"})
}

// RegisterRoutes mounts all pipeline endpoints on the given engine.
// m2mJobsAuth protects both job submission and publishing-target discovery:
// they are two steps of the same trusted machine-to-machine workflow.
func (h *Handlers) RegisterRoutes(r *gin.Engine, adminAuth, m2mJobsAuth gin.HandlerFunc, pipelineAuth ...gin.HandlerFunc) {
	creator := r.Group("/api/v1/creator")
	if adminAuth != nil {
		creator.Use(adminAuth)
	}
	creator.POST("/jobs", h.CreatorPush())
	creator.POST("/assets", h.CreatorAssetUpload())

	if m2mJobsAuth == nil {
		panic("pipeline.RegisterRoutes: M2M publishing/job routes require m2mJobsAuth")
	}

	// Social target discovery is InstaEdit-owned. Velox intentionally does
	// not expose a global groups/channels catalog; old callers receive 410.
	publishing := r.Group("/api/v1/publishing")
	publishing.Use(m2mJobsAuth)
	publishing.POST("/targets", retiredCatalogHandler)
	publishing.POST("/catalog", retiredCatalogHandler)

	// Simplified job submission for external M2M automation.
	jobs := r.Group("/api/v1/jobs")
	jobs.Use(m2mJobsAuth)
	jobs.POST("", h.SubmitJob())
	jobs.POST("/batch", h.SubmitJobBatch())
	jobs.POST("/validate", h.ValidateJob())
	jobs.POST("/estimate", h.EstimateJob())
	jobs.GET("/:id", h.GetSubmittedJob())
	jobs.GET("/:id/asset-progress", h.AssetDownloadProgress())

	// Creator Push uses the operator admin bearer rather than an M2M client
	// identity. Give that trusted caller a dedicated unscoped read surface;
	// the public M2M route above remains strictly ownership-scoped.
	if adminAuth != nil {
		adminJobs := r.Group("/api/v1/admin/jobs")
		adminJobs.Use(adminAuth)
		adminJobs.GET("/:id", h.GetAdminSubmittedJob())
	}

	publications := r.Group("/api/v1/publications")
	publications.Use(m2mJobsAuth)
	publications.POST("/preview", h.PreviewPublication())

	// The former /api/v1/pipeline-runs surface is retired. Job submission and
	// polling use the canonical /api/v1/jobs endpoints above.
}
