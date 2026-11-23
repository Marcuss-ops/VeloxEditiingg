// Package pipeline: routes.go carries h.RegisterRoutes, the single Gin
// mount surface for all pipeline-installed HTTP endpoints.
package pipeline

import (
	"net/http"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
)

func retiredCatalogHandler(c *gin.Context) {
	c.JSON(http.StatusGone, gin.H{"ok": false, "error": "editor_catalog_removed", "owner": "instaedit"})
}

func listJobTypes(c *gin.Context) {
	types := make([]gin.H, 0, len(recipeRegistry))
	jobTypes := make([]string, 0, len(recipeRegistry))
	for jobType := range recipeRegistry {
		jobTypes = append(jobTypes, jobType)
	}
	sort.Strings(jobTypes)
	for _, jobType := range jobTypes {
		version := ""
		if dot := strings.LastIndex(jobType, "."); dot >= 0 {
			version = jobType[dot+1:]
		}
		types = append(types, gin.H{"type": jobType, "version": version})
	}
	c.JSON(http.StatusOK, gin.H{"types": types})
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
	jobs.GET("/types", listJobTypes)
	jobs.POST("", h.SubmitJob())
	jobs.POST("/pre", h.PrepareJob())
	jobs.POST("/batch", h.SubmitJobBatch())
	jobs.POST("/validate", h.ValidateJob())
	jobs.POST("/estimate", h.EstimateJob())
	jobs.GET("/:id", h.GetSubmittedJob())
	jobs.DELETE("/:id", h.CancelSubmittedJob())
	jobs.POST("/:id/cancel", h.CancelSubmittedJob())
	jobs.POST("/:id/finalize", h.FinalizeJob())
	jobs.GET("/:id/asset-progress", h.AssetDownloadProgress())
	jobs.GET("/:id/prefetch", h.PrefetchDiagnostic())

	// Operator diagnosis lives under the admin surface: the /api/v1/jobs
	// group is gated by the M2M guard, which rejects the admin token before
	// the handler runs.
	if adminAuth != nil {
		r.GET("/api/v1/admin/jobs/:job_id/prefetch", adminAuth, h.PrefetchDiagnostic())
	}

	// Read-only media catalogue (M2M scope media.read): the Master is the
	// system of record for produced assets, so listing, inspecting and serving
	// them belongs here rather than on a second service.
	mediaAuth := NewM2MJwScopeAuthMiddleware(h.cfg, h.store, nil, "media.read")
	media := r.Group("/api/v1/media", mediaAuth)
	media.GET("/facets", h.MediaFacets())
	media.GET("/assets", h.MediaAssets())
	media.GET("/assets/:asset_id", h.MediaAsset())
	media.GET("/assets/:asset_id/download", h.MediaAssetDownload())
	media.POST("/search", h.MediaSearch())
	r.GET("/api/v1/jobs/:id/artifact", mediaAuth, h.JobArtifact())

	publications := r.Group("/api/v1/publications")
	publications.Use(m2mJobsAuth)
	publications.POST("/preview", h.PreviewPublication())

	// The former /api/v1/pipeline-runs surface is retired. Job submission and
	// polling use the canonical /api/v1/jobs endpoints above.
}
