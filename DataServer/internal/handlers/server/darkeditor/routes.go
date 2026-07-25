package darkeditor

import (
	"log"

	"github.com/gin-gonic/gin"
)

// RegisterAPIRoutes registers all Dark Editor API routes on the
// supplied router. The caller is responsible for choosing the mount
// point and any middleware (e.g. /dark_editor_v2 with admin auth, or
// /api/v1/instaedit/editor with the InstaEdit control JWT).
func RegisterAPIRoutes(r gin.IRouter, h *Handler) {
	// ============== CORE IMAGE OPERATIONS ==============

	// Image upload and processing
	r.POST("/upload", h.UploadImage)
	r.POST("/process/filter", h.ApplyFilter)
	r.POST("/process/transform", h.TransformImage)
	r.POST("/export", h.ExportImage)

	// AI Generation
	r.POST("/generate", h.GenerateImage)

	// Upscaling
	r.POST("/api/upscale", h.UpscaleImage)

	// ============== PROJECTS API ==============

	r.GET("/api/projects", h.ListProjects)
	r.POST("/api/projects", h.SaveProject)
	r.GET("/api/projects/:id", h.LoadProject)
	r.DELETE("/api/projects/:id", h.DeleteProject)

	// ============== LOGS ==============

	r.GET("/api/logs", h.GetLogs)
	r.POST("/api/logs/client", h.ClientLog)

	log.Printf("[OK] Dark Editor API routes registered on %T", r)
}
