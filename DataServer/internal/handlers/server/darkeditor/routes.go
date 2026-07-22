package darkeditor

import (
	"log"

	"github.com/gin-gonic/gin"
)

// RegisterAPIRoutes registers all Dark Editor API routes.
// The router argument is typically *gin.Engine or *gin.RouterGroup so
// callers can mount the dark editor surface behind an own middleware
// group (e.g. an admin-auth gate).
func RegisterAPIRoutes(r gin.IRouter, h *Handler) {
	// API group for dark editor
	api := r.Group("/dark_editor_v2")
	{
		// ============== CORE IMAGE OPERATIONS ==============

		// Image upload and processing
		api.POST("/upload", h.UploadImage)
		api.POST("/process/filter", h.ApplyFilter)
		api.POST("/process/transform", h.TransformImage)
		api.POST("/export", h.ExportImage)

		// AI Generation
		api.POST("/generate", h.GenerateImage)

		// Upscaling
		api.POST("/api/upscale", h.UpscaleImage)

		// ============== PROJECTS API ==============

		api.GET("/api/projects", h.ListProjects)
		api.POST("/api/projects", h.SaveProject)
		api.GET("/api/projects/:id", h.LoadProject)
		api.DELETE("/api/projects/:id", h.DeleteProject)

		// ============== LOGS ==============

		api.GET("/api/logs", h.GetLogs)
		api.POST("/api/logs/client", h.ClientLog)
	}

	log.Printf("[OK] Dark Editor API routes registered at /dark_editor_v2/*")
}
