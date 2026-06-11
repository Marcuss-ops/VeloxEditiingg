package darkeditor

import (
	"log"

	"github.com/gin-gonic/gin"
)

// RegisterAPIRoutes registers all Dark Editor API routes
func RegisterAPIRoutes(r *gin.Engine, h *Handler) {
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

		// YouTube thumbnail grab
		api.POST("/api/tools/youtube_grab", h.YouTubeGrab)

		// ============== PROJECTS API ==============

		api.GET("/api/projects", h.ListProjects)
		api.POST("/api/projects", h.SaveProject)
		api.GET("/api/projects/:id", h.LoadProject)
		api.DELETE("/api/projects/:id", h.DeleteProject)
		api.PUT("/api/projects/:id/folder", h.AssignProjectToFolder)

		api.GET("/api/folders", h.ListFolders)
		api.GET("/api/folders/:id", h.GetFolder)
		api.POST("/api/folders", h.CreateFolder)
		api.PUT("/api/folders/:id", h.UpdateFolder)
		api.DELETE("/api/folders/:id", h.DeleteFolder)

		// ============== FILE SERVING ==============

		api.GET("/temp/:filename", h.ServeTempFile)
		api.GET("/projects/:id/:filename", h.ServeProjectFile)

		// ============== LOGS ==============

		api.GET("/api/logs", h.GetLogs)
		api.POST("/api/logs/client", h.ClientLog)
	}

	log.Printf("[OK] Dark Editor API routes registered at /dark_editor_v2/*")
}
