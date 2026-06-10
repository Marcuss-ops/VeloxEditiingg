package darkeditor

import (
	"log"

	"github.com/gin-gonic/gin"
	"velox-server/internal/handlers/server/drive"
)

// RegisterAPIRoutesWithIntegrations registers all Dark Editor API routes with integration handlers
func RegisterAPIRoutesWithIntegrations(r *gin.Engine, h *Handler, integrations *IntegrationHandlers) {
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

		// ============== INTEGRATION ROUTES ==============

		if integrations != nil {
			// Background Removal
			if integrations.BackgroundRemoval != nil {
				api.POST("/api/remove-bg", integrations.BackgroundRemoval.RemoveBackground)
				api.POST("/api/remove-bg/upload", integrations.BackgroundRemoval.RemoveBackgroundSimple)
				api.GET("/api/remove-bg/status/:task_id", integrations.BackgroundRemoval.GetBackgroundRemovalStatus)
				api.GET("/api/remove-bg/models", integrations.BackgroundRemoval.ListModels)
				api.GET("/api/remove-bg/health", integrations.BackgroundRemoval.HealthCheck)
				log.Printf("✅ Dark Editor: Background removal routes registered")
			}

			// YouTube Integration
			if integrations.YouTube != nil {
				// Thumbnail operations
				api.POST("/api/youtube/thumbnail", integrations.YouTube.SetThumbnail)
				api.POST("/api/youtube/thumbnail/upload", integrations.YouTube.UploadThumbnailDirect)

				// Channel management
				api.GET("/api/youtube/channels", integrations.YouTube.GetChannels)
				api.GET("/api/youtube/channels/:channel_id/validate", integrations.YouTube.ValidateChannel)

				// Video info
				api.GET("/api/youtube/videos/:video_id", integrations.YouTube.GetVideoInfo)

				// OAuth
				api.GET("/api/youtube/oauth/start", integrations.YouTube.StartOAuthFlow)

				// Health
				api.GET("/api/youtube/health", integrations.YouTube.HealthCheck)

				log.Printf("✅ Dark Editor: YouTube integration routes registered")
			}

			// Drive Integration
			if integrations.Drive != nil {
				// Upload operations
				api.POST("/api/drive/upload", integrations.Drive.UploadToDrive)
				api.POST("/api/drive/upload/direct", integrations.Drive.UploadDirect)

				// Folder operations
				api.POST("/api/drive/folders", integrations.Drive.CreateFolder)
				api.GET("/api/drive/folders", drive.GetDriveFoldersHandler)
				api.GET("/api/drive/files", integrations.Drive.ListFiles)
				api.GET("/api/drive/files/:file_id", integrations.Drive.GetFileInfo)

				// Additional Drive routes for dashboard/editor compatibility
				api.GET("/api/drive/groups", drive.GetDriveGroupsHandler)
				api.GET("/api/drive/links", drive.GetDriveLinksHandler)

				// File operations
				api.GET("/api/drive/files/:file_id/download", integrations.Drive.DownloadFile)
				api.POST("/api/drive/files/:file_id/share", integrations.Drive.ShareFile)
				api.DELETE("/api/drive/files/:file_id", integrations.Drive.DeleteFile)

				// Storage info
				api.GET("/api/drive/storage", integrations.Drive.GetStorageInfo)

				// Project sync
				api.POST("/api/drive/sync", integrations.Drive.SyncProject)

				// Health
				api.GET("/api/drive/health", integrations.Drive.HealthCheck)

				log.Printf("✅ Dark Editor: Drive integration routes registered")
			}
		}
	}

	log.Printf("✅ Dark Editor API routes registered at /dark_editor_v2/*")
}
