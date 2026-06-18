package youtube

import (
	"github.com/gin-gonic/gin"

	"velox-server/internal/handlers/server/youtube/creative"
	"velox-server/internal/handlers/server/youtube/videos"
)

// RegisterYouTubeRoutes registers all YouTube API routes.
// ListTokens / GET /api/v1/youtube/tokens/list has been removed: the
// runtime path is SQLite-only (S6 verdict) and no server component
// reads from <TokensDir>/account_*.json any more. The frontend bundle
// was patched in the same commit to drop its call to this endpoint,
// so the related-files panel reads from youtubeFiles state (= []) and
// the Drive panel still calls /api/drive/tokens/list unchanged. No
// deprecation stub is needed once both sides agree the surface is
// gone.
func RegisterYouTubeRoutes(rg gin.IRouter, h *YouTubeHandlers) {
	vh := videos.NewHandler(h.GetService(), h.ClearPrivateVideosCache)
	cr := creative.NewHandler(h.GetService())

	// Channels
	rg.GET("/channels", h.ListChannels)
	rg.GET("/channels/undefined", h.ListUndefinedChannels)
	rg.POST("/channels/refresh-metadata", h.RefreshChannelsMetadata)
	rg.POST("/channels/validate-all", h.ValidateAllTokens)
	rg.POST("/channels/bulk-delete", h.BulkDeleteChannels)
	rg.POST("/channels/batch-language", h.BatchUpdateLanguage)
	rg.GET("/channels/stats", h.GetChannelStats)
	rg.GET("/channels/duplicates", h.DetectDuplicateChannels)
	rg.GET("/channels/export", h.ExportChannels)
	rg.GET("/channels/:id", h.GetChannel)
	rg.GET("/channels/:id/groups", h.GetChannelGroups)
	rg.PATCH("/channels/:id", h.UpdateChannel)
	rg.POST("/channels/:id/move", h.MoveChannelToGroupV1)
	rg.POST("/channels/:id/language/auto-detect", h.AutoDetectLanguage)
	rg.DELETE("/channels/:id", h.DeleteChannel)

	// Groups
	rg.GET("/groups", h.ListGroups)
	rg.POST("/groups", h.CreateGroup)
	rg.DELETE("/groups/:name", h.DeleteGroup)
	rg.POST("/groups/:name/channels", h.AddChannelToGroup)
	rg.DELETE("/groups/:name/channels/:channel", h.RemoveChannelFromGroup)

	// Upload
	rg.POST("/upload", vh.UploadVideo)
	rg.POST("/upload-path", vh.UploadVideoFromPath)
	rg.POST("/batch-upload", vh.BatchUpload)

	// Video Management
	rg.GET("/videos", h.ListVideos)
	rg.POST("/videos/:video_id/thumbnail", vh.SetThumbnail)
	rg.POST("/videos/:video_id/metadata", vh.UpdateMetadata)
	rg.POST("/videos/:video_id/publish", vh.PublishVideo)
	rg.DELETE("/videos/:video_id", vh.DeleteVideo)

	// Group Private Videos
	rg.GET("/group-private-videos", h.ListGroupPrivateVideos)

	// OAuth Start (initiating auth is fine, the callback is public)
	rg.GET("/oauth/start", h.StartOAuth)

	// Resolve channel by language
	rg.GET("/resolve-channel", h.ResolveChannelByLanguage)

	// Status
	rg.GET("/credentials/health", h.GetHealth)
	rg.GET("/credentials/quota", h.GetQuota)
	rg.GET("/credentials/validate/:id", h.ValidateToken)
	rg.DELETE("/credentials/revoke/:id", h.RevokeToken)
	rg.POST("/credentials/refresh/:id", h.RefreshToken)

	// Analytics
	rg.GET("/analytics/channel/:id", h.GetChannelAnalytics)
	rg.POST("/analytics/refresh/:id", h.RefreshAnalytics)
	rg.GET("/analytics/refresh/:id", h.RefreshAnalytics) // Allow GET for easier testing

	// AI Generation
	rg.POST("/ai/titles", cr.GenerateTitles)
	rg.POST("/ai/description", cr.GenerateDescription)
	rg.POST("/ai/tags", cr.GenerateTags)
	rg.POST("/ai/translate", cr.TranslateText)
	rg.POST("/ai/covers", cr.GenerateCoverPack)
}
