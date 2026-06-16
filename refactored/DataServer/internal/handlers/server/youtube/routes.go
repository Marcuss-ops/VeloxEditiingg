package youtube

import (
	"github.com/gin-gonic/gin"

	"velox-server/internal/handlers/server/youtube/channels"
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
	ch := channels.NewHandler(h.GetService(), h.GetStorage())
	vh := videos.NewHandler(h.GetService(), h.ClearPrivateVideosCache)
	cr := creative.NewHandler(h.GetService())

	// Channels
	rg.GET("/channels", ch.ListChannels)
	rg.GET("/channels/undefined", ch.ListUndefinedChannels)
	rg.POST("/channels/refresh-metadata", ch.RefreshChannelsMetadata)
	rg.POST("/channels/validate-all", ch.ValidateAllTokens)
	rg.POST("/channels/bulk-delete", ch.BulkDeleteChannels)
	rg.POST("/channels/batch-language", ch.BatchUpdateLanguage)
	rg.GET("/channels/stats", ch.GetChannelStats)
	rg.GET("/channels/duplicates", ch.DetectDuplicateChannels)
	rg.GET("/channels/export", ch.ExportChannels)
	rg.GET("/channels/:id", ch.GetChannel)
	rg.GET("/channels/:id/groups", ch.GetChannelGroups)
	rg.PATCH("/channels/:id", ch.UpdateChannel)
	rg.POST("/channels/:id/move", ch.MoveChannelToGroup)
	rg.POST("/channels/:id/language/auto-detect", ch.AutoDetectLanguage)
	rg.DELETE("/channels/:id", ch.DeleteChannel)

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
	rg.GET("/analytics/channel/:id", ch.GetChannelAnalytics)
	rg.POST("/analytics/refresh/:id", h.RefreshAnalytics)
	rg.GET("/analytics/refresh/:id", h.RefreshAnalytics) // Allow GET for easier testing

	// AI Generation
	rg.POST("/ai/titles", cr.GenerateTitles)
	rg.POST("/ai/description", cr.GenerateDescription)
	rg.POST("/ai/tags", cr.GenerateTags)
	rg.POST("/ai/translate", cr.TranslateText)
	rg.POST("/ai/covers", cr.GenerateCoverPack)
}
