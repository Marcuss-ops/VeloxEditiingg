package youtube

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"velox-server/internal/handlers/server/youtube/channels"
	"velox-server/internal/handlers/server/youtube/creative"
	"velox-server/internal/handlers/server/youtube/videos"
)

// ListTokensStub is the deprecated stub for GET /api/v1/youtube/tokens/list.
// The runtime path is SQLite-only (S6 verdict): no server component
// enumerates <TokensDir>/account_*.json at runtime. The original
// ListTokens handler read the directory and returned the on-disk JSON
// token files; that surface is gone. This stub keeps the route registered
// so the existing frontend bundle (which still calls the endpoint from
// the related-files panel) does not 404. The empty-list response is
// semantically correct ("no token files tracked at runtime") and the
// `deprecated` flag tells any future operator / frontend reader that
// the endpoint is intentional noise, not a real backing-list.
//
// Frontend patch should switch this call off in a followup; the stub
// keeps the panel visually clean without any other server-side change.
func (h *YouTubeHandlers) ListTokensStub(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"ok":         true,
		"files":      []string{},
		"count":      0,
		"deprecated": true,
	})
}

// RegisterYouTubeRoutes registers all YouTube API routes.
// ListTokens / GET /api/v1/youtube/tokens/list is registered as a
// deprecated stub: the runtime path is SQLite-only (S6 verdict) and
// no server component reads from <TokensDir>/account_*.json any more.
// The stub returns an empty file list with a `deprecated` flag so the
// existing frontend bundle's call to /api/v1/youtube/tokens/list does
// not 404 (the related-files panel in the YouTube Manager UI surfaces
// the empty list cleanly). It exists solely to avoid a frontend
// regression on this commit; remove the route + frontend call in a
// follow-up.
func RegisterYouTubeRoutes(rg gin.IRouter, h *YouTubeHandlers) {
	ch := channels.NewHandler(h.GetService(), h.GetStorage())
	vh := videos.NewHandler(h.GetService(), h.ClearPrivateVideosCache)
	cr := creative.NewHandler(h.GetService())

	// Tokens (legacy JSON directory enumeration — deprecated, returns empty).
	rg.GET("/tokens/list", h.ListTokensStub)

	// Channels
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
