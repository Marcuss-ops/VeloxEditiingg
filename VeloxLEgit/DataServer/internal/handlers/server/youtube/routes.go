package youtube

import (
	"github.com/gin-gonic/gin"

	"velox-server/internal/handlers/server/youtube/creative"
	"velox-server/internal/handlers/server/youtube/videos"
)

// RegisterYouTubeRoutes registers all YouTube API routes on the given
// router group. The body is a thin orchestrator that delegates each
// route category to a focused helper; see the helper docstrings for
// the route inventory.
func RegisterYouTubeRoutes(rg gin.IRouter, h *YouTubeHandlers) {
	vh := videos.NewHandler(h.GetService(), h.ClearPrivateVideosCache)
	cr := creative.NewHandler(h.GetService())

	registerChannelRoutes(rg, h)
	registerGroupRoutes(rg, h)
	registerVideoRoutes(rg, h, vh)
	registerCredentialRoutes(rg, h)
	registerAIRoutes(rg, cr)
}

// registerChannelRoutes wires channel CRUD, refresh, dedup, stats,
// and export endpoints.
func registerChannelRoutes(rg gin.IRouter, h *YouTubeHandlers) {
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
}

// registerGroupRoutes wires group CRUD and channel-to-group membership
// endpoints.
func registerGroupRoutes(rg gin.IRouter, h *YouTubeHandlers) {
	rg.GET("/groups", h.ListGroups)
	rg.POST("/groups", h.CreateGroup)
	rg.DELETE("/groups/:name", h.DeleteGroup)
	rg.POST("/groups/:name/channels", h.AddChannelToGroup)
	rg.DELETE("/groups/:name/channels/:channel", h.RemoveChannelFromGroup)
}

// registerVideoRoutes wires upload (single + batch + path-based) and
// per-video management (thumbnail, metadata, publish, delete), plus
// the group-videos read endpoints.
//
// /group-videos is the canonical route after the ListGroupPrivateVideos →
// ListGroupVideos rename. /group-private-videos is kept as a deprecated
// alias for callers that haven't migrated yet and is slated for removal
// once the last downstream consumer (the channel-languages dashboard)
// confirms full migration to /group-videos?status=any.
//
// Cache state is shared across both paths via key `v2:<group>:<days>:<status>`
// with TTL=12h (bypassed with refresh=true / force=true). The DEPRECATED alias
// handler injects a `Deprecation: true` response header and a `Link` rel=successor-version
// so clients can self-detect the rename.
func registerVideoRoutes(rg gin.IRouter, h *YouTubeHandlers, vh *videos.Handler) {
	rg.POST("/upload", vh.UploadVideo)
	rg.POST("/upload-path", vh.UploadVideoFromPath)
	rg.POST("/batch-upload", vh.BatchUpload)

	rg.GET("/videos", h.ListVideos)
	rg.POST("/videos/:video_id/thumbnail", vh.SetThumbnail)
	rg.POST("/videos/:video_id/metadata", vh.UpdateMetadata)
	rg.POST("/videos/:video_id/publish", vh.PublishVideo)
	rg.DELETE("/videos/:video_id", vh.DeleteVideo)

	rg.GET("/group-videos", h.ListGroupVideos)

	// DEPRECATED alias — prefer /group-videos?status=any for new callers.
	// Delegates to ListGroupVideos but injects Deprecation response headers
	// (see listGroupVideosDeprecatedAlias). Will be removed once downstream
	// migration is complete.
	rg.GET("/group-private-videos", h.listGroupVideosDeprecatedAlias)
}

// registerCredentialRoutes wires OAuth start (initiating auth is fine;
// the callback lives elsewhere and is public), resolve-by-language,
// credential health/quota/validate/revoke/refresh, and analytics.
// The GET on /analytics/refresh/:id is intentional: it lets test
// harnesses trigger the refresh without an HTTP method override.
func registerCredentialRoutes(rg gin.IRouter, h *YouTubeHandlers) {
	rg.GET("/oauth/start", h.StartOAuth)
	rg.GET("/resolve-channel", h.ResolveChannelByLanguage)

	rg.GET("/credentials/health", h.GetHealth)
	rg.GET("/credentials/quota", h.GetQuota)
	rg.GET("/credentials/validate/:id", h.ValidateToken)
	rg.DELETE("/credentials/revoke/:id", h.RevokeToken)
	rg.POST("/credentials/refresh/:id", h.RefreshToken)

	rg.GET("/analytics/channel/:id", h.GetChannelAnalytics)
	rg.POST("/analytics/refresh/:id", h.RefreshAnalytics)
	rg.GET("/analytics/refresh/:id", h.RefreshAnalytics) // Allow GET for easier testing
}

// registerAIRoutes wires AI generation endpoints (titles, description,
// tags, translation, cover packs) backed by the creative handler.
func registerAIRoutes(rg gin.IRouter, cr *creative.Handler) {
	rg.POST("/ai/titles", cr.GenerateTitles)
	rg.POST("/ai/description", cr.GenerateDescription)
	rg.POST("/ai/tags", cr.GenerateTags)
	rg.POST("/ai/translate", cr.TranslateText)
	rg.POST("/ai/covers", cr.GenerateCoverPack)
}
