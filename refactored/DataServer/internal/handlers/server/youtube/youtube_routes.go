// Package handlers provides HTTP handlers for the Velox server.
package youtube

import (
	"log"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
)

// YouTubeRoutes registers all YouTube manager routes.
//
// Routing strategy:
//   - /api/youtube/* - Legacy routes for backward compatibility with existing frontend
//   - /api/v1/youtube/* - V1 routes (used by Vite proxy rewrite)
//
// Both paths are registered because the Vite dev server proxies /api/youtube/ -> /api/v1/youtube/.
func YouTubeRoutes(r *gin.Engine, cfg *config.Config, ym *YouTubeManager) {
	// ── Accounts (frontend OAuth account management) ──
	r.GET("/api/youtube/accounts", ym.ListAccountsHandler())
	r.GET("/api/youtube/accounts/:id", ym.GetAccountHandler())
	r.POST("/api/youtube/accounts/:id/refresh", ym.RefreshAccountHandler())

	// ── Groups (legacy /api/youtube/groups — without /manager/ segment) ──
	r.GET("/api/youtube/groups", ym.ListGroupsHandler())
	r.POST("/api/youtube/groups", ym.CreateGroupHandler())
	r.DELETE("/api/youtube/groups/:group_name", ym.DeleteGroupHandler())
	r.POST("/api/youtube/groups/:group_name/channels", ym.AddChannelHandler())
	r.DELETE("/api/youtube/groups/:group_name/channels/:channel_id", ym.DeleteChannelHandler())
	r.POST("/api/youtube/groups/:group_name/channels/:channel_id/move", ym.MoveChannelHandler())
	r.POST("/api/youtube/groups/:group_name/channels/:channel_id/stats", ym.RefreshChannelStatsHandler())

	// ── Manager Groups (legacy /api/youtube/manager/*) ──
	r.GET("/api/youtube/manager/groups", ym.ListGroupsHandler())
	r.POST("/api/youtube/manager/groups", ym.CreateGroupHandler())
	r.DELETE("/api/youtube/manager/groups/:group_name", ym.DeleteGroupHandler())
	r.POST("/api/youtube/manager/groups/:group_name/channels", ym.AddChannelHandler())
	r.DELETE("/api/youtube/manager/groups/:group_name/channels/:channel_id", ym.DeleteChannelHandler())
	r.POST("/api/youtube/manager/groups/:group_name/channels/:channel_id/move", ym.MoveChannelHandler())
	r.POST("/api/youtube/manager/groups/:group_name/channels/:channel_id/stats", ym.RefreshChannelStatsHandler())
	r.POST("/api/youtube/manager/groups/auto-organize-undefined", ym.AutoOrganizeUndefinedChannelsHandler())

	// ── Feed ──
	r.GET("/api/youtube/manager/feed", ym.GetVideoFeedHandler())
	r.POST("/api/youtube/manager/feed/refresh", ym.RefreshFeedHandler())
	r.GET("/api/youtube/manager/news/trending", ym.TrendingNewsHandler())

	// ── Resolve ──
	r.GET("/api/youtube/manager/resolve", ym.ResolveChannelHandler())

	// ── Tools ──
	r.POST("/api/youtube/manager/channels/:channel_id/move-to/:target_group", ym.MoveChannelToGroupHandler())
	r.POST("/api/youtube/manager/tools/scrape", ym.ScrapeToolHandler())
	r.POST("/api/youtube/manager/tools/viral", ym.ViralSearchHandler())
	r.GET("/api/youtube/manager/tools/similar", ym.SimilarChannelsHandler())
	r.POST("/api/youtube/manager/tools/similar", ym.SimilarChannelsHandler())
	r.GET("/api/youtube/manager/tools/video-info", ym.VideoInfoHandler())
	r.GET("/api/youtube/manager/tools/thumbnail", ym.ThumbnailAPIHandler())

	// ── Discovery ──
	r.GET("/api/youtube/manager/discovery", ym.DiscoveryHandler())
	r.GET("/api/youtube/manager/similar/auto", ym.AutoSimilarChannelsHandler())

	// ── Trends & Digest ──
	r.GET("/api/youtube/manager/trends", ym.TrendsHandler())
	r.GET("/api/youtube/manager/ai-digest", ym.AIDigestHandler())

	// ── Script ──
	r.GET("/api/youtube/manager/generate-script", ym.GenerateScriptHandler())

	// ── Thumbnail download ──
	r.GET("/api/youtube/manager/download_thumbnail", ym.DownloadThumbnailHandler())

	// ── V1 duplicates for Vite proxy rewrite ──
	// The Vite dev server proxies /api/youtube/ -> /api/v1/youtube/
	r.GET("/api/v1/youtube/manager/groups", ym.ListGroupsHandler())
	r.POST("/api/v1/youtube/manager/groups", ym.CreateGroupHandler())
	r.DELETE("/api/v1/youtube/manager/groups/:group_name", ym.DeleteGroupHandler())
	r.POST("/api/v1/youtube/manager/groups/:group_name/channels", ym.AddChannelHandler())
	r.DELETE("/api/v1/youtube/manager/groups/:group_name/channels/:channel_id", ym.DeleteChannelHandler())
	r.POST("/api/v1/youtube/manager/groups/:group_name/channels/:channel_id/move", ym.MoveChannelHandler())
	r.POST("/api/v1/youtube/manager/groups/:group_name/channels/:channel_id/stats", ym.RefreshChannelStatsHandler())
	r.POST("/api/v1/youtube/manager/groups/auto-organize-undefined", ym.AutoOrganizeUndefinedChannelsHandler())
	r.GET("/api/v1/youtube/manager/feed", ym.GetVideoFeedHandler())
	r.POST("/api/v1/youtube/manager/feed/refresh", ym.RefreshFeedHandler())
	r.GET("/api/v1/youtube/manager/news/trending", ym.TrendingNewsHandler())
	r.GET("/api/v1/youtube/manager/resolve", ym.ResolveChannelHandler())
	r.POST("/api/v1/youtube/manager/channels/:channel_id/move-to/:target_group", ym.MoveChannelToGroupHandler())
	r.POST("/api/v1/youtube/manager/tools/scrape", ym.ScrapeToolHandler())
	r.POST("/api/v1/youtube/manager/tools/viral", ym.ViralSearchHandler())
	r.GET("/api/v1/youtube/manager/tools/similar", ym.SimilarChannelsHandler())
	r.POST("/api/v1/youtube/manager/tools/similar", ym.SimilarChannelsHandler())
	r.GET("/api/v1/youtube/manager/tools/video-info", ym.VideoInfoHandler())
	r.GET("/api/v1/youtube/manager/tools/thumbnail", ym.ThumbnailAPIHandler())
	r.GET("/api/v1/youtube/manager/discovery", ym.DiscoveryHandler())
	r.GET("/api/v1/youtube/manager/similar/auto", ym.AutoSimilarChannelsHandler())
	r.GET("/api/v1/youtube/manager/trends", ym.TrendsHandler())
	r.GET("/api/v1/youtube/manager/ai-digest", ym.AIDigestHandler())
	r.GET("/api/v1/youtube/manager/generate-script", ym.GenerateScriptHandler())
	r.GET("/api/v1/youtube/manager/download_thumbnail", ym.DownloadThumbnailHandler())

	log.Printf("[OK] YouTube Manager routes registered")
}
