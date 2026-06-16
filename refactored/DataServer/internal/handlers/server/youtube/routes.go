package youtube

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
)

// ListTokens lists all YouTube token files
// GET /api/v1/youtube/tokens/list
func (h *YouTubeHandlers) ListTokens(c *gin.Context) {
	tokensDir := h.service.GetConfig().TokensDir
	if tokensDir == "" {
		// Try to use DataDir
		dataDir := h.service.GetConfig().DataDir
		if dataDir != "" {
			tokensDir = filepath.Join(dataDir, "youtube", "tokens")
		}
	}

	if tokensDir == "" {
		c.JSON(http.StatusOK, gin.H{
			"ok":    true,
			"files": []string{},
			"count": 0,
		})
		return
	}

	files := []string{}
	entries, err := os.ReadDir(tokensDir)
	if err != nil {
		log.Printf("[WARN] Failed to read tokens directory %s: %v", tokensDir, err)
		c.JSON(http.StatusOK, gin.H{
			"ok":    true,
			"files": []string{},
			"count": 0,
		})
		return
	}

	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			// Get file info for size
			info, err := entry.Info()
			if err != nil {
				continue
			}
			files = append(files, fmt.Sprintf("%s (%.1f KB)", entry.Name(), float64(info.Size())/1024))
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":    true,
		"files": files,
		"count": len(files),
		"path":  tokensDir,
	})
}

// RegisterYouTubeRoutes registers all YouTube API routes
func RegisterYouTubeRoutes(rg gin.IRouter, h *YouTubeHandlers) {
	// Tokens
	rg.GET("/tokens/list", h.ListTokens)

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
	rg.POST("/channels/:id/move", h.MoveChannelToGroup)
	rg.POST("/channels/:id/language/auto-detect", h.AutoDetectLanguage)
	rg.DELETE("/channels/:id", h.DeleteChannel)

	// Groups
	rg.GET("/groups", h.ListGroups)
	rg.POST("/groups", h.CreateGroup)
	rg.DELETE("/groups/:name", h.DeleteGroup)
	rg.POST("/groups/:name/channels", h.AddChannelToGroup)
	rg.DELETE("/groups/:name/channels/:channel", h.RemoveChannelFromGroup)

	// Upload
	rg.POST("/upload", h.UploadVideo)
	rg.POST("/upload-path", h.UploadVideoFromPath)
	rg.POST("/batch-upload", h.BatchUpload)

	// Video Management
	rg.GET("/videos", h.ListVideos)
	rg.POST("/videos/:video_id/thumbnail", h.SetThumbnail)
	rg.POST("/videos/:video_id/metadata", h.UpdateMetadata)
	rg.POST("/videos/:video_id/publish", h.PublishVideo)
	rg.DELETE("/videos/:video_id", h.DeleteVideo)

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
	rg.POST("/ai/titles", h.GenerateTitles)
	rg.POST("/ai/description", h.GenerateDescription)
	rg.POST("/ai/tags", h.GenerateTags)
	rg.POST("/ai/translate", h.TranslateText)
	rg.POST("/ai/covers", h.GenerateCoverPack)
}
