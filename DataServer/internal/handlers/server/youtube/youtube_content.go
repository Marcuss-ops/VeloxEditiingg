package youtube

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"velox-server/internal/integrations/youtube"
)

func (ym *YouTubeManager) ScrapeToolHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req youtube.ScrapeRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{OK: false, Error: "Invalid request: " + err.Error()})
			return
		}

		ctx := c.Request.Context()
		videoInfo, err := ym.svc.ScrapeVideo(ctx, req.URL)
		if err != nil {
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{OK: false, Error: err.Error()})
			return
		}

		c.JSON(http.StatusOK, youtube.APIResponse{OK: true, Data: videoInfo})
	}
}

func (ym *YouTubeManager) VideoInfoHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		videoID := c.Query("video_id")
		if videoID == "" {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{OK: false, Error: "video_id required"})
			return
		}

		ctx := c.Request.Context()
		info, err := ym.svc.GetVideoInfo(ctx, videoID)
		if err != nil {
			c.JSON(http.StatusBadGateway, youtube.APIResponse{OK: false, Error: err.Error()})
			return
		}

		c.JSON(http.StatusOK, youtube.VideoInfoResponse{
			OK:   true,
			Info: map[string]interface{}{"video": info},
		})
	}
}

func (ym *YouTubeManager) GenerateScriptHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		query := c.Query("query")
		language := c.DefaultQuery("language", "it")

		if query == "" {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{OK: false, Error: "Query required"})
			return
		}

		script := ym.svc.GenerateScript(query, language)
		c.JSON(http.StatusOK, youtube.ScriptResponse{OK: true, Script: script})
	}
}
