package youtube

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/integrations/youtube"
)

func (ym *YouTubeManager) ScrapeToolHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req youtube.ScrapeRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "Invalid request: " + err.Error(),
			})
			return
		}

		ctx := c.Request.Context()

		videoID := extractVideoID(req.URL)
		if videoID == "" {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "Invalid YouTube URL",
			})
			return
		}

		videoInfo, err := ym.apiClient.SearchVideos(ctx, req.URL, 1, 365, 0, 0, false)
		if err != nil {
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{
				OK:    false,
				Error: "Internal server error",
			})
			return
		}

		if len(videoInfo) > 0 {
			c.JSON(http.StatusOK, youtube.APIResponse{
				OK:   true,
				Data: videoInfo[0],
			})
			return
		}

		c.JSON(http.StatusOK, youtube.APIResponse{
			OK:   true,
			Data: map[string]string{"url": req.URL},
		})
	}
}

func (ym *YouTubeManager) VideoInfoHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		videoID := c.Query("video_id")
		if videoID == "" {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "video_id required",
			})
			return
		}

		ctx := c.Request.Context()
		info, err := ym.apiClient.SearchVideos(ctx, videoID, 1, 365, 0, 0, false)
		if err != nil {
			c.JSON(http.StatusBadGateway, youtube.APIResponse{
				OK:    false,
				Error: err.Error(),
			})
			return
		}

		if len(info) > 0 {
			c.JSON(http.StatusOK, youtube.VideoInfoResponse{
				OK:   true,
				Info: map[string]interface{}{"video": info[0]},
			})
			return
		}

		c.JSON(http.StatusOK, youtube.VideoInfoResponse{
			OK:   true,
			Info: map[string]interface{}{"video_id": videoID},
		})
	}
}

func (ym *YouTubeManager) GenerateScriptHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		query := c.Query("query")
		language := c.DefaultQuery("language", "it")

		if query == "" {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "Query required",
			})
			return
		}

		script := fmt.Sprintf(`# Script: %s

## Introduzione (0:00-0:30)
Benvenuti a questo video su %s!

## Corpo principale (0:30-5:00)
- Punto 1
- Punto 2
- Punto 3

## Conclusione (5:00-5:30)
Grazie per aver guardato! Iscrivetevi per altri contenuti.

---
Language: %s
Generated: %s
`, query, query, language, time.Now().Format("2006-01-02 15:04:05"))

		c.JSON(http.StatusOK, youtube.ScriptResponse{
			OK:     true,
			Script: script,
		})
	}
}
