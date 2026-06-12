package youtube

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/integrations/youtube"
)

func (ym *YouTubeManager) DownloadThumbnailHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		thumbURL := c.Query("url")
		filename := c.Query("filename")

		if thumbURL == "" {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "URL required",
			})
			return
		}

		ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, "GET", thumbURL, nil)
		if err != nil {
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{
				OK:    false,
				Error: err.Error(),
			})
			return
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "Failed to fetch image",
			})
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "Failed to fetch image",
			})
			return
		}

		contentType := resp.Header.Get("Content-Type")
		if contentType == "" {
			contentType = "image/jpeg"
		}

		if filename == "" {
			filename = "thumbnail"
		}
		if !strings.HasSuffix(strings.ToLower(filename), ".jpg") &&
			!strings.HasSuffix(strings.ToLower(filename), ".png") {
			filename += ".jpg"
		}

		c.Header("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
		c.Header("Access-Control-Expose-Headers", "Content-Disposition")
		c.DataFromReader(resp.StatusCode, resp.ContentLength, contentType, resp.Body, nil)
	}
}

func (ym *YouTubeManager) ThumbnailAPIHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		videoID := c.Query("video_id")
		if videoID == "" {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "video_id required",
			})
			return
		}

		thumbnails := map[string]string{
			"default":  fmt.Sprintf("https://i.ytimg.com/vi/%s/default.jpg", videoID),
			"medium":   fmt.Sprintf("https://i.ytimg.com/vi/%s/mqdefault.jpg", videoID),
			"high":     fmt.Sprintf("https://i.ytimg.com/vi/%s/hqdefault.jpg", videoID),
			"standard": fmt.Sprintf("https://i.ytimg.com/vi/%s/sddefault.jpg", videoID),
			"maxres":   fmt.Sprintf("https://i.ytimg.com/vi/%s/maxresdefault.jpg", videoID),
		}

		c.JSON(http.StatusOK, youtube.ThumbnailResponse{
			OK:         true,
			Thumbnails: thumbnails,
		})
	}
}
