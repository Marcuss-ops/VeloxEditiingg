package youtube

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/integrations/news"
	"velox-server/internal/integrations/youtube"
)

func (ym *YouTubeManager) TrendingNewsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		query := c.DefaultQuery("query", "")
		limit := 9
		if l := c.Query("limit"); l != "" {
			if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
				limit = parsed
			}
		}

		if query == "" {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "query parameter is required",
			})
			return
		}

		ctx, cancel := context.WithTimeout(c.Request.Context(), 15*time.Second)
		defer cancel()

		newsItems, err := ym.newsFetcher.FetchTrendingNews(ctx, query, limit)
		if err != nil {
			c.JSON(http.StatusBadGateway, youtube.APIResponse{
				OK:    false,
				Error: "Failed to fetch news: " + err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, news.TrendingResponse{
			OK:    true,
			Query: query,
			News:  newsItems,
			Count: len(newsItems),
		})
	}
}
