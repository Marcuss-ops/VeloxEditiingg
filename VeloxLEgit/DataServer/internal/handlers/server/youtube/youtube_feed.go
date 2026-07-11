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

func (ym *YouTubeManager) GetVideoFeedHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		sortBy := c.DefaultQuery("sort_by", "date")
		timeRange := c.DefaultQuery("time_range", "week")
		groupName := c.Query("group_name")

		cacheKey := "feed_" + groupName + "_" + timeRange + "_" + sortBy

		if cached, ok := ym.svc.FeedCacheGet(cacheKey); ok {
			c.JSON(http.StatusOK, cached)
			return
		}

		ctx := c.Request.Context()
		result, err := ym.svc.GetVideoFeed(ctx, groupName, timeRange, sortBy)
		if err != nil {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{OK: false, Error: err.Error()})
			return
		}

		ym.svc.FeedCacheSet(cacheKey, result)
		c.JSON(http.StatusOK, result)
	}
}

func (ym *YouTubeManager) RefreshAllGroupsFeed(ctx context.Context) (int, error) {
	return ym.svc.RefreshAllGroupsFeed(ctx)
}

func (ym *YouTubeManager) RefreshFeedHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Minute)
		defer cancel()

		count, err := ym.svc.RefreshAllGroupsFeed(ctx)
		if err != nil {
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{OK: false, Error: err.Error()})
			return
		}

		c.JSON(http.StatusOK, youtube.APIResponse{
			OK:      true,
			Message: "Feed refreshed",
			Data: gin.H{
				"videos_cached": count,
			},
		})
	}
}

func (ym *YouTubeManager) ResolveChannelHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		urlParam := c.Query("url")
		if urlParam == "" {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{OK: false, Error: "URL required"})
			return
		}

		ctx := c.Request.Context()
		info, err := ym.svc.ResolveChannel(ctx, urlParam)
		if err != nil {
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{OK: false, Error: err.Error()})
			return
		}

		if info == nil {
			c.JSON(http.StatusNotFound, youtube.APIResponse{OK: false, Error: "Could not resolve channel"})
			return
		}

		c.JSON(http.StatusOK, map[string]interface{}{
			"ok":          true,
			"url":         info.URL,
			"channel_url": info.URL,
			"title":       info.Title,
			"thumbnail":   info.Thumbnail,
			"description": info.Description,
		})
	}
}

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
			c.JSON(http.StatusBadRequest, youtube.APIResponse{OK: false, Error: "query parameter is required"})
			return
		}

		ctx, cancel := context.WithTimeout(c.Request.Context(), 15*time.Second)
		defer cancel()

		newsItems, err := ym.svc.TrendingNews(ctx, query, limit)
		if err != nil {
			c.JSON(http.StatusBadGateway, youtube.APIResponse{OK: false, Error: "Failed to fetch news: " + err.Error()})
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
