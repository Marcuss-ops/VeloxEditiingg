package youtube

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"velox-server/internal/integrations/youtube"
)

func (ym *YouTubeManager) ViralSearchHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req youtube.ViralSearchRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			req.Query = c.Query("query")
			req.FilterDate = c.DefaultQuery("filter_date", "all")
			req.Limit, _ = parseIntParam(c.Query("limit"), 50)
			req.MinViews, _ = parseIntParam64(c.Query("min_views"), 0)
			req.MinVelocity, _ = parseFloatParam(c.Query("min_velocity"), 0)
			req.HideShorts = c.Query("hide_shorts") == "true"
		}

		if req.Query == "" {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{OK: false, Error: "Query required"})
			return
		}

		ctx := c.Request.Context()
		videos, err := ym.svc.ViralSearch(ctx, req.Query, req.Limit, req.FilterDate, req.MinViews, req.MinVelocity, req.HideShorts)
		if err != nil {
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{OK: false, Error: err.Error()})
			return
		}

		c.JSON(http.StatusOK, map[string]interface{}{OKFlag: true, "results": videos})
	}
}

func (ym *YouTubeManager) DiscoveryHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		query := c.Query("query")
		if query == "" {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{OK: false, Error: "Query required"})
			return
		}

		days, _ := parseIntParam(c.Query("days"), 7)
		minViews, _ := parseIntParam64(c.Query("min_views"), 0)
		minVelocity, _ := parseFloatParam(c.Query("min_velocity"), 0)
		hideShorts := c.Query("hide_shorts") == "true"

		ctx := c.Request.Context()
		videos, err := ym.svc.DiscoverySearch(ctx, query, days, 30, minViews, minVelocity, hideShorts)
		if err != nil {
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{OK: false, Error: err.Error()})
			return
		}

		c.JSON(http.StatusOK, youtube.DiscoveryResponse{OK: true, Videos: videos})
	}
}

func (ym *YouTubeManager) SimilarChannelsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		channel := c.Query("channel")
		query := c.Query("query")
		limit, _ := parseIntParam(c.Query("limit"), 5)

		searchQuery := channel
		if searchQuery == "" {
			searchQuery = query
		}

		if searchQuery == "" {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{OK: false, Error: "Provide 'channel' or 'query' parameter"})
			return
		}

		ctx := c.Request.Context()
		channels, err := ym.svc.SimilarChannels(ctx, searchQuery, limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{OK: false, Error: err.Error()})
			return
		}

		c.JSON(http.StatusOK, map[string]interface{}{OKFlag: true, "channels": channels})
	}
}

func (ym *YouTubeManager) AutoSimilarChannelsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		limit, _ := parseIntParam(c.Query("limit"), 10)
		minVelocity, _ := parseIntParam64(c.Query("min_velocity"), 500)

		ctx := c.Request.Context()
		similar, keywordsList, trackedCount, err := ym.svc.AutoSimilarChannels(ctx, limit, minVelocity)
		if err != nil {
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{OK: false, Error: err.Error()})
			return
		}

		if len(similar) == 0 && trackedCount == 0 {
			c.JSON(http.StatusOK, youtube.SimilarChannelsResponse{OK: true, Channels: []youtube.SimilarChannelHit{}, Message: "No channels tracked yet"})
			return
		}

		c.JSON(http.StatusOK, youtube.SimilarChannelsResponse{
			OK:           true,
			Channels:     similar,
			KeywordsUsed: keywordsList,
			Tracked:      trackedCount,
		})
	}
}

func (ym *YouTubeManager) TrendsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		query := c.DefaultQuery("query", "HipHop")
		ctx := c.Request.Context()

		trends, err := ym.svc.Trends(ctx, query)
		if err != nil {
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{OK: false, Error: err.Error()})
			return
		}

		c.JSON(http.StatusOK, youtube.TrendsResponse{OK: true, Trends: trends})
	}
}

func (ym *YouTubeManager) AIDigestHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		query := c.Query("query")
		if query == "" {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{OK: false, Error: "Query required"})
			return
		}

		ctx := c.Request.Context()
		digestText, videos, err := ym.svc.AIDigest(ctx, query)
		if err != nil {
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{OK: false, Error: err.Error()})
			return
		}

		c.JSON(http.StatusOK, map[string]interface{}{
			OKFlag:   true,
			"query":  query,
			"digest": digestText,
			"videos": videos,
		})
	}
}

const OKFlag = "ok"
