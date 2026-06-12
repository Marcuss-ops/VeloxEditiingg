package youtube

import (
	"fmt"
	"net/http"
	"sort"
	"strings"

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
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "Query required",
			})
			return
		}

		days := 30
		switch req.FilterDate {
		case "today":
			days = 2
		case "week":
			days = 7
		case "month":
			days = 30
		case "all":
			days = 365
		}

		ctx := c.Request.Context()
		videos, err := ym.apiClient.SearchVideos(ctx, req.Query, req.Limit, days, req.MinViews, req.MinVelocity, req.HideShorts)
		if err != nil {
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{
				OK:    false,
				Error: err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, map[string]interface{}{
			"ok":      true,
			"results": videos,
		})
	}
}

func (ym *YouTubeManager) DiscoveryHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		query := c.Query("query")
		if query == "" {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "Query required",
			})
			return
		}

		days, _ := parseIntParam(c.Query("days"), 7)
		minViews, _ := parseIntParam64(c.Query("min_views"), 0)
		minVelocity, _ := parseFloatParam(c.Query("min_velocity"), 0)
		hideShorts := c.Query("hide_shorts") == "true"

		ctx := c.Request.Context()
		videos, err := ym.apiClient.SearchVideos(ctx, query, 30, days, minViews, minVelocity, hideShorts)
		if err != nil {
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{
				OK:    false,
				Error: err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, youtube.DiscoveryResponse{
			OK:     true,
			Videos: videos,
		})
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
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "Provide 'channel' or 'query' parameter",
			})
			return
		}

		ctx := c.Request.Context()
		videos, err := ym.apiClient.SearchVideos(ctx, searchQuery, 20, 30, 0, 0, false)
		if err != nil {
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{
				OK:    false,
				Error: err.Error(),
			})
			return
		}

		seen := make(map[string]bool)
		var channels []youtube.SimilarChannelHit

		for _, v := range videos {
			chURL := v.ChannelURL
			chTitle := v.Uploader
			if chURL != "" && !seen[chURL] && chTitle != "" {
				seen[chURL] = true
				channels = append(channels, youtube.SimilarChannelHit{
					Title:     chTitle,
					URL:       chURL,
					Thumbnail: v.Thumbnail,
					ViewCount: v.ViewCount,
					Velocity:  v.Velocity,
				})
			}
			if len(channels) >= limit {
				break
			}
		}

		c.JSON(http.StatusOK, map[string]interface{}{
			"ok":       true,
			"channels": channels,
		})
	}
}

func (ym *YouTubeManager) AutoSimilarChannelsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		limit, _ := parseIntParam(c.Query("limit"), 10)
		minVelocity, _ := parseIntParam64(c.Query("min_velocity"), 500)

		data := ym.storage.LoadData()

		var allChannels []youtube.Channel
		seenURLs := make(map[string]bool)
		allKeywords := make(map[string]bool)

		for _, group := range data.Groups {
			for _, ch := range group.Channels {
				if !seenURLs[ch.URL] {
					seenURLs[ch.URL] = true
					allChannels = append(allChannels, ch)
					for _, kw := range ch.Keywords {
						if len(kw) > 2 {
							allKeywords[strings.ToLower(kw)] = true
						}
					}
				}
			}
		}

		if len(allChannels) == 0 {
			c.JSON(http.StatusOK, youtube.SimilarChannelsResponse{
				OK:       true,
				Channels: []youtube.SimilarChannelHit{},
				Message:  "No channels tracked yet",
			})
			return
		}

		var keywordsList []string
		for kw := range allKeywords {
			keywordsList = append(keywordsList, kw)
			if len(keywordsList) >= 15 {
				break
			}
		}

		searchQuery := strings.Join(keywordsList, " ")

		ctx := c.Request.Context()
		videos, _ := ym.apiClient.SearchVideos(ctx, searchQuery, 50, 30, 0, float64(minVelocity), false)

		seenChannelURLs := make(map[string]bool)
		for _, ch := range allChannels {
			seenChannelURLs[ch.URL] = true
		}

		var similar []youtube.SimilarChannelHit
		seenResults := make(map[string]bool)

		for _, v := range videos {
			chURL := v.ChannelURL
			if chURL != "" && !seenResults[chURL] && !seenChannelURLs[chURL] && v.Uploader != "" {
				seenResults[chURL] = true
				similar = append(similar, youtube.SimilarChannelHit{
					Title:     v.Uploader,
					URL:       chURL,
					Thumbnail: v.Thumbnail,
					ViewCount: v.ViewCount,
					Velocity:  v.Velocity,
					Reason:    "Related to: " + searchQuery[:50] + "...",
				})
			}
		}

		sort.Slice(similar, func(i, j int) bool {
			return similar[i].Velocity > similar[j].Velocity
		})

		if len(similar) > limit {
			similar = similar[:limit]
		}

		c.JSON(http.StatusOK, youtube.SimilarChannelsResponse{
			OK:           true,
			Channels:     similar,
			KeywordsUsed: keywordsList,
			Tracked:      len(allChannels),
		})
	}
}

func (ym *YouTubeManager) TrendsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		query := c.DefaultQuery("query", "HipHop")

		ctx := c.Request.Context()
		videos, _ := ym.apiClient.SearchVideos(ctx, query+" news", 10, 7, 1000, 0, false)

		var trends []youtube.TrendTopic
		for _, v := range videos {
			trends = append(trends, youtube.TrendTopic{
				Title:     v.Title,
				URL:       v.URL,
				Views:     v.ChannelTitle,
				Thumbnail: v.Thumbnail,
			})
		}

		c.JSON(http.StatusOK, youtube.TrendsResponse{
			OK:     true,
			Trends: trends,
		})
	}
}

func (ym *YouTubeManager) AIDigestHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		query := c.Query("query")
		if query == "" {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "Query required",
			})
			return
		}

		ctx := c.Request.Context()
		videos, _ := ym.apiClient.SearchVideos(ctx, query, 5, 7, 0, 0, false)

		var digest strings.Builder
		digest.WriteString(fmt.Sprintf("# %s - Weekly Digest\n\n", query))
		digest.WriteString(fmt.Sprintf("Found %d trending videos this week.\n\n", len(videos)))

		for i, v := range videos {
			digest.WriteString(fmt.Sprintf("%d. **%s**\n", i+1, v.Title))
			digest.WriteString(fmt.Sprintf("   - Views: %d\n", v.ViewCount))
			digest.WriteString(fmt.Sprintf("   - Channel: %s\n\n", v.Uploader))
		}

		c.JSON(http.StatusOK, map[string]interface{}{
			"ok":     true,
			"query":  query,
			"digest": digest.String(),
			"videos": videos,
		})
	}
}
