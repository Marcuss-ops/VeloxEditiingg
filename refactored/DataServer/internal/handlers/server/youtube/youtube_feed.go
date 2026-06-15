package youtube

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
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

		cacheKey := fmt.Sprintf("feed_%s_%s_%s", groupName, timeRange, sortBy)

		if cached, ok := ym.feedCache.Get(cacheKey); ok {
			c.JSON(http.StatusOK, cached)
			return
		}

		data := ym.storage.LoadData()
		var targetChannels []youtube.Channel

		if groupName != "" {
			if group, ok := data.Groups[groupName]; ok {
				targetChannels = group.Channels
			} else {
				c.JSON(http.StatusBadRequest, youtube.APIResponse{
					OK:    false,
					Error: "Group not found: " + groupName,
				})
				return
			}
		} else {
			for _, group := range data.Groups {
				targetChannels = append(targetChannels, group.Channels...)
			}
		}

		if len(targetChannels) == 0 {
			c.JSON(http.StatusOK, youtube.FeedResponse{
				OK:     true,
				Videos: []youtube.Video{},
				Count:  0,
			})
			return
		}

		seenURLs := make(map[string]bool)
		var uniqueChannels []youtube.Channel
		for _, ch := range targetChannels {
			if !seenURLs[ch.URL] {
				seenURLs[ch.URL] = true
				uniqueChannels = append(uniqueChannels, ch)
			}
		}

		daysBack := 21
		limitPerChannel := 10
		switch timeRange {
		case "today":
			daysBack = 2
			limitPerChannel = 4
		case "week":
			daysBack = 30
			limitPerChannel = 12
		case "twoweeks":
			daysBack = 45
			limitPerChannel = 16
		case "month":
			daysBack = 90
			limitPerChannel = 24
		case "all":
			daysBack = 365
			limitPerChannel = 30
		}

		ctx := c.Request.Context()
		var aggregatedVideos []youtube.Video

		for i, ch := range uniqueChannels {
			if i >= 15 {
				break
			}

			channelID, err := ym.apiClient.GetChannelID(ctx, ch.URL)
			if err != nil {
				continue
			}

			var videos []youtube.Video
			if channelID != "" {
				videos, err = ym.apiClient.GetRecentChannelVideos(ctx, channelID, limitPerChannel, daysBack)
			}

			if len(videos) == 0 {
				videos, _ = ym.apiClient.SearchVideos(ctx, ch.Title, limitPerChannel, daysBack, 0, 0, false)
			}

			for i := range videos {
				videos[i].SourceChannel = ch.Title
				videos[i].GroupName = groupName
				if videos[i].Thumbnail == "" && ch.Thumbnail != "" {
					videos[i].Thumbnail = ch.Thumbnail
				}
			}

			aggregatedVideos = append(aggregatedVideos, videos...)
		}

		if sortBy == "views" {
			sort.Slice(aggregatedVideos, func(i, j int) bool {
				return aggregatedVideos[i].ViewCount > aggregatedVideos[j].ViewCount
			})
		} else {
			sort.Slice(aggregatedVideos, func(i, j int) bool {
				return aggregatedVideos[i].UploadDate > aggregatedVideos[j].UploadDate
			})
		}

		result := &youtube.FeedResponse{
			OK:     true,
			Group:  groupName,
			Videos: aggregatedVideos,
			Count:  len(aggregatedVideos),
		}

		ym.feedCache.Set(cacheKey, result)

		c.JSON(http.StatusOK, result)
	}
}

func (ym *YouTubeManager) RefreshAllGroupsFeed(ctx context.Context) (int, error) {
	data := ym.storage.LoadData()
	if len(data.Groups) == 0 {
		return 0, nil
	}

	totalVideos := 0
	for groupName := range data.Groups {
		count, err := ym.refreshGroupFeed(ctx, groupName)
		if err != nil {
			continue
		}
		totalVideos += count
	}

	return totalVideos, nil
}

func (ym *YouTubeManager) refreshGroupFeed(ctx context.Context, groupName string) (int, error) {
	group, ok := ym.storage.LoadData().Groups[groupName]
	if !ok {
		return 0, fmt.Errorf("group not found: %s", groupName)
	}

	if len(group.Channels) == 0 {
		return 0, nil
	}

	daysBack := 30
	limitPerChannel := 12

	var aggregatedVideos []youtube.Video

	for i, ch := range group.Channels {
		if i >= 15 {
			break
		}

		channelID, err := ym.apiClient.GetChannelID(ctx, ch.URL)
		if err != nil {
			continue
		}

		var videos []youtube.Video
		if channelID != "" {
			videos, err = ym.apiClient.GetRecentChannelVideos(ctx, channelID, limitPerChannel, daysBack)
		}

		if len(videos) == 0 {
			videos, _ = ym.apiClient.SearchVideos(ctx, ch.Title, limitPerChannel, daysBack, 0, 0, false)
		}

		for i := range videos {
			videos[i].SourceChannel = ch.Title
			videos[i].GroupName = groupName
			if videos[i].Thumbnail == "" && ch.Thumbnail != "" {
				videos[i].Thumbnail = ch.Thumbnail
			}
		}

		aggregatedVideos = append(aggregatedVideos, videos...)
	}

	sort.Slice(aggregatedVideos, func(i, j int) bool {
		return aggregatedVideos[i].UploadDate > aggregatedVideos[j].UploadDate
	})

	cacheKey := fmt.Sprintf("feed_%s_week_date", groupName)
	result := &youtube.FeedResponse{
		OK:     true,
		Group:  groupName,
		Videos: aggregatedVideos,
		Count:  len(aggregatedVideos),
	}
	ym.feedCache.Set(cacheKey, result)

	return len(aggregatedVideos), nil
}

func (ym *YouTubeManager) RefreshFeedHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Minute)
		defer cancel()

		count, err := ym.RefreshAllGroupsFeed(ctx)
		if err != nil {
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{
				OK:    false,
				Error: err.Error(),
			})
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
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "URL required",
			})
			return
		}

		urlParam = strings.TrimSpace(urlParam)

		ctx := c.Request.Context()
		info, err := ym.apiClient.GetChannelInfo(ctx, urlParam)
		if err != nil {
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{
				OK:    false,
				Error: err.Error(),
			})
			return
		}

		if info == nil {
			c.JSON(http.StatusNotFound, youtube.APIResponse{
				OK:    false,
				Error: "Could not resolve channel",
			})
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
