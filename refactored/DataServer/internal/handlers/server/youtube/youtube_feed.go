// Package handlers provides HTTP handlers for the Velox server.
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

// GetVideoFeedHandler returns aggregated videos from tracked channels
func (ym *YouTubeManager) GetVideoFeedHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		sortBy := c.DefaultQuery("sort_by", "date")
		timeRange := c.DefaultQuery("time_range", "week")
		groupName := c.Query("group_name")

		// Build cache key
		cacheKey := fmt.Sprintf("feed_%s_%s_%s", groupName, timeRange, sortBy)

		// Check cache first
		if cached, ok := ym.feedCache.Get(cacheKey); ok {
			c.JSON(http.StatusOK, cached)
			return
		}

		// Get channels
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

		// Deduplicate by URL
		seenURLs := make(map[string]bool)
		var uniqueChannels []youtube.Channel
		for _, ch := range targetChannels {
			if !seenURLs[ch.URL] {
				seenURLs[ch.URL] = true
				uniqueChannels = append(uniqueChannels, ch)
			}
		}

		// Calculate parameters based on time range
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

		// Fetch videos from each channel
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

			// Add source info
			for i := range videos {
				videos[i].SourceChannel = ch.Title
				videos[i].GroupName = groupName
				if videos[i].Thumbnail == "" && ch.Thumbnail != "" {
					videos[i].Thumbnail = ch.Thumbnail
				}
			}

			aggregatedVideos = append(aggregatedVideos, videos...)
		}

		// Sort videos
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

		// Cache the result
		ym.feedCache.Set(cacheKey, result)

		c.JSON(http.StatusOK, result)
	}
}

// ResolveChannelHandler resolves a YouTube URL to channel info
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

// ScrapeToolHandler scrapes metadata from a YouTube URL
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

		// For scraping, we use the fallback remote scraper
		ctx := c.Request.Context()

		// Extract video ID if it's a video URL
		videoID := extractVideoID(req.URL)
		if videoID == "" {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "Invalid YouTube URL",
			})
			return
		}

		info, err := ym.apiClient.GetRecentChannelVideos(ctx, videoID, 1, 365)
		_ = info // We just want to trigger the API/fallback

		// Use fallback for detailed info
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

// --- Discovery Handlers ---

// ViralSearchHandler searches for viral videos
func (ym *YouTubeManager) ViralSearchHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req youtube.ViralSearchRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			// Try query params instead
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

		// Map filter_date to days
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

// DiscoveryHandler finds viral hits in a niche
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

// SimilarChannelsHandler finds similar channels
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

		// Deduplicate by channel URL
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

// AutoSimilarChannelsHandler finds similar channels based on all tracked channels
func (ym *YouTubeManager) AutoSimilarChannelsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		limit, _ := parseIntParam(c.Query("limit"), 10)
		minVelocity, _ := parseIntParam64(c.Query("min_velocity"), 500)

		data := ym.storage.LoadData()

		// Collect all tracked channels and keywords
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

		// Build search query from keywords
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

		// Deduplicate and exclude already tracked
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

		// Sort by velocity
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

// --- Trends & AI Digest (Simplified) ---

// TrendsHandler returns trending topics for a niche
func (ym *YouTubeManager) TrendsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		query := c.DefaultQuery("query", "HipHop")

		// For trends, we search for recent popular videos
		ctx := c.Request.Context()
		videos, _ := ym.apiClient.SearchVideos(ctx, query+" news", 10, 7, 1000, 0, false)

		var trends []youtube.TrendTopic
		for _, v := range videos {
			trends = append(trends, youtube.TrendTopic{
				Title:     v.Title,
				URL:       v.URL,
				Views:     v.ChannelTitle, // Reusing field for source
				Thumbnail: v.Thumbnail,
			})
		}

		c.JSON(http.StatusOK, youtube.TrendsResponse{
			OK:     true,
			Trends: trends,
		})
	}
}

// AIDigestHandler generates an AI digest for a niche
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

		// Get trending videos
		ctx := c.Request.Context()
		videos, _ := ym.apiClient.SearchVideos(ctx, query, 5, 7, 0, 0, false)

		// Build a simple digest (in production, would call AI API)
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

// GenerateScriptHandler generates a YouTube script
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

		// Simple script template (in production, would call AI API)
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

// --- Thumbnail Handler ---

// DownloadThumbnailHandler downloads and converts a thumbnail
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

		// Fetch the image
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

		// Set headers for download
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

// VideoInfoHandler fetches video info from remote
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

// ThumbnailAPIHandler fetches thumbnail URLs
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

		// YouTube provides predictable thumbnail URLs
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

// RefreshAllGroupsFeed refreshes the feed cache for all manager groups (for background sync)
func (ym *YouTubeManager) RefreshAllGroupsFeed(ctx context.Context) (int, error) {
	data := ym.storage.LoadData()
	if len(data.Groups) == 0 {
		return 0, nil
	}

	totalVideos := 0
	for groupName := range data.Groups {
		count, err := ym.refreshGroupFeed(ctx, groupName)
		if err != nil {
			continue // Skip errors, log and continue with next group
		}
		totalVideos += count
	}

	return totalVideos, nil
}

// refreshGroupFeed fetches and caches videos for a specific group
func (ym *YouTubeManager) refreshGroupFeed(ctx context.Context, groupName string) (int, error) {
	group, ok := ym.storage.LoadData().Groups[groupName]
	if !ok {
		return 0, fmt.Errorf("group not found: %s", groupName)
	}

	if len(group.Channels) == 0 {
		return 0, nil
	}

	// Use default parameters (week, date sort)
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

	// Sort by date
	sort.Slice(aggregatedVideos, func(i, j int) bool {
		return aggregatedVideos[i].UploadDate > aggregatedVideos[j].UploadDate
	})

	// Update cache
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

// RefreshFeedHandler manually triggers feed refresh for all groups
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

// TrendingNewsHandler fetches trending news from external sources for a niche
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

// --- Helper functions ---

func extractVideoID(url string) string {
	// Various YouTube URL formats
	if strings.Contains(url, "youtu.be/") {
		parts := strings.Split(url, "youtu.be/")
		if len(parts) > 1 {
			return strings.Split(strings.Split(parts[1], "?")[0], "/")[0]
		}
	}
	if strings.Contains(url, "watch?v=") {
		parts := strings.Split(url, "watch?v=")
		if len(parts) > 1 {
			return strings.Split(strings.Split(parts[1], "&")[0], "#")[0]
		}
	}
	if strings.Contains(url, "/shorts/") {
		parts := strings.Split(url, "/shorts/")
		if len(parts) > 1 {
			return strings.Split(strings.Split(parts[1], "?")[0], "/")[0]
		}
	}
	if strings.Contains(url, "/embed/") {
		parts := strings.Split(url, "/embed/")
		if len(parts) > 1 {
			return strings.Split(strings.Split(parts[1], "?")[0], "/")[0]
		}
	}
	return ""
}

func parseIntParam(s string, def int) (int, error) {
	if s == "" {
		return def, nil
	}
	var i int
	_, err := fmt.Sscanf(s, "%d", &i)
	if err != nil {
		return def, err
	}
	return i, nil
}

func parseIntParam64(s string, def int64) (int64, error) {
	if s == "" {
		return def, nil
	}
	var i int64
	_, err := fmt.Sscanf(s, "%d", &i)
	if err != nil {
		return def, err
	}
	return i, nil
}

func parseFloatParam(s string, def float64) (float64, error) {
	if s == "" {
		return def, nil
	}
	var f float64
	_, err := fmt.Sscanf(s, "%f", &f)
	if err != nil {
		return def, err
	}
	return f, nil
}
