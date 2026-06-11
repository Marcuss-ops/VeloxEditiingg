package youtube

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func (c *APIClient) SearchVideos(ctx context.Context, query string, limit, days int, minViews int64, minVelocity float64, hideShorts bool) ([]Video, error) {
	cacheKey := fmt.Sprintf("search_%s_%d_%d_%d_%.0f_%v", query, limit, days, minViews, minVelocity, hideShorts)

	if cached, ok := c.cache.Get(cacheKey); ok {
		if videos, ok := cached.([]Video); ok {
			return videos, nil
		}
	}

	var videos []Video
	var err error

	if c.apiKey != "" {
		videos, err = c.searchVideosAPI(ctx, query, limit, days)
	}

	if len(videos) == 0 {
		fallbackVideos, ferr := c.fallback.SearchVideos(ctx, query, limit, days)
		if ferr == nil {
			videos = fallbackVideos
		} else if err == nil {
			err = ferr
		}
	}

	if len(videos) > 0 {
		videos = c.filterVideos(videos, minViews, minVelocity, hideShorts)
	}

	if len(videos) > 0 {
		c.cache.Set(cacheKey, videos)
	}

	return videos, err
}

func (c *APIClient) GetRecentChannelVideos(ctx context.Context, channelID string, limit, days int) ([]Video, error) {
	cacheKey := fmt.Sprintf("channel_uploads:%s_%d_%d", channelID, limit, days)

	if cached, ok := c.cache.Get(cacheKey); ok {
		if videos, ok := cached.([]Video); ok {
			return videos, nil
		}
	}

	var videos []Video
	var err error

	if c.apiKey != "" {
		videos, err = c.fetchChannelVideos(ctx, channelID, limit, days)
	}

	if len(videos) == 0 {
		channelURL := "https://www.youtube.com/channel/" + channelID
		fallbackVideos, ferr := c.fallback.GetChannelVideos(ctx, channelURL, limit, days)
		if ferr == nil {
			videos = fallbackVideos
		} else if err == nil {
			err = ferr
		}
	}

	if len(videos) > 0 {
		c.cache.Set(cacheKey, videos)
	}

	return videos, err
}

func (c *APIClient) searchVideosAPI(ctx context.Context, query string, limit, days int) ([]Video, error) {
	publishedAfter := ""
	if days > 0 {
		publishedAfter = time.Now().AddDate(0, 0, -days).Format(time.RFC3339)
	}

	maxResults := limit
	if maxResults > 50 {
		maxResults = 50
	}

	u := fmt.Sprintf("https://www.googleapis.com/youtube/v3/search?part=snippet&q=%s&type=video&maxResults=%d&order=relevance&key=%s",
		url.QueryEscape(query), maxResults, c.apiKey)
	if publishedAfter != "" {
		u += "&publishedAfter=" + publishedAfter
	}

	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var searchResult struct {
		Items []struct {
			ID struct {
				VideoID string `json:"videoId"`
			} `json:"id"`
			Snippet struct {
				Title        string    `json:"title"`
				ChannelID    string    `json:"channelId"`
				ChannelTitle string    `json:"channelTitle"`
				PublishedAt  time.Time `json:"publishedAt"`
				Thumbnails   struct {
					High    struct{ URL string } `json:"high"`
					Medium  struct{ URL string } `json:"medium"`
					Default struct{ URL string } `json:"default"`
				} `json:"thumbnails"`
			} `json:"snippet"`
		} `json:"items"`
	}

	body, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &searchResult); err != nil {
		return nil, err
	}

	if len(searchResult.Items) == 0 {
		return nil, nil
	}

	var videoIDs []string
	for _, item := range searchResult.Items {
		videoIDs = append(videoIDs, item.ID.VideoID)
	}

	videoDetails, err := c.fetchVideoDetails(ctx, videoIDs)
	if err != nil {
		log.Printf("[WARN] Failed to fetch video details: %v", err)
	}

	var videos []Video
	now := time.Now()

	for _, item := range searchResult.Items {
		vid := item.ID.VideoID
		thumbnail := item.Snippet.Thumbnails.High.URL
		if thumbnail == "" {
			thumbnail = item.Snippet.Thumbnails.Medium.URL
		}
		if thumbnail == "" {
			thumbnail = item.Snippet.Thumbnails.Default.URL
		}

		daysOld := int(now.Sub(item.Snippet.PublishedAt).Hours() / 24)
		if daysOld < 1 {
			daysOld = 1
		}

		viewCount := int64(0)
		duration := ""
		durationSecs := 0

		if details, ok := videoDetails[vid]; ok {
			viewCount = details.ViewCount
			duration = details.Duration
			durationSecs = details.DurationSecs
		}

		velocity := float64(viewCount) / float64(daysOld)

		relativeDate := fmt.Sprintf("%d days ago", daysOld)
		if daysOld == 1 {
			relativeDate = "Yesterday"
		} else if daysOld < 1 {
			relativeDate = "Today"
		}

		videos = append(videos, Video{
			ID:            vid,
			Title:         item.Snippet.Title,
			URL:           "https://www.youtube.com/watch?v=" + vid,
			Thumbnail:     thumbnail,
			ChannelID:     item.Snippet.ChannelID,
			ChannelURL:    "https://www.youtube.com/channel/" + item.Snippet.ChannelID,
			ChannelTitle:  item.Snippet.ChannelTitle,
			Uploader:      item.Snippet.ChannelTitle,
			ViewCount:     viewCount,
			UploadDate:    item.Snippet.PublishedAt.Format("20060102"),
			Duration:      duration,
			DurationSecs:  durationSecs,
			Velocity:      velocity,
			DaysOld:       daysOld,
			RelativeDate:  relativeDate,
			FormattedDate: item.Snippet.PublishedAt.Format("02/01/2006"),
		})
	}

	return videos, nil
}

func (c *APIClient) fetchVideoDetails(ctx context.Context, videoIDs []string) (map[string]videoDetail, error) {
	if len(videoIDs) == 0 {
		return nil, nil
	}

	u := fmt.Sprintf("https://www.googleapis.com/youtube/v3/videos?part=statistics,contentDetails&id=%s&key=%s",
		strings.Join(videoIDs, ","), c.apiKey)

	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Items []struct {
			ID    string `json:"id"`
			Stats struct {
				ViewCount string `json:"viewCount"`
			} `json:"statistics"`
			ContentDetails struct {
				Duration string `json:"duration"`
			} `json:"contentDetails"`
		} `json:"items"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	details := make(map[string]videoDetail)
	for _, item := range result.Items {
		viewCount, _ := strconv.ParseInt(item.Stats.ViewCount, 10, 64)
		duration, durationSecs := parseISO8601Duration(item.ContentDetails.Duration)

		details[item.ID] = videoDetail{
			ViewCount:    viewCount,
			Duration:     duration,
			DurationSecs: durationSecs,
		}
	}

	return details, nil
}

func (c *APIClient) fetchChannelVideos(ctx context.Context, channelID string, limit, days int) ([]Video, error) {
	u := fmt.Sprintf("https://www.googleapis.com/youtube/v3/channels?part=contentDetails&id=%s&key=%s",
		channelID, c.apiKey)

	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var channelResult struct {
		Items []struct {
			ContentDetails struct {
				RelatedPlaylists struct {
					Uploads string `json:"uploads"`
				} `json:"relatedPlaylists"`
			} `json:"contentDetails"`
		} `json:"items"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&channelResult); err != nil {
		return nil, err
	}

	if len(channelResult.Items) == 0 {
		return nil, nil
	}

	uploadsPlaylistID := channelResult.Items[0].ContentDetails.RelatedPlaylists.Uploads

	if uploadsPlaylistID == "" {
		return nil, nil
	}

	u = fmt.Sprintf("https://www.googleapis.com/youtube/v3/playlistItems?part=snippet&playlistId=%s&maxResults=%d&key=%s",
		uploadsPlaylistID, limit*2, c.apiKey)

	req, err = http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, err
	}

	resp, err = c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var playlistResult struct {
		Items []struct {
			Snippet struct {
				ResourceID struct {
					VideoID string `json:"videoId"`
				} `json:"resourceId"`
				Title       string    `json:"title"`
				PublishedAt time.Time `json:"publishedAt"`
			} `json:"snippet"`
		} `json:"items"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&playlistResult); err != nil {
		return nil, err
	}

	videoTitles := make(map[string]string)
	var videoIDs []string
	cutoff := time.Now().AddDate(0, 0, -days)

	for _, item := range playlistResult.Items {
		if item.Snippet.PublishedAt.After(cutoff) {
			vid := item.Snippet.ResourceID.VideoID
			videoIDs = append(videoIDs, vid)
			videoTitles[vid] = item.Snippet.Title
		}
	}

	if len(videoIDs) > limit {
		videoIDs = videoIDs[:limit]
	}

	videoDetails, err := c.fetchVideoDetails(ctx, videoIDs)
	if err != nil {
		return nil, err
	}

	var videos []Video

	for _, vid := range videoIDs {
		details := videoDetails[vid]
		daysOld := 1

		viewCount := details.ViewCount
		velocity := float64(viewCount) / float64(daysOld)

		videos = append(videos, Video{
			ID:           vid,
			Title:        videoTitles[vid],
			URL:          "https://www.youtube.com/watch?v=" + vid,
			Thumbnail:    fmt.Sprintf("https://img.youtube.com/vi/%s/hqdefault.jpg", vid),
			ChannelID:    channelID,
			ChannelURL:   "https://www.youtube.com/channel/" + channelID,
			ViewCount:    viewCount,
			Duration:     details.Duration,
			DurationSecs: details.DurationSecs,
			Velocity:     velocity,
			DaysOld:      daysOld,
			RelativeDate: "Today",
		})
	}

	return videos, nil
}

func (c *APIClient) filterVideos(videos []Video, minViews int64, minVelocity float64, hideShorts bool) []Video {
	var filtered []Video

	for _, v := range videos {
		if v.ViewCount < minViews {
			continue
		}
		if v.Velocity < minVelocity {
			continue
		}
		if hideShorts && v.DurationSecs > 0 && v.DurationSecs <= 60 {
			continue
		}
		filtered = append(filtered, v)
	}

	return filtered
}
