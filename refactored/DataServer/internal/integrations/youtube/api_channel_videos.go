package youtube

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// GetRecentChannelVideos returns up to `limit` recent uploads for the given
// YouTube channel, restricted to the trailing `days` window. The implementation
// is YouTube Data API v3 only: there used to be a remote-scraper fallback
// here that kicked in when the API quota was exhausted, but that path has
// been removed in the S6 cleanup. Operators see an empty result if the API
// has no items to return, rather than being silently masked by a third-party
// scraper.
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

	if len(videos) > 0 {
		c.cache.Set(cacheKey, videos)
	}

	return videos, err
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
