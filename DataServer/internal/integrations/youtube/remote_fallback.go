// Package youtube provides YouTube Data API integration and management functionality.
package youtube

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// RemoteFallback provides fallback to remote scraper when API quota is exceeded
type RemoteFallback struct {
	baseURL string
	http    *http.Client
	cache   *Cache
}

// NewRemoteFallback creates a new remote fallback client
func NewRemoteFallback(baseURL string, cache *Cache) *RemoteFallback {
	if baseURL == "" {
		baseURL = "http://77.93.152.122:5000"
	}

	return &RemoteFallback{
		baseURL: baseURL,
		http: &http.Client{
			Timeout: 30 * time.Second,
		},
		cache: cache,
	}
}

// GetChannelID resolves a channel ID using the remote scraper
func (r *RemoteFallback) GetChannelID(ctx context.Context, urlOrHandle string) (string, error) {
	info, err := r.GetChannelInfo(ctx, urlOrHandle)
	if err != nil {
		return "", err
	}
	if info == nil {
		return "", nil
	}
	return info.ChannelID, nil
}

// GetChannelInfo fetches channel info using the remote scraper
func (r *RemoteFallback) GetChannelInfo(ctx context.Context, urlOrHandle string) (*ChannelInfo, error) {
	payload := map[string]string{"url": urlOrHandle}

	var result struct {
		OK        bool   `json:"ok"`
		URL       string `json:"url"`
		Title     string `json:"title"`
		Thumbnail string `json:"thumbnail"`
		ChannelID string `json:"channel_id"`
	}

	if err := r.post(ctx, "/api/remote/channel-info", payload, &result); err != nil {
		return nil, err
	}

	if !result.OK {
		return nil, nil
	}

	return &ChannelInfo{
		URL:       result.URL,
		Title:     result.Title,
		Thumbnail: result.Thumbnail,
		ChannelID: result.ChannelID,
	}, nil
}

// SearchVideos searches videos using the remote scraper
func (r *RemoteFallback) SearchVideos(ctx context.Context, query string, limit, days int) ([]Video, error) {
	payload := map[string]interface{}{
		"query": query,
		"limit": limit,
		"days":  days,
	}

	var result struct {
		OK     bool                  `json:"ok"`
		Videos []remoteFallbackVideo `json:"videos"`
	}

	if err := r.post(ctx, "/api/remote/search", payload, &result); err != nil {
		return nil, err
	}

	if !result.OK {
		return nil, nil
	}

	return r.transformVideos(result.Videos), nil
}

// GetChannelVideos fetches videos from a channel using the remote scraper
func (r *RemoteFallback) GetChannelVideos(ctx context.Context, channelURL string, limit, days int) ([]Video, error) {
	payload := map[string]interface{}{
		"channel_url": channelURL,
		"limit":       limit,
		"days":        days,
	}

	var result struct {
		OK     bool                  `json:"ok"`
		Videos []remoteFallbackVideo `json:"videos"`
	}

	if err := r.post(ctx, "/api/remote/search", payload, &result); err != nil {
		return nil, err
	}

	if !result.OK {
		return nil, nil
	}

	return r.transformVideos(result.Videos), nil
}

// GetVideoInfo fetches video metadata using the remote scraper
func (r *RemoteFallback) GetVideoInfo(ctx context.Context, videoID string) (map[string]interface{}, error) {
	payload := map[string]string{"video_id": videoID}

	var result struct {
		OK   bool                   `json:"ok"`
		Info map[string]interface{} `json:"info"`
	}

	if err := r.post(ctx, "/api/remote/video-info", payload, &result); err != nil {
		return nil, err
	}

	if !result.OK {
		return nil, nil
	}

	return result.Info, nil
}

// GetThumbnail fetches thumbnail URLs using the remote scraper
func (r *RemoteFallback) GetThumbnail(ctx context.Context, videoID string) (map[string]string, error) {
	payload := map[string]string{"video_id": videoID}

	var result struct {
		OK         bool              `json:"ok"`
		Thumbnails map[string]string `json:"thumbnails"`
	}

	if err := r.post(ctx, "/api/remote/thumbnail", payload, &result); err != nil {
		return nil, err
	}

	if !result.OK {
		return nil, nil
	}

	return result.Thumbnails, nil
}

// --- Internal ---

type remoteFallbackVideo struct {
	ID            string  `json:"id"`
	Title         string  `json:"title"`
	URL           string  `json:"url"`
	Thumbnail     string  `json:"thumbnail"`
	ChannelID     string  `json:"channel_id"`
	ChannelURL    string  `json:"channel_url"`
	ChannelTitle  string  `json:"channel_title"`
	Uploader      string  `json:"uploader"`
	ViewCount     int64   `json:"view_count"`
	UploadDate    string  `json:"upload_date"`
	Duration      string  `json:"duration"`
	DurationSecs  int     `json:"duration_seconds"`
	Velocity      float64 `json:"velocity"`
	DaysOld       int     `json:"days_old"`
	RelativeDate  string  `json:"relative_date"`
	FormattedDate string  `json:"formatted_date"`
}

func (r *RemoteFallback) transformVideos(videos []remoteFallbackVideo) []Video {
	result := make([]Video, len(videos))
	now := time.Now()

	for i, v := range videos {
		// Calculate velocity if not provided
		velocity := v.Velocity
		if velocity == 0 && v.DaysOld > 0 {
			velocity = float64(v.ViewCount) / float64(v.DaysOld)
		}

		// Calculate relative date if not provided
		relativeDate := v.RelativeDate
		if relativeDate == "" {
			if v.DaysOld < 1 {
				relativeDate = "Today"
			} else if v.DaysOld == 1 {
				relativeDate = "Yesterday"
			} else {
				relativeDate = fmt.Sprintf("%d days ago", v.DaysOld)
			}
		}

		// Generate thumbnail URL if not provided
		thumbnail := v.Thumbnail
		if thumbnail == "" && v.ID != "" {
			thumbnail = fmt.Sprintf("https://img.youtube.com/vi/%s/hqdefault.jpg", v.ID)
		}

		// Generate URL if not provided
		videoURL := v.URL
		if videoURL == "" && v.ID != "" {
			videoURL = "https://www.youtube.com/watch?v=" + v.ID
		}

		// Generate channel URL if not provided
		channelURL := v.ChannelURL
		if channelURL == "" && v.ChannelID != "" {
			channelURL = "https://www.youtube.com/channel/" + v.ChannelID
		}

		daysOld := v.DaysOld
		if daysOld == 0 {
			daysOld = int(now.Sub(time.Now()).Hours()/24) + 1
		}

		result[i] = Video{
			ID:            v.ID,
			Title:         v.Title,
			URL:           videoURL,
			Thumbnail:     thumbnail,
			ChannelID:     v.ChannelID,
			ChannelURL:    channelURL,
			ChannelTitle:  v.ChannelTitle,
			Uploader:      v.Uploader,
			ViewCount:     v.ViewCount,
			UploadDate:    v.UploadDate,
			Duration:      v.Duration,
			DurationSecs:  v.DurationSecs,
			Velocity:      velocity,
			DaysOld:       daysOld,
			RelativeDate:  relativeDate,
			FormattedDate: v.FormattedDate,
			Source:        "remote_fallback",
		}
	}

	return result
}

func (r *RemoteFallback) post(ctx context.Context, path string, payload interface{}, result interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	url := r.baseURL + path
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := r.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("remote fallback error: %d - %s", resp.StatusCode, string(respBody))
	}

	return json.NewDecoder(resp.Body).Decode(result)
}

// --- Helper functions ---

// isVideoURL checks if a URL is a YouTube video URL
func isVideoURL(url string) bool {
	url = strings.ToLower(url)
	return strings.Contains(url, "watch?v=") ||
		strings.Contains(url, "youtu.be/") ||
		strings.Contains(url, "/shorts/") ||
		strings.Contains(url, "/embed/") ||
		strings.Contains(url, "/live/")
}
