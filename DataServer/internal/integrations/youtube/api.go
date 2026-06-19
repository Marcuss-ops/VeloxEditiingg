// Package youtube provides YouTube API integration for the Velox server.
// This file contains the APIClient type, constructor, and shared utilities.
package youtube

import (
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

type APIClient struct {
	apiKey     string
	httpClient *http.Client
	cache      *Cache
}

// NewAPIClient constructs a YouTube Data API v3 client. The remote-scraper
// fallback that used to live here (when the Google API key was missing or
// the quota was exhausted) has been removed: callers must supply a real
// API key via VELOX_YOUTUBE_API_KEY and let the operator handle quota
// via the official channels.
func NewAPIClient(apiKey string, cache *Cache) *APIClient {
	return &APIClient{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		cache: cache,
	}
}

type videoDetail struct {
	ViewCount    int64
	Duration     string
	DurationSecs int
}

func (c *APIClient) CleanupCache() int {
	if c.cache != nil {
		return c.cache.Cleanup()
	}
	return 0
}

func parseISO8601Duration(d string) (string, int) {
	re := regexp.MustCompile(`PT(?:(?P<H>\d+)H)?(?:(?P<M>\d+)M)?(?:(?P<S>\d+)S)?`)
	matches := re.FindStringSubmatch(d)

	if len(matches) == 0 {
		return d, 0
	}

	h, _ := strconv.Atoi(matches[1])
	m, _ := strconv.Atoi(matches[2])
	s, _ := strconv.Atoi(matches[3])

	totalSecs := h*3600 + m*60 + s

	if h > 0 {
		return fmt.Sprintf("%d:%02d:%02d", h, m, s), totalSecs
	}
	return fmt.Sprintf("%d:%02d", m, s), totalSecs
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
