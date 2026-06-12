package youtube

import (
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

type APIClient struct {
	apiKey     string
	httpClient *http.Client
	cache      *Cache
	fallback   *RemoteFallback
}

func NewAPIClient(apiKey string, cache *Cache, fallbackURL string) *APIClient {
	if apiKey == "" {
		log.Printf("[WARN] YouTube API: no API key provided, will use fallback only")
	}

	return &APIClient{
		apiKey: apiKey,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		cache:    cache,
		fallback: NewRemoteFallback(fallbackURL, cache),
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
	re := regexp.MustCompile(`PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?`)
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
