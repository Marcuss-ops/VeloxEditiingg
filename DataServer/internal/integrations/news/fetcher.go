// Package news provides external news fetching for content manager niches
package news

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// defaultFetchTimeout bounds every outbound request. http.DefaultClient has
// no timeout: a stalled upstream would pin the calling handler goroutine
// indefinitely, so all fetchers share this bounded client instead.
const defaultFetchTimeout = 30 * time.Second

// cacheTTL is how long a successful result set is served from cache.
const cacheTTL = 2 * time.Hour

// maxCacheEntries bounds the per-query cache. Distinct queries arrive from
// operator input; without a cap the map grows without bound. On overflow the
// oldest-expiring entry is evicted (approximates LRU without extra bookkeeping).
const maxCacheEntries = 256

// NewsItem represents a news article from external sources
type NewsItem struct {
	Title       string    `json:"title"`
	URL         string    `json:"url"`
	Source      string    `json:"source"`
	PublishedAt time.Time `json:"published_at"`
	Description string    `json:"description"`
	ImageURL    string    `json:"image_url,omitempty"`
}

// TrendingResponse is the response from the trending news endpoint
type TrendingResponse struct {
	OK    bool       `json:"ok"`
	Query string     `json:"query"`
	News  []NewsItem `json:"news"`
	Count int        `json:"count"`
}

// Fetcher fetches trending news from external APIs. Safe for concurrent use:
// the cache map and its TTL bookkeeping are guarded by mu.
type Fetcher struct {
	apiKeys   map[string]string // e.g., "newsapi": "key"
	userAgent string
	client    *http.Client

	mu    sync.Mutex
	cache map[string]*cachedResult
}

type cachedResult struct {
	data      []NewsItem
	expiresAt time.Time
}

// NewFetcher creates a news fetcher with optional API keys
func NewFetcher(apiKeys map[string]string) *Fetcher {
	return &Fetcher{
		apiKeys:   apiKeys,
		userAgent: "VeloxBot/1.0",
		client:    &http.Client{Timeout: defaultFetchTimeout},
		cache:     make(map[string]*cachedResult),
	}
}

// SetUserAgent sets the User-Agent header for HTTP requests
func (f *Fetcher) SetUserAgent(ua string) {
	if ua != "" {
		f.userAgent = ua
	}
}

// parsePublishedAt parses a source timestamp without silently zeroing it.
// RFC3339 is the canonical wire format for both NewsAPI and GNews, but a few
// feeds emit RFC1123 or date-only values; those are tried before giving up so
// a valid article is never persisted with a zero PublishedAt just because the
// upstream chose a friendlier format. An empty string yields the zero time
// ("unknown"), which is distinct from "present but unparseable".
func parsePublishedAt(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, nil
	}
	for _, layout := range []string{time.RFC3339, time.RFC3339Nano, time.RFC1123Z, time.RFC1123, "2006-01-02"} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unparseable published_at %q", raw)
}

// cachedNews returns the cached result for key if present and unexpired.
// Expired entries are dropped on sight so the map never accumulates stale rows.
func (f *Fetcher) cachedNews(key string) ([]NewsItem, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cached, ok := f.cache[key]
	if !ok {
		return nil, false
	}
	if time.Now().After(cached.expiresAt) {
		delete(f.cache, key)
		return nil, false
	}
	return cached.data, true
}

// storeNews caches the result set and enforces the entry cap.
func (f *Fetcher) storeNews(key string, data []NewsItem) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.cache) >= maxCacheEntries {
		var oldestKey string
		var oldest time.Time
		first := true
		for k, v := range f.cache {
			if first || v.expiresAt.Before(oldest) {
				oldestKey, oldest, first = k, v.expiresAt, false
			}
		}
		delete(f.cache, oldestKey)
	}
	f.cache[key] = &cachedResult{data: data, expiresAt: time.Now().Add(cacheTTL)}
}

// FetchTrendingNews fetches trending news for a niche/query.
// Uses multiple free sources with fallback logic. When every source fails
// the returned error joins all underlying causes so operators see why each
// source was rejected instead of a generic "no sources" message.
func (f *Fetcher) FetchTrendingNews(ctx context.Context, query string, limit int) ([]NewsItem, error) {
	cacheKey := strings.ToLower(query)
	if news, ok := f.cachedNews(cacheKey); ok {
		return news, nil
	}

	// Try sources in order of preference, collecting failures.
	var errs []error

	// Source 1: Google News RSS (free, no API key needed)
	news, err := f.fetchFromGoogleNews(ctx, query, limit)
	if err == nil && len(news) > 0 {
		f.storeNews(cacheKey, news)
		return news, nil
	}
	if err != nil {
		errs = append(errs, fmt.Errorf("google news: %w", err))
	} else {
		errs = append(errs, errors.New("google news: no items returned"))
	}

	// Source 2: NewsAPI.org (free tier, needs API key)
	if apiKey, ok := f.apiKeys["newsapi"]; ok {
		news, err = f.fetchFromNewsAPI(ctx, query, apiKey, limit)
		if err == nil && len(news) > 0 {
			f.storeNews(cacheKey, news)
			return news, nil
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("newsapi: %w", err))
		} else {
			errs = append(errs, errors.New("newsapi: no items returned"))
		}
	}

	// Source 3: GNews API (free tier, needs API key)
	if apiKey, ok := f.apiKeys["gnews"]; ok {
		news, err = f.fetchFromGNews(ctx, query, apiKey, limit)
		if err == nil && len(news) > 0 {
			f.storeNews(cacheKey, news)
			return news, nil
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("gnews: %w", err))
		} else {
			errs = append(errs, errors.New("gnews: no items returned"))
		}
	}

	return nil, fmt.Errorf("no news sources available for query %q: %w", query, errors.Join(errs...))
}

// fetchFromGoogleNews fetches from Google News RSS (no API key needed)
func (f *Fetcher) fetchFromGoogleNews(ctx context.Context, query string, limit int) ([]NewsItem, error) {
	// Google News RSS endpoint
	rssURL := fmt.Sprintf("https://news.google.com/rss/search?q=%s&hl=en-US&gl=US&ceid=US:en", url.QueryEscape(query))

	req, err := http.NewRequestWithContext(ctx, "GET", rssURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", f.userAgent)

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}

	// Bound the read: a hostile or broken upstream must not balloon memory.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}

	// Parse RSS XML
	return parseGoogleNewsRSS(string(body), limit)
}

// fetchFromNewsAPI fetches from NewsAPI.org
func (f *Fetcher) fetchFromNewsAPI(ctx context.Context, query string, apiKey string, limit int) ([]NewsItem, error) {
	apiURL := fmt.Sprintf("https://newsapi.org/v2/everything?q=%s&apiKey=%s&pageSize=%d&sortBy=publishedAt&language=en",
		url.QueryEscape(query), apiKey, limit)

	req, err := http.NewRequestWithContext(ctx, "GET", apiURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}

	// Bound the read: a hostile or broken upstream must not balloon memory.
	var result struct {
		Status       string `json:"status"`
		TotalResults int    `json:"totalResults"`
		Articles     []struct {
			Title       string `json:"title"`
			Description string `json:"description"`
			URL         string `json:"url"`
			Source      struct {
				Name string `json:"name"`
			} `json:"source"`
			PublishedAt string `json:"publishedAt"`
			URLToImage  string `json:"urlToImage"`
		} `json:"articles"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&result); err != nil {
		return nil, err
	}

	if result.Status != "ok" {
		return nil, fmt.Errorf("status: %s", result.Status)
	}

	var news []NewsItem
	for _, article := range result.Articles {
		pubTime, perr := parsePublishedAt(article.PublishedAt)
		if perr != nil {
			return nil, fmt.Errorf("newsapi article %q: %w", article.URL, perr)
		}
		news = append(news, NewsItem{
			Title:       article.Title,
			URL:         article.URL,
			Source:      article.Source.Name,
			PublishedAt: pubTime,
			Description: article.Description,
			ImageURL:    article.URLToImage,
		})
	}

	return news, nil
}

// fetchFromGNews fetches from GNews API
func (f *Fetcher) fetchFromGNews(ctx context.Context, query string, apiKey string, limit int) ([]NewsItem, error) {
	apiURL := fmt.Sprintf("https://gnews.io/api/v4/search?q=%s&token=%s&max=%d&lang=en&sortby=publishedAt",
		url.QueryEscape(query), apiKey, limit)

	req, err := http.NewRequestWithContext(ctx, "GET", apiURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http %d", resp.StatusCode)
	}

	// Bound the read: a hostile or broken upstream must not balloon memory.
	var result struct {
		TotalArticles int `json:"totalArticles"`
		Articles      []struct {
			Title       string `json:"title"`
			Description string `json:"description"`
			URL         string `json:"url"`
			Source      struct {
				Name string `json:"name"`
			} `json:"source"`
			PublishedAt string `json:"publishedAt"`
			Image       string `json:"image"`
		} `json:"articles"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&result); err != nil {
		return nil, err
	}

	var news []NewsItem
	for _, article := range result.Articles {
		pubTime, perr := parsePublishedAt(article.PublishedAt)
		if perr != nil {
			return nil, fmt.Errorf("gnews article %q: %w", article.URL, perr)
		}
		news = append(news, NewsItem{
			Title:       article.Title,
			URL:         article.URL,
			Source:      article.Source.Name,
			PublishedAt: pubTime,
			Description: article.Description,
			ImageURL:    article.Image,
		})
	}

	return news, nil
}
