// Package news provides external news fetching for YouTube manager niches
package news

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

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

// Fetcher fetches trending news from external APIs
type Fetcher struct {
	apiKeys   map[string]string // e.g., "newsapi": "key"
	userAgent string
	cache     map[string]*cachedResult
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
		cache:     make(map[string]*cachedResult),
	}
}

// SetUserAgent sets the User-Agent header for HTTP requests
func (f *Fetcher) SetUserAgent(ua string) {
	if ua != "" {
		f.userAgent = ua
	}
}

// FetchTrendingNews fetches trending news for a niche/query
// Uses multiple free sources with fallback logic
func (f *Fetcher) FetchTrendingNews(ctx context.Context, query string, limit int) ([]NewsItem, error) {
	// Check cache first
	cacheKey := strings.ToLower(query)
	if cached, ok := f.cache[cacheKey]; ok && time.Now().Before(cached.expiresAt) {
		return cached.data, nil
	}

	// Try sources in order of preference
	var news []NewsItem
	var err error

	// Source 1: Google News RSS (free, no API key needed)
	news, err = f.fetchFromGoogleNews(ctx, query, limit)
	if err == nil && len(news) > 0 {
		f.cache[cacheKey] = &cachedResult{data: news, expiresAt: time.Now().Add(2 * time.Hour)}
		return news, nil
	}

	// Source 2: NewsAPI.org (free tier, needs API key)
	if apiKey, ok := f.apiKeys["newsapi"]; ok {
		news, err = f.fetchFromNewsAPI(ctx, query, apiKey, limit)
		if err == nil && len(news) > 0 {
			f.cache[cacheKey] = &cachedResult{data: news, expiresAt: time.Now().Add(2 * time.Hour)}
			return news, nil
		}
	}

	// Source 3: GNews API (free tier, needs API key)
	if apiKey, ok := f.apiKeys["gnews"]; ok {
		news, err = f.fetchFromGNews(ctx, query, apiKey, limit)
		if err == nil && len(news) > 0 {
			f.cache[cacheKey] = &cachedResult{data: news, expiresAt: time.Now().Add(2 * time.Hour)}
			return news, nil
		}
	}

	return nil, fmt.Errorf("no news sources available for query: %s", query)
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

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("google news http %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
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

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("newsapi http %d", resp.StatusCode)
	}

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

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	if result.Status != "ok" {
		return nil, fmt.Errorf("newsapi status: %s", result.Status)
	}

	var news []NewsItem
	for _, article := range result.Articles {
		pubTime, _ := time.Parse(time.RFC3339, article.PublishedAt)
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

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("gnews http %d", resp.StatusCode)
	}

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

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	var news []NewsItem
	for _, article := range result.Articles {
		pubTime, _ := time.Parse(time.RFC3339, article.PublishedAt)
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
