package youtube

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

// channelQuery is the normalized query returned by parseYouTubeChannelQuery.
// When isVideoID is false, query is a channel-search term (handle, username,
// or custom URL segment). When isVideoID is true, query is a YouTube
// video ID and the caller must resolve the channel via getChannelFromVideo.
type channelQuery struct {
	query string
}

// parseYouTubeChannelQuery normalizes a raw channel/handle/ID input
// into either a channel-search query or a video ID. Returns
// isVideoID=true when the input is a video URL and the returned query
// holds the video ID (caller routes through getChannelFromVideo);
// isVideoID=false when the input is a channel/handle/ID (caller routes
// through searchChannel).
//
// The fast-path for direct channel URLs (youtube.com/channel/<id>) is
// handled separately by GetChannelID; this helper focuses on the
// ambiguous-query cases.
func parseYouTubeChannelQuery(raw string) (q channelQuery, isVideoID bool, err error) {
	parsed, parseErr := url.Parse(raw)
	if parseErr != nil {
		// Unparseable URL — treat as raw search term.
		return channelQuery{query: raw}, false, nil
	}
	path := parsed.Path

	// /@handle (canonical YouTube handle format)
	if match := regexp.MustCompile(`/@([^/?#]+)`).FindStringSubmatch(path); match != nil {
		return channelQuery{query: match[1]}, false, nil
	}
	// /c/<custom> (legacy custom URL)
	if strings.Contains(raw, "youtube.com/c/") {
		if id := firstSegmentAfter(raw, "youtube.com/c/"); id != "" {
			return channelQuery{query: id}, false, nil
		}
	}
	// /user/<username> (legacy user URL)
	if strings.Contains(raw, "youtube.com/user/") {
		if id := firstSegmentAfter(raw, "youtube.com/user/"); id != "" {
			return channelQuery{query: id}, false, nil
		}
	}
	// @handle (bare-handle input)
	if strings.HasPrefix(raw, "@") {
		return channelQuery{query: raw[1:]}, false, nil
	}

	// Video URL → extract the video ID, caller resolves channel via
	// the videos API.
	if videoID, ok := extractVideoIDFromURL(raw); ok {
		return channelQuery{query: videoID}, true, nil
	}

	return channelQuery{query: raw}, false, nil
}

// extractVideoIDFromURL pulls the YouTube video ID out of a URL-shaped
// input. Returns ("", false) when the input is not a recognized video
// URL (including raw 11-char video IDs without a URL prefix).
//
// Supported URL shapes:
//   - youtu.be/<id>
//   - youtube.com/watch?v=<id>
//   - youtube.com/shorts/<id>
//   - youtube.com/live/<id>
func extractVideoIDFromURL(raw string) (string, bool) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", false
	}
	host := strings.ToLower(parsed.Host)
	path := parsed.Path

	if strings.Contains(host, "youtu.be") {
		id := strings.Trim(path, "/")
		if id == "" {
			return "", false
		}
		return id, true
	}
	if strings.Contains(host, "youtube.com") {
		if strings.Contains(path, "/watch") {
			if v := parsed.Query().Get("v"); v != "" {
				return v, true
			}
		}
		if id := firstSegmentAfter(path, "/shorts/"); id != "" {
			return id, true
		}
		if id := firstSegmentAfter(path, "/live/"); id != "" {
			return id, true
		}
	}
	return "", false
}

// firstSegmentAfter returns the first path segment that follows prefix
// in raw, trimmed of any subsequent "/..." or "?..." tail. Returns ""
// when prefix is not present. Used to extract the trailing identifier
// from legacy YouTube custom URLs (/c/, /user/) and path-based video
// URLs (/shorts/, /live/).
func firstSegmentAfter(raw, prefix string) string {
	if !strings.Contains(raw, prefix) {
		return ""
	}
	parts := strings.Split(raw, prefix)
	if len(parts) < 2 {
		return ""
	}
	return strings.Split(strings.Split(parts[1], "/")[0], "?")[0]
}

func (c *APIClient) GetChannelID(ctx context.Context, urlOrHandle string) (string, error) {
	raw := strings.TrimSpace(urlOrHandle)
	if raw == "" {
		return "", nil
	}

	if strings.Contains(raw, "youtube.com/channel/") {
		if id := firstSegmentAfter(raw, "youtube.com/channel/"); id != "" {
			return id, nil
		}
	}

	cacheKey := "cid_" + raw
	if cached, ok := c.cache.Get(cacheKey); ok {
		if s, ok := cached.(string); ok {
			return s, nil
		}
	}

	q, isVideoID, err := parseYouTubeChannelQuery(raw)
	if err != nil {
		return "", err
	}

	if isVideoID {
		if channelID, err := c.getChannelFromVideo(ctx, q.query); err == nil && channelID != "" {
			c.cache.Set(cacheKey, channelID)
			return channelID, nil
		}
	}

	if c.apiKey != "" {
		if channelID, err := c.searchChannel(ctx, q.query); err == nil && channelID != "" {
			c.cache.Set(cacheKey, channelID)
			return channelID, nil
		}
	}

	c.cache.Set(cacheKey, "")
	return "", nil
}

func (c *APIClient) GetChannelInfo(ctx context.Context, urlOrHandle string) (*ChannelInfo, error) {
	channelID, err := c.GetChannelID(ctx, urlOrHandle)
	if err != nil {
		return nil, err
	}
	if channelID == "" {
		return nil, nil
	}

	cacheKey := "cinfo_" + channelID
	if cached, ok := c.cache.Get(cacheKey); ok {
		if info, ok := cached.(*ChannelInfo); ok {
			return info, nil
		}
	}

	if c.apiKey != "" {
		info, err := c.fetchChannelInfo(ctx, channelID)
		if err == nil && info != nil {
			c.cache.Set(cacheKey, info)
			return info, nil
		}
	}

	return nil, nil
}

func (c *APIClient) searchChannel(ctx context.Context, query string) (string, error) {
	u := fmt.Sprintf("https://www.googleapis.com/youtube/v3/search?part=snippet&q=%s&type=channel&maxResults=1&key=%s",
		url.QueryEscape(query), c.apiKey)

	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return "", err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API error: %d", resp.StatusCode)
	}

	var result struct {
		Items []struct {
			Snippet struct {
				ChannelID string `json:"channelId"`
			} `json:"snippet"`
		} `json:"items"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	if len(result.Items) == 0 {
		return "", nil
	}

	return result.Items[0].Snippet.ChannelID, nil
}

func (c *APIClient) getChannelFromVideo(ctx context.Context, videoID string) (string, error) {
	u := fmt.Sprintf("https://www.googleapis.com/youtube/v3/videos?part=snippet&id=%s&key=%s",
		videoID, c.apiKey)

	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return "", err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var result struct {
		Items []struct {
			Snippet struct {
				ChannelID string `json:"channelId"`
			} `json:"snippet"`
		} `json:"items"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	if len(result.Items) == 0 {
		return "", nil
	}

	return result.Items[0].Snippet.ChannelID, nil
}

func (c *APIClient) fetchChannelInfo(ctx context.Context, channelID string) (*ChannelInfo, error) {
	u := fmt.Sprintf("https://www.googleapis.com/youtube/v3/channels?part=snippet,statistics&id=%s&key=%s",
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

	var result struct {
		Items []struct {
			Snippet struct {
				Title       string `json:"title"`
				Description string `json:"description"`
				Thumbnails  struct {
					High    struct{ URL string } `json:"high"`
					Default struct{ URL string } `json:"default"`
				} `json:"thumbnails"`
			} `json:"snippet"`
		} `json:"items"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, nil
	}

	if len(result.Items) == 0 {
		return nil, nil
	}

	item := result.Items[0]
	thumbnail := item.Snippet.Thumbnails.High.URL
	if thumbnail == "" {
		thumbnail = item.Snippet.Thumbnails.Default.URL
	}

	return &ChannelInfo{
		URL:         "https://www.youtube.com/channel/" + channelID,
		Title:       item.Snippet.Title,
		Thumbnail:   thumbnail,
		Description: item.Snippet.Description,
		ChannelID:   channelID,
	}, nil
}
