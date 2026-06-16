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

func (c *APIClient) GetChannelID(ctx context.Context, urlOrHandle string) (string, error) {
	raw := strings.TrimSpace(urlOrHandle)
	if raw == "" {
		return "", nil
	}

	if strings.Contains(raw, "youtube.com/channel/") {
		parts := strings.Split(raw, "youtube.com/channel/")
		if len(parts) > 1 {
			return strings.Split(strings.Split(parts[1], "/")[0], "?")[0], nil
		}
	}

	cacheKey := "cid_" + raw
	if cached, ok := c.cache.Get(cacheKey); ok {
		if s, ok := cached.(string); ok {
			return s, nil
		}
	}

	query := raw
	parsed, err := url.Parse(raw)
	if err == nil {
		host := strings.ToLower(parsed.Host)
		path := parsed.Path

		if match := regexp.MustCompile(`/@([^/?#]+)`).FindStringSubmatch(path); match != nil {
			query = match[1]
		} else if strings.Contains(raw, "youtube.com/c/") {
			parts := strings.Split(raw, "youtube.com/c/")
			if len(parts) > 1 {
				query = strings.Split(strings.Split(parts[1], "/")[0], "?")[0]
			}
		} else if strings.Contains(raw, "youtube.com/user/") {
			parts := strings.Split(raw, "youtube.com/user/")
			if len(parts) > 1 {
				query = strings.Split(strings.Split(parts[1], "/")[0], "?")[0]
			}
		} else if strings.HasPrefix(raw, "@") {
			query = raw[1:]
		}

		videoID := ""
		if strings.Contains(host, "youtu.be") {
			videoID = strings.Trim(path, "/")
		} else if strings.Contains(host, "youtube.com") {
			if strings.Contains(path, "/watch") {
				videoID = parsed.Query().Get("v")
			} else if strings.Contains(path, "/shorts/") {
				parts := strings.Split(path, "/shorts/")
				if len(parts) > 1 {
					videoID = strings.Split(parts[1], "/")[0]
				}
			} else if strings.Contains(path, "/live/") {
				parts := strings.Split(path, "/live/")
				if len(parts) > 1 {
					videoID = strings.Split(parts[1], "/")[0]
				}
			}
		}

		if videoID != "" {
			channelID, err := c.getChannelFromVideo(ctx, videoID)
			if err == nil && channelID != "" {
				c.cache.Set(cacheKey, channelID)
				return channelID, nil
			}
		}
	}

	if c.apiKey != "" {
		channelID, err := c.searchChannel(ctx, query)
		if err == nil && channelID != "" {
			c.cache.Set(cacheKey, channelID)
			return channelID, nil
		}
	}

	channelID, err := c.fallback.GetChannelID(ctx, urlOrHandle)
	if err == nil && channelID != "" {
		c.cache.Set(cacheKey, channelID)
		return channelID, nil
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

	info, err := c.fallback.GetChannelInfo(ctx, channelID)
	if err == nil && info != nil {
		c.cache.Set(cacheKey, info)
		return info, nil
	}

	return nil, err
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
		return nil, err
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
