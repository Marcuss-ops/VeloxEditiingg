package youtube

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"velox-server/internal/integrations/youtube"
)

// DownloadThumbnail downloads the thumbnail image from the given URL.
func (s *Service) DownloadThumbnail(ctx context.Context, thumbURL string) (io.ReadCloser, int64, string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", thumbURL, nil)
	if err != nil {
		return nil, 0, "", err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, "", err
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, 0, "", fmt.Errorf("failed to fetch image: status %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "image/jpeg"
	}

	return resp.Body, resp.ContentLength, contentType, nil
}

// GetVideoInfo retrieves video information by ID.
func (s *Service) GetVideoInfo(ctx context.Context, videoID string) (youtube.Video, error) {
	info, err := s.apiClient.SearchVideos(ctx, videoID, 1, 365, 0, 0, false)
	if err != nil {
		return youtube.Video{}, err
	}
	if len(info) > 0 {
		return info[0], nil
	}
	return youtube.Video{URL: "https://www.youtube.com/watch?v=" + videoID}, nil
}

// ScrapeVideo retrieves video info from a URL.
func (s *Service) ScrapeVideo(ctx context.Context, url string) (youtube.Video, error) {
	videoInfo, err := s.apiClient.SearchVideos(ctx, url, 1, 365, 0, 0, false)
	if err != nil {
		return youtube.Video{}, err
	}
	if len(videoInfo) > 0 {
		return videoInfo[0], nil
	}
	return youtube.Video{URL: url}, nil
}

// GenerateScript drafts an video script.
func (s *Service) GenerateScript(query, language string) string {
	return fmt.Sprintf(`# Script: %s

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
}

// ViralSearch searches for high-performing videos.
func (s *Service) ViralSearch(ctx context.Context, query string, limit int, filterDate string, minViews int64, minVelocity float64, hideShorts bool) ([]youtube.Video, error) {
	days := 30
	switch filterDate {
	case "today":
		days = 2
	case "week":
		days = 7
	case "month":
		days = 30
	case "all":
		days = 365
	}

	return s.apiClient.SearchVideos(ctx, query, limit, days, minViews, minVelocity, hideShorts)
}

// DiscoverySearch searches for videos.
func (s *Service) DiscoverySearch(ctx context.Context, query string, days, limit int, minViews int64, minVelocity float64, hideShorts bool) ([]youtube.Video, error) {
	return s.apiClient.SearchVideos(ctx, query, limit, days, minViews, minVelocity, hideShorts)
}

// SimilarChannels finds channels related to the query.
func (s *Service) SimilarChannels(ctx context.Context, searchQuery string, limit int) ([]youtube.SimilarChannelHit, error) {
	videos, err := s.apiClient.SearchVideos(ctx, searchQuery, 20, 30, 0, 0, false)
	if err != nil {
		return nil, err
	}

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
	return channels, nil
}

// AutoSimilarChannels automatically finds similar channels based on stored keywords.
func (s *Service) AutoSimilarChannels(ctx context.Context, limit int, minVelocity int64) ([]youtube.SimilarChannelHit, []string, int, error) {
	data := s.storage.LoadData()

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
		return []youtube.SimilarChannelHit{}, []string{}, 0, nil
	}

	var keywordsList []string
	for kw := range allKeywords {
		keywordsList = append(keywordsList, kw)
		if len(keywordsList) >= 15 {
			break
		}
	}

	searchQuery := strings.Join(keywordsList, " ")
	videos, _ := s.apiClient.SearchVideos(ctx, searchQuery, 50, 30, 0, float64(minVelocity), false)

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

	sort.Slice(similar, func(i, j int) bool {
		return similar[i].Velocity > similar[j].Velocity
	})

	if len(similar) > limit {
		similar = similar[:limit]
	}

	return similar, keywordsList, len(allChannels), nil
}

// Trends lists current trends for a news topic.
func (s *Service) Trends(ctx context.Context, query string) ([]youtube.TrendTopic, error) {
	videos, err := s.apiClient.SearchVideos(ctx, query+" news", 10, 7, 1000, 0, false)
	if err != nil {
		return nil, err
	}

	var trends []youtube.TrendTopic
	for _, v := range videos {
		viewStr := fmt.Sprintf("%d", v.ViewCount)
		trends = append(trends, youtube.TrendTopic{
			Title:     v.Title,
			URL:       v.URL,
			Views:     viewStr,
			Thumbnail: v.Thumbnail,
		})
	}
	return trends, nil
}

// AIDigest creates an AI Digest response for a topic.
func (s *Service) AIDigest(ctx context.Context, query string) (string, []youtube.Video, error) {
	videos, err := s.apiClient.SearchVideos(ctx, query, 5, 7, 0, 0, false)
	if err != nil {
		return "", nil, err
	}

	var digest strings.Builder
	digest.WriteString(fmt.Sprintf("# %s - Weekly Digest\n\n", query))
	digest.WriteString(fmt.Sprintf("Found %d trending videos this week.\n\n", len(videos)))

	for i, v := range videos {
		digest.WriteString(fmt.Sprintf("%d. **%s**\n", i+1, v.Title))
		digest.WriteString(fmt.Sprintf("   - Views: %d\n", v.ViewCount))
		digest.WriteString(fmt.Sprintf("   - Channel: %s\n\n", v.Uploader))
	}

	return digest.String(), videos, nil
}
