package youtube

import (
	"context"
	"fmt"
	"log"
	"sort"

	"velox-server/internal/integrations/youtube"
)

// GetVideoFeed gets aggregated videos from channels in a group.
func (s *Service) GetVideoFeed(ctx context.Context, groupName, timeRange, sortBy string) (*youtube.FeedResponse, error) {
	data := s.storage.LoadData()
	var targetChannels []youtube.Channel

	if groupName != "" {
		if group, ok := data.Groups[groupName]; ok {
			targetChannels = group.Channels
		} else {
			return nil, fmt.Errorf("group not found: %s", groupName)
		}
	} else {
		for _, group := range data.Groups {
			targetChannels = append(targetChannels, group.Channels...)
		}
	}

	if len(targetChannels) == 0 {
		return &youtube.FeedResponse{
			OK:     true,
			Videos: []youtube.Video{},
			Count:  0,
		}, nil
	}

	seenURLs := make(map[string]bool)
	var uniqueChannels []youtube.Channel
	for _, ch := range targetChannels {
		if !seenURLs[ch.URL] {
			seenURLs[ch.URL] = true
			uniqueChannels = append(uniqueChannels, ch)
		}
	}

	daysBack := 21
	limitPerChannel := 10
	switch timeRange {
	case "today":
		daysBack = 2
		limitPerChannel = 4
	case "week":
		daysBack = 30
		limitPerChannel = 12
	case "twoweeks":
		daysBack = 45
		limitPerChannel = 16
	case "month":
		daysBack = 90
		limitPerChannel = 24
	case "all":
		daysBack = 365
		limitPerChannel = 30
	}

	var aggregatedVideos []youtube.Video

	for i, ch := range uniqueChannels {
		if i >= 15 {
			break
		}

		channelID, err := s.apiClient.GetChannelID(ctx, ch.URL)
		if err != nil {
			continue
		}

		var videos []youtube.Video
		if channelID != "" {
			videos, err = s.apiClient.GetRecentChannelVideos(ctx, channelID, limitPerChannel, daysBack)
			if err != nil {
				log.Printf("[FEED] GetRecentChannelVideos failed for %s: %v", ch.Title, err)
			}
		}

		if len(videos) == 0 {
			videos, _ = s.apiClient.SearchVideos(ctx, ch.Title, limitPerChannel, daysBack, 0, 0, false)
		}

		for i := range videos {
			videos[i].SourceChannel = ch.Title
			videos[i].GroupName = groupName
			if videos[i].Thumbnail == "" && ch.Thumbnail != "" {
				videos[i].Thumbnail = ch.Thumbnail
			}
		}

		aggregatedVideos = append(aggregatedVideos, videos...)
	}

	if sortBy == "views" {
		sort.Slice(aggregatedVideos, func(i, j int) bool {
			return aggregatedVideos[i].ViewCount > aggregatedVideos[j].ViewCount
		})
	} else {
		sort.Slice(aggregatedVideos, func(i, j int) bool {
			return aggregatedVideos[i].UploadDate > aggregatedVideos[j].UploadDate
		})
	}

	return &youtube.FeedResponse{
		OK:     true,
		Group:  groupName,
		Videos: aggregatedVideos,
		Count:  len(aggregatedVideos),
	}, nil
}

// RefreshAllGroupsFeed refreshes feed caches for all groups.
func (s *Service) RefreshAllGroupsFeed(ctx context.Context) (int, error) {
	data := s.storage.LoadData()
	if len(data.Groups) == 0 {
		return 0, nil
	}

	totalVideos := 0
	for groupName := range data.Groups {
		count, err := s.RefreshGroupFeed(ctx, groupName)
		if err != nil {
			continue
		}
		totalVideos += count
	}

	return totalVideos, nil
}

// RefreshGroupFeed refreshes the feed cache for a group.
func (s *Service) RefreshGroupFeed(ctx context.Context, groupName string) (int, error) {
	group, ok := s.storage.LoadData().Groups[groupName]
	if !ok {
		return 0, fmt.Errorf("group not found: %s", groupName)
	}

	if len(group.Channels) == 0 {
		return 0, nil
	}

	daysBack := 30
	limitPerChannel := 12

	var aggregatedVideos []youtube.Video

	for i, ch := range group.Channels {
		if i >= 15 {
			break
		}

		channelID, err := s.apiClient.GetChannelID(ctx, ch.URL)
		if err != nil {
			continue
		}

		var videos []youtube.Video
		if channelID != "" {
			videos, err = s.apiClient.GetRecentChannelVideos(ctx, channelID, limitPerChannel, daysBack)
			if err != nil {
				log.Printf("[FEED] GetRecentChannelVideos failed for %s: %v", ch.Title, err)
			}
		}

		if len(videos) == 0 {
			videos, _ = s.apiClient.SearchVideos(ctx, ch.Title, limitPerChannel, daysBack, 0, 0, false)
		}

		for i := range videos {
			videos[i].SourceChannel = ch.Title
			videos[i].GroupName = groupName
			if videos[i].Thumbnail == "" && ch.Thumbnail != "" {
				videos[i].Thumbnail = ch.Thumbnail
			}
		}

		aggregatedVideos = append(aggregatedVideos, videos...)
	}

	sort.Slice(aggregatedVideos, func(i, j int) bool {
		return aggregatedVideos[i].UploadDate > aggregatedVideos[j].UploadDate
	})

	cacheKey := fmt.Sprintf("feed_%s_week_date", groupName)
	result := &youtube.FeedResponse{
		OK:     true,
		Group:  groupName,
		Videos: aggregatedVideos,
		Count:  len(aggregatedVideos),
	}
	s.feedCache.Set(cacheKey, result)

	return len(aggregatedVideos), nil
}

// FeedCacheSet sets a value in the feed cache.
func (s *Service) FeedCacheSet(key string, val *youtube.FeedResponse) {
	s.feedCache.Set(key, val)
}

// FeedCacheGet gets a value from the feed cache.
func (s *Service) FeedCacheGet(key string) (*youtube.FeedResponse, bool) {
	return s.feedCache.Get(key)
}

// FeedCacheClear clears the feed cache.
func (s *Service) FeedCacheClear() {
	s.feedCache.Clear()
}
