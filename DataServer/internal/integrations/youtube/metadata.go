// Package youtube provides YouTube API integration for the Velox server.
// This file contains video metadata management functionality.
package youtube

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"google.golang.org/api/youtube/v3"
)

// VideoManager handles video metadata operations
type VideoManager struct {
	service *Service
}

// NewVideoManager creates a new VideoManager
func NewVideoManager(s *Service) *VideoManager {
	return &VideoManager{
		service: s,
	}
}

// UpdateVideoMetadata updates a video's metadata
func (vm *VideoManager) UpdateVideoMetadata(ctx context.Context, channelID string, videoID string, config UploadConfig) error {
	service, err := vm.service.GetYouTubeService(ctx, channelID)
	if err != nil {
		return fmt.Errorf("failed to get YouTube service: %w", err)
	}

	// Get current video
	video, err := service.Videos.List([]string{"snippet", "status"}).Id(videoID).Do()
	if err != nil {
		return fmt.Errorf("failed to get video: %w", err)
	}

	if len(video.Items) == 0 {
		return fmt.Errorf("video not found: %s", videoID)
	}

	item := video.Items[0]

	// Update fields
	if config.Title != "" {
		item.Snippet.Title = config.Title
	}
	if config.Description != "" {
		item.Snippet.Description = config.Description
	}
	if len(config.Tags) > 0 {
		item.Snippet.Tags = config.Tags
	}
	if config.PrivacyStatus != "" {
		item.Status.PrivacyStatus = config.PrivacyStatus
	}

	// Update video
	_, err = service.Videos.Update([]string{"snippet", "status"}, item).Do()
	if err != nil {
		return fmt.Errorf("failed to update video: %w", err)
	}

	log.Printf("[OK] Video metadata updated: %s", videoID)
	return nil
}

// DeleteVideo deletes a video from YouTube
func (vm *VideoManager) DeleteVideo(ctx context.Context, channelID string, videoID string) error {
	service, err := vm.service.GetYouTubeService(ctx, channelID)
	if err != nil {
		return fmt.Errorf("failed to get YouTube service: %w", err)
	}

	err = service.Videos.Delete(videoID).Do()
	if err != nil {
		return fmt.Errorf("failed to delete video: %w", err)
	}

	log.Printf("[OK] Video deleted: %s", videoID)
	return nil
}
// ListVideos lists videos for a channel with cache support
func (vm *VideoManager) ListVideos(ctx context.Context, channelID string, maxResults int64) ([]*youtube.Video, error) {
	// 1. Check cache first
	cacheKey := fmt.Sprintf("videos:%s:%d", channelID, maxResults)
	if vm.service.cache != nil {
		if cachedVal, found := vm.service.cache.Get(cacheKey); found {
			// Try direct cast first
			if cachedVideos, ok := cachedVal.([]*youtube.Video); ok {
				return cachedVideos, nil
			}
			// Fallback: marshal to json and unmarshal to []*youtube.Video
			if rawJSON, err := json.Marshal(cachedVal); err == nil {
				var cachedVideos []*youtube.Video
				if err := json.Unmarshal(rawJSON, &cachedVideos); err == nil {
					return cachedVideos, nil
				}
			}
		}
	}

	service, err := vm.service.GetYouTubeService(ctx, channelID)
	if err != nil {
		return nil, fmt.Errorf("failed to get YouTube service: %w", err)
	}

	// Resolve the uploads playlist ID (standard YouTube API feature: replace second char UC -> UU)
	uploadsPlaylistID := channelID
	if len(channelID) > 2 && channelID[0:2] == "UC" {
		uploadsPlaylistID = "UU" + channelID[2:]
	}

	// Retrieve playlist items from the uploads playlist (costs 1 unit instead of 100)
	playlistCall := service.PlaylistItems.List([]string{"snippet"}).
		PlaylistId(uploadsPlaylistID).
		MaxResults(maxResults)

	playlistResponse, err := playlistCall.Do()
	if err != nil {
		// Fallback to Search.List if playlist fetching fails
		log.Printf("[WARN] PlaylistItems failed for %s, falling back to Search.List: %v", channelID, err)
		searchCall := service.Search.List([]string{"snippet"}).
			ForMine(true).
			Type("video").
			MaxResults(maxResults)
		searchResponse, err := searchCall.Do()
		if err != nil {
			return nil, fmt.Errorf("failed to list videos (search fallback): %w", err)
		}
		
		videoIDs := make([]string, 0, len(searchResponse.Items))
		for _, item := range searchResponse.Items {
			videoIDs = append(videoIDs, item.Id.VideoId)
		}

		if len(videoIDs) == 0 {
			return []*youtube.Video{}, nil
		}

		videosResponse, err := service.Videos.List([]string{"snippet", "status", "statistics"}).
			Id(strings.Join(videoIDs, ",")).Do()
		if err != nil {
			return nil, fmt.Errorf("failed to get video details: %w", err)
		}

		// Cache search fallback results
		if vm.service.cache != nil {
			vm.service.cache.Set(cacheKey, videosResponse.Items)
		}
		return videosResponse.Items, nil
	}

	// Get video IDs from playlist items
	videoIDs := make([]string, 0, len(playlistResponse.Items))
	for _, item := range playlistResponse.Items {
		if item.Snippet != nil && item.Snippet.ResourceId != nil {
			videoIDs = append(videoIDs, item.Snippet.ResourceId.VideoId)
		}
	}

	// Get video details (costs 1 unit)
	if len(videoIDs) == 0 {
		return []*youtube.Video{}, nil
	}

	videosResponse, err := service.Videos.List([]string{"snippet", "status", "statistics"}).
		Id(strings.Join(videoIDs, ",")).Do()
	if err != nil {
		return nil, fmt.Errorf("failed to get video details: %w", err)
	}

	// Cache playlist results
	if vm.service.cache != nil {
		vm.service.cache.Set(cacheKey, videosResponse.Items)
	}

	return videosResponse.Items, nil
}
// --- Advanced Automation Methods ---

// CheckCopyrightStatus queries the API for copyright claims on a video
func (vm *VideoManager) CheckCopyrightStatus(ctx context.Context, channelID string, videoID string) (*CopyrightStatus, error) {
	service, err := vm.service.GetYouTubeService(ctx, channelID)
	if err != nil {
		return nil, err
	}

	// For copyright details, we need contentDetails and status
	call := service.Videos.List([]string{"contentDetails", "status", "topicDetails"}).Id(videoID)
	resp, err := call.Do()
	if err != nil {
		return nil, fmt.Errorf("failed to check copyright: %w", err)
	}

	if len(resp.Items) == 0 {
		return nil, fmt.Errorf("video not found: %s", videoID)
	}

	item := resp.Items[0]
	status := &CopyrightStatus{
		VideoID:       videoID,
		PrivacyStatus: item.Status.PrivacyStatus,
		HasClaims:     false,
	}

	// ContentDetails might contain region restriction which is a form of claim
	if item.ContentDetails.RegionRestriction != nil {
		status.HasClaims = true
		status.Allowed = item.ContentDetails.RegionRestriction.Allowed
		status.Blocked = item.ContentDetails.RegionRestriction.Blocked
	}

	// Note: Detailed Content ID claims often require the YouTube Content ID API
	// which is different from Data API v3. However, we can infer some info from
	// the monetization status if the channel is part of an MCN.

	return status, nil
}

// LogMetadataChange records a change in the database for future A/B analysis
func (vm *VideoManager) LogMetadataChange(ctx context.Context, channelID string, videoID string, testType string, oldValue, newValue string) error {
	// 1. Get current stats as "Before" snapshot
	stats, err := vm.service.QuotaManager().FetchAnalytics(ctx, channelID, 7) // Last 7 days
	if err == nil && stats != nil {
		log.Printf("[YT] Stats snapshot taken for video %s: %v", videoID, stats["totals"])
	}

	// 2. Log to database
	// This would use a sqlite table 'metadata_ab_tests'
	log.Printf("[YT] Logging A/B test: %s changed %s from '%s' to '%s'", videoID, testType, oldValue, newValue)
	
	return nil
}

// PostToCommunity creates a community post (limited support in Data API v3)
// Note: Official Data API v3 doesn't support Community Posts directly yet.
// This is a placeholder for when the API or a web-automation fallback is used.
func (vm *VideoManager) PostToCommunity(ctx context.Context, channelID string, post CommunityPost) error {
	log.Printf("[YT] Preparing Community Post for channel %s: %s", channelID, post.Text)
	// Currently, YouTube requires the YouTube Social API or Partner API for community posts.
	return fmt.Errorf("community posts are currently restricted by YouTube API v3")
}

// GetStreamKey retrieves the stream key for a channel's active/upcoming live broadcast
func (vm *VideoManager) GetStreamKey(ctx context.Context, channelID string) (string, error) {
	service, err := vm.service.GetYouTubeService(ctx, channelID)
	if err != nil {
		return "", err
	}

	// List active live streams
	call := service.LiveStreams.List([]string{"cdn"}).Mine(true)
	resp, err := call.Do()
	if err != nil {
		return "", fmt.Errorf("failed to list live streams: %w", err)
	}

	if len(resp.Items) == 0 {
		return "", fmt.Errorf("no active live streams found")
	}

	// Return the primary ingest stream key
	return resp.Items[0].Cdn.IngestionInfo.StreamName, nil
}
