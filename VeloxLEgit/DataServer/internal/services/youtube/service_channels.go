package youtube

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"velox-server/internal/integrations/youtube"
)

var youtubeChannelIDRegex = regexp.MustCompile(`^UC[\w-]{21,22}$`)

// isVideoURL checks if a URL is a YouTube video URL.
func isVideoURL(url string) bool {
	url = strings.ToLower(url)
	return strings.Contains(url, "watch?v=") ||
		strings.Contains(url, "youtu.be/") ||
		strings.Contains(url, "/shorts/") ||
		strings.Contains(url, "/embed/") ||
		strings.Contains(url, "/live/")
}

// AddChannelToGroup adds a channel to a group, resolving metadata and extracting keywords.
//
// PR-YT-REPO: s.storage.GetGroup → s.ytService.GetGroup (returns *ChannelGroup,
// nil when absent); s.storage.AddChannel → s.ytService.AddChannel.
// All other behavior unchanged.
func (s *Service) AddChannelToGroup(ctx context.Context, groupName string, req youtube.AddChannelRequest) (youtube.Channel, error) {
	url := strings.TrimSpace(req.URL)
	if url == "" {
		return youtube.Channel{}, fmt.Errorf("URL cannot be empty")
	}

	if s.ytService.GetGroup(groupName) == nil {
		return youtube.Channel{}, fmt.Errorf("group not found")
	}

	if isVideoURL(url) {
		return youtube.Channel{}, fmt.Errorf("invalid Channel URL. Please provide a channel URL or handle (@name)")
	}

	channelInfo, err := s.apiClient.GetChannelInfo(ctx, url)
	if err != nil {
		channelInfo = &youtube.ChannelInfo{URL: url, Title: req.Title, Thumbnail: req.Thumbnail}
	}

	channelTitle := req.Title
	channelThumbnail := req.Thumbnail
	resolvedURL := url

	if channelInfo != nil && channelInfo.URL != "" {
		resolvedURL = channelInfo.URL
		if channelInfo.Title != "" {
			channelTitle = channelInfo.Title
		}
		if channelInfo.Thumbnail != "" {
			channelThumbnail = channelInfo.Thumbnail
		}
	}

	if isVideoURL(resolvedURL) {
		return youtube.Channel{}, fmt.Errorf("invalid Channel URL resolved. Please provide a channel URL or handle (@name)")
	}

	var keywords []string
	if channelTitle != "" {
		keywords = extractKeywords(channelTitle)
	}
	if channelInfo != nil && channelInfo.Description != "" {
		keywords = append(keywords, extractKeywords(channelInfo.Description)...)
		seen := make(map[string]bool)
		var unique []string
		for _, k := range keywords {
			if !seen[k] && len(unique) < 10 {
				seen[k] = true
				unique = append(unique, k)
			}
		}
		keywords = unique
	}

	channelID := req.ChannelID
	if channelID == "" && channelInfo != nil && channelInfo.ChannelID != "" {
		channelID = channelInfo.ChannelID
	}
	if channelID == "" {
		channelID = strconv.FormatInt(time.Now().UnixMilli(), 10)
	}

	channel := youtube.Channel{
		ID:        channelID,
		URL:       resolvedURL,
		Title:     channelTitle,
		Thumbnail: channelThumbnail,
		Notes:     req.Notes,
		AddedAt:   time.Now(),
		Keywords:  keywords,
	}

	if err := s.ytService.AddChannel(groupName, channel); err != nil {
		return youtube.Channel{}, err
	}

	s.feedCache.Clear()
	return channel, nil
}

// RemoveChannelFromGroup removes a channel from a group.
//
// PR-YT-REPO: s.storage.RemoveChannel → s.ytService.RemoveChannel.
func (s *Service) RemoveChannelFromGroup(groupName string, channelID string) error {
	if err := s.ytService.RemoveChannel(groupName, channelID); err != nil {
		return err
	}
	s.feedCache.Clear()
	return nil
}

// MoveChannel moves a channel between two groups.
//
// PR-YT-REPO: s.storage.MoveChannel → s.ytService.MoveChannel.
func (s *Service) MoveChannel(sourceGroup string, channelID string, targetGroup string) error {
	if err := s.ytService.MoveChannel(sourceGroup, channelID, targetGroup); err != nil {
		return err
	}
	return nil
}

// RefreshChannelStats updates stats for a channel in a group.
//
// PR-YT-REPO: GetGroup returns *ChannelGroup whose `Channels` is now
// `[]string` (channel IDs) — we only need a presence check here. The
// post-write `updatedGroup.Channels` slice is similarly ID-only; the
// returned *Channel is hydrated from SQLite (Membership) so callers
// get the canonical Title/Thumbnail.
func (s *Service) RefreshChannelStats(ctx context.Context, groupName string, channelID string) (youtube.Channel, error) {
	group := s.ytService.GetGroup(groupName)
	if group == nil {
		return youtube.Channel{}, fmt.Errorf("group not found")
	}

	found := false
	for _, chID := range group.Channels {
		if chID == channelID {
			found = true
			break
		}
	}
	if !found {
		return youtube.Channel{}, fmt.Errorf("channel not found")
	}

	validation, err := s.ValidateOAuthAccessToken(ctx, channelID)
	if err != nil {
		return youtube.Channel{}, fmt.Errorf("failed to fetch channel stats: %w", err)
	}

	var viewCount, subCount int64
	if vc, ok := validation["view_count"].(int64); ok {
		viewCount = vc
	} else if vc, ok := validation["view_count"].(float64); ok {
		viewCount = int64(vc)
	}
	if sc, ok := validation["subscriber_count"].(int64); ok {
		subCount = sc
	} else if sc, ok := validation["subscriber_count"].(float64); ok {
		subCount = int64(sc)
	}

	if err := s.ytService.UpdateChannelStats(groupName, channelID, viewCount, subCount); err != nil {
		return youtube.Channel{}, err
	}

	updatedGroup := s.ytService.GetGroup(groupName)
	if updatedGroup != nil {
		for _, chID := range updatedGroup.Channels {
			if chID == channelID {
				// Hydrate the canonical row instead of returning the
				// legacy in-memory Channel slice (which no longer exists).
				if full, mErr := s.ytService.Membership(channelID); mErr == nil && full != nil {
					return *full, nil
				}
				return youtube.Channel{ID: channelID}, nil
			}
		}
	}

	return youtube.Channel{ID: channelID}, nil
}

// MoveChannelToGroupResult holds the result of MoveChannelToGroup operation.
type MoveChannelToGroupResult struct {
	ChannelID   string      `json:"channel_id"`
	SourceGroup *string     `json:"source_group"`
	TargetGroup string      `json:"target_group"`
	Channel     interface{} `json:"channel,omitempty"`
	Message     string      `json:"message"`
}

// MoveChannelToGroup handles drag & drop move of a channel to a target group.
//
// PR-YT-REPO: every storage call migrates to s.ytService.*. The
// previous `s.storage.CreateGroup(targetGroup, "manager")` two-arg
// overload is gone — s.ytService.CreateGroup hardcodes "upload" so
// `manager` groups fall back to the Repository's UpsertYouTubeGroup
// so the original semantic is preserved.
func (s *Service) MoveChannelToGroup(ctx context.Context, channelID string, targetGroup string) (*MoveChannelToGroupResult, error) {
	var sourceGroup string
	groups := s.ytService.GetGroups()
	for groupName, group := range groups {
		if group == nil {
			continue
		}
		// GetGroups() returns map of *ChannelGroup whose Channels is
		// `[]string` (channel IDs) — drop the `.ID` accessor that was
		// type-correct under the legacy Storage.data.Groups shape.
		for _, chID := range group.Channels {
			if chID == channelID {
				sourceGroup = groupName
				break
			}
		}
		if sourceGroup != "" {
			break
		}
	}

	if sourceGroup == "" {
		if !youtubeChannelIDRegex.MatchString(channelID) {
			return nil, fmt.Errorf("channel not found in any group and invalid channel ID format")
		}

		if s.ytService.GetGroup(targetGroup) == nil {
			if _, err := s.ytService.Repo().UpsertYouTubeGroup(targetGroup, "manager", "", ""); err != nil {
				return nil, fmt.Errorf("failed to create target group: %w", err)
			}
		}

		channelURL := "https://www.youtube.com/channel/" + channelID
		channelTitle := ""
		channelThumbnail := ""
		if info, err := s.apiClient.GetChannelInfo(ctx, channelURL); err == nil && info != nil {
			if info.Title != "" {
				channelTitle = info.Title
			}
			if info.Thumbnail != "" {
				channelThumbnail = info.Thumbnail
			}
		}
		if channelTitle == "" {
			channelTitle = channelID
		}

		ch := youtube.Channel{
			ID:        channelID,
			URL:       channelURL,
			Title:     channelTitle,
			Thumbnail: channelThumbnail,
			AddedAt:   time.Now(),
			Notes:     "Added via drag & drop / bulk move",
		}

		if err := s.ytService.AddChannel(targetGroup, ch); err != nil {
			return nil, err
		}

		s.feedCache.Clear()

		return &MoveChannelToGroupResult{
			ChannelID:   channelID,
			SourceGroup: nil,
			TargetGroup: targetGroup,
			Channel:     ch,
			Message:     "Channel added to group",
		}, nil
	}

	if sourceGroup == targetGroup {
		s.feedCache.Clear()
		return &MoveChannelToGroupResult{
			ChannelID:   channelID,
			SourceGroup: &sourceGroup,
			TargetGroup: targetGroup,
			Message:     "Channel already in target group",
		}, nil
	}

	if s.ytService.GetGroup(targetGroup) == nil {
		if _, err := s.ytService.Repo().UpsertYouTubeGroup(targetGroup, "manager", "", ""); err != nil {
			return nil, fmt.Errorf("failed to create target group: %w", err)
		}
	}

	if err := s.ytService.MoveChannel(sourceGroup, channelID, targetGroup); err != nil {
		return nil, err
	}

	s.feedCache.Clear()

	return &MoveChannelToGroupResult{
		ChannelID:   channelID,
		SourceGroup: &sourceGroup,
		TargetGroup: targetGroup,
		Message:     "Channel moved successfully",
	}, nil
}
