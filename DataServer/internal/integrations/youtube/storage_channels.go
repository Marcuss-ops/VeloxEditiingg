package youtube

import (
	"fmt"
	"time"
)

// AddChannel adds a channel to a group. Persists via direct store calls
// (UpsertYouTubeGroupV2 + UpsertYouTubeChannel + AddChannelToGroupV2)
// instead of the full-diff syncGroupLocked — avoids the per-channel upserts
// and membership-list round-trip that syncGroupLocked would pay for a
// single-channel add.
func (s *Storage) AddChannel(groupName string, channel Channel) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, ok := s.data.Groups[groupName]
	if !ok {
		return ErrGroupNotFound
	}
	for _, ch := range group.Channels {
		if ch.URL == channel.URL {
			return ErrChannelExists
		}
	}

	group.Channels = append(group.Channels, channel)

	if s.store == nil {
		return nil
	}

	groupType := group.GroupType
	if groupType == "" {
		groupType = "manager"
	}

	groupID, err := s.store.UpsertYouTubeGroupV2(groupName, groupType, "", "")
	if err != nil {
		return fmt.Errorf("add channel upsert group %q: %w", groupName, err)
	}

	addedAt := ""
	if !channel.AddedAt.IsZero() {
		addedAt = channel.AddedAt.Format(time.RFC3339)
	}
	lastSync := ""
	if !channel.LastSync.IsZero() {
		lastSync = channel.LastSync.Format(time.RFC3339)
	}
	if err := s.store.UpsertYouTubeChannel(
		channel.ID, channel.Title, channel.Name, channel.URL, channel.Thumbnail,
		channel.Language, channel.Notes,
		channel.ViewCount, channel.SubCount,
		addedAt, lastSync, "",
	); err != nil {
		return fmt.Errorf("add channel upsert channel %s: %w", safeChannelID(channel.ID), err)
	}

	return s.store.AddChannelToGroupV2(groupID, channel.ID)
}

// RemoveChannel removes a channel from a group. Persists only the affected
// group via SyncGroup (non-destructive per-group diff).
func (s *Storage) RemoveChannel(groupName, channelID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, ok := s.data.Groups[groupName]
	if !ok {
		return ErrGroupNotFound
	}

	for i, ch := range group.Channels {
		if ch.ID == channelID {
			group.Channels = append(group.Channels[:i], group.Channels[i+1:]...)
			if s.store == nil {
				return nil
			}
			return s.syncGroupLocked(groupName, group)
		}
	}
	return ErrChannelNotFound
}

// MoveChannel moves a channel from one group to another. Uses direct store
// calls (UpsertYouTubeGroupV2 + AddChannelToGroupV2 / RemoveChannelFromGroupV2)
// instead of the full-diff syncGroupLocked — avoids per-channel upserts and
// membership-list round-trips that syncGroupLocked would pay for a single
// membership change.
//
// Atomicity contract:
//   - Snapshots both source and target channel slices BEFORE any mutation.
//   - Persists the target group FIRST so the channel is at-rest in its new
//     home, then removes the channel from the source group.
//   - On any DB error during the target phase, both in-memory slices are
//     restored from the pre-move snapshot.
//   - On DB error during the source phase, the channel membership that was
//     just added to the target in the DB is explicitly removed before the
//     in-memory slices are restored, so memory and DB stay coherent.
func (s *Storage) MoveChannel(sourceGroup, channelID, targetGroup string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	source, ok := s.data.Groups[sourceGroup]
	if !ok {
		return ErrGroupNotFound
	}
	target, ok := s.data.Groups[targetGroup]
	if !ok {
		return ErrTargetGroupNotFound
	}

	channelIdx := -1
	var channel Channel
	for i, ch := range source.Channels {
		if ch.ID == channelID {
			channel = ch
			channelIdx = i
			break
		}
	}
	if channelIdx == -1 {
		return ErrChannelNotFound
	}

	// Snapshot pre-move state for safe rollback. We allocate FRESH slices so
	// that restoring them later cannot accidentally alias already-mutated state.
	originalSource := append([]Channel{}, source.Channels...)
	originalTarget := append([]Channel{}, target.Channels...)

	// Collision path: target already has a channel with the same URL. Treat
	// as a drop-from-source only — no channel goes to target.
	for _, ch := range target.Channels {
		if ch.URL == channel.URL {
			source.Channels = append(source.Channels[:channelIdx], source.Channels[channelIdx+1:]...)
			if s.store == nil {
				return nil
			}
			return s.removeChannelFromGroup(sourceGroup, source, channel.ID, originalSource)
		}
	}

	// Apply the move in memory first; then persist target, then source.
	source.Channels = append(source.Channels[:channelIdx], source.Channels[channelIdx+1:]...)
	target.Channels = append(target.Channels, channel)

	if s.store == nil {
		return nil
	}

	// Persist target first.
	targetType := target.GroupType
	if targetType == "" {
		targetType = "manager"
	}
	targetID, err := s.store.UpsertYouTubeGroupV2(targetGroup, targetType, "", "")
	if err != nil {
		source.Channels = originalSource
		target.Channels = originalTarget
		return fmt.Errorf("move channel upsert target group %q: %w", targetGroup, err)
	}
	if err := s.store.AddChannelToGroupV2(targetID, channel.ID); err != nil {
		source.Channels = originalSource
		target.Channels = originalTarget
		return fmt.Errorf("move channel add membership %s to target %q: %w", safeChannelID(channel.ID), targetGroup, err)
	}

	// Persist source removal.
	sourceType := source.GroupType
	if sourceType == "" {
		sourceType = "manager"
	}
	sourceID, err := s.store.UpsertYouTubeGroupV2(sourceGroup, sourceType, "", "")
	if err != nil {
		// DB-target succeeded; undo the membership in DB before restoring
		// in-memory state.
		_ = s.store.RemoveChannelFromGroupV2(targetID, channel.ID)
		source.Channels = originalSource
		target.Channels = originalTarget
		return fmt.Errorf("move channel upsert source group %q: %w", sourceGroup, err)
	}
	if err := s.store.RemoveChannelFromGroupV2(sourceID, channel.ID); err != nil {
		// DB-target succeeded; undo the membership in DB.
		_ = s.store.RemoveChannelFromGroupV2(targetID, channel.ID)
		source.Channels = originalSource
		target.Channels = originalTarget
		return fmt.Errorf("move channel remove membership %s from source %q: %w", safeChannelID(channel.ID), sourceGroup, err)
	}
	return nil
}

// removeChannelFromGroup removes a channel from a group's DB membership.
// Helper extracted from MoveChannel's collision path.
func (s *Storage) removeChannelFromGroup(groupName string, group *Group, channelID string, original []Channel) error {
	groupType := group.GroupType
	if groupType == "" {
		groupType = "manager"
	}
	sourceID, err := s.store.UpsertYouTubeGroupV2(groupName, groupType, "", "")
	if err != nil {
		group.Channels = original
		return fmt.Errorf("remove channel upsert group %q: %w", groupName, err)
	}
	if err := s.store.RemoveChannelFromGroupV2(sourceID, channelID); err != nil {
		group.Channels = original
		return fmt.Errorf("remove channel remove membership %s from %q: %w", safeChannelID(channelID), groupName, err)
	}
	return nil
}

// UpdateChannelLanguage updates the language for a channel in a group.
// Persists only the affected group via SyncGroup (non-destructive diff).
func (s *Storage) UpdateChannelLanguage(groupName, channelID, language string) (*Channel, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, ok := s.data.Groups[groupName]
	if !ok {
		return nil, ErrGroupNotFound
	}

	for i, ch := range group.Channels {
		if ch.ID == channelID {
			group.Channels[i].Language = language
			if s.store != nil {
				if err := s.syncGroupLocked(groupName, group); err != nil {
					return nil, err
				}
			}
			result := group.Channels[i]
			return &result, nil
		}
	}
	return nil, ErrChannelNotFound
}

// UpdateChannelMetadata updates Title, Name, and Thumbnail for a channel in a
// group. Persists only the affected group via SyncGroup (non-destructive diff).
// metadata_json was retired in S7/S8 of the verdict plan and is no longer
// part of the channel upsert contract.
func (s *Storage) UpdateChannelMetadata(groupName, channelID, title, name, thumbnail string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, ok := s.data.Groups[groupName]
	if !ok {
		return ErrGroupNotFound
	}

	for i, ch := range group.Channels {
		if ch.ID == channelID {
			if title != "" {
				group.Channels[i].Title = title
			}
			if name != "" {
				group.Channels[i].Name = name
			}
			if thumbnail != "" {
				group.Channels[i].Thumbnail = thumbnail
			}
			if s.store == nil {
				return nil
			}
			return s.syncGroupLocked(groupName, group)
		}
	}
	return ErrChannelNotFound
}

// UpdateChannelStats updates the stats for a channel in a group. Persists
// only the affected group via SyncGroup (non-destructive diff).
func (s *Storage) UpdateChannelStats(groupName, channelID string, viewCount, subCount int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, ok := s.data.Groups[groupName]
	if !ok {
		return ErrGroupNotFound
	}

	for i, ch := range group.Channels {
		if ch.ID == channelID {
			group.Channels[i].ViewCount = viewCount
			group.Channels[i].SubCount = subCount
			group.Channels[i].LastSync = time.Now()
			if s.store == nil {
				return nil
			}
			return s.syncGroupLocked(groupName, group)
		}
	}
	return ErrChannelNotFound
}

// GetAllChannels returns all channels across all groups
func (s *Storage) GetAllChannels() []Channel {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var channels []Channel
	for _, group := range s.data.Groups {
		channels = append(channels, group.Channels...)
	}
	return channels
}

// GetGroupChannels returns channel IDs for a specific group
func (s *Storage) GetGroupChannels(groupName string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	group, ok := s.data.Groups[groupName]
	if !ok {
		return nil, ErrGroupNotFound
	}

	var urls []string
	for _, ch := range group.Channels {
		urls = append(urls, ch.URL)
	}
	return urls, nil
}
