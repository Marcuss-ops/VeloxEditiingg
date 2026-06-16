package youtube

import (
	"time"
)

// AddChannel adds a channel to a group. Persists only the affected group via
// SyncGroup (non-destructive per-group diff) — does NOT rewrite other groups.
func (s *Storage) AddChannel(groupName string, channel Channel) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.store == nil {
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
		return nil
	}

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
	return s.syncGroupLocked(groupName, group)
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

// MoveChannel moves a channel from one group to another. Persists only the
// affected source and target groups via SyncGroup (non-destructive diff).
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
			if err := s.syncGroupLocked(sourceGroup, source); err != nil {
				source.Channels = originalSource
				return err
			}
			return nil
		}
	}

	// Apply the move in memory first; then persist target, then source.
	source.Channels = append(source.Channels[:channelIdx], source.Channels[channelIdx+1:]...)
	target.Channels = append(target.Channels, channel)

	if s.store == nil {
		return nil
	}

	// Persist target first.
	if err := s.syncGroupLocked(targetGroup, target); err != nil {
		source.Channels = originalSource
		target.Channels = originalTarget
		return err
	}

	// Persist source removal.
	if err := s.syncGroupLocked(sourceGroup, source); err != nil {
		// DB-target succeeded; undo the membership in DB before restoring
		// in-memory state so SQLite and memory stay aligned. The type passed
		// to GetYouTubeGroupV2ID must match UpsertYouTubeGroupV2's
		// normalisation: an empty GroupType becomes "manager" on insert,
		// so the rollback lookup has to use the same value or the row
		// won't be found.
		targetType := target.GroupType
		if targetType == "" {
			targetType = "manager"
		}
		if targetID, terr := s.store.GetYouTubeGroupV2ID(targetGroup, targetType); terr == nil && targetID > 0 {
			_ = s.store.RemoveChannelFromGroupV2(targetID, channel.ID)
		}
		source.Channels = originalSource
		target.Channels = originalTarget
		return err
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
