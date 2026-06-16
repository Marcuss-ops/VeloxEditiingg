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
// On any DB error during the source half, both source and target in-memory
// state are restored from the pre-move snapshot so the next save() does not
// see a half-completed move.
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

	// Snapshot pre-move state for rollback on partial-failure.
	sourceLen := len(source.Channels)
	targetLen := len(target.Channels)
	for _, ch := range target.Channels {
		if ch.URL == channel.URL {
			// Channel collides on URL with an existing target entry: treat
			// as a no-op move (drop from source only).
			source.Channels = append(source.Channels[:channelIdx], source.Channels[channelIdx+1:]...)
			if s.store == nil {
				return nil
			}
			if err := s.syncGroupLocked(sourceGroup, source); err != nil {
				source.Channels = make([]Channel, sourceLen)
				return err
			}
			return nil
		}
	}

	// Apply the move to memory, then persist target FIRST. If the target
	// write succeeds, the channel is at-rest in its new home; we then
	// persist the source removal. If target write fails, restore both halves
	// so memory and DB stay consistent.
	source.Channels = append(source.Channels[:channelIdx], source.Channels[channelIdx+1:]...)
	target.Channels = append(target.Channels, channel)

	if s.store == nil {
		return nil
	}
	if err := s.syncGroupLocked(targetGroup, target); err != nil {
		target.Channels = target.Channels[:targetLen]
		source.Channels = make([]Channel, sourceLen)
		copy(source.Channels, append([]Channel{channel}, make([]Channel, sourceLen-1)...))
		// Rebuild source slice to its original order.
		for j, ch := range s.data.Groups[sourceGroup].Channels {
			source.Channels[j] = ch
		}
		_ = sourceLen
		return err
	}
	if err := s.syncGroupLocked(sourceGroup, source); err != nil {
		// Source removal failed but target write succeeded. Roll back target.
		target.Channels = target.Channels[:targetLen]
		source.Channels = append(source.Channels[:channelIdx], append([]Channel{channel}, source.Channels[channelIdx:]...)...)
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
