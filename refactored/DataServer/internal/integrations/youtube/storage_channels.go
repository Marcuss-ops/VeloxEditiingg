package youtube

import "time"

// AddChannel adds a channel to a group
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
	return s.save()
}

// RemoveChannel removes a channel from a group
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
			return s.save()
		}
	}

	return ErrChannelNotFound
}

// MoveChannel moves a channel from one group to another
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

	var channel *Channel
	var channelIdx = -1
	for i, ch := range source.Channels {
		if ch.ID == channelID {
			channelCopy := ch
			channel = &channelCopy
			channelIdx = i
			break
		}
	}

	if channel == nil {
		return ErrChannelNotFound
	}

	for _, ch := range target.Channels {
		if ch.URL == channel.URL {
			source.Channels = append(source.Channels[:channelIdx], source.Channels[channelIdx+1:]...)
			return s.save()
		}
	}

	target.Channels = append(target.Channels, *channel)
	source.Channels = append(source.Channels[:channelIdx], source.Channels[channelIdx+1:]...)

	return s.save()
}

// UpdateChannelLanguage updates the language for a channel in a group.
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
			if err := s.save(); err != nil {
				return nil, err
			}
			result := group.Channels[i]
			return &result, nil
		}
	}

	return nil, ErrChannelNotFound
}

// UpdateChannelMetadata updates Title, Name, and Thumbnail for a channel in a group.
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
			return s.save()
		}
	}

	return ErrChannelNotFound
}

// UpdateChannelStats updates the stats for a channel
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
			return s.save()
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
