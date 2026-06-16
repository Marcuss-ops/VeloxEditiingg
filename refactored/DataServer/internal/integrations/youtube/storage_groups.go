package youtube

import "time"

// ListGroups returns all groups
func (s *Storage) ListGroups() (map[string]*Group, []string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	groups := make(map[string]*Group)
	for k, v := range s.data.Groups {
		group := *v
		group.Channels = make([]Channel, len(v.Channels))
		copy(group.Channels, v.Channels)
		groups[k] = &group
	}

	return groups, s.data.TrackedNiches
}

// GetGroup returns a specific group
func (s *Storage) GetGroup(name string) (*Group, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	group, ok := s.data.Groups[name]
	if !ok {
		return nil, false
	}

	g := *group
	g.Channels = make([]Channel, len(group.Channels))
	copy(g.Channels, group.Channels)
	return &g, true
}

// CreateGroup creates a new group with the specified type
func (s *Storage) CreateGroup(name string, groupType string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.data.Groups[name]; exists {
		return ErrGroupExists
	}

	s.data.Groups[name] = &Group{
		Name:      name,
		CreatedAt: time.Now(),
		Channels:  []Channel{},
		GroupType: groupType,
	}

	return s.save()
}

// DeleteGroup removes a group
func (s *Storage) DeleteGroup(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.data.Groups[name]; !exists {
		return ErrGroupNotFound
	}

	delete(s.data.Groups, name)
	return s.save()
}

// CleanupOldData removes cached channel metadata older than the retention period.
func (s *Storage) CleanupOldData(retention time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	removedCount := 0

	for _, group := range s.data.Groups {
		for i := range group.Channels {
			ch := &group.Channels[i]
			if !ch.LastSync.IsZero() && now.Sub(ch.LastSync) > retention {
				ch.Title = ""
				ch.Thumbnail = ""
				ch.ViewCount = 0
				ch.SubCount = 0
				ch.Keywords = nil
				removedCount++
			}
		}
	}

	if removedCount > 0 {
		s.save()
	}
	return removedCount
}
