package youtube

import (
	"fmt"
	"log"
	"time"
)

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

// CreateGroup creates a new group with the specified type. Persists only this
// group's row via a direct UpsertYouTubeGroupV2 call — does NOT touch any
// other groups in the DB (avoids the destructive full-state rewrite).
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

	if s.store == nil {
		return nil
	}
	_, err := s.store.UpsertYouTubeGroupV2(name, groupType, "", "")
	if err != nil {
		return fmt.Errorf("create group %q: %w", name, err)
	}
	return nil
}

// DeleteGroup removes a group. Persists only via direct SQL on this single
// group's row and membership rows — does NOT touch any other groups.
func (s *Storage) DeleteGroup(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.data.Groups[name]
	if !exists {
		return ErrGroupNotFound
	}

	delete(s.data.Groups, name)
	if s.store == nil {
		return nil
	}

	groupType := group.GroupType
	if groupType == "" {
		groupType = "manager"
	}

	groupID, err := s.store.GetYouTubeGroupV2ID(name, groupType)
	if err != nil {
		return err
	}
	if groupID > 0 {
		if err := s.store.DeleteYouTubeGroupChannelsByGroupID(groupID); err != nil {
			return err
		}
		if err := s.store.DeleteYouTubeGroupV2(groupID); err != nil {
			return err
		}
	}
	return nil
}

// CleanupOldData removes cached channel metadata older than the retention period.
// Persists every affected group individually via SyncGroup (non-destructive
// per-group diff) so untouched groups are left alone. Previously this called
// the global save() which destructively rewrote every group.
func (s *Storage) CleanupOldData(retention time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	removedCount := 0

	for name, group := range s.data.Groups {
		if group == nil {
			continue
		}
		groupDirty := false
		for i := range group.Channels {
			ch := &group.Channels[i]
			if !ch.LastSync.IsZero() && now.Sub(ch.LastSync) > retention {
				ch.Title = ""
				ch.Thumbnail = ""
				ch.ViewCount = 0
				ch.SubCount = 0
				ch.Keywords = nil
				removedCount++
				groupDirty = true
			}
		}
		if groupDirty && s.store != nil {
			if err := s.syncGroupLocked(name, group); err != nil {
				log.Printf("[WARN] CleanupOldData: per-group sync failed for %q: %v", name, err)
			}
		}
	}
	return removedCount
}
