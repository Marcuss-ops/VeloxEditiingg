package youtube

import (
	"fmt"
	"log"
	"strings"
	"time"
)

// loadGroups loads channel groups from SQLite.
func (s *Service) loadGroups() {
	if s.store != nil {
		s.loadGroupsFromSQLite()
	}
}

// loadGroupsFromSQLite loads groups from the SQLite store.
func (s *Service) loadGroupsFromSQLite() bool {
	rows, err := s.store.ListYouTubeGroups()
	if err != nil || len(rows) == 0 {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, row := range rows {
		name, _ := row["name"].(string)
		desc, _ := row["description"].(string)
		privacy, _ := row["privacy"].(string)
		channelsRaw, _ := row["channels"].([]string)
		s.groups[name] = &ChannelGroup{
			Name:        name,
			Description: desc,
			Privacy:     privacy,
			Channels:    channelsRaw,
		}
	}
	log.Printf("[OK] Loaded %d YouTube groups from SQLite", len(s.groups))
	return true
}



// CreateGroup creates a new channel group and persists it
func (s *Service) CreateGroup(name, description string, channelIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.groups[name]; exists {
		return fmt.Errorf("group '%s' already exists", name)
	}

	s.groups[name] = &ChannelGroup{
		Name:        name,
		Description: description,
		Channels:    channelIDs,
	}

	s.saveGroups()
	return nil
}

// DeleteGroup deletes a channel group and persists the change
func (s *Service) DeleteGroup(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.groups[name]; !exists {
		return fmt.Errorf("group '%s' not found", name)
	}

	delete(s.groups, name)
	s.saveGroups()
	return nil
}

// AddChannelToGroup adds a channel to a group and persists the change
func (s *Service) AddChannelToGroup(groupName, channelID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupName]
	if !exists {
		return fmt.Errorf("group '%s' not found", groupName)
	}

	for _, chID := range group.Channels {
		if chID == channelID {
			return fmt.Errorf("channel '%s' already in group '%s'", channelID, groupName)
		}
	}

	group.Channels = append(group.Channels, channelID)
	s.saveGroups()
	return nil
}

// RemoveChannelFromGroup removes a channel from a group and persists the change
func (s *Service) RemoveChannelFromGroup(groupName, channelID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupName]
	if !exists {
		return fmt.Errorf("group '%s' not found", groupName)
	}

	for i, chID := range group.Channels {
		if chID == channelID {
			group.Channels = append(group.Channels[:i], group.Channels[i+1:]...)
			s.saveGroups()
			return nil
		}
	}

	return fmt.Errorf("channel '%s' not found in group '%s'", channelID, groupName)
}

// GetGroups returns all channel groups
func (s *Service) GetGroups() map[string]*ChannelGroup {
	s.mu.RLock()
	defer s.mu.RUnlock()

	groups := make(map[string]*ChannelGroup, len(s.groups))
	for name, group := range s.groups {
		groupCopy := *group
		groups[name] = &groupCopy
	}
	return groups
}

// GetGroup returns a specific group by name
func (s *Service) GetGroup(name string) *ChannelGroup {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.groups[name]
}

// ResolveChannelByLanguage finds a channel in a group whose Language field matches
// the requested language code. Returns the first matching AuthChannel or an error
// if the group doesn't exist, no channels are found, or no channel has the requested language.
func (s *Service) ResolveChannelByLanguage(groupName, language string) (*AuthChannel, error) {
	if groupName == "" {
		return nil, fmt.Errorf("group name is required")
	}
	if language == "" {
		return nil, fmt.Errorf("language code is required")
	}

	group := s.GetGroup(groupName)
	if group == nil {
		return nil, fmt.Errorf("group '%s' not found", groupName)
	}

	if len(group.Channels) == 0 {
		return nil, fmt.Errorf("group '%s' has no channels", groupName)
	}

	lang := strings.TrimSpace(strings.ToLower(language))

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Phase 1: exact language match
	for _, chID := range group.Channels {
		if ch, exists := s.channels[chID]; exists {
			if strings.EqualFold(strings.TrimSpace(ch.Language), lang) {
				chCopy := *ch
				return &chCopy, nil
			}
		}
	}

	// Phase 2: fallback to first channel that has no language set (unconfigured)
	for _, chID := range group.Channels {
		if ch, exists := s.channels[chID]; exists {
			if strings.TrimSpace(ch.Language) == "" {
				chCopy := *ch
				return &chCopy, nil
			}
		}
	}

	// Phase 3: fallback to first channel in group (any channel)
	for _, chID := range group.Channels {
		if ch, exists := s.channels[chID]; exists {
			chCopy := *ch
			return &chCopy, nil
		}
	}

	return nil, fmt.Errorf("no channel found in group '%s' matching language '%s'", groupName, language)
}

// AuthChannelToChannel converts an AuthChannel to a public Channel
func AuthChannelToChannel(ac *AuthChannel) *Channel {
	if ac == nil {
		return nil
	}
	return &Channel{
		ID:        ac.ID,
		Title:     ac.Title,
		Thumbnail: ac.Thumbnail,
		Notes:     ac.Name,
		Language:  ac.Language,
	}
}

// ChannelGroupToGroup converts a ChannelGroup to a public Group with full channel details
func (s *Service) ChannelGroupToGroup(cg *ChannelGroup) *Group {
	if cg == nil {
		return nil
	}
	group := &Group{
		Name:      cg.Name,
		CreatedAt: time.Now(),
		Channels:  make([]Channel, 0, len(cg.Channels)),
	}
	s.mu.RLock()
	for _, chID := range cg.Channels {
		if ac, exists := s.channels[chID]; exists {
			group.Channels = append(group.Channels, *AuthChannelToChannel(ac))
		} else {
			group.Channels = append(group.Channels, Channel{ID: chID})
		}
	}
	s.mu.RUnlock()
	return group
}

// GetGroupsWithChannels returns groups with full channel details
func (s *Service) GetGroupsWithChannels() []map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]map[string]interface{}, 0, len(s.groups))

	for _, g := range s.groups {
		groupData := map[string]interface{}{
			"name":        g.Name,
			"description": g.Description,
			"privacy":     g.Privacy,
			"channels":    make([]map[string]interface{}, 0, len(g.Channels)),
			"count":       len(g.Channels),
		}

		for _, chID := range g.Channels {
			if ch, exists := s.channels[chID]; exists {
				groupData["channels"] = append(groupData["channels"].([]map[string]interface{}), map[string]interface{}{
					"id":        ch.ID,
					"title":     ch.Title,
					"name":      ch.Name,
					"thumbnail": ch.Thumbnail,
					"language":  ch.Language,
				})
			} else {
				groupData["channels"] = append(groupData["channels"].([]map[string]interface{}), map[string]interface{}{
					"id":    chID,
					"title": "Unknown",
					"name":  chID,
				})
			}
		}

		result = append(result, groupData)
	}

	return result
}

// GetUndefinedChannels returns channels not assigned to any group
func (s *Service) GetUndefinedChannels() []*Channel {
	s.mu.RLock()
	defer s.mu.RUnlock()

	assigned := make(map[string]bool)
	for _, cg := range s.groups {
		for _, chID := range cg.Channels {
			assigned[chID] = true
		}
	}

	var undefined []*Channel
	for id, ac := range s.channels {
		if !assigned[id] {
			undefined = append(undefined, AuthChannelToChannel(ac))
		}
	}

	return undefined
}

// saveGroups saves groups to SQLite.
func (s *Service) saveGroups() {
	if s.store != nil {
		for _, g := range s.groups {
			s.store.UpsertYouTubeGroup(g.Name, g.Description, g.Privacy, g.Channels, "")
		}
	}
}
