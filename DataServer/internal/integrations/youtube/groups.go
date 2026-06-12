package youtube

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

// loadGroups loads channel groups from groups.json
func (s *Service) loadGroups() {
	var data []byte
	var err error
	paths := []string{
		filepath.Join(s.config.YoutubePostingPath, "Modules", "groups.json"),
	}
	if s.config.DataDir != "" {
		paths = append([]string{filepath.Join(s.config.DataDir, "youtube", "groups.json")}, paths...)
	}
	for _, groupsPath := range paths {
		data, err = os.ReadFile(groupsPath)
		if err == nil {
			break
		}
	}
	if err != nil || data == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var groupsArray []ChannelGroup
	if err := json.Unmarshal(data, &groupsArray); err == nil && len(groupsArray) > 0 {
		for i := range groupsArray {
			g := &groupsArray[i]
			if g.Name != "" {
				s.groups[g.Name] = g
			}
		}
		log.Printf("[OK] Loaded %d YouTube groups from array", len(s.groups))
		return
	}

	var groupsData map[string]ChannelGroup
	if err := json.Unmarshal(data, &groupsData); err != nil {
		return
	}
	for name, group := range groupsData {
		groupCopy := group
		s.groups[name] = &groupCopy
	}
	log.Printf("[OK] Loaded %d YouTube groups", len(s.groups))
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

// saveGroups saves groups to groups.json
func (s *Service) saveGroups() {
	var groupsPath string
	if s.config.DataDir != "" {
		groupsPath = filepath.Join(s.config.DataDir, "youtube", "groups.json")
	} else {
		groupsPath = filepath.Join(s.config.YoutubePostingPath, "Modules", "groups.json")
	}

	groupsArray := make([]ChannelGroup, 0, len(s.groups))
	for _, g := range s.groups {
		groupsArray = append(groupsArray, *g)
	}

	data, err := json.MarshalIndent(groupsArray, "", "  ")
	if err != nil {
		log.Printf("[WARN] Failed to marshal groups: %v", err)
		return
	}

	if err := os.WriteFile(groupsPath, data, 0644); err != nil {
		log.Printf("[WARN] Failed to save groups: %v", err)
		return
	}

	log.Printf("[OK] Groups saved to %s", groupsPath)
}
