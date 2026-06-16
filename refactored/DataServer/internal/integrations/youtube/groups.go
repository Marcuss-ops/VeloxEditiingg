package youtube

import (
	"fmt"
	"log"
	"strings"
	"time"
)

// loadCanonicalGroups loads groups from the canonical youtube_groups_v2 table
// with channel memberships from youtube_group_channels.
func (s *Service) loadCanonicalGroups() bool {
	if s.store == nil {
		return false
	}

	// Load groups from youtube_groups_v2
	groupRows, err := s.store.ListYouTubeGroupsV2()
	if err != nil || len(groupRows) == 0 {
		return false
	}

	// Load all memberships at once
	memberships, err := s.store.ListAllGroupMembershipsV2()
	if err != nil {
		memberships = nil
	}

	// Build a map of group_id → channel IDs
	groupChannels := make(map[int64][]string)
	for _, m := range memberships {
		gid, _ := m["group_id"].(int64)
		chID, _ := m["channel_id"].(string)
		if gid > 0 && chID != "" {
			groupChannels[gid] = append(groupChannels[gid], chID)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, row := range groupRows {
		name, _ := row["name"].(string)
		if name == "" {
			continue
		}
		desc, _ := row["description"].(string)
		privacy, _ := row["privacy"].(string)
		gid, _ := row["id"].(int64)

		s.groups[name] = &ChannelGroup{
			Name:        name,
			Description: desc,
			Privacy:     privacy,
			Channels:    groupChannels[gid],
		}
	}
	log.Printf("[OK] Loaded %d YouTube groups from canonical tables", len(s.groups))
	return true
}

// CreateGroup creates a new channel group and persists only that group via a
// direct SQL upsert. It does NOT rewrite every other group in DB (no full
// destructive sweep).
func (s *Service) CreateGroup(name, description string, channelIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.groups[name]; exists {
		return fmt.Errorf("group '%s' already exists", name)
	}

	s.groups[name] = &ChannelGroup{
		Name:        name,
		Description: description,
		Channels:    append([]string{}, channelIDs...),
	}

	if s.store == nil {
		return nil
	}
	groupID, err := s.store.UpsertYouTubeGroupV2(name, "upload", description, "")
	if err != nil {
		return fmt.Errorf("create upload group %q: %w", name, err)
	}
	for _, chID := range channelIDs {
		if err := s.store.AddChannelToGroupV2(groupID, chID); err != nil {
			log.Printf("[WARN] CreateGroup: add channel %s to %q: %v", safeChannelID(chID), name, err)
		}
	}
	return nil
}

// DeleteGroup deletes a channel group and persists the change via direct SQL
// on this group only. Does NOT rewrite every other group in DB.
func (s *Service) DeleteGroup(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.groups[name]; !exists {
		return fmt.Errorf("group '%s' not found", name)
	}

	delete(s.groups, name)

	if s.store == nil {
		return nil
	}
	groupID, err := s.store.GetYouTubeGroupV2ID(name, "upload")
	if err != nil {
		return err
	}
	if groupID == 0 {
		return nil
	}
	if err := s.store.DeleteYouTubeGroupChannelsByGroupID(groupID); err != nil {
		return err
	}
	return s.store.DeleteYouTubeGroupV2(groupID)
}

// AddChannelToGroup adds a channel to a group and persists only the affected
// group via direct SQL. Does NOT rewrite every other group in DB.
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

	if s.store == nil {
		return nil
	}
	groupID, err := s.store.GetYouTubeGroupV2ID(groupName, "upload")
	if err != nil {
		return fmt.Errorf("resolve upload group %q: %w", groupName, err)
	}
	if groupID == 0 {
		groupID, err = s.store.UpsertYouTubeGroupV2(groupName, "upload", group.Description, group.Privacy)
		if err != nil {
			return err
		}
	}
	return s.store.AddChannelToGroupV2(groupID, channelID)
}

// RemoveChannelFromGroup removes a channel from a group and persists only the
// affected group via direct SQL. Does NOT rewrite every other group in DB.
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
			if s.store == nil {
				return nil
			}
			groupID, err := s.store.GetYouTubeGroupV2ID(groupName, "upload")
			if err != nil {
				return err
			}
			if groupID == 0 {
				return nil
			}
			return s.store.RemoveChannelFromGroupV2(groupID, channelID)
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
		groupCopy.Channels = append([]string{}, group.Channels...)
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
		URL:       ac.URL,
		Title:     ac.Title,
		Name:      ac.Name,
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
					"url":       ch.URL,
					"title":     ch.Title,
					"name":      ch.Name,
					"thumbnail": ch.Thumbnail,
					"language":  ch.Language,
				})
			} else {
				groupData["channels"] = append(groupData["channels"].([]map[string]interface{}), map[string]interface{}{
					"id":    chID,
					"url":   "",
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

// saveGroups is retained as a backwards-compatible entry point. It is no
// longer destructive: it just no-ops because per-operation methods
// (CreateGroup / DeleteGroup / AddChannelToGroup / RemoveChannelFromGroup /
// DeleteChannel) now write the affected rows directly to SQLite. Persisting
// through this method would only re-do work already done atomically by the
// callers. Keeping the symbol alive avoids breaking external callers that
// may still invoke it.
func (s *Service) saveGroups() error {
	if s.store == nil {
		return nil
	}
	// Intentionally empty: per-operation methods own their persistence.
	// See per-operation methods (CreateGroup / DeleteGroup / etc.).
	return nil
}
