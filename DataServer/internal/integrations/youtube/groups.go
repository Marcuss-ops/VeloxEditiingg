package youtube

import (
	"fmt"
	"log"
	"strings"
	"time"
)

// loadChannelGroup hydrates a single ChannelGroup from the canonical
// tables. Returns nil when the group is absent or the store is unavailable.
func (s *Service) loadChannelGroup(name string) (*ChannelGroup, error) {
	if s.store == nil {
		return nil, nil
	}
	rows, err := s.store.ListYouTubeGroups()
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		n, _ := row["name"].(string)
		if n != name {
			continue
		}
		gid, _ := row["id"].(int64)
		groupType, _ := row["group_type"].(string)
		desc, _ := row["description"].(string)
		privacy, _ := row["privacy"].(string)
		channelIDs, _ := s.store.ListGroupChannels(gid)
		return &ChannelGroup{
			Name:        n,
			Description: desc,
			Privacy:     privacy,
			GroupType:   groupType,
			Channels:    channelIDs,
		}, nil
	}
	return nil, nil
}

// loadChannelGroups hydrates every group from the canonical tables.
func (s *Service) loadChannelGroups() (map[string]*ChannelGroup, error) {
	if s.store == nil {
		return map[string]*ChannelGroup{}, nil
	}
	groupRows, err := s.store.ListYouTubeGroups()
	if err != nil {
		return map[string]*ChannelGroup{}, err
	}
	memberships, err := s.store.ListAllGroupMemberships()
	if err != nil {
		memberships = nil
	}
	groupChannels := make(map[int64][]string)
	for _, m := range memberships {
		gid, _ := m["group_id"].(int64)
		chID, _ := m["channel_id"].(string)
		if gid > 0 && chID != "" {
			groupChannels[gid] = append(groupChannels[gid], chID)
		}
	}
	out := make(map[string]*ChannelGroup, len(groupRows))
	for _, row := range groupRows {
		name, _ := row["name"].(string)
		if name == "" {
			continue
		}
		gid, _ := row["id"].(int64)
		groupType, _ := row["group_type"].(string)
		desc, _ := row["description"].(string)
		privacy, _ := row["privacy"].(string)
		out[name] = &ChannelGroup{
			Name:        name,
			Description: desc,
			Privacy:     privacy,
			GroupType:   groupType,
			Channels:    append([]string{}, groupChannels[gid]...),
		}
	}
	return out, nil
}

// CreateGroup creates a new channel group and persists only that group via a
// direct SQL upsert.
func (s *Service) CreateGroup(name, description string, channelIDs []string) error {
	if s.GetGroup(name) != nil {
		return fmt.Errorf("group '%s' already exists", name)
	}

	if s.store == nil {
		return nil
	}
	groupID, err := s.store.UpsertYouTubeGroup(name, "upload", description, "")
	if err != nil {
		return fmt.Errorf("create upload group %q: %w", name, err)
	}
	for _, chID := range channelIDs {
		if err := s.store.AddChannelToGroup(groupID, chID); err != nil {
			log.Printf("[WARN] CreateGroup: add channel %s to %q: %v", safeChannelID(chID), name, err)
		}
	}
	return nil
}

// DeleteGroup deletes a channel group and persists the change via direct SQL.
func (s *Service) DeleteGroup(name string) error {
	if s.GetGroup(name) == nil {
		return fmt.Errorf("group '%s' not found", name)
	}

	if s.store == nil {
		return nil
	}
	groupID, err := s.store.GetYouTubeGroupID(name, "upload")
	if err != nil {
		return err
	}
	if groupID == 0 {
		return nil
	}
	if err := s.store.DeleteYouTubeGroupChannelsByGroupID(groupID); err != nil {
		return err
	}
	return s.store.DeleteYouTubeGroup(groupID)
}

// AddChannelToGroup adds a channel to a group and persists via direct SQL.
func (s *Service) AddChannelToGroup(groupName, channelID string) error {
	group := s.GetGroup(groupName)
	if group == nil {
		return fmt.Errorf("group '%s' not found", groupName)
	}

	for _, chID := range group.Channels {
		if chID == channelID {
			return fmt.Errorf("channel '%s' already in group '%s'", channelID, groupName)
		}
	}

	if s.store == nil {
		return nil
	}
	groupID, err := s.store.GetYouTubeGroupID(groupName, "upload")
	if err != nil {
		return fmt.Errorf("resolve upload group %q: %w", groupName, err)
	}
	if groupID == 0 {
		groupID, err = s.store.UpsertYouTubeGroup(groupName, "upload", group.Description, group.Privacy)
		if err != nil {
			return err
		}
	}
	return s.store.AddChannelToGroup(groupID, channelID)
}

// RemoveChannelFromGroup removes a channel from a group.
func (s *Service) RemoveChannelFromGroup(groupName, channelID string) error {
	if s.GetGroup(groupName) == nil {
		return fmt.Errorf("group '%s' not found", groupName)
	}

	if s.store == nil {
		return nil
	}
	groupID, err := s.store.GetYouTubeGroupID(groupName, "upload")
	if err != nil {
		return err
	}
	if groupID == 0 {
		return nil
	}

	// Verify membership so we keep the same error semantics.
	currentIDs, err := s.store.ListGroupChannels(groupID)
	if err != nil {
		return err
	}
	found := false
	for _, id := range currentIDs {
		if id == channelID {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("channel '%s' not found in group '%s'", channelID, groupName)
	}
	return s.store.RemoveChannelFromGroup(groupID, channelID)
}

// GetGroups returns all channel groups hydrated from SQLite.
func (s *Service) GetGroups() map[string]*ChannelGroup {
	groups, _ := s.loadChannelGroups()
	return groups
}

// GetGroup returns a specific group by name, hydrating from SQLite on demand.
func (s *Service) GetGroup(name string) *ChannelGroup {
	cg, _ := s.loadChannelGroup(name)
	return cg
}

// ResolveChannelByLanguage finds a channel in a group whose Language field matches.
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

	for _, chID := range group.Channels {
		if ch := s.GetAuthChannel(chID); ch != nil {
			if strings.EqualFold(strings.TrimSpace(ch.Language), lang) {
				chCopy := *ch
				return &chCopy, nil
			}
		}
	}
	for _, chID := range group.Channels {
		if ch := s.GetAuthChannel(chID); ch != nil {
			if strings.TrimSpace(ch.Language) == "" {
				chCopy := *ch
				return &chCopy, nil
			}
		}
	}
	for _, chID := range group.Channels {
		if ch := s.GetAuthChannel(chID); ch != nil {
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
	for _, chID := range cg.Channels {
		if ac := s.GetAuthChannel(chID); ac != nil {
			group.Channels = append(group.Channels, *AuthChannelToChannel(ac))
		} else {
			group.Channels = append(group.Channels, Channel{ID: chID})
		}
	}
	return group
}

// GetGroupsWithChannels returns groups with full channel details.
func (s *Service) GetGroupsWithChannels() []map[string]interface{} {
	groups := s.GetGroups()
	result := make([]map[string]interface{}, 0, len(groups))

	for _, g := range groups {
		groupData := map[string]interface{}{
			"name":        g.Name,
			"description": g.Description,
			"privacy":     g.Privacy,
			"channels":    make([]map[string]interface{}, 0, len(g.Channels)),
			"count":       len(g.Channels),
		}

		for _, chID := range g.Channels {
			if ch := s.GetAuthChannel(chID); ch != nil {
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

// GetUndefinedChannels returns channels not assigned to any group.
func (s *Service) GetUndefinedChannels() []*Channel {
	groups := s.GetGroups()
	assigned := make(map[string]bool)
	for _, cg := range groups {
		for _, chID := range cg.Channels {
			assigned[chID] = true
		}
	}

	var undefined []*Channel
	for _, ac := range s.GetAuthChannels() {
		if !assigned[ac.ID] {
			undefined = append(undefined, AuthChannelToChannel(ac))
		}
	}

	return undefined
}
