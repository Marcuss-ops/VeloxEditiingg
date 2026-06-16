// Package youtube provides YouTube Data API integration and management functionality.
package youtube

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// Custom errors
var (
	ErrGroupExists         = errors.New("group already exists")
	ErrGroupNotFound       = errors.New("group not found")
	ErrTargetGroupNotFound = errors.New("target group not found")
	ErrChannelExists       = errors.New("channel already in group")
	ErrChannelNotFound     = errors.New("channel not found")
)

// StorageStore defines the SQLite operations for YouTube manager persistence.
// Uses canonical tables (youtube_channels, youtube_groups_v2, youtube_group_channels).
type StorageStore interface {
	// Canonical channels
	UpsertYouTubeChannel(channelID, title, displayName, channelURL, thumbnailURL, language, notes string, viewCount, subCount int64, addedAt, lastSyncAt, metadataJSON string) error
	ListYouTubeChannels() ([]map[string]interface{}, error)
	GetYouTubeChannel(channelID string) (map[string]interface{}, error)
	DeleteYouTubeChannel(channelID string) error

	// Canonical groups v2
	UpsertYouTubeGroupV2(name, groupType, description, privacy string) (int64, error)
	ListYouTubeGroupsV2() ([]map[string]interface{}, error)
	DeleteYouTubeGroupV2(id int64) error

	// Canonical group-channel memberships
	AddChannelToGroupV2(groupID int64, channelID string) error
	RemoveChannelFromGroupV2(groupID int64, channelID string) error
	ListGroupChannelsV2(groupID int64) ([]string, error)
	ListAllGroupMembershipsV2() ([]map[string]interface{}, error)

	// Tracked niches
	UpsertYouTubeTrackedNiche(niche string) error
	ListYouTubeTrackedNiches() ([]string, error)
}

// Storage handles persistence of YouTube manager data
type Storage struct {
	mu    sync.RWMutex
	data  *StorageData
	store StorageStore
}

// NewStorage creates a new Storage instance backed by SQLite.
func NewStorage(dataDir string, storageStore ...StorageStore) (*Storage, error) {
	var stStore StorageStore
	if len(storageStore) > 0 {
		stStore = storageStore[0]
	}

	st := &Storage{
		store: stStore,
		data: &StorageData{
			Groups: make(map[string]*Group),
		},
	}

	// Load existing data from SQLite
	if err := st.load(); err != nil {
		log.Printf("[WARN] YouTube storage: starting with empty data (%v)", err)
	}

	return st, nil
}

// load reads data from canonical SQLite tables (youtube_groups_v2, youtube_channels).
func (s *Storage) load() error {
	if s.store == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	groupRows, err := s.store.ListYouTubeGroupsV2()
	if err != nil {
		return fmt.Errorf("load groups: %w", err)
	}

	for _, row := range groupRows {
		name, _ := row["name"].(string)
		if name == "" {
			continue
		}
		groupType, _ := row["group_type"].(string)
		createdAt, _ := row["created_at"].(string)
		gid, _ := row["id"].(int64)

		createdAtTime := parseFlexTime(createdAt)

		s.data.Groups[name] = &Group{
			Name:      name,
			CreatedAt: createdAtTime,
			Channels:  []Channel{},
			GroupType: groupType,
		}

		// Load channel memberships for this group
		if gid > 0 {
			channelIDs, err := s.store.ListGroupChannelsV2(gid)
			if err == nil {
				for _, chID := range channelIDs {
					ch, err := s.store.GetYouTubeChannel(chID)
					if err == nil && ch != nil {
						channel := channelFromCanonicalRow(ch)
						if channel != nil {
							s.data.Groups[name].Channels = append(s.data.Groups[name].Channels, *channel)
						}
					} else {
						s.data.Groups[name].Channels = append(s.data.Groups[name].Channels, Channel{ID: chID})
					}
				}
			}
		}
	}

	// Load tracked niches
	niches, err := s.store.ListYouTubeTrackedNiches()
	if err == nil && len(niches) > 0 {
		s.data.TrackedNiches = niches
	}

	log.Printf("[OK] Loaded %d groups from canonical tables", len(s.data.Groups))
	return nil
}

// channelFromCanonicalRow converts a canonical youtube_channels row to a Channel.
func channelFromCanonicalRow(row map[string]interface{}) *Channel {
	id, _ := row["channel_id"].(string)
	if id == "" {
		return nil
	}
	return &Channel{
		ID:        id,
		Title:     asStringField(row, "title"),
		Name:      asStringField(row, "display_name"),
		URL:       asStringField(row, "channel_url"),
		Thumbnail: asStringField(row, "thumbnail_url"),
		Language:  asStringField(row, "language"),
		ViewCount: asInt64Field(row, "view_count"),
		SubCount:  asInt64Field(row, "subscriber_count"),
	}
}

func safeChannelID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func asStringField(m map[string]interface{}, key string) string {
	v, _ := m[key].(string)
	return v
}

func asInt64Field(m map[string]interface{}, key string) int64 {
	switch v := m[key].(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	case string:
		var i int64
		if _, err := fmt.Sscanf(v, "%d", &i); err == nil {
			return i
		}
	}
	return 0
}

func parseFlexTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	layouts := []string{
		"2006-01-02T15:04:05.999999999Z07:00",
		"2006-01-02T15:04:05Z07:00",
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

// save persists data to canonical SQLite tables (youtube_groups_v2, youtube_channels, youtube_group_channels).
// Returns error on first failure — no errors are silently swallowed.
func (s *Storage) save() error {
	if s.store == nil {
		return nil
	}

	// Persist groups to youtube_groups_v2
	for name, g := range s.data.Groups {
		groupType := g.GroupType
		if groupType == "" {
			groupType = "manager"
		}

		groupID, err := s.store.UpsertYouTubeGroupV2(name, groupType, "", "")
		if err != nil {
			return fmt.Errorf("save group %q: %w", name, err)
		}

		// Persist channels to youtube_channels and link via youtube_group_channels
		for _, ch := range g.Channels {
			addedAt := ""
			if !ch.AddedAt.IsZero() {
				addedAt = ch.AddedAt.Format(time.RFC3339)
			}
			lastSync := ""
			if !ch.LastSync.IsZero() {
				lastSync = ch.LastSync.Format(time.RFC3339)
			}
			rawMetadata, _ := json.Marshal(ch)

			// Upsert into youtube_channels
			if err := s.store.UpsertYouTubeChannel(
				ch.ID, ch.Title, ch.Name, ch.URL, ch.Thumbnail,
				ch.Language, ch.Notes,
				ch.ViewCount, ch.SubCount,
				addedAt, lastSync, string(rawMetadata),
			); err != nil {
				return fmt.Errorf("save channel %s: %w", safeChannelID(ch.ID), err)
			}

			// Link channel to group
			if err := s.store.AddChannelToGroupV2(groupID, ch.ID); err != nil {
				return fmt.Errorf("link channel %s to group %q: %w", ch.ID[:8], name, err)
			}
		}
	}

	// Persist tracked niches
	for _, niche := range s.data.TrackedNiches {
		if err := s.store.UpsertYouTubeTrackedNiche(niche); err != nil {
			return fmt.Errorf("save tracked niche %q: %w", niche, err)
		}
	}

	return nil
}

// LoadData returns the current storage data
func (s *Storage) LoadData() *StorageData {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Return a copy to prevent race conditions
	data := &StorageData{
		Groups:        make(map[string]*Group),
		TrackedNiches: s.data.TrackedNiches,
	}
	for k, v := range s.data.Groups {
		group := *v
		group.Channels = make([]Channel, len(v.Channels))
		copy(group.Channels, v.Channels)
		data.Groups[k] = &group
	}
	return data
}

// SaveData replaces the storage data
func (s *Storage) SaveData(data *StorageData) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = data
	return s.save()
}

// AddChannel adds a channel to a group
func (s *Storage) AddChannel(groupName string, channel Channel) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, ok := s.data.Groups[groupName]
	if !ok {
		return ErrGroupNotFound
	}

	// Check for duplicates
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

	// Find the channel
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

	// Check if already in target
	for _, ch := range target.Channels {
		if ch.URL == channel.URL {
			// Just remove from source
			source.Channels = append(source.Channels[:channelIdx], source.Channels[channelIdx+1:]...)
			return s.save()
		}
	}

	// Add to target
	target.Channels = append(target.Channels, *channel)
	// Remove from source
	source.Channels = append(source.Channels[:channelIdx], source.Channels[channelIdx+1:]...)

	return s.save()
}

// UpdateChannelLanguage updates the language for a channel in a group.
// Returns the updated channel and any error.
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

	// Return a copy
	g := *group
	g.Channels = make([]Channel, len(group.Channels))
	copy(g.Channels, group.Channels)
	return &g, true
}

// CreateGroup creates a new group with the specified type ("upload", "manager", or empty)
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

// CleanupOldData removes cached channel metadata that is older than the retention period.
// This is required to comply with YouTube's data retention policies (max 13 days).
// Purges ALL YouTube API-derived fields: Title, Thumbnail, ViewCount, SubCount, Keywords.
func (s *Storage) CleanupOldData(retention time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	removedCount := 0

	// We don't remove the channel itself from the group (to keep the tracking link),
	// but we purge ALL "YouTube API Content" fields if not synced recently.
	for _, group := range s.data.Groups {
		for i := range group.Channels {
			ch := &group.Channels[i]
			if !ch.LastSync.IsZero() && now.Sub(ch.LastSync) > retention {
				// Purge ALL YouTube API-derived content to comply with data retention policies
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

// ClearCache invalidates any cached data.
func (s *Storage) ClearCache() {
	// No-op for now since we don't cache reads
	// In future, this could clear feed cache
}
