package youtube

import (
	"errors"
	"fmt"
	"log"
	"sync"
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

	// Legacy (kept for backward compat during migration)
	UpsertYouTubeManagerChannel(channelID, groupName, url, title, name, thumbnail, notes, language string, keywords []string, addedAt, lastSync string, viewCount, subCount int64, rawJSON string) error
	DeleteYouTubeManagerChannel(channelID string) error
	GetYouTubeManagerChannel(channelID string) (string, error)
	ListYouTubeManagerChannels() ([]map[string]interface{}, error)
	UpsertYouTubeManagerGroup(name, createdAt, groupType string, trackedNiches []string) error
	DeleteYouTubeManagerGroup(name string) error
	ListYouTubeManagerGroups() ([]map[string]interface{}, error)
}

// Storage handles persistence of YouTube manager data
type Storage struct {
	mu           sync.RWMutex
	data         *StorageData
	store        StorageStore
	lastStatusMu sync.RWMutex
	lastStatus   *SaveStatus
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

	if err := st.load(); err != nil {
		log.Printf("[WARN] YouTube storage: starting with empty data (%v)", err)
	}

	return st, nil
}

// load reads data from canonical SQLite tables (youtube_groups_v2, youtube_channels).
// Falls back to legacy tables if canonical tables are empty.
func (s *Storage) load() error {
	if s.store == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Try canonical groups first
	groupRows, err := s.store.ListYouTubeGroupsV2()
	hasCanonical := err == nil && len(groupRows) > 0

	if hasCanonical {
		// Load from canonical tables
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

	// Fallback: load from legacy manager tables
	legacyGroupRows, err := s.store.ListYouTubeManagerGroups()
	if err == nil && len(legacyGroupRows) > 0 {
		for _, row := range legacyGroupRows {
			name, _ := row["name"].(string)
			createdAt, _ := row["created_at"].(string)
			groupType, _ := row["group_type"].(string)

			var trackedNiches []string
			if niches, ok := row["tracked_niches"].(string); ok && niches != "" {
				json.Unmarshal([]byte(niches), &trackedNiches)
			}

			createdAtTime := parseFlexTime(createdAt)

			s.data.Groups[name] = &Group{
				Name:      name,
				CreatedAt: createdAtTime,
				Channels:  []Channel{},
				GroupType: groupType,
			}
			if len(trackedNiches) > 0 {
				s.data.TrackedNiches = append(s.data.TrackedNiches, trackedNiches...)
			}
		}

		// Load channels from legacy manager channels
		legacyChannelRows, err := s.store.ListYouTubeManagerChannels()
		if err == nil && len(legacyChannelRows) > 0 {
			for _, row := range legacyChannelRows {
				ch := channelFromRow(row)
				if ch == nil {
					continue
				}
				groupName, _ := row["group_name"].(string)
				if group, ok := s.data.Groups[groupName]; ok {
					group.Channels = append(group.Channels, *ch)
				}
			}
		}

		log.Printf("[OK] Loaded %d groups from legacy tables", len(s.data.Groups))
	}

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

func channelFromRow(row map[string]interface{}) *Channel {
	ch := &Channel{
		ID:        asStringField(row, "channel_id"),
		URL:       asStringField(row, "url"),
		Title:     asStringField(row, "title"),
		Name:      asStringField(row, "name"),
		Thumbnail: asStringField(row, "thumbnail"),
		Notes:     asStringField(row, "notes"),
		Language:  asStringField(row, "language"),
		ViewCount: asInt64Field(row, "view_count"),
		SubCount:  asInt64Field(row, "sub_count"),
		AddedAt:   parseFlexTime(asStringField(row, "added_at")),
		LastSync:  parseFlexTime(asStringField(row, "last_sync")),
	}
	if keywordsJSON, ok := row["keywords_json"].(string); ok && keywordsJSON != "" {
		json.Unmarshal([]byte(keywordsJSON), &ch.Keywords)
	}
	if ch.ID == "" {
		return nil
	}
	return ch
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

// SaveData replaces the storage data with a caller-supplied snapshot.
//
// NOTE: SaveData is NOT destructive-bypass. It runs through save() with
// bypassGuard=false so an accidentally tiny in-memory set cannot silently
// wipe the DB. Callers that genuinely want a destructive full-state
// reconciliation (import, repair, manual rebuild) must call
// saveAllReconcile() instead.
func (s *Storage) SaveData(data *StorageData) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = data
	return s.saveWithStatus("save_data", "", false)
}

// ClearCache invalidates any cached data.
func (s *Storage) ClearCache() {
	// No-op for now since we don't cache reads
}
