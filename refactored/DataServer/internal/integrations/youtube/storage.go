package youtube

import (
	"encoding/json"
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
	GetYouTubeGroupV2ID(name, groupType string) (int64, error)
	ListYouTubeGroupsV2() ([]map[string]interface{}, error)
	DeleteYouTubeGroupV2(id int64) error
	DeleteYouTubeGroupChannelsByGroupID(groupID int64) error

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

// (s *Storage) load() / save() live in storage_persistence.go. Earlier
// PR1 cleanup removed the duplicate, non-canonical bodies (which both
// called or proxied logic that didn't exist on this file) so that Go's
// per-package method dispatch picks up the single canonical
// implementation in storage_persistence.go. The struct + storage
// lifecycle (NewStorage, LoadData, SaveData, ClearCache) stays here.

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
