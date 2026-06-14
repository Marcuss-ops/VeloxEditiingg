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
type StorageStore interface {
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

// load reads data from SQLite store.
func (s *Storage) load() error {
	if s.store == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Load groups
	groupRows, err := s.store.ListYouTubeManagerGroups()
	if err == nil && len(groupRows) > 0 {
		for _, row := range groupRows {
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
	}

	// Load channels
	channelRows, err := s.store.ListYouTubeManagerChannels()
	if err == nil && len(channelRows) > 0 {
		for _, row := range channelRows {
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

	log.Printf("[OK] Loaded %d groups and channels from SQLite", len(s.data.Groups))
	return nil
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

// save persists data to SQLite.
func (s *Storage) save() error {
	if s.store == nil {
		return nil
	}

	// Persist groups
	for name, g := range s.data.Groups {
		createdAt := ""
		if !g.CreatedAt.IsZero() {
			createdAt = g.CreatedAt.Format(time.RFC3339)
		}
		_ = s.store.UpsertYouTubeManagerGroup(name, createdAt, g.GroupType, nil)
	}

	// Persist channels
	for _, g := range s.data.Groups {
		for _, ch := range g.Channels {
			addedAt := ""
			if !ch.AddedAt.IsZero() {
				addedAt = ch.AddedAt.Format(time.RFC3339)
			}
			lastSync := ""
			if !ch.LastSync.IsZero() {
				lastSync = ch.LastSync.Format(time.RFC3339)
			}
			raw, _ := json.Marshal(ch)
			_ = s.store.UpsertYouTubeManagerChannel(
				ch.ID, g.Name, ch.URL, ch.Title, ch.Name, ch.Thumbnail,
				ch.Notes, ch.Language, ch.Keywords, addedAt, lastSync,
				ch.ViewCount, ch.SubCount, string(raw),
			)
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
