// Package youtube provides YouTube Data API integration and management functionality.
package youtube

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
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

// Storage handles persistence of YouTube manager data
type Storage struct {
	mu       sync.RWMutex
	data     *StorageData
	filePath string
}

// NewStorage creates a new Storage instance
func NewStorage(dataDir string) (*Storage, error) {
	// Default data directory
	if dataDir == "" {
		dataDir = "./data"
	}

	// Use the GroupYoutubeManager directory for ChannelsSaved.json
	ytDir := filepath.Join(dataDir, "youtube", "GroupYoutubeManager")
	if err := os.MkdirAll(ytDir, 0755); err != nil {
		return nil, err
	}

	// Use ChannelsSaved.json as the storage file
	filePath := filepath.Join(ytDir, "ChannelsSaved.json")

	s := &Storage{
		filePath: filePath,
		data: &StorageData{
			Groups: make(map[string]*Group),
		},
	}

	// Load existing data
	if err := s.load(); err != nil {
		log.Printf("⚠️ YouTube storage: starting with empty data (%v)", err)
	}

	return s, nil
}

// load reads data from the JSON file (accepts Python-style ISO times without timezone)
func (s *Storage) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // File doesn't exist yet, use defaults
		}
		return err
	}

	var raw storageDataLoad
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	s.data.TrackedNiches = raw.TrackedNiches
	s.data.Groups = make(map[string]*Group)
	for name, g := range raw.Groups {	group := &Group{Name: name}
			group.CreatedAt = parseFlexTime(g.CreatedAt)
			group.Channels = make([]Channel, 0, len(g.Channels))
			for _, c := range g.Channels {
				ch := Channel{
					ID: c.ID, URL: c.URL, Title: c.Title, Name: c.Name, Thumbnail: c.Thumbnail,
					Notes: c.Notes, Keywords: c.Keywords, ViewCount: c.ViewCount, SubCount: c.SubCount,
					Language: c.Language,
				}
				ch.AddedAt = parseFlexTime(c.AddedAt)
				ch.LastSync = parseFlexTime(c.LastSync)
				group.Channels = append(group.Channels, ch)
		}
		s.data.Groups[name] = group
	}
	return nil
}

type storageDataLoad struct {
	Groups        map[string]*groupLoad `json:"groups"`
	TrackedNiches []string              `json:"tracked_niches,omitempty"`
}
type groupLoad struct {
	Name      string        `json:"name"`
	CreatedAt string        `json:"created_at"`
	Channels  []channelLoad `json:"channels"`
}
type channelLoad struct {
	ID        string   `json:"id"`
	URL       string   `json:"url"`
	Title     string   `json:"title"`
	Name      string   `json:"name,omitempty"`
	Thumbnail string   `json:"thumbnail"`
	Notes     string   `json:"notes,omitempty"`
	AddedAt   string   `json:"added_at"`
	Keywords  []string `json:"keywords,omitempty"`
	ViewCount int64    `json:"view_count,omitempty"`
	SubCount  int64    `json:"subscriber_count,omitempty"`
	Language  string   `json:"language,omitempty"`
	LastSync  string   `json:"last_sync,omitempty"`
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

// save writes data to the JSON file atomically
func (s *Storage) save() error {
	data, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}

	// Atomic write: write to temp file then rename
	tempPath := s.filePath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}

	return os.Rename(tempPath, s.filePath)
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


