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

// NicheStore is the orthogonal tracked-niche contract used by *Storage.
// Pulled out so callers that only care about niche persistence can hand
// a NicheStore-conforming value to a method that needs just the niche
// surface (rare — *Storage is the main consumer). Listed separately so
// storage_persistence.go's `s.store.ListYouTubeTrackedNiches()` /
// `s.store.UpsertYouTubeTrackedNiche(...)` calls have a named contract
// instead of appearing as drift on YouTubeStore / ServiceStore.
type NicheStore interface {
	ListYouTubeTrackedNiches() ([]string, error)
	UpsertYouTubeTrackedNiche(niche string) error
}

// StorageStore defines the SQLite operations for YouTube MANAGER
// persistence — the *Storage type's own store field. Pulled out of
// YouTubeStore because *Storage calls niche methods which *Service.body
// NEVER calls and module-level wiring doesn't expose. Embedding
// NicheStore keeps the niche contract explicit at the type level so
// future readers don't have to grep storage_persistence.go to know
// which methods *Storage.store expects. *SQLiteStore already implements
// every method here, so the embedding is transparent to module wiring.
//
// Companion interfaces in this package (do NOT collapse them):
//   - ServiceStore  — what *Service.body uses (21-method subset)
//   - YouTubeStore  — what module-level consumers (Cache.SetStore,
//   QuotaManager.SetStore, etc.) use (full SQL surface)
//   - StorageStore  — what *Storage uses (channel/group V2 + NicheStore)
//
// Each is a purpose-built typed contract. The S11 cleanup commits have
// repeatedly chased drift across these three; new code MUST pick the
// narrowest interface that fits its call site, and consumers whose
// call patterns span categories should hand a wider type rather than
// mutating the narrower contract.
type StorageStore interface {
	UpsertYouTubeChannel(channelID, title, displayName, channelURL, thumbnailURL, language, notes string, viewCount, subCount int64, addedAt, lastSyncAt string) error
	ListYouTubeChannels() ([]map[string]interface{}, error)
	GetYouTubeChannel(channelID string) (map[string]interface{}, error)
	DeleteYouTubeChannel(channelID string) error
	UpsertYouTubeGroupV2(name, groupType, description, privacy string) (int64, error)
	ListYouTubeGroupsV2() ([]map[string]interface{}, error)
	GetYouTubeGroupV2ID(name, groupType string) (int64, error)
	DeleteYouTubeGroupV2(id int64) error
	DeleteYouTubeGroupChannelsByGroupID(groupID int64) error
	DeleteYouTubeGroupChannelsByChannelID(channelID string) error
	AddChannelToGroupV2(groupID int64, channelID string) error
	RemoveChannelFromGroupV2(groupID int64, channelID string) error
	ListGroupChannelsV2(groupID int64) ([]string, error)
	ListAllGroupMembershipsV2() ([]map[string]interface{}, error)

	// Tracked niches — orthogonal to groups. Embedded sub-interface,
	// not an inline method list, so future readers can refer to
	// NicheStore by name instead of inferring niche usage from grep.
	NicheStore
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
