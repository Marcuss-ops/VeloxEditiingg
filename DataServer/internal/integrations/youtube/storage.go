package youtube

import (
	"errors"
	"fmt"
)

// Custom errors
var (
	ErrGroupExists           = errors.New("group already exists")
	ErrGroupNotFound         = errors.New("group not found")
	ErrTargetGroupNotFound   = errors.New("target group not found")
	ErrChannelExists         = errors.New("channel already in group")
	ErrChannelNotFound       = errors.New("channel not found")
	ErrStoreNotConfigured    = errors.New("storage store not configured")
)

// StorageStore defines the SQLite operations for YouTube manager persistence.
// PR15.4: V2 suffix dropped on every method — YouTubeStore and StorageStore
// now use the bare canonical names. *SQLiteStore satisfies this interface
// 1-for-1 thanks to the matching rename in store/youtube_groups.go.
//
// All writes are NON-DESTRUCTIVE and per-row: there is no in-memory mirror
// of the groups/channels in this layer anymore, so no reconciler / safety
// guard / saveAll is needed. The Storage methods are direct pass-throughs
// to the SQL store.
type StorageStore interface {
	// Canonical channels
	UpsertYouTubeChannel(channelID, title, displayName, channelURL, thumbnailURL, language, notes string, viewCount, subCount int64, addedAt, lastSyncAt, metadataJSON string) error
	ListYouTubeChannels() ([]map[string]interface{}, error)
	GetYouTubeChannel(channelID string) (map[string]interface{}, error)
	DeleteYouTubeChannel(channelID string) error

	// Targeted per-column channel updates (PR15.4: prevent the destructive
	// wide-upsert path from clobbering user-edited typed columns when only a
	// single column changes — language refresh, stats refresh, etc.).
	UpdateChannelLanguage(channelID, language string) error
	UpdateChannelStats(channelID string, viewCount, subCount int64, lastSyncAt string) error
	UpdateChannelTitle(channelID, title string) error
	UpdateChannelDisplayName(channelID, name string) error

	// Canonical groups
	UpsertYouTubeGroup(name, groupType, description, privacy string) (int64, error)
	GetYouTubeGroupID(name, groupType string) (int64, error)
	ListYouTubeGroups() ([]map[string]interface{}, error)
	DeleteYouTubeGroup(id int64) error
	DeleteYouTubeGroupChannelsByGroupID(groupID int64) error
	DeleteYouTubeGroupChannelsByChannelID(channelID string) error

	// Canonical group-channel memberships
	AddChannelToGroup(groupID int64, channelID string) error
	RemoveChannelFromGroup(groupID int64, channelID string) error
	ListGroupChannels(groupID int64) ([]string, error)
	ListAllGroupMemberships() ([]map[string]interface{}, error)

	// Tracked niches
	UpsertYouTubeTrackedNiche(niche string) error
	DeleteYouTubeTrackedNiche(niche string) error
	ListYouTubeTrackedNiches() ([]string, error)
}

// Storage is a thin SQL-only facade over StorageStore.
//
// PR15.4: this struct no longer holds a `data *StorageData` mirror. Every
// read goes through a fresh SQL query, every write goes through a
// targeted SQL mutation. There is no reconciler / safety guard because
// there is no in-memory state for memory-vs-DB to diverge.
//
// The set of methods on Storage is preserved from the pre-PR15.4 surface
// so existing callers (services/youtube/*, handlers/server/youtube/*)
// keep compiling. Their bodies now route directly to the underlying
// YouTubeStore rather than mutating a RAM snapshot.
type Storage struct {
	store StorageStore
}

// NewStorage creates a new Storage instance backed by SQLite.
//
// PR15.4: NewStorage no longer performs an eager load() to populate an
// in-RAM `data.Groups` mirror. The struct is initialised empty and every
// read is hydrated from SQL on demand. If dataDir is empty (legacy
// degraded mode) the storage still works but with store == nil —
// methods that need SQL return nil/zero/ErrStoreNotConfigured in that
// mode.
func NewStorage(dataDir string, storageStore ...StorageStore) (*Storage, error) {
	var stStore StorageStore
	if len(storageStore) > 0 {
		stStore = storageStore[0]
	}
	return &Storage{store: stStore}, nil
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

// LoadData returns a fresh *StorageData hydrated from the canonical
// SQLite tables. PR15.4: was an in-RAM mirror snapshot; is now a
// one-shot SQL read populated on every call. Used by
// services/youtube.Service.LoadStorageData (SPA accessor) and a few
// read-paths in service_feed.go / service_search.go whose signature
// requires a *StorageData.
//
// Always returns a non-nil *StorageData so callers can iterate
// data.Groups without a nil-check.
func (s *Storage) LoadData() *StorageData {
	data := &StorageData{Groups: make(map[string]*Group)}
	if s.store == nil {
		return data
	}

	groupRows, gErr := s.store.ListYouTubeGroups()
	if gErr != nil {
		return data
	}

	for _, row := range groupRows {
		name, _ := row["name"].(string)
		if name == "" {
			continue
		}
		groupType, _ := row["group_type"].(string)
		createdAt, _ := row["created_at"].(string)
		gid, _ := row["id"].(int64)

		data.Groups[name] = &Group{
			Name:      name,
			CreatedAt: parseFlexTime(createdAt),
			Channels:  s.channelsForGroupLocked(gid),
			GroupType: groupType,
		}
	}

	niches, nErr := s.store.ListYouTubeTrackedNiches()
	if nErr == nil {
		data.TrackedNiches = niches
	}
	return data
}

// LastSaveStatus returned a snapshot of the most recent save outcome.
// Removed entirely in PR15.4 — there is no save path on Storage anymore
// (writes are per-row). The structurally similar tracking that
// save_status.go used to consume (ErrSaveRefusedBySafetyGuard,
// ErrGroupMembershipRefusedEmptyMemory, recordStatus) is dropped as
// well; see save_status.go's deprecation header for context.
//
// This function is no longer present in the source; callers that
// previously invoked it have been migrated.
