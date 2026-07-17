// Package youtube: repository.go hosts the canonical Repository
// interface that the YouTube domain stores implement, plus the
// YouTubeStore and YouTubeRepository type aliases kept during the
// PR-YT-REPO transition window.
//
// Migration status: the canonical name for new code is Repository;
// YouTubeStore and YouTubeRepository are aliases only. Existing
// references in tests, in the interface_compliance_test assertion
// (`var _ youtube.YouTubeStore = (*store.SQLiteStore)(nil)`), and
// in downstream packages that historically held the older names,
// continue to compile because `type Alias = Repository` is a pure
// alias.
package youtube

import (
	"velox-server/internal/store/youtubetypes"
)

// Repository is the SINGLE canonical read/write surface for the YouTube
// domain. It collapses YouTubeStore (channel + oauth + cache CRUD) and
// the unique methods previously declared on StorageStore (per-column
// channel updates, group-channel deletion by channel id, tracked-niche
// upsert/delete/list).
//
// Conflict resolution: every method name uses bare canonical names
// (UpsertYouTubeChannel, AddChannelToGroup, ...). When YouTubeStore and
// StorageStore disagreed on a signature pre-PR-YT-REPO, the one used
// widely is kept verbatim — both shared the same shape because
// *SQLiteStore was the only implementor.
//
// *SQLiteStore satisfies Repository via a compile-time assertion in
// store/interface_compliance_test.go (`var _ youtube.YouTubeStore =
// (*store.SQLiteStore)(nil)`); since YouTubeStore is now an alias of
// Repository, that line continues to assert union satisfaction.
type Repository interface {
	// Canonical: YouTube Channels (youtube_channels table).
	ListYouTubeChannels() ([]youtubetypes.YouTubeChannel, error)
	GetYouTubeChannel(channelID string) (*youtubetypes.YouTubeChannel, error)
	UpsertYouTubeChannel(channelID, title, displayName, channelURL, thumbnailURL, language, notes string, viewCount, subCount int64, addedAt, lastSyncAt, metadataJSON string) error
	DeleteYouTubeChannel(channelID string) error
	DeleteChannelAtomic(channelID string) (int64, error)

	// Targeted per-column channel updates — promoted from StorageStore
	// so user-edited typed columns are preserved when only a single
	// column changes (language refresh, stats refresh, title edit, etc.).
	UpdateChannelLanguage(channelID, language string) error
	UpdateChannelStats(channelID string, viewCount, subCount int64, lastSyncAt string) error
	UpdateChannelTitle(channelID, title string) error
	UpdateChannelDisplayName(channelID, name string) error
	UpdateChannelNotes(channelID, notes string) error

	// Typed metadata refresh — distinct from wide UpsertYouTubeChannel
	// (only touches title + thumbnail_url + last_sync_at + updated_at).
	UpdateYouTubeChannelMetadata(channelID, title, thumbnailURL string) error

	// Canonical: YouTube Groups (youtube_groups + youtube_group_channels).
	ListYouTubeGroups() ([]youtubetypes.YouTubeGroup, error)
	UpsertYouTubeGroup(name, groupType, description, privacy string) (int64, error)
	GetYouTubeGroupID(name, groupType string) (int64, error)
	DeleteYouTubeGroup(id int64) error
	DeleteYouTubeGroupByName(name, groupType string) error
	DeleteYouTubeGroupChannelsByGroupID(groupID int64) error
	DeleteYouTubeGroupChannelsByChannelID(channelID string) error
	AddChannelToGroup(groupID int64, channelID string) error
	RemoveChannelFromGroup(groupID int64, channelID string) error
	ListGroupChannels(groupID int64) ([]string, error)
	ListAllGroupMemberships() ([]youtubetypes.GroupMembership, error)

	// Tracked niches (was: StorageStore-only).
	UpsertYouTubeTrackedNiche(niche string) error
	DeleteYouTubeTrackedNiche(niche string) error
	ListYouTubeTrackedNiches() ([]string, error)

	// Canonical: OAuth tokens (youtube_oauth_tokens table; S5-S11 boot hydrator).
	// GetYouTubeOAuthToken returns (nil, nil) when no row exists so callers can
	// use the row presence to drive merge-with-existing-refresh-token-blob.
	// ListActiveYouTubeOAuthTokens is the boot-hydrator enumeration; revoked
	// rows are filtered out so a stale revoke cannot silently re-enter RAM.
	// AuditYouTubeOAuthTokenOrphans surfaces oauth rows whose parent
	// youtube_channels row is missing so operators see the canonical set is
	// fully consistent on boot.
	GetYouTubeOAuthToken(channelID string) (*youtubetypes.YouTubeOAuthToken, error)
	UpsertYouTubeOAuthToken(channelID string, accessTokenEnc, refreshTokenEnc []byte, tokenType, expiry, scopes string, keyVersion int) error
	MarkYouTubeOAuthTokenRevoked(channelID string) error
	ListActiveYouTubeOAuthTokens() ([]youtubetypes.YouTubeOAuthToken, error)
	AuditYouTubeOAuthTokenOrphans() ([]youtubetypes.YouTubeTokenOrphan, error)

	// Cache (SQL-backed key/json cache; TTL enforced at the Cache wrapper layer).
	GetYouTubeCache(key string) (int64, string, error)
	SetYouTubeCache(key string, timestamp int64, dataJSON string) error
	CleanupYouTubeCache(maxAge int64) (int64, error)
	ClearYouTubeCache() error
	MigrateYouTubeCache(entries map[string]struct {
		Timestamp int64       `json:"timestamp"`
		Data      interface{} `json:"data"`
	}) (int, error)
}

// YouTubeStore is kept as a strict type alias of Repository for the
// transition period. Every method that compiles against YouTubeStore
// transitively depends on Repository. New code MUST spell the canonical
// name Repository directly.
type YouTubeStore = Repository

// YouTubeRepository is a strict type alias of Repository for the
// transition period (window opened in PR15.4). New code MUST spell
// the canonical name Repository directly.
type YouTubeRepository = Repository

// Compile-time assertion: YouTubeStore (= Repository) is assignable to
// the Repository-declared contract.
var _ Repository = (YouTubeStore)(nil)

// Compile-time assertion: a nil Repository value is assignable to a
// YouTubeRepository variable (verifies the alias chain).
var _ YouTubeRepository = Repository(nil)
