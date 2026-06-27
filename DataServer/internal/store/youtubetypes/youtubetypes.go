// Package youtubetypes contains shared YouTube data shapes used by both the
// SQLite store implementation and the YouTube integration package.
//
// Why a separate package: YouTubeStore (declared in
// internal/integrations/youtube/service.go) is a consumer-facing interface
// that needs to name the channel-seed and orphan types owned by the SQL
// layer, but the SQLite implementation lives in internal/store and would
// otherwise create an import cycle if it tried to consume types declared
// inside the integration package. Putting the structs in a sibling
// sub-package that both internal/store and internal/integrations/youtube
// can import breaks the cycle while keeping the types close to the schema
// they describe.
package youtubetypes

// YouTubeChannelSeed is the minimal channel-row data passed to
// ConnectChannelAtomic on first-time OAuth connect. Fields not set are
// empty / zero; the caller is responsible for filling them when known.
//
// On INSERT, AddedAt defaults to time.Now() when empty so the
// "discover date" is set automatically. On UPDATE (re-auth flow) the
// added_at column is preserved because the SQL UPDATE branch omits it,
// so a re-connect on an existing channel does NOT bump added_at.
type YouTubeChannelSeed struct {
	ChannelID    string
	Title        string
	DisplayName  string
	ChannelURL   string
	ThumbnailURL string
	Language     string
	Notes        string
	ViewCount    int64
	SubCount     int64
	AddedAt      string
	LastSyncAt   string
}

// YouTubeChannel is the typed representation of a youtube_channels row.
// Used as the return type of ListYouTubeChannels / GetYouTubeChannel on
// both the YouTubeStore and StorageStore interfaces.
type YouTubeChannel struct {
	ChannelID      string
	Title          string
	DisplayName    string
	ChannelURL     string
	ThumbnailURL   string
	Language       string
	Notes          string
	ViewCount      int64
	SubscriberCount int64
	AddedAt        string
	LastSyncAt     string
	CreatedAt      string
	UpdatedAt      string
}

// YouTubeOAuthToken is the typed row from youtube_oauth_tokens.
// Used as the return type of GetYouTubeOAuthToken / ListActiveYouTubeOAuthTokens
// on both the YouTubeStore and StorageStore interfaces.
type YouTubeOAuthToken struct {
	ChannelID             string
	AccessTokenEncrypted  []byte
	RefreshTokenEncrypted []byte
	TokenType             string
	Expiry                string
	Scopes                string
	KeyVersion            int64
	RevokedAt             string // empty if not revoked
	CreatedAt             string
	UpdatedAt             string
}

// YouTubeGroup is the typed representation of a youtube_groups row.
// Used as the return type of ListYouTubeGroups on both the YouTubeStore
// and StorageStore interfaces.
type YouTubeGroup struct {
	ID          int64
	Name        string
	GroupType   string
	Description string
	Privacy     string
	CreatedAt   string
	UpdatedAt   string
}

// GroupMembership is the typed representation of a youtube_group_channels
// row joined with youtube_groups (group_name, group_type).
// Used as the return type of ListAllGroupMemberships on both the
// YouTubeStore and StorageStore interfaces.
type GroupMembership struct {
	GroupID   int64
	ChannelID string
	Position  int
	GroupName string
	GroupType string
}

// YouTubeTokenOrphan describes an oauth token row whose parent channel row
// is missing. The boot audit (AuditYouTubeOAuthTokenOrphans) returns slices
// of these so operators can decide whether to backfill the parent row or
// drop the orphan. Idempotent and safe to call on every startup.
type YouTubeTokenOrphan struct {
	ChannelID string
	UpdatedAt string
}
