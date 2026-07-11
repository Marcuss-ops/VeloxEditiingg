// Package youtube provides YouTube API integration for the Velox server.
//
// PR-YT-REPO: the previous two-interface layout (YouTubeStore + StorageStore +
// thin Storage facade) is consolidated into a single canonical
// `youtube.Repository` that *SQLiteStore satisfies 1-for-1. YouTubeStore
// and YouTubeRepository are kept as type aliases of Repository so existing
// callers (tests that embed YouTubeStore as a nil field, the
// interface_compliance_test assertion, downstream packages that already
// depend on those names) keep compiling without churn.
//
// Service stores its dependency on the struct (s.repo Repository). There is
// no SetStore late binding and no in-memory fallback: every operation
// either hits the canonical repo or returns an explicit error.
package youtube

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"velox-server/internal/store/youtubetypes"

	"golang.org/x/oauth2"
	"google.golang.org/api/youtube/v3"
	ytanalytics "google.golang.org/api/youtubeanalytics/v2"
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

// (Re-exported via repository.go for consistency with the previous
// alias declaration.)
// YouTubeRepository = Repository.

var _ Repository = (YouTubeStore)(nil) // compile-time: alias identity is preserved.

// Service provides YouTube API functionality.
//
// PR-YT-REPO: the legacy `var store YouTubeStore` package-level cache
// pointer and `SetStore` late-binding mutator are gone. The repo is
// injected at construction and held for the lifetime of the Service.
// SQLite is the single source of truth for channels and groups; every
// read goes through a fresh SQL query and every write is persisted
// immediately.
type Service struct {
	config      *ServiceConfig
	oauthConfig *oauth2.Config
	cache       *Cache
	repo        Repository
	oauthBuf    OAuthCipher

	authManager  *AuthManager
	uploader     *Uploader
	videoManager *VideoManager
	quotaManager *QuotaManager
}

// NewService creates a new YouTube service.
//
// PR-YT-REPO: the repo argument is REQUIRED. Passing nil is a programmer
// error and returns a non-nil error so the call site fails fast instead
// of silently entering a degraded mode (the previous "store is optional
// → in-memory-only" behaviour is removed entirely).
func NewService(cfg *ServiceConfig, repo Repository) (*Service, error) {
	if cfg == nil {
		return nil, fmt.Errorf("youtube.NewService: cfg is required")
	}
	if repo == nil {
		return nil, fmt.Errorf("youtube.NewService: Repository is required")
	}

	if cfg.TokensDir == "" {
		if env := os.Getenv("VELOX_YOUTUBE_TOKENS_DIR"); env != "" {
			cfg.TokensDir = env
		} else {
			cfg.TokensDir = filepath.Join(cfg.DataDir, "secrets", "youtube", "tokens")
		}
	}
	if cfg.YoutubePostingPath == "" {
		if env := os.Getenv("VELOX_YOUTUBE_POSTING_PATH"); env != "" {
			cfg.YoutubePostingPath = env
		} else {
			cfg.YoutubePostingPath = "YoutubePosting"
		}
	}

	s := &Service{
		config: cfg,
		repo:   repo,
		cache:  NewCache(cfg.DataDir, 12*time.Hour, repo),
	}

	s.authManager = NewAuthManager(s)
	s.uploader = NewUploader(s)
	s.videoManager = NewVideoManager(s)
	s.quotaManager = NewQuotaManager(s) // PR-YT-REPO: repo/db wiring done by app/youtube.go via the QuotaManager's own SetStore/SetDB.

	if err := s.loadOAuthConfig(); err != nil {
		log.Printf("[WARN] YouTube OAuth config not loaded: %v", err)
	}

	return s, nil
}

// AuthManager returns the auth manager.
func (s *Service) AuthManager() *AuthManager { return s.authManager }

// Uploader returns the uploader.
func (s *Service) Uploader() *Uploader { return s.uploader }

// VideoManager returns the video manager.
func (s *Service) VideoManager() *VideoManager { return s.videoManager }

// QuotaManager returns the quota manager.
func (s *Service) QuotaManager() *QuotaManager { return s.quotaManager }

// Repo returns the Repository this Service was wired to. Exposed for
// handlers/tests that need to chain SQL operations; production code
// should preferentially call Service methods (which centralise the
// SQL→domain mapping).
func (s *Service) Repo() Repository { return s.repo }

// --- Public API: OAuth (Delegated to AuthManager) ---

func (s *Service) GetOAuthStartURL(channelName string) string {
	return s.authManager.GetOAuthStartURL(channelName)
}

func (s *Service) HandleOAuthCallback(ctx context.Context, code string, channelName string) (*Channel, error) {
	return s.authManager.HandleOAuthCallback(ctx, code, channelName)
}

// ValidateOAuthAccessToken validates a channel's OAuth access token
// against the live YouTube API.
func (s *Service) ValidateOAuthAccessToken(ctx context.Context, channelID string) (map[string]interface{}, error) {
	return s.authManager.ValidateStoredYouTubeCredentials(ctx, channelID)
}

func (s *Service) RevokeToken(ctx context.Context, channelID string) error {
	return s.authManager.RevokeToken(ctx, channelID)
}

// --- Public API: Upload (Delegated to Uploader) ---

func (s *Service) UploadVideo(ctx context.Context, channelID string, videoPath string, config UploadConfig) (*UploadResult, error) {
	return s.uploader.UploadVideo(ctx, channelID, videoPath, config)
}

func (s *Service) SetThumbnail(ctx context.Context, channelID string, videoID string, thumbnailPath string) (string, error) {
	return s.uploader.SetThumbnail(ctx, channelID, videoID, thumbnailPath)
}

// --- Public API: Video Metadata (Delegated to VideoManager) ---

func (s *Service) UpdateVideoMetadata(ctx context.Context, channelID string, videoID string, config UploadConfig) error {
	return s.videoManager.UpdateVideoMetadata(ctx, channelID, videoID, config)
}

func (s *Service) DeleteVideo(ctx context.Context, channelID string, videoID string) error {
	return s.videoManager.DeleteVideo(ctx, channelID, videoID)
}

func (s *Service) ListVideos(ctx context.Context, channelID string, maxResults int64) ([]*youtube.Video, error) {
	return s.videoManager.ListVideos(ctx, channelID, maxResults)
}

// --- Public API: Quota/Analytics (Delegated to QuotaManager) ---

func (s *Service) GetQuotaUsage(ctx context.Context) map[string]interface{} {
	return s.quotaManager.GetQuotaUsage(ctx)
}

func (s *Service) GetAnalyticsService(ctx context.Context, channelID string) (*ytanalytics.Service, error) {
	return s.quotaManager.GetAnalyticsService(ctx, channelID)
}

func (s *Service) FetchAnalytics(ctx context.Context, channelID string, days int) (map[string]interface{}, error) {
	return s.quotaManager.FetchAnalytics(ctx, channelID, days)
}

func (s *Service) UpdateAnalyticsCache(ctx context.Context, channelID string, days int, data map[string]interface{}) error {
	return s.quotaManager.UpdateAnalyticsCache(ctx, channelID, days, data)
}
