// Package youtube provides YouTube API integration for the Velox server.
package youtube

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"velox-server/internal/store/youtubetypes"

	"golang.org/x/oauth2"
	"google.golang.org/api/youtube/v3"
	ytanalytics "google.golang.org/api/youtubeanalytics/v2"
)

// YouTubeStore defines the interface for SQLite-backed YouTube persistence,
// avoiding a direct import of the store package.
type YouTubeStore interface {
	// Canonical: YouTube Channels (youtube_channels table)
	ListYouTubeChannels() ([]map[string]interface{}, error)
	GetYouTubeChannel(channelID string) (map[string]interface{}, error)
	UpsertYouTubeChannel(channelID, title, displayName, channelURL, thumbnailURL, language, notes string, viewCount, subCount int64, addedAt, lastSyncAt string) error

	// Canonical: YouTube Groups V2 (youtube_groups_v2 + youtube_group_channels)
	ListYouTubeGroupsV2() ([]map[string]interface{}, error)
	UpsertYouTubeGroupV2(name, groupType, description, privacy string) (int64, error)
	AddChannelToGroupV2(groupID int64, channelID string) error
	RemoveChannelFromGroupV2(groupID int64, channelID string) error
	ListGroupChannelsV2(groupID int64) ([]string, error)
	ListAllGroupMembershipsV2() ([]map[string]interface{}, error)

	// OAuth token persistence (youtube_oauth_tokens table)
	UpsertYouTubeOAuthToken(channelID string, accessTokenEnc, refreshTokenEnc []byte, tokenType, expiry, scopes string, keyVersion int) error
	GetYouTubeOAuthToken(channelID string) (map[string]interface{}, error)
	ListActiveYouTubeOAuthTokens() ([]map[string]interface{}, error)
	AuditYouTubeOAuthTokenOrphans() ([]youtubetypes.YouTubeTokenOrphan, error)

	// Channel mutation methods
	UpdateChannelTitle(channelID, title string) error
	UpdateChannelLanguage(channelID, language string) error
	DeleteChannelAtomic(channelID string) (int64, error)
	UpdateYouTubeChannelMetadata(channelID, title, thumbnailURL string) error

	// Group mutation methods
	GetYouTubeGroupV2ID(name, groupType string) (int64, error)
	DeleteYouTubeGroupChannelsByGroupID(groupID int64) error
	DeleteYouTubeGroupV2(groupID int64) error

	// Cache (shared)
	GetYouTubeCache(key string) (int64, string, error)
	SetYouTubeCache(key string, timestamp int64, dataJSON string) error
	CleanupYouTubeCache(maxAge int64) (int64, error)
	ClearYouTubeCache() error
	MigrateYouTubeCache(entries map[string]struct {
		Timestamp int64       `json:"timestamp"`
		Data      interface{} `json:"data"`
	}) (int, error)
}

// Service provides YouTube API functionality
type Service struct {
	config      *ServiceConfig
	oauthConfig *oauth2.Config
	channels    map[string]*AuthChannel
	groups      map[string]*ChannelGroup
	mu          sync.RWMutex
	cache       *Cache
	store       YouTubeStore
	oauthBuf    OAuthCipher // Encryption cipher for OAuth token persistence

	authManager  *AuthManager
	uploader     *Uploader
	videoManager *VideoManager
	quotaManager *QuotaManager
}

// OAuthCipher is an interface for encrypting/decrypting OAuth tokens.
type OAuthCipher interface {
	Encrypt(plaintext []byte) ([]byte, error)
	Decrypt(ciphertext []byte) ([]byte, error)
	KeyVersion() int
}

// NewService creates a new YouTube service.
// store is optional — if nil, in-memory-only mode is used.
// cipher is required: the OAuth callback, the auto-refresh path, and the
// boot hydrator all need an AES-GCM cipher to read/write the encrypted
// blobs in youtube_oauth_tokens. Passing nil returns an error so the
// module wiring fails closed instead of silently degrading. The
// previously-present SetOAuthSecretCipher side-channel is gone — a
// service without a cipher is a programmer error at construction
// time, not an operator choice. Integration tests that exercise
// non-OAuth paths can still construct *Service via the struct literal
// directly (oauthBuf field), which keeps the unit tests independent
// of this fail-closed gate.
//
// The previous two-phase boot hydrator (SQLite-first followed by a
// JSON-fallback) has been collapsed to a single SQLite-only path
// since the S6 verdict and the cleanup of remote-engine-bridge +
// the OAuth JSON consolidate / migrate CLIs. New installs are
// expected to land credentials directly via the canonical OAuth
// callback; legacy JSON layouts in <DataDir>/youtube/tokens/ are
// no longer read or written at runtime.
// NewService takes the wider YouTubeStore (not the narrower ServiceStore)
// because Cache.SetStore / NewCache both need the broader surface
// (GetYouTubeCache / SetYouTubeCache / MigrateYouTubeCache etc. live
// there). The assignment `s.store = store` is a structural narrowing
// inside the constructor — Go recognises that any YouTubeStore value
// also satisfies the ServiceStore interface at runtime via the same
// duck-typed subtyping rule module.go (passing *SQLiteStore) already
// relies on. Reverse direction (ServiceStore -> YouTubeStore) does NOT
// type-check because ServiceStore lacks Cache methods.
func NewService(cfg *ServiceConfig, store YouTubeStore, cipher *aesgcm.Encryptor) (*Service, error) {
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
		config:   cfg,
		store:    store,
		channels: make(map[string]*AuthChannel),
		groups:   make(map[string]*ChannelGroup),
		cache:    NewCache(cfg.DataDir, 12*time.Hour, store),
	}

	s.authManager = NewAuthManager(s)
	s.uploader = NewUploader(s)
	s.videoManager = NewVideoManager(s)
	s.quotaManager = NewQuotaManager(s)

	if err := s.loadOAuthConfig(); err != nil {
		log.Printf("[WARN] YouTube OAuth config not loaded: %v", err)
	}

	// SQLite-only boot rehydration: loadOAuthChannelsFromSQLite reads
	// every non-revoked youtube_oauth_tokens row, decrypts via the
	// cipher mounted at construction time, and rebuilds s.channels.
	// This is the only path; the JSON dual-write + JSON-fallback paths
	// have been removed under the S6/S11 verdict (S6 = SQLite-only
	// canonical layout, S11 = DB-first ordering). The module wiring
	// gates OAuth on a non-nil cipher (requireIfMissing=true in
	// module.go) so a missing VELOX_YT_OAUTH_TOKEN_KEY refuses to
	// boot rather than degrading into a JSON reader.
	s.loadOAuthChannelsFromSQLite()
	// Load from canonical tables — store is already set, so this works immediately
	s.loadCanonicalChannels()
	s.loadCanonicalGroups()

	return s, nil
}

// AuthManager returns the auth manager
func (s *Service) AuthManager() *AuthManager {
	return s.authManager
}

// Uploader returns the uploader
func (s *Service) Uploader() *Uploader {
	return s.uploader
}

// VideoManager returns the video manager
func (s *Service) VideoManager() *VideoManager {
	return s.videoManager
}

// QuotaManager returns the quota manager
func (s *Service) QuotaManager() *QuotaManager {
	return s.quotaManager
}

// SetStore sets the SQLite store for persistence, type-asserting from interface{}.
// If a store was already provided via NewService, this is a no-op.
// If called for the first time, it reloads data from the store.
func (s *Service) SetStore(st interface{}) {
	if s.store != nil {
		return // Already set via NewService
	}
	if store, ok := st.(YouTubeStore); ok {
		s.store = store
		s.cache.SetStore(store)
		s.loadCanonicalChannels()
		s.loadCanonicalGroups()
	}
}

// --- Public API: OAuth (Delegated to AuthManager) ---

func (s *Service) GetOAuthStartURL(channelName string) string {
	return s.authManager.GetOAuthStartURL(channelName)
}

func (s *Service) HandleOAuthCallback(ctx context.Context, code string, channelName string) (*Channel, error) {
	return s.authManager.HandleOAuthCallback(ctx, code, channelName)
}

func (s *Service) ValidateToken(ctx context.Context, channelID string) (map[string]interface{}, error) {
	return s.authManager.ValidateToken(ctx, channelID)
}

// RevokeToken is the canonical orchestration for taking a channel's OAuth
// credentials out of service WITHOUT removing the channel row from
// youtube_channels. Distinct from DeleteChannel (which nukes the channel
// + oauth row + groups, FK-cascaded) — see verdict/rationale in
// docs/youtube_sqlite_migration_plan.md step S5d.
//
// Sequence (deterministic order, three steps now that the JSON-fallback
// file-delete path has been removed under S6):
//  1. HTTP POST to Google oauth2 revoke endpoint (best-effort: the credential
//     may already be invalid, so a non-200 is logged but does not abort).
//  2. UPDATE youtube_oauth_tokens SET revoked_at = now WHERE channel_id = ? AND
//     revoked_at IS NULL (atomic SQL via the repository's
//     MarkYouTubeOAuthTokenRevoked; idempotent).
//  3. Delete the channel from the in-memory s.channels under the service's
//     RWMutex (so concurrent reads cannot see a half-revoked entry).
//
// Returns nil on success; returns an error if step 2 fails so the caller
// can retry without leaving SQL state / RAM state inconsistent. Step 3
// is gated on step 2 succeeding so the in-RAM cache never diverges from
// the canonical row.
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
