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
// It includes both canonical methods (youtube_channels, youtube_groups_v2)
// and legacy methods (youtube_channel_metadata, youtube_groups) for migration.
type YouTubeStore interface {
	// Canonical: YouTube Channels (youtube_channels table)
	ListYouTubeChannels() ([]map[string]interface{}, error)
	GetYouTubeChannel(channelID string) (map[string]interface{}, error)
	UpsertYouTubeChannel(channelID, title, displayName, channelURL, thumbnailURL, language, notes string, viewCount, subCount int64, addedAt, lastSyncAt, metadataJSON string) error
	DeleteYouTubeChannel(channelID string) error
	DeleteChannelAtomic(channelID string) (int64, error)

	// Canonical: YouTube Groups (youtube_groups + youtube_group_channels)
	ListYouTubeGroups() ([]map[string]interface{}, error)
	UpsertYouTubeGroup(name, groupType, description, privacy string) (int64, error)
	GetYouTubeGroupID(name, groupType string) (int64, error)
	DeleteYouTubeGroup(id int64) error
	DeleteYouTubeGroupChannelsByGroupID(groupID int64) error
	AddChannelToGroup(groupID int64, channelID string) error
	RemoveChannelFromGroup(groupID int64, channelID string) error
	ListGroupChannels(groupID int64) ([]string, error)
	ListAllGroupMemberships() ([]map[string]interface{}, error)

	// Typed metadata update (refresh path). Distinct from UpsertYouTubeChannel:
	// only touches title + thumbnail_url + last_sync_at + updated_at so
	// user-edited typed columns are preserved (S11 contract).
	UpdateYouTubeChannelMetadata(channelID, title, thumbnailURL string) error

	// Canonical: OAuth tokens (youtube_oauth_tokens table; S5-S11 boot hydrator)
	// GetYouTubeOAuthToken returns (nil, nil) when no row exists so callers can
	// use the row presence to drive merge-with-existing-refresh-token-blob.
	// ListActiveYouTubeOAuthTokens is the boot-hydrator enumeration; revoked
	// rows are filtered out so a stale revoke cannot silently re-enter RAM
	// after a server restart.
	// AuditYouTubeOAuthTokenOrphans surfaces oauth rows whose parent
	// youtube_channels row is missing so operators see the canonical set is
	// fully consistent on boot.
	GetYouTubeOAuthToken(channelID string) (map[string]interface{}, error)
	UpsertYouTubeOAuthToken(channelID string, accessTokenEnc, refreshTokenEnc []byte, tokenType, expiry, scopes string, keyVersion int) error
	MarkYouTubeOAuthTokenRevoked(channelID string) error
	ListActiveYouTubeOAuthTokens() ([]map[string]interface{}, error)
	AuditYouTubeOAuthTokenOrphans() ([]youtubetypes.YouTubeTokenOrphan, error)

	// Cache (shared, not legacy)
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

// NewService creates a new YouTube service.
// store is optional — if nil, in-memory-only mode is used.
func NewService(cfg *ServiceConfig, store YouTubeStore) (*Service, error) {
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

// ValidateOAuthAccessToken validates a channel's OAuth access token by
// calling the remote YouTube API. Renamed from ValidateToken to eliminate
// ambiguity with the stored-credentials validator and the worker token validator.
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
