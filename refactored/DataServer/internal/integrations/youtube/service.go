// Package youtube provides YouTube API integration for the Velox server.
package youtube

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

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
	UpsertYouTubeChannel(channelID, title, displayName, channelURL, thumbnailURL, language, notes string, viewCount, subCount int64, addedAt, lastSyncAt, metadataJSON string) error

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
	AuditYouTubeOAuthTokenOrphans() ([]interface{}, error)

	// Channel mutation methods
	UpdateChannelTitle(channelID, title string) error
	UpdateChannelLanguage(channelID, language string) error
	DeleteChannelAtomic(channelID string, tx interface{}) error
	UpdateYouTubeChannelMetadata(channelID, metadataJSON string) error

	// Group mutation methods
	GetYouTubeGroupV2ID(name string) (int64, error)
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

	s.loadChannels()
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
