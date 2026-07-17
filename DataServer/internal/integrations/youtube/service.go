// Package youtube provides YouTube API integration for the Velox server.
//
// PR-YT-REPO: the previous two-interface layout (YouTubeStore + StorageStore +
// thin Storage facade) is consolidated into a single canonical
// `youtube.Repository` (declared in repository.go) that *SQLiteStore
// satisfies 1-for-1. YouTubeStore and YouTubeRepository are kept as
// type aliases of Repository so existing callers (tests that embed
// YouTubeStore as a nil field, the interface_compliance_test
// assertion, downstream packages that already depend on those names)
// keep compiling without churn.
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

	"golang.org/x/oauth2"
	"google.golang.org/api/youtube/v3"
	ytanalytics "google.golang.org/api/youtubeanalytics/v2"
)

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
