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
	"time"

	"golang.org/x/oauth2"
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
//
// Construction order:
//  1. Validate cfg + repo (fail fast on programmer error).
//  2. applyConfigDefaults (config.go) — fills empty TokensDir /
//     YoutubePostingPath from env or documented default.
//  3. Build the Service struct with config + repo + cache.
//  4. wireServiceManagers (config.go) — installs AuthManager +
//     Uploader + VideoManager + QuotaManager and triggers the OAuth
//     config load.
func NewService(cfg *ServiceConfig, repo Repository) (*Service, error) {
	if cfg == nil {
		return nil, fmt.Errorf("youtube.NewService: cfg is required")
	}
	if repo == nil {
		return nil, fmt.Errorf("youtube.NewService: Repository is required")
	}

	applyConfigDefaults(cfg)

	s := &Service{
		config: cfg,
		repo:   repo,
		cache:  NewCache(cfg.DataDir, 12*time.Hour, repo),
	}

	wireServiceManagers(s)

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
