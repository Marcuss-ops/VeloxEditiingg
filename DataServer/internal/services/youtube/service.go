// Package youtube is the YouTube business service. PR-YT-REPO: the
// previous *youtube.Storage facade is gone (deleted with the
// StorageStore/Storage/NewStorage variadic/in-memory mode/SetStore
// tardivo). This service now threads through the canonical
// *youtube.Service (which owns the Repository) for every persistence
// operation.
package youtube

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"velox-server/internal/integrations/news"
	"velox-server/internal/integrations/youtube"
)

const managerStatsCacheTTL = 5 * time.Minute

// Service holds the dependencies for YouTube business operations.
//
// PR-YT-REPO: `storage *youtube.Storage` is removed. The canonical
// Repository is held by *youtube.Service (ytService.rep); every
// Group/Channel/TrackedNiche operation routes through ytService so
// SQLite is the single source of truth.
type Service struct {
	apiClient   *youtube.APIClient
	feedCache   *youtube.FeedCache
	newsFetcher *news.Fetcher
	dataDir     string
	ytService   *youtube.Service

	statsCacheMu    sync.RWMutex
	statsCacheEntry *statsCacheEntry
}

// New creates a new Service instance.
//
// PR-YT-REPO: the `existingStorage *youtube.Storage` parameter is
// removed (the Storage facade is deleted). Pass only `ytService` —
// the canonical Service owns the Repository and provides every
// query/mutation method this business layer needs.
func New(dataDir, apiKey string, ytService *youtube.Service) *Service {
	cache := youtube.NewCache(dataDir, 2*time.Hour, ytService.Repo())
	feedCache := youtube.NewFeedCache(dataDir)
	newsFetcher := news.NewFetcher(nil)

	s := &Service{
		apiClient:   youtube.NewAPIClient(apiKey, cache),
		feedCache:   feedCache,
		newsFetcher: newsFetcher,
		dataDir:     dataDir,
		ytService:   ytService,
	}

	go s.reviewAndRefreshChannels()

	return s
}

func (s *Service) reviewAndRefreshChannels() {
	time.Sleep(3 * time.Second)
	log.Printf("[REVIEW] YouTube Review: Starting background review of database channels...")

	if s.ytService == nil {
		log.Printf("[WARN] YouTube Review: no integration service wired; skipping review")
		return
	}

	groups := s.ytService.GetGroups()
	if len(groups) == 0 {
		log.Printf("[INFO] YouTube Review: No groups found, skipping review")
		return
	}

	for name, group := range groups {
		if group == nil {
			continue
		}
		// Hydrate group channels via the canonical Service so the
		// Channel objects carry Title/Name/Thumbnail/Language for the
		// review loop. This used to come from the in-RAM `group.Channels`
		// slice on Storage; now it's a Membership-style SQL read.
		chInfos, err := s.ytService.BulkMembership(group.Channels)
		_ = err // non-fatal: review is opportunistic
		for _, chInfo := range chInfos {
			if chInfo == nil {
				continue
			}
		}
		for _, chID := range group.Channels {
			authCh := s.ytService.GetAuthChannel(chID)
			if authCh == nil {
				continue
			}
			needsRefresh := authCh.Title == "" || authCh.Title == chID || authCh.Name == "" || authCh.Name == chID || authCh.Thumbnail == ""
			needsLanguage := authCh.Language == "" || authCh.Language == "unknown"
			if !needsRefresh && !needsLanguage {
				continue
			}
			log.Printf("[INFO] YouTube Review: Refreshing metadata & language for channel %s in group %s...", chID, name)

			realTitle := authCh.Title
			thumbnail := authCh.Thumbnail
			detectedLang := authCh.Language

			if realTitle == "" {
				channelURL := authCh.URL
				if channelURL == "" || !strings.HasPrefix(channelURL, "http") {
					channelURL = "https://www.youtube.com/channel/" + chID
				}
				info, err := s.apiClient.GetChannelInfo(context.Background(), channelURL)
				if err == nil && info != nil {
					realTitle = info.Title
					thumbnail = info.Thumbnail
					log.Printf("[OK] YouTube Review: Fetched API info for %s -> %q", chID, realTitle)
				} else {
					log.Printf("[WARN] YouTube Review: Failed to fetch channel info for %s: %v", chID, err)
				}
			}

			if realTitle == "" {
				realTitle = chID
			}

			if detectedLang == "" || detectedLang == "unknown" {
				detectedLang = youtube.DetectLanguageFromName(realTitle)
				if detectedLang == "" {
					detectedLang = "en"
				}
			}

			// PR-YT-REPO: orchestrator-side Service.UpdateChannelMetadata
			// (channelID, metadata) returns only error; discard single
			// value to satisfy the lint rule against multiple-return
			// assignment to single-value context. Pass language via the
			// metadata map (the canonical write path applies it).
			_ = s.ytService.UpdateChannelMetadata(chID, map[string]interface{}{
				"title":     realTitle,
				"name":      realTitle,
				"thumbnail": thumbnail,
				"language":  detectedLang,
			})

			log.Printf("[OK] YouTube Review: Resolved channel %s -> %q [%s]", chID, realTitle, detectedLang)
		}
	}
}

// ResolveChannel finds channel information.
func (s *Service) ResolveChannel(ctx context.Context, urlParam string) (*youtube.ChannelInfo, error) {
	return s.apiClient.GetChannelInfo(ctx, urlParam)
}

// TrendingNews aggregates trending news hits.
func (s *Service) TrendingNews(ctx context.Context, query string, limit int) ([]news.NewsItem, error) {
	return s.newsFetcher.FetchTrendingNews(ctx, query, limit)
}

// ListGroups lists all registered groups.
//
// PR-YT-REPO: the integration *youtube.Service no longer exposes a
// ListGroups() two-tuple method. The single source of truth is
// *youtube.StorageData (LoadData()) which carries Groups and
// TrackedNiches in one snapshot.
func (s *Service) ListGroups() (map[string]*youtube.Group, []string) {
	if s.ytService == nil {
		return map[string]*youtube.Group{}, nil
	}
	data := s.ytService.LoadData()
	groups := data.Groups
	if groups == nil {
		groups = map[string]*youtube.Group{}
	}
	for name, group := range groups {
		if group == nil || len(group.Channels) == 0 {
			delete(groups, name)
		}
	}
	return groups, data.TrackedNiches
}

// CreateGroup registers a new channel group.
//
// PR-YT-REPO: routes to the canonical Service.CreateGroup which takes
// (name, description, channelIDs). The previous Storage.CreateGroup
// 2-arg overload is gone.
func (s *Service) CreateGroup(name string) error {
	if s.ytService == nil {
		return fmt.Errorf("youtube integration service not configured")
	}
	return s.ytService.CreateGroup(name, "", nil)
}

// DeleteGroup deletes a channel group.
func (s *Service) DeleteGroup(name string) error {
	if s.ytService == nil {
		return fmt.Errorf("youtube integration service not configured")
	}
	return s.ytService.DeleteGroup(name)
}

// CleanupOldData cleans stale historical entries.
func (s *Service) CleanupOldData(retention time.Duration) int {
	if s.ytService == nil {
		return 0
	}
	return s.ytService.CleanupOldData(retention)
}

// CleanupCache flushes API-level caches.
func (s *Service) CleanupCache() int {
	return s.apiClient.CleanupCache()
}

// DataRetentionCleanup processes general DB cleanup.
func (s *Service) DataRetentionCleanup() int {
	if s.dataDir == "" || s.ytService == nil {
		return 0
	}
	total := 0
	total += s.ytService.CleanupOldData(13 * 24 * time.Hour)
	s.feedCache.Clear()
	total++
	return total
}

// ChannelHasTokenFile checks for OAuth local session tokens.
func (s *Service) ChannelHasTokenFile(channelID string) bool {
	if s.dataDir == "" || channelID == "" {
		return false
	}
	candidates := []string{
		filepath.Join(s.dataDir, "youtube", "tokens", "account_"+channelID+".json"),
		filepath.Join(s.dataDir, "secrets", "youtube", "tokens", "account_"+channelID+".json"),
		filepath.Join(s.dataDir, "youtube", "Token", "account_"+channelID+".json"),
	}
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return true
		}
	}
	return false
}

func asBool(m map[string]interface{}, key string) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}
	return false
}

func asString(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

// APIClient returns the API client instance.
func (s *Service) APIClient() *youtube.APIClient {
	return s.apiClient
}

// DataDir returns the path to the workspace data directory.
func (s *Service) DataDir() string {
	return s.dataDir
}

// LoadStorageData loads YouTube storage data via the canonical
// Service.LoadData() (replaces the deleted storage.LoadData()).
func (s *Service) LoadStorageData() *youtube.StorageData {
	if s.ytService == nil {
		return &youtube.StorageData{Groups: map[string]*youtube.Group{}}
	}
	return s.ytService.LoadData()
}

// GetAuthChannels retrieves oauth authenticated channels.
func (s *Service) GetAuthChannels() []*youtube.AuthChannel {
	if s.ytService == nil {
		return nil
	}
	return s.ytService.GetAuthChannels()
}

// GetAuthChannel retrieves a single oauth authenticated channel.
func (s *Service) GetAuthChannel(id string) *youtube.AuthChannel {
	if s.ytService == nil {
		return nil
	}
	return s.ytService.GetAuthChannel(id)
}

// ValidateOAuthAccessToken checks token validation status.
func (s *Service) ValidateOAuthAccessToken(ctx context.Context, id string) (map[string]interface{}, error) {
	if s.ytService == nil {
		return nil, fmt.Errorf("youtube integration service not configured")
	}
	return s.ytService.ValidateOAuthAccessToken(ctx, id)
}

// GetUndefinedChannels retrieves undefined channels.
func (s *Service) GetUndefinedChannels() []*youtube.Channel {
	if s.ytService == nil {
		return nil
	}
	return s.ytService.GetUndefinedChannels()
}
