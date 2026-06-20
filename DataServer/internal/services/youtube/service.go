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
type Service struct {
	storage     *youtube.Storage
	apiClient   *youtube.APIClient
	feedCache   *youtube.FeedCache
	newsFetcher *news.Fetcher
	dataDir     string
	ytService   *youtube.Service

	statsCacheMu    sync.RWMutex
	statsCacheEntry *statsCacheEntry
}

// New creates a new Service instance.
func New(dataDir, apiKey string, existingStorage *youtube.Storage, ytService *youtube.Service) *Service {
	cache := youtube.NewCache(dataDir, 2*time.Hour)
	feedCache := youtube.NewFeedCache(dataDir)
	newsFetcher := news.NewFetcher(nil)

	s := &Service{
		storage:     existingStorage,
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

	groups, _ := s.storage.ListGroups()
	if len(groups) == 0 {
		log.Printf("[INFO] YouTube Review: No groups found, skipping review")
		return
	}

	for _, group := range groups {
		for _, ch := range group.Channels {
			needsRefresh := ch.Title == "" || ch.Title == ch.ID || ch.Name == "" || ch.Name == ch.ID || ch.Thumbnail == ""
			needsLanguage := ch.Language == "" || ch.Language == "unknown"

			if needsRefresh || needsLanguage {
				log.Printf("[INFO] YouTube Review: Refreshing metadata & language for channel %s in group %s...", ch.ID, group.Name)

				var realTitle string
				var thumbnail string
				detectedLang := ch.Language

				if s.ytService != nil {
					if authCh := s.ytService.GetAuthChannel(ch.ID); authCh != nil {
						realTitle = authCh.Title
						thumbnail = authCh.Thumbnail
						if authCh.Language != "" && authCh.Language != "unknown" {
							detectedLang = authCh.Language
						}
						log.Printf("[OK] YouTube Review: Found OAuth metadata for %s -> %q", ch.ID, realTitle)
					}
				}

				if realTitle == "" {
					channelURL := ch.URL
					if channelURL == "" || !strings.HasPrefix(channelURL, "http") {
						channelURL = "https://www.youtube.com/channel/" + ch.ID
					}
					info, err := s.apiClient.GetChannelInfo(context.Background(), channelURL)
					if err == nil && info != nil {
						realTitle = info.Title
						thumbnail = info.Thumbnail
						log.Printf("[OK] YouTube Review: Fetched API info for %s -> %q", ch.ID, realTitle)
					} else {
						log.Printf("[WARN] YouTube Review: Failed to fetch channel info for %s: %v", ch.ID, err)
					}
				}

				if realTitle == "" {
					realTitle = ch.ID
				}

				if detectedLang == "" || detectedLang == "unknown" {
					detectedLang = youtube.DetectLanguageFromName(realTitle)
					if detectedLang == "" {
						detectedLang = "en"
					}
				}

				_ = s.storage.UpdateChannelMetadata(group.Name, ch.ID, realTitle, realTitle, thumbnail)
				_, _ = s.storage.UpdateChannelLanguage(group.Name, ch.ID, detectedLang)

				if s.ytService != nil && s.ytService.GetAuthChannel(ch.ID) != nil {
					_ = s.ytService.UpdateChannelMetadata(ch.ID, map[string]interface{}{
						"title":     realTitle,
						"thumbnail": thumbnail,
						"language":  detectedLang,
					})
				}

				log.Printf("[OK] YouTube Review: Resolved channel %s -> %q [%s]", ch.ID, realTitle, detectedLang)
			}
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
func (s *Service) ListGroups() (map[string]*youtube.Group, []string) {
	groups, trackedNiches := s.storage.ListGroups()
	for name, group := range groups {
		if group == nil || len(group.Channels) == 0 {
			delete(groups, name)
		}
	}
	return groups, trackedNiches
}

// CreateGroup registers a new channel group.
func (s *Service) CreateGroup(name string) error {
	return s.storage.CreateGroup(name, "manager")
}

// DeleteGroup deletes a channel group.
func (s *Service) DeleteGroup(name string) error {
	return s.storage.DeleteGroup(name)
}

// CleanupOldData cleans stale historical entries.
func (s *Service) CleanupOldData(retention time.Duration) int {
	return s.storage.CleanupOldData(retention)
}

// CleanupCache flushes API-level caches.
func (s *Service) CleanupCache() int {
	return s.apiClient.CleanupCache()
}

// DataRetentionCleanup processes general DB cleanup.
func (s *Service) DataRetentionCleanup() int {
	if s.dataDir == "" {
		return 0
	}
	total := 0
	total += s.storage.CleanupOldData(13 * 24 * time.Hour)
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

// LoadStorageData loads YouTube storage data.
func (s *Service) LoadStorageData() *youtube.StorageData {
	return s.storage.LoadData()
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
		return nil, fmt.Errorf("YouTube integration service not configured")
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

// Storage returns storage instance.
func (s *Service) Storage() *youtube.Storage {
	return s.storage
}
