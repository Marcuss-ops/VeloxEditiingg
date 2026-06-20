package youtube

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
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

type statsCacheEntry struct {
	data        ManagerStatsResponse
	generatedAt time.Time
	expiresAt   time.Time
}

// ManagerStatsResponse is the stats model.
type ManagerStatsResponse struct {
	OK                   bool                         `json:"ok"`
	Cached               bool                         `json:"cached"`
	GeneratedAt          time.Time                    `json:"generated_at"`
	ExpiresAt            time.Time                    `json:"expires_at"`
	CacheAgeSeconds      int                          `json:"cache_age_seconds"`
	TotalGroups          int                          `json:"total_groups"`
	TotalChannels        int                          `json:"total_channels"`
	ValidChannels        int                          `json:"valid_channels"`
	InvalidChannels      int                          `json:"invalid_channels"`
	QuotaSkippedChannels int                          `json:"quota_skipped_channels"`
	ServiceConfigured    bool                         `json:"service_configured"`
	Groups               map[string]ManagerGroupStats `json:"groups"`
	Error                string                       `json:"error,omitempty"`
}

type ManagerGroupStats struct {
	GroupName    string                `json:"group_name"`
	ChannelCount int                   `json:"channel_count"`
	ValidCount   int                   `json:"valid_count"`
	InvalidCount int                   `json:"invalid_count"`
	Channels     []ManagerChannelStats `json:"channels"`
}

type ManagerChannelStats struct {
	ChannelID         string `json:"channel_id"`
	Title             string `json:"title,omitempty"`
	HasTokenFile      bool   `json:"has_token_file"`
	Valid             bool   `json:"valid"`
	IsExpired         bool   `json:"is_expired"`
	HasRefreshToken   bool   `json:"has_refresh_token"`
	RefreshedThisCall bool   `json:"refreshed_this_call"`
	ErrorMessage      string `json:"error,omitempty"`
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

func (s *Service) DownloadThumbnail(ctx context.Context, thumbURL string) (io.ReadCloser, int64, string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", thumbURL, nil)
	if err != nil {
		return nil, 0, "", err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, "", err
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, 0, "", fmt.Errorf("failed to fetch image: status %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "image/jpeg"
	}

	return resp.Body, resp.ContentLength, contentType, nil
}

func (s *Service) GetVideoInfo(ctx context.Context, videoID string) (youtube.Video, error) {
	info, err := s.apiClient.SearchVideos(ctx, videoID, 1, 365, 0, 0, false)
	if err != nil {
		return youtube.Video{}, err
	}
	if len(info) > 0 {
		return info[0], nil
	}
	return youtube.Video{URL: "https://www.youtube.com/watch?v=" + videoID}, nil
}

func (s *Service) ScrapeVideo(ctx context.Context, url string) (youtube.Video, error) {
	videoInfo, err := s.apiClient.SearchVideos(ctx, url, 1, 365, 0, 0, false)
	if err != nil {
		return youtube.Video{}, err
	}
	if len(videoInfo) > 0 {
		return videoInfo[0], nil
	}
	return youtube.Video{URL: url}, nil
}

func (s *Service) GenerateScript(query, language string) string {
	return fmt.Sprintf(`# Script: %s

## Introduzione (0:00-0:30)
Benvenuti a questo video su %s!

## Corpo principale (0:30-5:00)
- Punto 1
- Punto 2
- Punto 3

## Conclusione (5:00-5:30)
Grazie per aver guardato! Iscrivetevi per altri contenuti.

---
Language: %s
Generated: %s
`, query, query, language, time.Now().Format("2006-01-02 15:04:05"))
}

func (s *Service) ViralSearch(ctx context.Context, query string, limit int, filterDate string, minViews int64, minVelocity float64, hideShorts bool) ([]youtube.Video, error) {
	days := 30
	switch filterDate {
	case "today":
		days = 2
	case "week":
		days = 7
	case "month":
		days = 30
	case "all":
		days = 365
	}

	return s.apiClient.SearchVideos(ctx, query, limit, days, minViews, minVelocity, hideShorts)
}

func (s *Service) DiscoverySearch(ctx context.Context, query string, days, limit int, minViews int64, minVelocity float64, hideShorts bool) ([]youtube.Video, error) {
	return s.apiClient.SearchVideos(ctx, query, limit, days, minViews, minVelocity, hideShorts)
}

func (s *Service) SimilarChannels(ctx context.Context, searchQuery string, limit int) ([]youtube.SimilarChannelHit, error) {
	videos, err := s.apiClient.SearchVideos(ctx, searchQuery, 20, 30, 0, 0, false)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool)
	var channels []youtube.SimilarChannelHit

	for _, v := range videos {
		chURL := v.ChannelURL
		chTitle := v.Uploader
		if chURL != "" && !seen[chURL] && chTitle != "" {
			seen[chURL] = true
			channels = append(channels, youtube.SimilarChannelHit{
				Title:     chTitle,
				URL:       chURL,
				Thumbnail: v.Thumbnail,
				ViewCount: v.ViewCount,
				Velocity:  v.Velocity,
			})
		}
		if len(channels) >= limit {
			break
		}
	}
	return channels, nil
}

func (s *Service) AutoSimilarChannels(ctx context.Context, limit int, minVelocity int64) ([]youtube.SimilarChannelHit, []string, int, error) {
	data := s.storage.LoadData()

	var allChannels []youtube.Channel
	seenURLs := make(map[string]bool)
	allKeywords := make(map[string]bool)

	for _, group := range data.Groups {
		for _, ch := range group.Channels {
			if !seenURLs[ch.URL] {
				seenURLs[ch.URL] = true
				allChannels = append(allChannels, ch)
				for _, kw := range ch.Keywords {
					if len(kw) > 2 {
						allKeywords[strings.ToLower(kw)] = true
					}
				}
			}
		}
	}

	if len(allChannels) == 0 {
		return []youtube.SimilarChannelHit{}, []string{}, 0, nil
	}

	var keywordsList []string
	for kw := range allKeywords {
		keywordsList = append(keywordsList, kw)
		if len(keywordsList) >= 15 {
			break
		}
	}

	searchQuery := strings.Join(keywordsList, " ")
	videos, _ := s.apiClient.SearchVideos(ctx, searchQuery, 50, 30, 0, float64(minVelocity), false)

	seenChannelURLs := make(map[string]bool)
	for _, ch := range allChannels {
		seenChannelURLs[ch.URL] = true
	}

	var similar []youtube.SimilarChannelHit
	seenResults := make(map[string]bool)

	for _, v := range videos {
		chURL := v.ChannelURL
		if chURL != "" && !seenResults[chURL] && !seenChannelURLs[chURL] && v.Uploader != "" {
			seenResults[chURL] = true
			similar = append(similar, youtube.SimilarChannelHit{
				Title:     v.Uploader,
				URL:       chURL,
				Thumbnail: v.Thumbnail,
				ViewCount: v.ViewCount,
				Velocity:  v.Velocity,
				Reason:    "Related to: " + searchQuery[:50] + "...",
			})
		}
	}

	sort.Slice(similar, func(i, j int) bool {
		return similar[i].Velocity > similar[j].Velocity
	})

	if len(similar) > limit {
		similar = similar[:limit]
	}

	return similar, keywordsList, len(allChannels), nil
}

func (s *Service) Trends(ctx context.Context, query string) ([]youtube.TrendTopic, error) {
	videos, err := s.apiClient.SearchVideos(ctx, query+" news", 10, 7, 1000, 0, false)
	if err != nil {
		return nil, err
	}

	var trends []youtube.TrendTopic
	for _, v := range videos {
		viewStr := fmt.Sprintf("%d", v.ViewCount)
		trends = append(trends, youtube.TrendTopic{
			Title:     v.Title,
			URL:       v.URL,
			Views:     viewStr,
			Thumbnail: v.Thumbnail,
		})
	}
	return trends, nil
}

func (s *Service) AIDigest(ctx context.Context, query string) (string, []youtube.Video, error) {
	videos, err := s.apiClient.SearchVideos(ctx, query, 5, 7, 0, 0, false)
	if err != nil {
		return "", nil, err
	}

	var digest strings.Builder
	digest.WriteString(fmt.Sprintf("# %s - Weekly Digest\n\n", query))
	digest.WriteString(fmt.Sprintf("Found %d trending videos this week.\n\n", len(videos)))

	for i, v := range videos {
		digest.WriteString(fmt.Sprintf("%d. **%s**\n", i+1, v.Title))
		digest.WriteString(fmt.Sprintf("   - Views: %d\n", v.ViewCount))
		digest.WriteString(fmt.Sprintf("   - Channel: %s\n\n", v.Uploader))
	}

	return digest.String(), videos, nil
}

func (s *Service) GetVideoFeed(ctx context.Context, groupName, timeRange, sortBy string) (*youtube.FeedResponse, error) {
	data := s.storage.LoadData()
	var targetChannels []youtube.Channel

	if groupName != "" {
		if group, ok := data.Groups[groupName]; ok {
			targetChannels = group.Channels
		} else {
			return nil, fmt.Errorf("group not found: %s", groupName)
		}
	} else {
		for _, group := range data.Groups {
			targetChannels = append(targetChannels, group.Channels...)
		}
	}

	if len(targetChannels) == 0 {
		return &youtube.FeedResponse{
			OK:     true,
			Videos: []youtube.Video{},
			Count:  0,
		}, nil
	}

	seenURLs := make(map[string]bool)
	var uniqueChannels []youtube.Channel
	for _, ch := range targetChannels {
		if !seenURLs[ch.URL] {
			seenURLs[ch.URL] = true
			uniqueChannels = append(uniqueChannels, ch)
		}
	}

	daysBack := 21
	limitPerChannel := 10
	switch timeRange {
	case "today":
		daysBack = 2
		limitPerChannel = 4
	case "week":
		daysBack = 30
		limitPerChannel = 12
	case "twoweeks":
		daysBack = 45
		limitPerChannel = 16
	case "month":
		daysBack = 90
		limitPerChannel = 24
	case "all":
		daysBack = 365
		limitPerChannel = 30
	}

	var aggregatedVideos []youtube.Video

	for i, ch := range uniqueChannels {
		if i >= 15 {
			break
		}

		channelID, err := s.apiClient.GetChannelID(ctx, ch.URL)
		if err != nil {
			continue
		}

		var videos []youtube.Video
		if channelID != "" {
			videos, err = s.apiClient.GetRecentChannelVideos(ctx, channelID, limitPerChannel, daysBack)
			if err != nil {
				log.Printf("[FEED] GetRecentChannelVideos failed for %s: %v", ch.Title, err)
			}
		}

		if len(videos) == 0 {
			videos, _ = s.apiClient.SearchVideos(ctx, ch.Title, limitPerChannel, daysBack, 0, 0, false)
		}

		for i := range videos {
			videos[i].SourceChannel = ch.Title
			videos[i].GroupName = groupName
			if videos[i].Thumbnail == "" && ch.Thumbnail != "" {
				videos[i].Thumbnail = ch.Thumbnail
			}
		}

		aggregatedVideos = append(aggregatedVideos, videos...)
	}

	if sortBy == "views" {
		sort.Slice(aggregatedVideos, func(i, j int) bool {
			return aggregatedVideos[i].ViewCount > aggregatedVideos[j].ViewCount
		})
	} else {
		sort.Slice(aggregatedVideos, func(i, j int) bool {
			return aggregatedVideos[i].UploadDate > aggregatedVideos[j].UploadDate
		})
	}

	return &youtube.FeedResponse{
		OK:     true,
		Group:  groupName,
		Videos: aggregatedVideos,
		Count:  len(aggregatedVideos),
	}, nil
}

func (s *Service) RefreshAllGroupsFeed(ctx context.Context) (int, error) {
	data := s.storage.LoadData()
	if len(data.Groups) == 0 {
		return 0, nil
	}

	totalVideos := 0
	for groupName := range data.Groups {
		count, err := s.RefreshGroupFeed(ctx, groupName)
		if err != nil {
			continue
		}
		totalVideos += count
	}

	return totalVideos, nil
}

func (s *Service) RefreshGroupFeed(ctx context.Context, groupName string) (int, error) {
	group, ok := s.storage.LoadData().Groups[groupName]
	if !ok {
		return 0, fmt.Errorf("group not found: %s", groupName)
	}

	if len(group.Channels) == 0 {
		return 0, nil
	}

	daysBack := 30
	limitPerChannel := 12

	var aggregatedVideos []youtube.Video

	for i, ch := range group.Channels {
		if i >= 15 {
			break
		}

		channelID, err := s.apiClient.GetChannelID(ctx, ch.URL)
		if err != nil {
			continue
		}

		var videos []youtube.Video
		if channelID != "" {
			videos, err = s.apiClient.GetRecentChannelVideos(ctx, channelID, limitPerChannel, daysBack)
			if err != nil {
				log.Printf("[FEED] GetRecentChannelVideos failed for %s: %v", ch.Title, err)
			}
		}

		if len(videos) == 0 {
			videos, _ = s.apiClient.SearchVideos(ctx, ch.Title, limitPerChannel, daysBack, 0, 0, false)
		}

		for i := range videos {
			videos[i].SourceChannel = ch.Title
			videos[i].GroupName = groupName
			if videos[i].Thumbnail == "" && ch.Thumbnail != "" {
				videos[i].Thumbnail = ch.Thumbnail
			}
		}

		aggregatedVideos = append(aggregatedVideos, videos...)
	}

	sort.Slice(aggregatedVideos, func(i, j int) bool {
		return aggregatedVideos[i].UploadDate > aggregatedVideos[j].UploadDate
	})

	cacheKey := fmt.Sprintf("feed_%s_week_date", groupName)
	result := &youtube.FeedResponse{
		OK:     true,
		Group:  groupName,
		Videos: aggregatedVideos,
		Count:  len(aggregatedVideos),
	}
	s.feedCache.Set(cacheKey, result)

	return len(aggregatedVideos), nil
}

func (s *Service) ResolveChannel(ctx context.Context, urlParam string) (*youtube.ChannelInfo, error) {
	return s.apiClient.GetChannelInfo(ctx, urlParam)
}

func (s *Service) TrendingNews(ctx context.Context, query string, limit int) ([]news.NewsItem, error) {
	return s.newsFetcher.FetchTrendingNews(ctx, query, limit)
}

func (s *Service) ListGroups() (map[string]*youtube.Group, []string) {
	groups, trackedNiches := s.storage.ListGroups()
	for name, group := range groups {
		if group == nil || len(group.Channels) == 0 {
			delete(groups, name)
		}
	}
	return groups, trackedNiches
}

func (s *Service) CreateGroup(name string) error {
	return s.storage.CreateGroup(name, "manager")
}

func (s *Service) DeleteGroup(name string) error {
	return s.storage.DeleteGroup(name)
}

func (s *Service) CleanupOldData(retention time.Duration) int {
	return s.storage.CleanupOldData(retention)
}

func (s *Service) CleanupCache() int {
	return s.apiClient.CleanupCache()
}

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

func (s *Service) GetCachedStats(forceRefresh bool, ctx context.Context) (ManagerStatsResponse, bool, int, string, error) {
	if !forceRefresh {
		s.statsCacheMu.RLock()
		cached := s.statsCacheEntry
		s.statsCacheMu.RUnlock()

		if cached != nil && time.Now().Before(cached.expiresAt) {
			resp := cached.data
			age := int(time.Since(cached.generatedAt).Seconds())
			return resp, true, age, "HIT", nil
		}
	}

	resp, err := s.aggregateManagerStats(ctx)
	if err != nil {
		return ManagerStatsResponse{OK: false, Groups: map[string]ManagerGroupStats{}}, false, 0, "", err
	}

	resp.Cached = false
	resp.CacheAgeSeconds = 0
	resp.GeneratedAt = time.Now().UTC()
	resp.ExpiresAt = resp.GeneratedAt.Add(managerStatsCacheTTL)

	s.statsCacheMu.Lock()
	s.statsCacheEntry = &statsCacheEntry{
		data:        resp,
		generatedAt: resp.GeneratedAt,
		expiresAt:   resp.ExpiresAt,
	}
	s.statsCacheMu.Unlock()

	return resp, false, 0, "MISS", nil
}

func (s *Service) aggregateManagerStats(ctx context.Context) (ManagerStatsResponse, error) {
	if s.storage == nil {
		return ManagerStatsResponse{}, fmt.Errorf("storage not configured")
	}

	groups, _ := s.storage.ListGroups()

	resp := ManagerStatsResponse{
		OK:                true,
		Groups:            make(map[string]ManagerGroupStats, len(groups)),
		ServiceConfigured: s.ytService != nil,
	}

	for _, group := range groups {
		gs := ManagerGroupStats{
			GroupName: group.Name,
			Channels:  make([]ManagerChannelStats, 0, len(group.Channels)),
		}
		for _, ch := range group.Channels {
			stat := ManagerChannelStats{
				ChannelID:    ch.ID,
				Title:        ch.Title,
				HasTokenFile: s.ChannelHasTokenFile(ch.ID),
				Valid:        false,
			}

			if s.ytService != nil {
				result, _ := s.ytService.ValidateOAuthAccessToken(ctx, ch.ID)
				stat.Valid = asBool(result, "valid")
				stat.IsExpired = asBool(result, "is_expired")
				stat.HasRefreshToken = asBool(result, "has_refresh_token")
				stat.RefreshedThisCall = asBool(result, "refreshed")
				stat.ErrorMessage = asString(result, "error")
				if title, ok := result["channel_title"].(string); ok && title != "" && stat.Title == "" {
					stat.Title = title
				}
			} else {
				stat.ErrorMessage = "service not configured (degraded mode)"
				resp.QuotaSkippedChannels++
			}

			gs.Channels = append(gs.Channels, stat)
			gs.ChannelCount++
			if stat.Valid {
				gs.ValidCount++
			} else {
				gs.InvalidCount++
			}
		}
		resp.Groups[group.Name] = gs
		resp.TotalChannels += gs.ChannelCount
		resp.ValidChannels += gs.ValidCount
		resp.InvalidChannels += gs.InvalidCount
	}
	resp.TotalGroups = len(groups)

	return resp, nil
}

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

func (s *Service) FeedCacheSet(key string, val *youtube.FeedResponse) {
	s.feedCache.Set(key, val)
}

func (s *Service) FeedCacheGet(key string) (*youtube.FeedResponse, bool) {
	return s.feedCache.Get(key)
}

func (s *Service) FeedCacheClear() {
	s.feedCache.Clear()
}

func (s *Service) LoadStorageData() *youtube.StorageData {
	return s.storage.LoadData()
}

func (s *Service) GetAuthChannels() []*youtube.AuthChannel {
	if s.ytService == nil {
		return nil
	}
	return s.ytService.GetAuthChannels()
}

func (s *Service) GetAuthChannel(id string) *youtube.AuthChannel {
	if s.ytService == nil {
		return nil
	}
	return s.ytService.GetAuthChannel(id)
}

func (s *Service) ValidateOAuthAccessToken(ctx context.Context, id string) (map[string]interface{}, error) {
	if s.ytService == nil {
		return nil, fmt.Errorf("YouTube integration service not configured")
	}
	return s.ytService.ValidateOAuthAccessToken(ctx, id)
}

func (s *Service) GetUndefinedChannels() []*youtube.Channel {
	if s.ytService == nil {
		return nil
	}
	return s.ytService.GetUndefinedChannels()
}

func (s *Service) Storage() *youtube.Storage {
	return s.storage
}

func (s *Service) APIClient() *youtube.APIClient {
	return s.apiClient
}

func (s *Service) DataDir() string {
	return s.dataDir
}

