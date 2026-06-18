package youtube

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/integrations/news"
	"velox-server/internal/integrations/youtube"
)

// managerStatsCacheTTL bounds the in-memory cache for the aggregated
// manager-stats response. YouTube Data API v3 charges quota per ValidateToken
// call (channels.list uses 1 unit); keeping the cache above the typical
// 5-minute operator refresh interval avoids burning ~50-200 units per dashboard
// refresh while keeping the visible state "fresh enough" for ops decisions.
const managerStatsCacheTTL = 5 * time.Minute

// YouTubeManager holds the dependencies for YouTube manager handlers
type YouTubeManager struct {
	storage     *youtube.Storage
	apiClient   *youtube.APIClient
	feedCache   *youtube.FeedCache
	newsFetcher *news.Fetcher
	dataDir     string
	service     *youtube.Service

	// statsCacheMu guards statsCacheEntry. Reads are via RLock so concurrent
	// refreshes can proceed while dashboards serve cached payloads.
	statsCacheMu    sync.RWMutex
	statsCacheEntry *statsCacheEntry
}

// statsCacheEntry is the cached aggregate payload + its freshness horizon.
type statsCacheEntry struct {
	data        ManagerStatsResponse
	generatedAt time.Time
	expiresAt   time.Time
}

// ManagerStatsResponse is the JSON shape returned by ManagerStatsHandler.
// `CacheHit` lets callers distinguish fresh aggregations from cached ones;
// `QuotaSkippedChannels` reports how many channels were served from the
// channel-file presence check alone (no ValidateToken API call) so dashboards
// can flag quota-skip behavior.
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

// ManagerGroupStats is the per-group breakdown embedded in ManagerStatsResponse.
type ManagerGroupStats struct {
	GroupName    string                `json:"group_name"`
	ChannelCount int                   `json:"channel_count"`
	ValidCount   int                   `json:"valid_count"`
	InvalidCount int                   `json:"invalid_count"`
	Channels     []ManagerChannelStats `json:"channels"`
}

// ManagerChannelStats is the per-channel row; only the fields that came back
// from ValidateToken are populated (Title is filled when present).
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

// NewYouTubeManager creates a new YouTube manager handler instance.
// The remote-scraper fallback URL has been removed: when apiKey is empty
// the manager degrades to file-presence stats only (no ValidateToken
// calls) and the operator sees a clear "service not configured" signal.
func NewYouTubeManager(dataDir, apiKey string, existingStorage *youtube.Storage, ytService *youtube.Service) *YouTubeManager {
	var storage *youtube.Storage
	if existingStorage != nil {
		storage = existingStorage
	}

	cache := youtube.NewCache(dataDir, 2*time.Hour)
	feedCache := youtube.NewFeedCache(dataDir)
	newsFetcher := news.NewFetcher(nil)

	ym := &YouTubeManager{
		storage:     storage,
		apiClient:   youtube.NewAPIClient(apiKey, cache),
		feedCache:   feedCache,
		newsFetcher: newsFetcher,
		dataDir:     dataDir,
		service:     ytService,
	}

	go ym.reviewAndRefreshChannels()

	return ym
}

func (ym *YouTubeManager) reviewAndRefreshChannels() {
	time.Sleep(3 * time.Second)
	log.Printf("[REVIEW] YouTube Review: Starting background review of database channels...")

	groups, _ := ym.storage.ListGroups()
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

				if ym.service != nil {
					if authCh := ym.service.GetAuthChannel(ch.ID); authCh != nil {
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
				info, err := ym.apiClient.GetChannelInfo(context.Background(), channelURL)
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

				_ = ym.storage.UpdateChannelMetadata(group.Name, ch.ID, realTitle, realTitle, thumbnail)
				_, _ = ym.storage.UpdateChannelLanguage(group.Name, ch.ID, detectedLang)

				// If it's an OAuth channel, sync back to Service too
				if ym.service != nil && ym.service.GetAuthChannel(ch.ID) != nil {
					_ = ym.service.UpdateChannelMetadata(ch.ID, map[string]interface{}{
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

// CleanupOldData purges YouTube data older than the retention period
func (ym *YouTubeManager) CleanupOldData(retention time.Duration) int {
	return ym.storage.CleanupOldData(retention)
}

// CleanupCache removes expired entries from the API cache
func (ym *YouTubeManager) CleanupCache() int {
	return ym.apiClient.CleanupCache()
}

// DataRetentionCleanup performs a comprehensive cleanup of all YouTube cached data.
// SQLite is the single source of truth — no legacy JSON files are touched.
func (ym *YouTubeManager) DataRetentionCleanup() int {
	if ym.dataDir == "" {
		log.Printf("[WARN] YouTube Policy: dataDir not set, skipping data retention cleanup")
		return 0
	}

	total := 0
	total += ym.storage.CleanupOldData(13 * 24 * time.Hour)
	ym.feedCache.Clear()
	total++

	log.Printf("[CLEANUP] YouTube Policy: data retention cleanup complete (%d entries cleared)", total)
	return total
}

// --- Groups Handlers ---

// ListGroupsHandler returns all groups and their channels
func (ym *YouTubeManager) ListGroupsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		groups, trackedNiches := ym.storage.ListGroups()
		for name, group := range groups {
			if group == nil || len(group.Channels) == 0 {
				delete(groups, name)
			}
		}

		c.JSON(http.StatusOK, youtube.GroupsListResponse{
			OK:            true,
			Groups:        groups,
			TrackedNiches: trackedNiches,
		})
	}
}

// CreateGroupHandler creates a new group
func (ym *YouTubeManager) CreateGroupHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req youtube.CreateGroupRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "Invalid request: " + err.Error(),
			})
			return
		}

		name := strings.TrimSpace(req.Name)
		if name == "" {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "Group name cannot be empty",
			})
			return
		}

		if err := ym.storage.CreateGroup(name, "manager"); err != nil {
			if err == youtube.ErrGroupExists {
				c.JSON(http.StatusConflict, youtube.APIResponse{
					OK:    false,
					Error: "Group already exists",
				})
				return
			}
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{
				OK:    false,
				Error: err.Error(),
			})
			return
		}

		ym.feedCache.Clear()

		c.JSON(http.StatusOK, youtube.APIResponse{
			OK:      true,
			Message: "Group '" + name + "' created",
		})
	}
}

// DeleteGroupHandler deletes a group
func (ym *YouTubeManager) DeleteGroupHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		groupName := c.Param("group_name")

		if err := ym.storage.DeleteGroup(groupName); err != nil {
			if err == youtube.ErrGroupNotFound {
				c.JSON(http.StatusNotFound, youtube.APIResponse{
					OK:    false,
					Error: "Group not found",
				})
				return
			}
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{
				OK:    false,
				Error: err.Error(),
			})
			return
		}

		ym.feedCache.Clear()

		c.JSON(http.StatusOK, youtube.APIResponse{
			OK:      true,
			Message: "Group '" + groupName + "' deleted",
		})
	}
}

// --- Helper functions ---

// isVideoURL checks if a URL is a YouTube video URL
func isVideoURL(url string) bool {
	url = strings.ToLower(url)
	return strings.Contains(url, "watch?v=") ||
		strings.Contains(url, "youtu.be/") ||
		strings.Contains(url, "/shorts/") ||
		strings.Contains(url, "/embed/") ||
		strings.Contains(url, "/live/")
}

// extractKeywords extracts keywords from a string
func extractKeywords(s string) []string {
	s = strings.ToLower(s)
	words := strings.FieldsFunc(s, func(r rune) bool {
		return r == ' ' || r == ',' || r == '.' || r == '!' || r == '?' || r == '-' || r == '_'
	})

	var keywords []string
	for _, word := range words {
		word = strings.TrimSpace(word)
		if len(word) > 3 {
			keywords = append(keywords, word)
		}
	}

	if len(keywords) > 10 {
		keywords = keywords[:10]
	}

	return keywords
}

// --- Manager Stats endpoint ---
//
// ManagerStatsHandler returns the aggregate per-group channel + token-validity
// snapshot. Cached for managerStatsCacheTTL to bound YouTube Data API quota
// consumption (each ValidateToken is a channels.list call).
//
// Query params:
//   - refresh=1|true  force a fresh aggregation (skips cache, repopulates it).
//
// Cache strategy:
//   - Cache HIT  -> returns the cached payload immediately with X-Cache: HIT
//     and X-Cache-Age-Seconds header.  Bodies still flag cached=true.
//   - Cache MISS -> invokes aggregateManagerStats under a 30s timeout,
//     populates the cache, returns with X-Cache: MISS and cached=false.
//   - Concurrent-miss race: between RUnlock and Lock a sibling goroutine may
//     have populated statsCacheEntry; we then redundantly aggregate and
//     overwrite.  This is best-effort / last-writer-wins and is intentional
//     (the small extra API cost is bounded by managerStatsCacheTTL).
func (ym *YouTubeManager) ManagerStatsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		forceRefresh := isTruthyQuery(c.Query("refresh"))

		if !forceRefresh {
			ym.statsCacheMu.RLock()
			cached := ym.statsCacheEntry
			ym.statsCacheMu.RUnlock()

			if cached != nil && time.Now().Before(cached.expiresAt) {
				resp := cached.data
				resp.Cached = true
				resp.CacheAgeSeconds = int(time.Since(cached.generatedAt).Seconds())
				c.Header("X-Cache", "HIT")
				c.Header("X-Cache-Age-Seconds", strconv.Itoa(resp.CacheAgeSeconds))
				c.JSON(http.StatusOK, resp)
				return
			}
		}

		ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
		defer cancel()

		resp, err := ym.aggregateManagerStats(ctx)
		if err != nil {
			c.JSON(http.StatusInternalServerError, ManagerStatsResponse{
				OK:     false,
				Groups: map[string]ManagerGroupStats{}, // keep schema stable on error
				Error:  err.Error(),
			})
			return
		}
		resp.Cached = false
		resp.CacheAgeSeconds = 0
		resp.GeneratedAt = time.Now().UTC()
		resp.ExpiresAt = resp.GeneratedAt.Add(managerStatsCacheTTL)

		ym.statsCacheMu.Lock()
		ym.statsCacheEntry = &statsCacheEntry{
			data:        resp,
			generatedAt: resp.GeneratedAt,
			expiresAt:   resp.ExpiresAt,
		}
		ym.statsCacheMu.Unlock()

		c.Header("X-Cache", "MISS")
		c.JSON(http.StatusOK, resp)
	}
}

// isTruthyQuery accepts the canonical truthy spellings from query strings:
// "1", "true", "t", "yes", "y" (case-insensitive, trimmed).  Keeps the
// bool-parse behavior consistent with internal/config[boolFromEnv] so ops
// don't need to remember two separate vocabularies.
func isTruthyQuery(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "t", "yes", "y":
		return true
	}
	return false
}

// aggregateManagerStats builds the JSON aggregation by walking the storage
// groups and calling ValidateToken per channel. If ym.service is nil
// (e.g. minimal install), the response degrades gracefully (valid=false,
// error_message set, but file-presence still reported).
func (ym *YouTubeManager) aggregateManagerStats(ctx context.Context) (ManagerStatsResponse, error) {
	if ym.storage == nil {
		return ManagerStatsResponse{}, fmt.Errorf("storage not configured")
	}

	groups, _ := ym.storage.ListGroups()

	resp := ManagerStatsResponse{
		OK:                true,
		Groups:            make(map[string]ManagerGroupStats, len(groups)),
		ServiceConfigured: ym.service != nil,
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
				HasTokenFile: ym.channelHasTokenFile(ch.ID),
				Valid:        false,
			}

			if ym.service != nil {
				result, _ := ym.service.ValidateToken(ctx, ch.ID)
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

// channelHasTokenFile reports whether a token file exists for this channel.
// The token-dir candidates mirror the storage fallback chain in
// `service.go::NewService`.
func (ym *YouTubeManager) channelHasTokenFile(channelID string) bool {
	if ym.dataDir == "" || channelID == "" {
		return false
	}
	candidates := []string{
		filepath.Join(ym.dataDir, "youtube", "tokens", "account_"+channelID+".json"),
		filepath.Join(ym.dataDir, "secrets", "youtube", "tokens", "account_"+channelID+".json"),
		filepath.Join(ym.dataDir, "youtube", "Token", "account_"+channelID+".json"),
	}
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return true
		}
	}
	return false
}

// asBool safely extracts a bool from a ValidateToken response map.
// Local to this file because ValidateToken's return shape (map[string]any)
// is not a stable cross-package contract — other packages use typed structs
// (TokenChannelInfo, AuthChannel) and have their own typed extractors.
func asBool(m map[string]interface{}, key string) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}
	return false
}

// asString safely extracts a string from a ValidateToken response map.
// See asBool for rationale on file-local scope.
func asString(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}
