package youtube

import (
	"context"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/integrations/news"
	"velox-server/internal/integrations/youtube"
)

// YouTubeManager holds the dependencies for YouTube manager handlers
type YouTubeManager struct {
	storage      *youtube.Storage
	apiClient    *youtube.APIClient
	feedCache    *youtube.FeedCache
	newsFetcher  *news.Fetcher
	dataDir      string
	service      *youtube.Service
}

// NewYouTubeManager creates a new YouTube manager handler instance.
func NewYouTubeManager(dataDir, apiKey, fallbackURL string, existingStorage *youtube.Storage, ytService *youtube.Service) *YouTubeManager {
	var storage *youtube.Storage
	if existingStorage != nil {
		storage = existingStorage
	}

	cache := youtube.NewCache(dataDir, 2*time.Hour)
	feedCache := youtube.NewFeedCache(dataDir)
	newsFetcher := news.NewFetcher(nil)

	ym := &YouTubeManager{
		storage:     storage,
		apiClient:   youtube.NewAPIClient(apiKey, cache, fallbackURL),
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	for _, group := range groups {
		for _, ch := range group.Channels {
			needsRefresh := ch.Title == "" || ch.Title == ch.ID || ch.Name == "" || ch.Name == ch.ID
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
						log.Printf("[OK] YouTube Review: Found OAuth metadata for %s -> %q (Preserved language: %s)", ch.ID, realTitle, detectedLang)
					}
				}

				if realTitle == "" {
					channelURL := "https://www.youtube.com/channel/" + ch.ID
					info, err := ym.apiClient.GetChannelInfo(ctx, channelURL)
					if err == nil && info != nil {
						realTitle = info.Title
						thumbnail = info.Thumbnail
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
