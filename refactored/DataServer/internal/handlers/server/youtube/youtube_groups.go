// Package handlers provides HTTP handlers for the Velox server.
package youtube

import (
	"context"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
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
// If existingStorage is non-nil, it reuses it (for unified Storage with YouTubeHandlers).
func NewYouTubeManager(dataDir, apiKey, fallbackURL string, existingStorage *youtube.Storage, ytService *youtube.Service) *YouTubeManager {
	var storage *youtube.Storage
	var err error
	if existingStorage != nil {
		storage = existingStorage
	} else {
		storage, err = youtube.NewStorage(dataDir)
		if err != nil {
			storage, _ = youtube.NewStorage("")
		}
	}

	cache := youtube.NewCache(dataDir, 2*time.Hour)
	feedCache := youtube.NewFeedCache(dataDir)
	newsFetcher := news.NewFetcher(nil) // No API keys yet, uses free Google News

	ym := &YouTubeManager{
		storage:      storage,
		apiClient:    youtube.NewAPIClient(apiKey, cache, fallbackURL),
		feedCache:    feedCache,
		newsFetcher:  newsFetcher,
		dataDir:      dataDir,
		service:      ytService,
	}

	// Trigger database cleanup & background review and refresh of all channels to fetch real names and detect languages
	go ym.reviewAndRefreshChannels()

	return ym
}

// reviewAndRefreshChannels runs in the background on startup.
// It scans all channels in storage (ChannelsSaved.json). For channels without a real name
// (where title is empty or is equal to ID), it fetches their real title & thumbnail via the API client.
// It then auto-detects their language based on their actual retrieved title and saves them back to storage.
func (ym *YouTubeManager) reviewAndRefreshChannels() {
	// Wait a moment for server initialization
	time.Sleep(3 * time.Second)
	log.Printf("🔍 YouTube Review: Starting background review of database channels...")

	groups, err := ym.storage.ListGroups()
	if err != nil {
		log.Printf("⚠️ YouTube Review: Failed to list groups: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	for _, group := range groups {
		for _, ch := range group.Channels {
			// Check if title is empty or just the ID itself (needs info refresh)
			needsRefresh := ch.Title == "" || ch.Title == ch.ID || ch.Name == "" || ch.Name == ch.ID
			needsLanguage := ch.Language == "" || ch.Language == "unknown"

			if needsRefresh || needsLanguage {
				log.Printf("🔄 YouTube Review: Refreshing metadata & language for channel %s in group %s...", ch.ID, group.Name)
				
				var realTitle string
				var thumbnail string
				detectedLang := ch.Language
				
				// 1. Try to get metadata from OAuth Service first (offline-first & robust)
				if ym.service != nil {
					if authCh := ym.service.GetAuthChannel(ch.ID); authCh != nil {
						realTitle = authCh.Title
						thumbnail = authCh.Thumbnail
						if authCh.Language != "" && authCh.Language != "unknown" {
							detectedLang = authCh.Language
						}
						log.Printf("✅ YouTube Review: Found OAuth metadata for %s -> %q (Preserved language: %s)", ch.ID, realTitle, detectedLang)
					}
				}
				
				// 2. If not found in OAuth Service, try fetching from API/Fallback
				if realTitle == "" {
					channelURL := "https://www.youtube.com/channel/" + ch.ID
					info, err := ym.apiClient.GetChannelInfo(ctx, channelURL)
					if err == nil && info != nil {
						realTitle = info.Title
						thumbnail = info.Thumbnail
					} else {
						log.Printf("⚠️ YouTube Review: Failed to fetch channel info for %s: %v", ch.ID, err)
					}
				}
				
				if realTitle == "" {
					realTitle = ch.ID
				}

				// Determine correct language from real title only if not already resolved from OAuth
				if detectedLang == "" || detectedLang == "unknown" {
					detectedLang = youtube.DetectLanguageFromName(realTitle)
					if detectedLang == "" {
						detectedLang = "en"
					}
				}

				// Update Storage (ChannelsSaved.json)
				_ = ym.storage.UpdateChannelMetadata(group.Name, ch.ID, realTitle, realTitle, thumbnail)
				_, _ = ym.storage.UpdateChannelLanguage(group.Name, ch.ID, detectedLang)
				
				log.Printf("✅ YouTube Review: Resolved channel %s -> %q [%s]", ch.ID, realTitle, detectedLang)
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

// DataRetentionCleanup performs a comprehensive cleanup of all YouTube cached data
// to comply with YouTube's data retention policies (max 13 days).
// It clears channels.json, API cache, feed cache, analytics cache, upload history,
// and all channel metadata from ChannelsSaved.json.
// OAuth tokens, group definitions, and credentials are NOT touched.
// Returns the number of data entries cleaned up.
func (ym *YouTubeManager) DataRetentionCleanup() int {
	if ym.dataDir == "" {
		log.Printf("⚠️ YouTube Policy: dataDir not set, skipping data retention cleanup")
		return 0
	}

	total := 0

	// 1. Clean channel metadata from ChannelsSaved.json (Title, Thumbnail, stats, keywords)
	total += ym.storage.CleanupOldData(13 * 24 * time.Hour)

	// 2. Clear the on-disk API cache file
	youtubeAPICachePath := filepath.Join(ym.dataDir, "youtube", "youtube_api_cache.json")
	if _, err := os.Stat(youtubeAPICachePath); err == nil {
		if err := os.WriteFile(youtubeAPICachePath, []byte("{}"), 0644); err == nil {
			log.Printf("🧹 YouTube Policy: cleared youtube_api_cache.json")
			total++
		}
	}

	// 3. Clear channels.json from all possible locations - Skipped to preserve user-configured language settings
	// which are NOT YouTube API-derived data and should not be wiped.

	// 4. Clear feed cache
	ym.feedCache.Clear()
	total++

	// 5. Clear analytics cache files
	analyticsDir := filepath.Join(ym.dataDir, "analytics")
	analyticsFiles := []string{
		"analytics_cache.json",
		"analytics_realtime_cache.json",
		"feed_cache.json",
	}
	for _, f := range analyticsFiles {
		fp := filepath.Join(analyticsDir, f)
		if _, err := os.Stat(fp); err == nil {
			if err := os.WriteFile(fp, []byte("{}"), 0644); err == nil {
				log.Printf("🧹 YouTube Policy: cleared %s", f)
				total++
			}
		}
	}

	// 6. Clear upload history
	uploadHistoryPaths := []string{
		filepath.Join(ym.dataDir, "youtube", "history", "upload_history.json"),
	}
	for _, uploadHistoryPath := range uploadHistoryPaths {
		if _, err := os.Stat(uploadHistoryPath); err == nil {
			if err := os.WriteFile(uploadHistoryPath, []byte("[]"), 0644); err == nil {
				log.Printf("🧹 YouTube Policy: cleared upload_history.json")
				total++
			}
		}
	}

	log.Printf("🧹 YouTube Policy: data retention cleanup complete (%d entries cleared)", total)
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

		// Clear feed cache when groups change
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

		// Clear feed cache when groups change
		ym.feedCache.Clear()

		c.JSON(http.StatusOK, youtube.APIResponse{
			OK:      true,
			Message: "Group '" + groupName + "' deleted",
		})
	}
}

// --- Channel Handlers ---

// AddChannelHandler adds a channel to a group
func (ym *YouTubeManager) AddChannelHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		groupName := c.Param("group_name")

		var req youtube.AddChannelRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "Invalid request: " + err.Error(),
			})
			return
		}

		url := strings.TrimSpace(req.URL)
		if url == "" {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "URL cannot be empty",
			})
			return
		}

		// Check if group exists
		if _, ok := ym.storage.GetGroup(groupName); !ok {
			c.JSON(http.StatusNotFound, youtube.APIResponse{
				OK:    false,
				Error: "Group not found",
			})
			return
		}

		// Check if URL is a video URL
		if isVideoURL(url) {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "Invalid Channel URL. Please provide a channel URL or handle (@name).",
			})
			return
		}

		// Resolve channel info using API
		ctx := c.Request.Context()
		channelInfo, err := ym.apiClient.GetChannelInfo(ctx, url)
		if err != nil {
			// Log but continue with fallback
			channelInfo = &youtube.ChannelInfo{URL: url, Title: req.Title, Thumbnail: req.Thumbnail}
		}

		// Prepare channel data
		channelTitle := req.Title
		channelThumbnail := req.Thumbnail
		resolvedURL := url

		if channelInfo != nil && channelInfo.URL != "" {
			resolvedURL = channelInfo.URL
			if channelInfo.Title != "" {
				channelTitle = channelInfo.Title
			}
			if channelInfo.Thumbnail != "" {
				channelThumbnail = channelInfo.Thumbnail
			}
		}

		// Check if still a video URL after resolution
		if isVideoURL(resolvedURL) {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "Invalid Channel URL resolved. Please provide a channel URL or handle (@name).",
			})
			return
		}

		// Extract keywords from title/description
		var keywords []string
		if channelTitle != "" {
			keywords = extractKeywords(channelTitle)
		}
		if channelInfo != nil && channelInfo.Description != "" {
			keywords = append(keywords, extractKeywords(channelInfo.Description)...)
			// Deduplicate
			seen := make(map[string]bool)
			var unique []string
			for _, k := range keywords {
				if !seen[k] && len(unique) < 10 {
					seen[k] = true
					unique = append(unique, k)
				}
			}
			keywords = unique
		}

		channel := youtube.Channel{
			ID:        strconv.FormatInt(time.Now().UnixMilli(), 10),
			URL:       resolvedURL,
			Title:     channelTitle,
			Thumbnail: channelThumbnail,
			Notes:     req.Notes,
			AddedAt:   time.Now(),
			Keywords:  keywords,
		}

		if err := ym.storage.AddChannel(groupName, channel); err != nil {
			if err == youtube.ErrChannelExists {
				c.JSON(http.StatusConflict, youtube.APIResponse{
					OK:    false,
					Error: "Channel already in group",
				})
				return
			}
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{
				OK:    false,
				Error: err.Error(),
			})
			return
		}

		// Clear feed cache when channels change
		ym.feedCache.Clear()

		c.JSON(http.StatusOK, youtube.APIResponse{
			OK:      true,
			Message: "Channel added",
			Data:    channel,
		})
	}
}

// DeleteChannelHandler removes a channel from a group
func (ym *YouTubeManager) DeleteChannelHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		groupName := c.Param("group_name")
		channelID := c.Param("channel_id")

		if err := ym.storage.RemoveChannel(groupName, channelID); err != nil {
			if err == youtube.ErrGroupNotFound {
				c.JSON(http.StatusNotFound, youtube.APIResponse{
					OK:    false,
					Error: "Group not found",
				})
				return
			}
			if err == youtube.ErrChannelNotFound {
				c.JSON(http.StatusNotFound, youtube.APIResponse{
					OK:    false,
					Error: "Channel not found in group",
				})
				return
			}
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{
				OK:    false,
				Error: err.Error(),
			})
			return
		}

		// Clear feed cache when channels change
		ym.feedCache.Clear()

		c.JSON(http.StatusOK, youtube.APIResponse{
			OK:      true,
			Message: "Channel removed",
		})
	}
}

// MoveChannelHandler moves a channel between groups
func (ym *YouTubeManager) MoveChannelHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		sourceGroup := c.Param("group_name")
		channelID := c.Param("channel_id")

		var req youtube.MoveChannelRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "Invalid request: " + err.Error(),
			})
			return
		}

		if err := ym.storage.MoveChannel(sourceGroup, channelID, req.TargetGroup); err != nil {
			if err == youtube.ErrGroupNotFound || err == youtube.ErrTargetGroupNotFound {
				c.JSON(http.StatusNotFound, youtube.APIResponse{
					OK:    false,
					Error: "Source or target group not found",
				})
				return
			}
			if err == youtube.ErrChannelNotFound {
				c.JSON(http.StatusNotFound, youtube.APIResponse{
					OK:    false,
					Error: "Channel not found in source group",
				})
				return
			}
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{
				OK:    false,
				Error: err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, youtube.APIResponse{
			OK:      true,
			Message: "Moved to " + req.TargetGroup,
		})
	}
}

// RefreshChannelStatsHandler updates stats for a channel
func (ym *YouTubeManager) RefreshChannelStatsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		groupName := c.Param("group_name")
		channelID := c.Param("channel_id")

		group, ok := ym.storage.GetGroup(groupName)
		if !ok {
			c.JSON(http.StatusNotFound, youtube.APIResponse{
				OK:    false,
				Error: "Group not found",
			})
			return
		}

		// Find the channel
		var channel *youtube.Channel
		for _, ch := range group.Channels {
			if ch.ID == channelID {
				channel = &ch
				break
			}
		}

		if channel == nil {
			c.JSON(http.StatusNotFound, youtube.APIResponse{
				OK:    false,
				Error: "Channel not found",
			})
			return
		}

		// Get fresh stats from API
		ctx := c.Request.Context()
		info, err := ym.apiClient.GetChannelInfo(ctx, channel.URL)
		if err != nil {
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{
				OK:    false,
				Error: "Failed to fetch channel stats: " + err.Error(),
			})
			return
		}

		// Update storage
		var viewCount, subCount int64
		if info != nil {
			// Note: In real implementation, would fetch from API
			// For now, just update timestamp
		}

		if err := ym.storage.UpdateChannelStats(groupName, channelID, viewCount, subCount); err != nil {
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{
				OK:    false,
				Error: err.Error(),
			})
			return
		}

		// Return updated channel
		updatedGroup, _ := ym.storage.GetGroup(groupName)
		for _, ch := range updatedGroup.Channels {
			if ch.ID == channelID {
				c.JSON(http.StatusOK, youtube.APIResponse{
					OK:   true,
					Data: ch,
				})
				return
			}
		}

		c.JSON(http.StatusOK, youtube.APIResponse{
			OK:   true,
			Data: channel,
		})
	}
}

// DeleteChannelPermanentlyHandler removes a channel from its group and deletes its token file
// DELETE /api/youtube/manager/channels/:channel_id/permanent
func (ym *YouTubeManager) DeleteChannelPermanentlyHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		channelID := c.Param("channel_id")

		// Find and remove channel from all groups
		var foundGroup string
		groups, _ := ym.storage.ListGroups()
		for groupName, group := range groups {
			for _, ch := range group.Channels {
				if ch.ID == channelID {
					foundGroup = groupName
					break
				}
			}
			if foundGroup != "" {
				break
			}
		}

		// Remove from group if found
		if foundGroup != "" {
			ym.storage.RemoveChannel(foundGroup, channelID)
		}

		// Delete token file
		tokenDeleted := false
		if ym.dataDir != "" {
			tokenPaths := []string{
				filepath.Join(ym.dataDir, "youtube", "group", foundGroup, "account_"+channelID+".json"),
				filepath.Join(ym.dataDir, "youtube", "Token", "account_"+channelID+".json"),
			}
			for _, tp := range tokenPaths {
				if _, err := os.Stat(tp); err == nil {
					if err := os.Remove(tp); err == nil {
						tokenDeleted = true
						log.Printf("🗑️ Deleted token file: %s", tp)
					}
				}
			}
		}

		// Clear feed cache
		ym.feedCache.Clear()

		c.JSON(http.StatusOK, youtube.APIResponse{
			OK:      true,
			Message: "Channel permanently deleted",
			Data: gin.H{
				"channel_id":    channelID,
				"removed_from":  foundGroup,
				"token_deleted": tokenDeleted,
			},
		})
	}
}

// youtubeChannelIDRegex matches standard YouTube channel IDs (UC prefix + alphanumeric/hyphens)
var youtubeChannelIDRegex = regexp.MustCompile(`^UC[\w-]{21,22}$`)

// MoveChannelToGroupHandler moves a channel to a target group (drag & drop support)
// POST /api/youtube/manager/channels/:channel_id/move-to/:target_group
//
// If the channel is found in an existing group, it moves it.
// If the channel is NOT found but the ID looks like a YouTube channel ID (UC...),
// it adds the channel to the target group instead of returning 404.
// This handles undefined/upload channels being dragged into a manager group.
func (ym *YouTubeManager) MoveChannelToGroupHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		channelID := c.Param("channel_id")
		targetGroup := c.Param("target_group")

		// Find channel in current groups
		var sourceGroup string
		groups, _ := ym.storage.ListGroups()
		for groupName, group := range groups {
			for _, ch := range group.Channels {
				if ch.ID == channelID {
					sourceGroup = groupName
					break
				}
			}
			if sourceGroup != "" {
				break
			}
		}

		// If channel not found in any group AND has a YouTube channel ID format,
		// treat this as an "add to group" operation
		if sourceGroup == "" {
			if !youtubeChannelIDRegex.MatchString(channelID) {
				c.JSON(http.StatusNotFound, youtube.APIResponse{
					OK:    false,
					Error: "Channel not found in any group",
				})
				return
			}

			// It's a YouTube channel ID — create the target group if needed and add the channel
			if _, ok := ym.storage.GetGroup(targetGroup); !ok {
				if err := ym.storage.CreateGroup(targetGroup, "manager"); err != nil {
					c.JSON(http.StatusInternalServerError, youtube.APIResponse{
						OK:    false,
						Error: "Failed to create target group: " + err.Error(),
					})
					return
				}
			}

			// Build a channel entry from the YouTube channel ID
			channelURL := "https://www.youtube.com/channel/" + channelID

			// Try to resolve via API for better metadata
			ctx := c.Request.Context()
			channelTitle := ""
			channelThumbnail := ""
			if info, err := ym.apiClient.GetChannelInfo(ctx, channelURL); err == nil && info != nil {
				if info.Title != "" {
					channelTitle = info.Title
				}
				if info.Thumbnail != "" {
					channelThumbnail = info.Thumbnail
				}
			}
			if channelTitle == "" {
				channelTitle = channelID
			}

			ch := youtube.Channel{
				ID:        channelID,
				URL:       channelURL,
				Title:     channelTitle,
				Thumbnail: channelThumbnail,
				AddedAt:   time.Now(),
				Notes:     "Added via drag & drop / bulk move",
			}

			if err := ym.storage.AddChannel(targetGroup, ch); err != nil {
				if err == youtube.ErrChannelExists {
					c.JSON(http.StatusConflict, youtube.APIResponse{
						OK:    false,
						Error: "Channel already in group",
					})
					return
				}
				c.JSON(http.StatusInternalServerError, youtube.APIResponse{
					OK:    false,
					Error: err.Error(),
				})
				return
			}

			// Move token file if exists
			if ym.dataDir != "" {
				targetDir := filepath.Join(ym.dataDir, "youtube", "group", targetGroup)
				sourceTokenPath := filepath.Join(ym.dataDir, "youtube", "Token", "account_"+channelID+".json")
				targetTokenPath := filepath.Join(targetDir, "account_"+channelID+".json")

				if _, err := os.Stat(sourceTokenPath); err == nil {
					os.MkdirAll(targetDir, 0755)
					if err := os.Rename(sourceTokenPath, targetTokenPath); err == nil {
						log.Printf("📦 Moved token file to %s", targetGroup)
					}
				}
			}

			// Clear feed cache
			ym.feedCache.Clear()

			c.JSON(http.StatusOK, youtube.APIResponse{
				OK:      true,
				Message: "Channel added to group",
				Data: gin.H{
					"channel_id":   channelID,
					"source_group": nil,
					"target_group": targetGroup,
				},
			})
			return
		}

		// Channel found — standard move operation
		// Check if target group exists, create if not
		if _, ok := ym.storage.GetGroup(targetGroup); !ok {
			if err := ym.storage.CreateGroup(targetGroup, "manager"); err != nil {
				c.JSON(http.StatusInternalServerError, youtube.APIResponse{
					OK:    false,
					Error: "Failed to create target group: " + err.Error(),
				})
				return
			}
		}

		// Move the channel
		if err := ym.storage.MoveChannel(sourceGroup, channelID, targetGroup); err != nil {
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{
				OK:    false,
				Error: err.Error(),
			})
			return
		}

		// Move token file if exists
		if ym.dataDir != "" {
			sourceTokenPath := filepath.Join(ym.dataDir, "youtube", "group", sourceGroup, "account_"+channelID+".json")
			targetDir := filepath.Join(ym.dataDir, "youtube", "group", targetGroup)
			targetTokenPath := filepath.Join(targetDir, "account_"+channelID+".json")

			if _, err := os.Stat(sourceTokenPath); err == nil {
				os.MkdirAll(targetDir, 0755)
				if err := os.Rename(sourceTokenPath, targetTokenPath); err == nil {
					log.Printf("📦 Moved token file from %s to %s", sourceGroup, targetGroup)
				}
			}
		}

		// Clear feed cache
		ym.feedCache.Clear()

		c.JSON(http.StatusOK, youtube.APIResponse{
			OK:      true,
			Message: "Channel moved successfully",
			Data: gin.H{
				"channel_id":   channelID,
				"source_group": sourceGroup,
				"target_group": targetGroup,
			},
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
	// Simple keyword extraction: split by common separators and filter
	s = strings.ToLower(s)
	words := strings.FieldsFunc(s, func(r rune) bool {
		return r == ' ' || r == ',' || r == '.' || r == '!' || r == '?' || r == '-' || r == '_'
	})

	var keywords []string
	for _, word := range words {
		word = strings.TrimSpace(word)
		if len(word) > 3 { // Skip short words
			keywords = append(keywords, word)
		}
	}

	// Limit to 10 keywords
	if len(keywords) > 10 {
		keywords = keywords[:10]
	}

	return keywords
}
