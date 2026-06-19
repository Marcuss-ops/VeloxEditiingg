package youtube

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// ListVideos lists videos for a channel
// GET /api/v1/youtube/videos
func (h *YouTubeHandlers) ListVideos(c *gin.Context) {
	channelID := c.Query("channel_id")
	maxResults := int64(50)

	if mr := c.Query("max_results"); mr != "" {
		if parsed, err := strconv.ParseInt(mr, 10, 64); err == nil {
			maxResults = parsed
		}
	}

	if channelID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "channel_id query parameter is required"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()

	videos, err := h.service.ListVideos(ctx, channelID, maxResults)
	if err != nil {
		errMsg := err.Error()
		if errMsg == "channel not found: "+channelID ||
			errMsg == "OAuth not configured" ||
			strings.Contains(errMsg, "token expired and refresh token is not set") ||
			strings.Contains(errMsg, "invalid_grant") ||
			strings.Contains(errMsg, "refresh token is not set") ||
			strings.Contains(errMsg, "oauth2:") {
			log.Printf("[WARN] ListVideos skipped for %s: %s", channelID, errMsg)
			c.JSON(http.StatusOK, gin.H{
				"ok":     true,
				"videos": []gin.H{},
				"count":  0,
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	result := make([]gin.H, 0, len(videos))
	for _, v := range videos {
		result = append(result, gin.H{
			"video_id":       v.Id,
			"title":          v.Snippet.Title,
			"description":    v.Snippet.Description,
			"privacy_status": v.Status.PrivacyStatus,
			"view_count":     v.Statistics.ViewCount,
			"published_at":   v.Snippet.PublishedAt,
			"thumbnail":      v.Snippet.Thumbnails.Default.Url,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":     true,
		"videos": result,
		"count":  len(result),
	})
}

// ListGroupPrivateVideos lists private videos for all authenticated channels in a group
// GET /api/v1/youtube/group-private-videos?group_name=X
func (h *YouTubeHandlers) ListGroupPrivateVideos(c *gin.Context) {
	groupName := c.Query("group_name")
	if groupName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "group_name required"})
		return
	}

	days := 90
	if daysStr := c.Query("days"); daysStr != "" {
		if parsed, err := strconv.Atoi(daysStr); err == nil && parsed > 0 {
			days = parsed
		}
	}
	cutoff := time.Now().AddDate(0, 0, -days)
	cacheKey := fmt.Sprintf("%s:%d", groupName, days)

	data := h.storage.LoadData()
	group, ok := data.Groups[groupName]
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "group not found: " + groupName})
		return
	}

	if len(group.Channels) == 0 {
		c.JSON(http.StatusOK, gin.H{"ok": true, "videos": []gin.H{}, "count": 0, "group": groupName})
		return
	}

	refresh := c.Query("refresh") == "true" || c.Query("force") == "true"
	if !refresh {
		h.privateVideosCacheMu.RLock()
		cache, found := h.privateVideosCache[cacheKey]
		h.privateVideosCacheMu.RUnlock()

		if found && time.Since(cache.Timestamp) < 12*time.Hour {
			c.JSON(http.StatusOK, gin.H{
				"ok":     true,
				"videos": cache.Videos,
				"count":  len(cache.Videos),
				"group":  groupName,
				"days":   days,
				"cached": true,
			})
			return
		}
	}

	ctx := c.Request.Context()
	var allVideos []gin.H
	seen := make(map[string]bool)

	for _, ch := range group.Channels {
		chID := strings.TrimSpace(ch.ID)
		if chID == "" {
			continue
		}

		videos, err := h.service.ListVideos(ctx, chID, 50)
		if err != nil {
			log.Printf("[WARN] ListGroupPrivateVideos: skipped channel %s (%s): %v", ch.Title, chID, err)
			continue
		}

		for _, v := range videos {
			vid := v.Id
			if vid == "" || seen[vid] {
				continue
			}
			seen[vid] = true

			privacy := v.Status.PrivacyStatus
			if privacy != "private" {
				continue
			}

			publishedAt := strings.TrimSpace(v.Snippet.PublishedAt)
			if publishedAt == "" {
				continue
			}
			pubTime, err := time.Parse(time.RFC3339, publishedAt)
			if err != nil {
				log.Printf("[WARN] ListGroupPrivateVideos: skipped video %s (%s) with invalid published_at %q: %v", vid, chID, publishedAt, err)
				continue
			}
			if pubTime.Before(cutoff) {
				continue
			}

			thumbnail := ""
			if v.Snippet.Thumbnails != nil && v.Snippet.Thumbnails.Default != nil {
				thumbnail = v.Snippet.Thumbnails.Default.Url
			}

			allVideos = append(allVideos, gin.H{
				"video_id":       vid,
				"title":          v.Snippet.Title,
				"description":    v.Snippet.Description,
				"privacy_status": privacy,
				"view_count":     v.Statistics.ViewCount,
				"published_at":   v.Snippet.PublishedAt,
				"thumbnail":      thumbnail,
				"channel_id":     chID,
				"channel_title":  ch.Title,
			})
		}
	}

	h.privateVideosCacheMu.Lock()
	h.privateVideosCache[cacheKey] = PrivateVideosCacheEntry{
		Videos:    allVideos,
		Timestamp: time.Now(),
	}
	h.privateVideosCacheMu.Unlock()

	c.JSON(http.StatusOK, gin.H{
		"ok":     true,
		"videos": allVideos,
		"count":  len(allVideos),
		"group":  groupName,
		"days":   days,
	})
}

// RefreshAnalytics refreshes analytics data for a channel
// GET /api/v1/youtube/analytics/refresh/:id
func (h *YouTubeHandlers) RefreshAnalytics(c *gin.Context) {
	channelID := c.Param("id")
	daysStr := c.DefaultQuery("days", "30")
	days, err := strconv.Atoi(daysStr)
	if err != nil {
		days = 30
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 1*time.Minute)
	defer cancel()

	data, err := h.service.FetchAnalytics(ctx, channelID, days)
	if err != nil {
		log.Printf("[ERROR] Analytics fetch failed for %s: %v", channelID, err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"ok":    false,
			"error": err.Error(),
		})
		return
	}

	err = h.service.UpdateAnalyticsCache(ctx, channelID, days, data)
	if err != nil {
		log.Printf("[ERROR] Analytics cache update failed for %s: %v", channelID, err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"ok":    false,
			"error": fmt.Sprintf("Failed to update cache: %v", err),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":      true,
		"message": "Analytics refreshed successfully",
		"data":    data,
	})
}
