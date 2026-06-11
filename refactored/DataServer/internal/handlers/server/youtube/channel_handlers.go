package youtube

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
)

// ListChannels lists all available YouTube channels
// GET /api/v1/youtube/channels
func (h *YouTubeHandlers) ListChannels(c *gin.Context) {
	validateParam := c.Query("validate_tokens")
	if validateParam == "" {
		validateParam = c.Query("validate")
	}
	validate := validateParam == "true"

	channels := h.service.GetChannels()

	result := make([]gin.H, 0, len(channels))
	for _, ch := range channels {
		channelData := gin.H{
			"id":        ch.ID,
			"name":      ch.Name,
			"title":     ch.Title,
			"thumbnail": ch.Thumbnail,
			"email":     ch.Email,
			"language":  ch.Language,
		}

		// Optionally validate tokens
		if validate {
			ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
			defer cancel()

			validation, err := h.service.ValidateToken(ctx, ch.ID)
			if err != nil {
				channelData["token_valid"] = false
			} else if ok, exists := validation["valid"].(bool); exists {
				channelData["token_valid"] = ok
			} else if ok, exists := validation["ok"].(bool); exists {
				channelData["token_valid"] = ok
			} else {
				channelData["token_valid"] = false
			}
		} else {
			channelData["token_valid"] = true // Assume valid if not validating
		}

		result = append(result, channelData)
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":       true,
		"channels": result,
		"count":    len(result),
	})
}

// GetChannel gets a specific channel
// GET /api/v1/youtube/channels/:id
func (h *YouTubeHandlers) GetChannel(c *gin.Context) {
	channelID := c.Param("id")

	channel := h.service.GetChannel(channelID)
	if channel == nil {
		c.JSON(http.StatusNotFound, gin.H{
			"ok":    false,
			"error": "Channel not found",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"ok": true,
		"channel": gin.H{
			"id":        channel.ID,
			"name":      channel.Name,
			"title":     channel.Title,
			"thumbnail": channel.Thumbnail,
			"email":     channel.Email,
		},
	})
}

// DeleteChannel deletes a channel permanently (removes from all groups, Storage, and deletes token)
// DELETE /api/v1/youtube/channels/:id
func (h *YouTubeHandlers) DeleteChannel(c *gin.Context) {
	channelID := c.Param("id")

	// Remove from unified Storage groups first (all types)
	groups, _ := h.storage.ListGroups()
	for groupName, group := range groups {
		for _, ch := range group.Channels {
			if ch.ID == channelID {
				h.storage.RemoveChannel(groupName, channelID)
				break
			}
		}
	}

	// Then remove via Service (handles token deletion and in-memory cleanup)
	err := h.service.DeleteChannel(channelID)
	if err != nil {
		if err.Error() == "channel not found" {
			c.JSON(http.StatusNotFound, gin.H{
				"ok":    false,
				"error": "Channel not found",
			})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{
			"ok":    false,
			"error": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":      true,
		"message": fmt.Sprintf("Channel '%s' deleted successfully", channelID),
	})
}

// RefreshChannelsMetadata refreshes the title and thumbnail for all channels with OAuth tokens
// POST /api/v1/youtube/channels/refresh-metadata
func (h *YouTubeHandlers) RefreshChannelsMetadata(c *gin.Context) {
	ctx := c.Request.Context()

	successCount, errors := h.service.RefreshAllChannelsMetadata(ctx)

	errStrings := make([]string, 0, len(errors))
	for _, err := range errors {
		errStrings = append(errStrings, err.Error())
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":             true,
		"refreshed":      successCount,
		"errors":         errStrings,
		"error_count":    len(errors),
		"message":        fmt.Sprintf("Refreshed metadata for %d channels", successCount),
	})
}

// GetChannelAnalytics returns analytics data for a specific channel from the analytics cache
// GET /api/v1/youtube/analytics/channel/:id?days=7
func (h *YouTubeHandlers) GetChannelAnalytics(c *gin.Context) {
    channelID := c.Param("id")
    daysStr := c.DefaultQuery("days", "7")

    dataDir := h.service.GetConfig().DataDir
    if dataDir == "" {
        c.JSON(http.StatusOK, gin.H{
            "ok":       true,
            "channel":  channelID,
            "days":     daysStr,
            "totals":   gin.H{},
            "stats":    []interface{}{},
            "message":  "Data directory not configured",
        })
        return
    }

    cachePath := filepath.Join(dataDir, "analytics", "analytics_cache.json")
    cacheData, err := os.ReadFile(cachePath)
    if err != nil {
        c.JSON(http.StatusOK, gin.H{
            "ok":       true,
            "channel":  channelID,
            "days":     daysStr,
            "totals":   gin.H{},
            "stats":    []interface{}{},
            "message":  "No analytics cache available yet. Run SyncAllAnalytics first.",
        })
        return
    }

    var cache map[string]interface{}
    if err := json.Unmarshal(cacheData, &cache); err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{
            "ok":    false,
            "error": "Failed to parse analytics cache",
        })
        return
    }

    // Look for the requested period entry
    periodKey := daysStr
    periodEntry, ok := cache[periodKey].(map[string]interface{})
    if !ok {
        // Try as number ("7" vs 7)
        for k, v := range cache {
            if fmt.Sprintf("%v", k) == daysStr {
                if entry, ok := v.(map[string]interface{}); ok {
                    periodEntry = entry
                    break
                }
            }
        }
    }

    if periodEntry == nil {
        c.JSON(http.StatusOK, gin.H{
            "ok":       true,
            "channel":  channelID,
            "days":     daysStr,
            "totals":   gin.H{},
            "stats":    []interface{}{},
            "message":  "No data for this period",
        })
        return
    }

    // Extract data
    entryData, _ := periodEntry["data"].(map[string]interface{})
    if entryData == nil {
        c.JSON(http.StatusOK, gin.H{
            "ok":       true,
            "channel":  channelID,
            "days":     daysStr,
            "totals":   gin.H{},
            "stats":    []interface{}{},
        })
        return
    }

    // Find channel-specific data in the channels array
    channels, _ := entryData["channels"].([]interface{})
    var channelStats map[string]interface{}
    for _, ch := range channels {
        if chMap, ok := ch.(map[string]interface{}); ok {
            if fmt.Sprintf("%v", chMap["id"]) == channelID {
                channelStats = chMap
                break
            }
        }
    }

    totals, _ := entryData["totals"].(map[string]interface{})

    // Also get daily_stats filtered for this channel
    dailyStats, _ := entryData["daily_stats"].([]interface{})

    c.JSON(http.StatusOK, gin.H{
        "ok":          true,
        "channel":     channelID,
        "days":        daysStr,
        "totals":      totals,
        "channel_data": channelStats,
        "stats":       dailyStats,
    })
}

// UpdateChannel handles updating channel metadata (language, token_status, etc.)
// PATCH /api/v1/youtube/channels/:id
func (h *YouTubeHandlers) UpdateChannel(c *gin.Context) {
	channelID := c.Param("id")

	var req map[string]interface{}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"ok":    false,
			"error": "Invalid request: " + err.Error(),
		})
		return
	}

	// Update Service first (OAuth channels.json)
	if err := h.service.UpdateChannelMetadata(channelID, req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"ok":    false,
			"error": err.Error(),
		})
		return
	}

	// Then sync language to Storage if present in request
	if lang, ok := req["language"].(string); ok && lang != "" {
		groups, _ := h.storage.ListGroups()
		for _, group := range groups {
			for i := range group.Channels {
				if group.Channels[i].ID == channelID {
					if _, err := h.storage.UpdateChannelLanguage(group.Name, channelID, lang); err != nil {
						log.Printf("[WARN] Failed to update language in storage for channel %s: %v", channelID, err)
					}
					break
				}
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":        true,
		"message":   "Channel metadata updated",
		"channel":   channelID,
	})
}

// AutoDetectLanguage auto-detects the language for a channel and saves it.
// POST /api/v1/youtube/channels/:id/language/auto-detect
func (h *YouTubeHandlers) AutoDetectLanguage(c *gin.Context) {
	channelID := c.Param("id")
	channelName := c.Query("channel_name")

	// Get channel name from service if not provided
	if channelName == "" {
		if ch := h.service.GetAuthChannel(channelID); ch != nil {
			channelName = ch.Title
			if channelName == "" {
				channelName = ch.Name
			}
		}
	}

	lang := h.service.DetectChannelLanguage(c.Request.Context(), channelID, channelName)

	// Save to Service (AuthChannel channels.json)
	_ = h.service.UpdateChannelMetadata(channelID, map[string]interface{}{
		"language": lang,
	})

	// Save to Storage (all groups where this channel exists)
	groups, _ := h.storage.ListGroups()
	for _, group := range groups {
		for i := range group.Channels {
			if group.Channels[i].ID == channelID {
				if _, err := h.storage.UpdateChannelLanguage(group.Name, channelID, lang); err != nil {
					log.Printf("[WARN] Failed to save language in storage for channel %s: %v", channelID, err)
				}
				break
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":              true,
		"channel_id":      channelID,
		"channel_name":    channelName,
		"language_code":   lang,
		"language_name":   languageCodeToName(lang),
		"flag":            languageCodeToFlag(lang),
		"auto_detected":   true,
	})
}

// Helper: map language code to readable name
func languageCodeToName(code string) string {
	names := map[string]string{
		"en": "English", "it": "Italiano", "es": "Español", "fr": "Français",
		"de": "Deutsch", "pt": "Português", "ru": "Русский", "ja": "日本語",
		"ko": "한국어", "zh": "中文", "ar": "العربية", "hi": "हिन्दी",
		"pl": "Polski", "tr": "Türkçe", "nl": "Nederlands",
	}
	if name, ok := names[code]; ok {
		return name
	}
	return "Unknown"
}

// Helper: map language code to flag emoji
func languageCodeToFlag(code string) string {
	flags := map[string]string{
		"en": "\U0001F1EC\U0001F1E7", "it": "\U0001F1EE\U0001F1F9", "es": "\U0001F1EA\U0001F1F8", "fr": "\U0001F1EB\U0001F1F7", "de": "\U0001F1E9\U0001F1EA",
		"pt": "\U0001F1F5\U0001F1F9", "ru": "\U0001F1F7\U0001F1FA", "ja": "\U0001F1EF\U0001F1F5", "ko": "\U0001F1F0\U0001F1F7", "zh": "\U0001F1E8\U0001F1F3",
		"ar": "\U0001F1F8\U0001F1E6", "hi": "\U0001F1EE\U0001F1F3", "pl": "\U0001F1F5\U0001F1F1", "tr": "\U0001F1F9\U0001F1F7", "nl": "\U0001F1F3\U0001F1F1",
	}
	if flag, ok := flags[code]; ok {
		return flag
	}
	return ""
}

// ListUndefinedChannels lists channels not in any upload group
// GET /api/v1/youtube/channels/undefined
// Uses the unified Storage to check group membership instead of Service.groups.
func (h *YouTubeHandlers) ListUndefinedChannels(c *gin.Context) {
	// Get all OAuth channels from Service
	authChannels := h.service.GetAuthChannels()

	// Get all upload groups from unified Storage
	groups, _ := h.storage.ListGroups()

	// Build set of channel IDs assigned to upload groups
	assigned := make(map[string]bool, len(authChannels))
	for _, g := range groups {
		if g.GroupType != "" && g.GroupType != "upload" {
			continue
		}
		for _, ch := range g.Channels {
			assigned[ch.ID] = true
		}
	}

	// Find channels not in any upload group
	result := make([]gin.H, 0)
	for _, ac := range authChannels {
		if !assigned[ac.ID] {
			result = append(result, gin.H{
				"id":        ac.ID,
				"name":      ac.Name,
				"title":     ac.Title,
				"thumbnail": ac.Thumbnail,
				"language":  ac.Language,
			})
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":       true,
		"channels": result,
		"count":    len(result),
	})
}
