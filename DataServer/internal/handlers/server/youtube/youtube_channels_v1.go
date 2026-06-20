package youtube

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/integrations/youtube"
)

// ── ListChannels ────────────────────────────────────────────────────────────

// ListChannels lists all available YouTube channels.
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
			"url":       ch.URL,
			"name":      ch.Name,
			"title":     ch.Title,
			"thumbnail": ch.Thumbnail,
			"email":     ch.Email,
			"language":  ch.Language,
		}

		if validate {
			ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
			defer cancel()

			validation, err := h.service.ValidateOAuthAccessToken(ctx, ch.ID)
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
			channelData["token_valid"] = true
		}

		result = append(result, channelData)
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":       true,
		"channels": result,
		"count":    len(result),
	})
}

// ── GetChannel ──────────────────────────────────────────────────────────────

// GetChannel gets a specific channel.
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
			"url":       channel.URL,
			"name":      channel.Name,
			"title":     channel.Title,
			"thumbnail": channel.Thumbnail,
			"email":     channel.Email,
		},
	})
}

// ── DeleteChannel ───────────────────────────────────────────────────────────

// DeleteChannel deletes a channel permanently.
// DELETE /api/v1/youtube/channels/:id
func (h *YouTubeHandlers) DeleteChannel(c *gin.Context) {
	channelID := c.Param("id")

	groups, _ := h.storage.ListGroups()
	for groupName, group := range groups {
		for _, ch := range group.Channels {
			if ch.ID == channelID {
				h.storage.RemoveChannel(groupName, channelID)
				break
			}
		}
	}

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

// ── UpdateChannel ───────────────────────────────────────────────────────────

// UpdateChannel handles updating channel metadata.
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

	if err := h.service.UpdateChannelMetadata(channelID, req); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"ok":    false,
			"error": err.Error(),
		})
		return
	}

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
		"ok":      true,
		"message": "Channel metadata updated",
		"channel": channelID,
	})
}

// ── AutoDetectLanguage ──────────────────────────────────────────────────────

// AutoDetectLanguage auto-detects the language for a channel.
// POST /api/v1/youtube/channels/:id/language/auto-detect
func (h *YouTubeHandlers) AutoDetectLanguage(c *gin.Context) {
	channelID := c.Param("id")
	channelName := c.Query("channel_name")

	if channelName == "" {
		if ch := h.service.GetAuthChannel(channelID); ch != nil {
			channelName = ch.Title
			if channelName == "" {
				channelName = ch.Name
			}
		}
	}

	lang := h.service.DetectChannelLanguage(c.Request.Context(), channelID, channelName)

	_ = h.service.UpdateChannelMetadata(channelID, map[string]interface{}{
		"language": lang,
	})

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
		"ok":            true,
		"channel_id":    channelID,
		"channel_name":  channelName,
		"language_code": lang,
		"language_name": languageCodeToName(lang),
		"flag":          languageCodeToFlag(lang),
		"auto_detected": true,
	})
}

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

// youtubeChannelIDs returns the list of channel IDs in a group.
func youtubeChannelIDs(channels []youtube.Channel) []string {
	ids := make([]string, len(channels))
	for i, ch := range channels {
		ids[i] = ch.ID
	}
	return ids
}
