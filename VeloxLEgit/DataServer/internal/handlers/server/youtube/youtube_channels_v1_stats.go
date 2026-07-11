package youtube

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// ── Stats & export ──────────────────────────────────────────────────────────

// DuplicateChannel represents a set of duplicate channels.
type DuplicateChannel struct {
	ChannelID string   `json:"channel_id"`
	Title     string   `json:"title"`
	Groups    []string `json:"groups"`
}

// GetChannelGroups returns all groups a channel belongs to.
// GET /api/v1/youtube/channels/:id/groups
func (h *YouTubeHandlers) GetChannelGroups(c *gin.Context) {
	channelID := c.Param("id")

	groups := h.service.GetGroups()
	memberGroups := []gin.H{}

	for name, cg := range groups {
		if cg == nil {
			continue
		}
		for _, chID := range cg.Channels {
			if chID == channelID {
				memberGroups = append(memberGroups, gin.H{
					"name":       name,
					"group_type": cg.GroupType,
				})
				break
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":      true,
		"channel": channelID,
		"groups":  memberGroups,
		"count":   len(memberGroups),
	})
}

// DetectDuplicateChannels finds channels that appear in multiple groups.
// GET /api/v1/youtube/channels/duplicates
func (h *YouTubeHandlers) DetectDuplicateChannels(c *gin.Context) {
	groups := h.service.GetGroups()

	channelGroups := make(map[string][]string)
	channelTitles := make(map[string]string)

	for name, cg := range groups {
		if cg == nil {
			continue
		}
		for _, chID := range cg.Channels {
			channelGroups[chID] = append(channelGroups[chID], name)
			if auth := h.service.GetAuthChannel(chID); auth != nil && auth.Title != "" {
				channelTitles[chID] = auth.Title
			}
		}
	}

	duplicates := []DuplicateChannel{}
	for chID, gNames := range channelGroups {
		if len(gNames) > 1 {
			duplicates = append(duplicates, DuplicateChannel{
				ChannelID: chID,
				Title:     channelTitles[chID],
				Groups:    gNames,
			})
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":         true,
		"duplicates": duplicates,
		"count":      len(duplicates),
	})
}

// ExportChannels exports all channels as JSON or CSV.
// GET /api/v1/youtube/channels/export?format=json|csv
func (h *YouTubeHandlers) ExportChannels(c *gin.Context) {
	format := c.DefaultQuery("format", "json")

	channels := h.service.GetAuthChannels()
	groups := h.service.GetGroups()

	channelGroupsMap := make(map[string][]string)
	for name, cg := range groups {
		if cg == nil {
			continue
		}
		for _, chID := range cg.Channels {
			channelGroupsMap[chID] = append(channelGroupsMap[chID], name)
		}
	}

	type exportChannel struct {
		ID        string   `json:"id" csv:"id"`
		Title     string   `json:"title" csv:"title"`
		Name      string   `json:"name" csv:"name"`
		Language  string   `json:"language" csv:"language"`
		Thumbnail string   `json:"thumbnail" csv:"thumbnail"`
		Groups    []string `json:"groups" csv:"groups"`
		HasToken  bool     `json:"has_token" csv:"has_token"`
	}

	exportData := make([]exportChannel, 0, len(channels))
	for _, ch := range channels {
		exportData = append(exportData, exportChannel{
			ID:        ch.ID,
			Title:     ch.Title,
			Name:      ch.Name,
			Language:  ch.Language,
			Thumbnail: ch.Thumbnail,
			Groups:    channelGroupsMap[ch.ID],
			HasToken:  ch.AccessToken != "" || ch.RefreshToken != "",
		})
	}

	if format == "csv" {
		c.Header("Content-Type", "text/csv")
		c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=channels_%s.csv", time.Now().Format("2006-01-02")))

		w := csv.NewWriter(c.Writer)
		w.Write([]string{"id", "title", "name", "language", "groups", "has_token"})
		for _, ch := range exportData {
			groupsStr := ""
			if len(ch.Groups) > 0 {
				b, _ := json.Marshal(ch.Groups)
				groupsStr = string(b)
			}
			w.Write([]string{ch.ID, ch.Title, ch.Name, ch.Language, groupsStr, fmt.Sprintf("%v", ch.HasToken)})
		}
		w.Flush()
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":       true,
		"channels": exportData,
		"count":    len(exportData),
	})
}

// ChannelStats represents aggregated channel statistics.
type ChannelStats struct {
	TotalChannels int            `json:"total_channels"`
	WithToken     int            `json:"with_token"`
	WithoutToken  int            `json:"without_token"`
	WithGroups    int            `json:"with_groups"`
	Ungrouped     int            `json:"ungrouped"`
	ByLanguage    map[string]int `json:"by_language"`
	ByGroup       map[string]int `json:"by_group"`
}

// GetChannelStats returns aggregated channel statistics.
// GET /api/v1/youtube/channels/stats
func (h *YouTubeHandlers) GetChannelStats(c *gin.Context) {
	channels := h.service.GetAuthChannels()
	groups := h.service.GetGroups()

	groupedChannels := make(map[string]bool)
	byGroup := make(map[string]int)
	for name, cg := range groups {
		if cg == nil {
			continue
		}
		byGroup[name] = len(cg.Channels)
		for _, chID := range cg.Channels {
			groupedChannels[chID] = true
		}
	}

	byLanguage := make(map[string]int)
	withToken := 0

	for _, ch := range channels {
		if ch.AccessToken != "" || ch.RefreshToken != "" {
			withToken++
		}
		if ch.Language != "" {
			byLanguage[ch.Language]++
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"ok": true,
		"stats": ChannelStats{
			TotalChannels: len(channels),
			WithToken:     withToken,
			WithoutToken:  len(channels) - withToken,
			WithGroups:    len(groupedChannels),
			Ungrouped:     len(channels) - len(groupedChannels),
			ByLanguage:    byLanguage,
			ByGroup:       byGroup,
		},
	})
}

// BatchUpdateLanguageRequest represents a request to update language for multiple channels.
type BatchUpdateLanguageRequest struct {
	ChannelIDs []string `json:"channel_ids" binding:"required"`
	Language   string   `json:"language" binding:"required"`
}

// BatchUpdateLanguage updates the language for multiple channels.
// POST /api/v1/youtube/channels/batch-language
func (h *YouTubeHandlers) BatchUpdateLanguage(c *gin.Context) {
	var req BatchUpdateLanguageRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "Invalid request: " + err.Error()})
		return
	}

	updated := 0
	errs := []gin.H{}

	for _, channelID := range req.ChannelIDs {
		if channelID == "" {
			continue
		}

		if err := h.service.UpdateChannelMetadata(channelID, map[string]interface{}{
			"language": req.Language,
		}); err != nil {
			errs = append(errs, gin.H{"channel_id": channelID, "error": err.Error()})
			continue
		}

		groups := h.service.GetGroups()
		for _, cg := range groups {
			if cg == nil {
				continue
			}
			for _, chID := range cg.Channels {
				if chID == channelID {
					h.service.UpdateChannelLanguage(cg.Name, channelID, req.Language)
					break
				}
			}
		}

		updated++
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":      true,
		"updated": updated,
		"errors":  errs,
		"message": fmt.Sprintf("Updated language for %d channels to %s", updated, req.Language),
	})
}
