package youtube

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/integrations/youtube"
)

// BulkDeleteRequest represents a request to delete multiple channels.
type BulkDeleteRequest struct {
	ChannelIDs []string `json:"channel_ids" binding:"required"`
}

// BulkDeleteChannels deletes multiple channels in a single operation.
// POST /api/v1/youtube/channels/bulk-delete
func (h *YouTubeHandlers) BulkDeleteChannels(c *gin.Context) {
	var req BulkDeleteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "Invalid request: " + err.Error()})
		return
	}

	deleted := 0
	errs := []gin.H{}

	for _, channelID := range req.ChannelIDs {
		if channelID == "" {
			continue
		}

		// Remove from all Storage groups
		groups, _ := h.storage.ListGroups()
		for groupName, group := range groups {
			for _, ch := range group.Channels {
				if ch.ID == channelID {
					h.storage.RemoveChannel(groupName, channelID)
					break
				}
			}
		}

		// Delete via Service (handles token deletion)
		if err := h.service.DeleteChannel(channelID); err != nil {
			errs = append(errs, gin.H{"channel_id": channelID, "error": err.Error()})
			continue
		}
		deleted++
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":      true,
		"deleted": deleted,
		"errors":  errs,
		"message": fmt.Sprintf("Deleted %d channels", deleted),
	})
}

// MoveChannelRequest represents a request to move a channel between groups.
type MoveChannelRequest struct {
	TargetGroup string `json:"target_group" binding:"required"`
	RemoveFrom  string `json:"remove_from,omitempty"`
}

// MoveChannelToGroup moves a channel from one group to another.
// POST /api/v1/youtube/channels/:id/move
func (h *YouTubeHandlers) MoveChannelToGroup(c *gin.Context) {
	channelID := c.Param("id")

	var req MoveChannelRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "Invalid request: " + err.Error()})
		return
	}

	// Find source group if not specified
	sourceGroup := req.RemoveFrom
	if sourceGroup == "" {
		groups, _ := h.storage.ListGroups()
		for name, group := range groups {
			for _, ch := range group.Channels {
				if ch.ID == channelID {
					sourceGroup = name
					break
				}
			}
			if sourceGroup != "" {
				break
			}
		}
	}

	if sourceGroup == "" {
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "Channel not found in any group"})
		return
	}

	// Use the built-in MoveChannel method
	if err := h.storage.MoveChannel(sourceGroup, channelID, req.TargetGroup); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":           true,
		"channel_id":   channelID,
		"source_group": sourceGroup,
		"target_group": req.TargetGroup,
	})
}

// ValidateAllTokens validates all channel OAuth tokens in parallel.
// POST /api/v1/youtube/channels/validate-all
func (h *YouTubeHandlers) ValidateAllTokens(c *gin.Context) {
	channels := h.service.GetAuthChannels()
	if len(channels) == 0 {
		c.JSON(http.StatusOK, gin.H{"ok": true, "results": []gin.H{}, "total": 0})
		return
	}

	type validateResult struct {
		ChannelID string `json:"channel_id"`
		Title     string `json:"title"`
		Valid     bool   `json:"valid"`
		Error     string `json:"error,omitempty"`
		HasToken  bool   `json:"has_token"`
	}

	results := make([]validateResult, len(channels))
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(c.Request.Context(), 60*time.Second)
	defer cancel()

	for i, ch := range channels {
		wg.Add(1)
		go func(idx int, channelID, title string) {
			defer wg.Done()

			r := validateResult{
				ChannelID: channelID,
				Title:     title,
				HasToken:  true,
			}

			validation, err := h.service.ValidateToken(ctx, channelID)
			if err != nil {
				r.Error = err.Error()
				r.Valid = false
			} else if ok, exists := validation["valid"].(bool); exists {
				r.Valid = ok
			} else if ok, exists := validation["ok"].(bool); exists {
				r.Valid = ok
			} else {
				r.Valid = false
			}

			results[idx] = r
		}(i, ch.ID, ch.Title)
	}

	wg.Wait()

	valid := 0
	for _, r := range results {
		if r.Valid {
			valid++
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":      true,
		"results": results,
		"summary": gin.H{
			"total":   len(results),
			"valid":   valid,
			"invalid": len(results) - valid,
		},
	})
}

// GetChannelGroups returns all groups a channel belongs to.
// GET /api/v1/youtube/channels/:id/groups
func (h *YouTubeHandlers) GetChannelGroups(c *gin.Context) {
	channelID := c.Param("id")

	groups, _ := h.storage.ListGroups()
	memberGroups := []gin.H{}

	for name, group := range groups {
		for _, ch := range group.Channels {
			if ch.ID == channelID {
				memberGroups = append(memberGroups, gin.H{
					"name":       name,
					"group_type": group.GroupType,
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

// DuplicateChannel represents a set of duplicate channels.
type DuplicateChannel struct {
	ChannelID string   `json:"channel_id"`
	Title     string   `json:"title"`
	Groups    []string `json:"groups"`
}

// DetectDuplicateChannels finds channels that appear in multiple groups.
// GET /api/v1/youtube/channels/duplicates
func (h *YouTubeHandlers) DetectDuplicateChannels(c *gin.Context) {
	groups, _ := h.storage.ListGroups()

	channelGroups := make(map[string][]string)
	channelTitles := make(map[string]string)

	for name, group := range groups {
		for _, ch := range group.Channels {
			channelGroups[ch.ID] = append(channelGroups[ch.ID], name)
			if ch.Title != "" {
				channelTitles[ch.ID] = ch.Title
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
	groups, _ := h.storage.ListGroups()

	channelGroupsMap := make(map[string][]string)
	for name, group := range groups {
		for _, ch := range channelIDs(group.Channels) {
			channelGroupsMap[ch] = append(channelGroupsMap[ch], name)
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
	groups, _ := h.storage.ListGroups()

	groupedChannels := make(map[string]bool)
	byGroup := make(map[string]int)
	for name, group := range groups {
		byGroup[name] = len(group.Channels)
		for _, ch := range group.Channels {
			groupedChannels[ch.ID] = true
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

		groups, _ := h.storage.ListGroups()
		for _, group := range groups {
			for _, ch := range group.Channels {
				if ch.ID == channelID {
					h.storage.UpdateChannelLanguage(group.Name, channelID, req.Language)
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

// ChannelIDs returns the list of channel IDs in a group.
func channelIDs(channels []youtube.Channel) []string {
	ids := make([]string, len(channels))
	for i, ch := range channels {
		ids[i] = ch.ID
	}
	return ids
}
