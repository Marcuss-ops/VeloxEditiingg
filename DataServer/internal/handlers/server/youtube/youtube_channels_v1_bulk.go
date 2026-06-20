package youtube

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// ── Bulk operations ─────────────────────────────────────────────────────────

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

		groups, _ := h.storage.ListGroups()
		for groupName, group := range groups {
			for _, ch := range group.Channels {
				if ch.ID == channelID {
					h.storage.RemoveChannel(groupName, channelID)
					break
				}
			}
		}

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

// MoveChannelV1Request represents a request to move a channel between groups.
type MoveChannelV1Request struct {
	TargetGroup string `json:"target_group" binding:"required"`
	RemoveFrom  string `json:"remove_from,omitempty"`
}

// MoveChannelToGroupV1 moves a channel from one group to another.
// POST /api/v1/youtube/channels/:id/move
func (h *YouTubeHandlers) MoveChannelToGroupV1(c *gin.Context) {
	channelID := c.Param("id")

	var req MoveChannelV1Request
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "Invalid request: " + err.Error()})
		return
	}

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

			validation, err := h.service.ValidateOAuthAccessToken(ctx, channelID)
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

// ── ListUndefinedChannels ───────────────────────────────────────────────────

// ListUndefinedChannels lists channels not in any upload group.
// GET /api/v1/youtube/channels/undefined
func (h *YouTubeHandlers) ListUndefinedChannels(c *gin.Context) {
	authChannels := h.service.GetAuthChannels()

	groups, _ := h.storage.ListGroups()

	assigned := make(map[string]bool, len(authChannels))
	for _, g := range groups {
		if g.GroupType != "" && g.GroupType != "upload" {
			continue
		}
		for _, ch := range g.Channels {
			assigned[ch.ID] = true
		}
	}

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

// ── RefreshChannelsMetadata ─────────────────────────────────────────────────

// RefreshChannelsMetadata refreshes the title and thumbnail for all channels.
// POST /api/v1/youtube/channels/refresh-metadata
func (h *YouTubeHandlers) RefreshChannelsMetadata(c *gin.Context) {
	ctx := c.Request.Context()

	successCount, errors := h.service.RefreshAllChannelsMetadata(ctx)

	errStrings := make([]string, 0, len(errors))
	for _, err := range errors {
		errStrings = append(errStrings, err.Error())
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":          true,
		"refreshed":   successCount,
		"errors":      errStrings,
		"error_count": len(errors),
		"message":     fmt.Sprintf("Refreshed metadata for %d channels", successCount),
	})
}

// ── GetChannelAnalytics ─────────────────────────────────────────────────────

// GetChannelAnalytics returns analytics data for a specific channel.
// GET /api/v1/youtube/analytics/channel/:id?days=7
func (h *YouTubeHandlers) GetChannelAnalytics(c *gin.Context) {
	channelID := c.Param("id")
	daysStr := c.DefaultQuery("days", "7")

	c.JSON(http.StatusOK, gin.H{
		"ok":      true,
		"channel": channelID,
		"days":    daysStr,
		"totals":  gin.H{},
		"stats":   []interface{}{},
		"message": "Channel analytics available via /api/v1/analytics endpoints",
	})
}
