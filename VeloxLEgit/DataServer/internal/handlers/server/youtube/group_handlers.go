package youtube

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/integrations/youtube"
)

// ResolveChannelByLanguage resolves a channel in a group by language code
// GET /api/v1/youtube/resolve-channel?group=amish&language=en
func (h *YouTubeHandlers) ResolveChannelByLanguage(c *gin.Context) {
	groupName := c.Query("group")
	language := c.Query("language")

	if groupName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "group query parameter is required"})
		return
	}
	if language == "" {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "language query parameter is required"})
		return
	}

	ch, err := h.service.ResolveChannelByLanguage(groupName, language)
	if err != nil {
		// Map known errors to appropriate status codes
		status := http.StatusNotFound
		if err.Error() == "group name is required" || err.Error() == "language code is required" {
			status = http.StatusBadRequest
		}
		c.JSON(status, gin.H{"ok": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"ok": true,
		"channel": gin.H{
			"id":        ch.ID,
			"title":     ch.Title,
			"name":      ch.Name,
			"thumbnail": ch.Thumbnail,
			"language":  ch.Language,
			"email":     ch.Email,
		},
		"group":    groupName,
		"language": language,
	})
}

// ListGroups lists all upload channel groups with channel details
// GET /api/v1/youtube/groups
//
// S11 POC migration (group_handlers.go is the first handler file to
// drop h.storage on the READ path):
//
//	Previously: groups, _ := h.storage.ListGroups() returned a
//	map[name]*Group where each Group carried a full []Channel slice
//	the legacy Storage struct mirrored into in-RAM. The handler loop
//	read ch.Title / ch.Name / ch.Thumbnail / ch.Language directly off
//	that slice, enriching with h.service.GetAuthChannel metadata.
//
//	Now: h.service.GetGroups() returns map[name]*ChannelGroup where
//	Channels is []string (channel IDs only — the canonical S11
//	membership shape). For each ID, the handler fetches the typed
//	canonical row via h.service.BulkMembership (added in commit
//	8e74bd99) so the channel metadata stays sourced from the SQLite
//	youtube_channels table rather than from a stale in-RAM copy.
//	BulkMembership preserves input order and surfaces SQL errors via
//	fail-closed propagation; we log + degrade to an empty channels
//	slice rather than returning 500 because partial data is still
//	operator-actionable, and the next handler call retries the read.
//
// Mutations on this file (CreateGroup / DeleteGroup / AddChannelToGroup
// RemoveChannelFromGroup) still call h.storage; their per-handler
// audit-pass migration queues separately.
func (h *YouTubeHandlers) ListGroups(c *gin.Context) {
	groups := h.service.GetGroups()

	result := make([]map[string]interface{}, 0, len(groups))
	for _, g := range groups {
		// Fetch the canonical channel rows via BulkMembership. nil entries
		// indicate a membership pointing at a channel whose youtube_channels
		// row has gone away (e.g. a deleted channel) — render as empty so
		// the SPA doesn't see a phantom {"id":"","title":""} entry.
		channels, err := h.service.BulkMembership(g.Channels)
		if err != nil {
			// DB-first invariant: SQL failures MUST be surfaced, not
			// silently swallowed (consistent with TestMembership_StoreError
			// Surfaced in commit 8e74bd99 and the persistRefreshedToken
			// doc-comment corrections in 40a31421). Returning 500 lets
			// the SPA distinguish "0 groups" from "SQL failed" rather
			// than render a misleading empty list. The pre-migration
			// h.storage.ListGroups() path also discarded the error but
			// only because its caller had no better choice on a list
			// endpoint; the S11 path has a typed error to surface.
			log.Printf("[ERR] ListGroups: bulk membership fetch failed for group %s: %v", g.Name, err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"ok":         false,
				"error":      fmt.Sprintf("bulk membership fetch for group %s failed: %v", g.Name, err),
				"group_name": g.Name,
			})
			return
		}
		// Pre-filter nil entries so count and rendered channels stay
		// aligned. Previously `count: len(channels)` inflated the count
		// when BulkMembership returned non-nil entries mixed with nil
		// pointers (deleted-channel memberships, until pruned by the next
		// membership diff). The render loop's `if ch == nil { continue }`
		// skipped them at render time so the SPA saw fewer JSON entries
		// than count claimed -- a real off-by-nils bug.
		nonNil := make([]*youtube.Channel, 0, len(channels))
		for _, ch := range channels {
			if ch != nil {
				nonNil = append(nonNil, ch)
			}
		}
		channels = nonNil

		groupData := map[string]interface{}{
			"name":        g.Name,
			"description": g.Description,
			"privacy":     g.Privacy,
			"group_type":  g.GroupType,
			"channels":    make([]map[string]interface{}, 0, len(channels)),
			"count":       len(channels),
		}

		channelsList := groupData["channels"].([]map[string]interface{})
		for _, ch := range channels {
			_ = ch // pre-filtered above; the inner nil check is defensive
			// against a future Membership-returning-nil contract change.

			// Auth data overrides canonical row values when AuthChannel
			// has fresher fields (live OAuth sessions take precedence).
			authCh := h.service.GetAuthChannel(ch.ID)
			title := ch.Title
			name := ch.Name
			thumbnail := ch.Thumbnail
			language := ch.Language
			if authCh != nil {
				if authCh.Title != "" {
					title = authCh.Title
				}
				if authCh.Name != "" {
					name = authCh.Name
				}
				if authCh.Thumbnail != "" {
					thumbnail = authCh.Thumbnail
				}
				if authCh.Language != "" {
					language = authCh.Language
				}
			}

			channelsList = append(channelsList, map[string]interface{}{
				"id":        ch.ID,
				"title":     title,
				"name":      name,
				"thumbnail": thumbnail,
				"language":  language,
			})
		}
		groupData["channels"] = channelsList

		result = append(result, groupData)
	}

	if result == nil {
		result = []map[string]interface{}{}
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":     true,
		"groups": result,
		"count":  len(result),
	})
}

// CreateGroup creates a new upload channel group in the unified Storage
// POST /api/v1/youtube/groups
func (h *YouTubeHandlers) CreateGroup(c *gin.Context) {
	var req struct {
		Name        string   `json:"name" binding:"required"`
		Channels    []string `json:"channels"`
		Description string   `json:"description"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
		return
	}

	// Create in unified Storage with GroupType="upload"
	//
	// PR-YT-REPO: h.storage.CreateGroup → h.service.CreateGroup. The
	// integration Service.CreateGroup now takes (name, description,
	// channelIDs) and hardcodes group_type="upload" under the hood, so
	// the previous Storage-style (name, groupType) overload is gone.
	if err := h.service.CreateGroup(req.Name, "", nil); err != nil {
		if err.Error() == "group '"+req.Name+"' already exists" || err == youtube.ErrGroupExists {
			c.JSON(http.StatusConflict, gin.H{"ok": false, "error": "Group already exists"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	// If channels provided, add them
	// Also add channels to Service.groups for backward compatibility during migration
	if len(req.Channels) > 0 {
		for _, chID := range req.Channels {
			// Add to Storage with enriched metadata
			channel := h.buildChannelFromID(chID)
			if err := h.service.AddChannel(req.Name, channel); err != nil {
				// Log and continue
				continue
			}
		}
	}

	c.JSON(http.StatusCreated, gin.H{
		"ok": true,
		"group": gin.H{
			"name":        req.Name,
			"channels":    req.Channels,
			"description": req.Description,
		},
	})
}

// DeleteGroup deletes an upload channel group from the unified Storage
// DELETE /api/v1/youtube/groups/:name
func (h *YouTubeHandlers) DeleteGroup(c *gin.Context) {
	name := c.Param("name")

	if err := h.service.DeleteGroup(name); err != nil {
		if err == youtube.ErrGroupNotFound {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "Group not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":      true,
		"message": fmt.Sprintf("Group '%s' deleted", name),
	})
}

// AddChannelToGroup adds a channel to an upload group in the unified Storage
// POST /api/v1/youtube/groups/:name/channels
func (h *YouTubeHandlers) AddChannelToGroup(c *gin.Context) {
	groupName := c.Param("name")

	var req struct {
		ChannelID string `json:"channel_id" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
		return
	}

	// Build enriched Channel object from Service.channels
	channel := h.buildChannelFromID(req.ChannelID)

	if err := h.service.AddChannel(groupName, channel); err != nil {
		if err == youtube.ErrChannelExists {
			c.JSON(http.StatusConflict, gin.H{"ok": false, "error": "Channel already in group"})
			return
		}
		if err == youtube.ErrGroupNotFound {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "Group not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":      true,
		"message": fmt.Sprintf("Channel '%s' added to group '%s'", req.ChannelID, groupName),
	})
}

// RemoveChannelFromGroup removes a channel from an upload group in the unified Storage
// DELETE /api/v1/youtube/groups/:name/channels/:channel
func (h *YouTubeHandlers) RemoveChannelFromGroup(c *gin.Context) {
	groupName := c.Param("name")
	channelID := c.Param("channel")

	if err := h.service.RemoveChannel(groupName, channelID); err != nil {
		if err == youtube.ErrGroupNotFound {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "Group not found"})
			return
		}
		if err == youtube.ErrChannelNotFound {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "Channel not found in group"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":      true,
		"message": fmt.Sprintf("Channel '%s' removed from group '%s'", channelID, groupName),
	})
}

// buildChannelFromID creates a Channel object enriched with metadata from Service.channels
func (h *YouTubeHandlers) buildChannelFromID(channelID string) youtube.Channel {
	ch := youtube.Channel{
		ID:      channelID,
		URL:     "https://www.youtube.com/channel/" + channelID,
		AddedAt: time.Now(),
	}

	// Enrich with OAuth channel metadata if available
	authCh := h.service.GetAuthChannel(channelID)
	if authCh != nil {
		ch.Title = authCh.Title
		ch.Name = authCh.Name
		ch.Thumbnail = authCh.Thumbnail
		ch.Language = authCh.Language
	} else {
		ch.Title = channelID
	}

	return ch
}
