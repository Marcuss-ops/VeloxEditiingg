package youtube

import (
	"fmt"
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
// Now reads from the unified Storage (shared with YouTubeManager) instead of Service.groups.
func (h *YouTubeHandlers) ListGroups(c *gin.Context) {
	groups, _ := h.storage.ListGroups()

	// Filter to upload groups (GroupType == "upload" or empty for backward compat)
	result := make([]map[string]interface{}, 0, len(groups))
	for _, g := range groups {
		if g.GroupType != "" && g.GroupType != "upload" {
			continue
		}

		groupData := map[string]interface{}{
			"name":        g.Name,
			"description": "",
			"privacy":     "",
			"channels":    make([]map[string]interface{}, 0, len(g.Channels)),
			"count":       len(g.Channels),
		}

		// Enrich channels with metadata from Service OAuth channels
		channelsList := groupData["channels"].([]map[string]interface{})
		for _, ch := range g.Channels {
			authCh := h.service.GetAuthChannel(ch.ID)
			if authCh != nil {
				channelsList = append(channelsList, map[string]interface{}{
					"id":        ch.ID,
					"title":     authCh.Title,
					"name":      authCh.Name,
					"thumbnail": authCh.Thumbnail,
					"language":  authCh.Language,
				})
			} else {
				channelsList = append(channelsList, map[string]interface{}{
					"id":    ch.ID,
					"title": ch.Title,
					"name":  ch.Name,
				})
			}
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
	if err := h.storage.CreateGroup(req.Name, "upload"); err != nil {
		if err == youtube.ErrGroupExists {
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
			if err := h.storage.AddChannel(req.Name, channel); err != nil {
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

	if err := h.storage.DeleteGroup(name); err != nil {
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

	if err := h.storage.AddChannel(groupName, channel); err != nil {
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

	if err := h.storage.RemoveChannel(groupName, channelID); err != nil {
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
