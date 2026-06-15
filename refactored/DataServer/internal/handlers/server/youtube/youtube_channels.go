package youtube

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/integrations/youtube"
)

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

		if _, ok := ym.storage.GetGroup(groupName); !ok {
			c.JSON(http.StatusNotFound, youtube.APIResponse{
				OK:    false,
				Error: "Group not found",
			})
			return
		}

		if isVideoURL(url) {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "Invalid Channel URL. Please provide a channel URL or handle (@name).",
			})
			return
		}

		ctx := c.Request.Context()
		channelInfo, err := ym.apiClient.GetChannelInfo(ctx, url)
		if err != nil {
			channelInfo = &youtube.ChannelInfo{URL: url, Title: req.Title, Thumbnail: req.Thumbnail}
		}

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

		if isVideoURL(resolvedURL) {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{
				OK:    false,
				Error: "Invalid Channel URL resolved. Please provide a channel URL or handle (@name).",
			})
			return
		}

		var keywords []string
		if channelTitle != "" {
			keywords = extractKeywords(channelTitle)
		}
		if channelInfo != nil && channelInfo.Description != "" {
			keywords = append(keywords, extractKeywords(channelInfo.Description)...)
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

		ctx := c.Request.Context()

		// Use ValidateToken which returns view_count and subscriber_count
		validation, err := ym.service.ValidateToken(ctx, channelID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{
				OK:    false,
				Error: "Failed to fetch channel stats: " + err.Error(),
			})
			return
		}

		var viewCount, subCount int64
		if vc, ok := validation["view_count"].(int64); ok {
			viewCount = vc
		} else if vc, ok := validation["view_count"].(float64); ok {
			viewCount = int64(vc)
		}
		if sc, ok := validation["subscriber_count"].(int64); ok {
			subCount = sc
		} else if sc, ok := validation["subscriber_count"].(float64); ok {
			subCount = int64(sc)
		}

		if err := ym.storage.UpdateChannelStats(groupName, channelID, viewCount, subCount); err != nil {
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{
				OK:    false,
				Error: err.Error(),
			})
			return
		}

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
func (ym *YouTubeManager) DeleteChannelPermanentlyHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		channelID := c.Param("channel_id")

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

		if foundGroup != "" {
			ym.storage.RemoveChannel(foundGroup, channelID)
		}

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
						log.Printf("[DEL] Deleted token file: %s", tp)
					}
				}
			}
		}

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

var youtubeChannelIDRegex = regexp.MustCompile(`^UC[\w-]{21,22}$`)

// MoveChannelToGroupHandler moves a channel to a target group
func (ym *YouTubeManager) MoveChannelToGroupHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		channelID := c.Param("channel_id")
		targetGroup := c.Param("target_group")

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

		if sourceGroup == "" {
			if !youtubeChannelIDRegex.MatchString(channelID) {
				c.JSON(http.StatusNotFound, youtube.APIResponse{
					OK:    false,
					Error: "Channel not found in any group",
				})
				return
			}

			if _, ok := ym.storage.GetGroup(targetGroup); !ok {
				if err := ym.storage.CreateGroup(targetGroup, "manager"); err != nil {
					c.JSON(http.StatusInternalServerError, youtube.APIResponse{
						OK:    false,
						Error: "Failed to create target group: " + err.Error(),
					})
					return
				}
			}

			channelURL := "https://www.youtube.com/channel/" + channelID

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

			if ym.dataDir != "" {
				targetDir := filepath.Join(ym.dataDir, "youtube", "group", targetGroup)
				sourceTokenPath := filepath.Join(ym.dataDir, "youtube", "Token", "account_"+channelID+".json")
				targetTokenPath := filepath.Join(targetDir, "account_"+channelID+".json")

				if _, err := os.Stat(sourceTokenPath); err == nil {
					os.MkdirAll(targetDir, 0755)
					if err := os.Rename(sourceTokenPath, targetTokenPath); err == nil {
						log.Printf("[MOVE] Moved token file to %s", targetGroup)
					}
				}
			}

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

		if _, ok := ym.storage.GetGroup(targetGroup); !ok {
			if err := ym.storage.CreateGroup(targetGroup, "manager"); err != nil {
				c.JSON(http.StatusInternalServerError, youtube.APIResponse{
					OK:    false,
					Error: "Failed to create target group: " + err.Error(),
				})
				return
			}
		}

		if err := ym.storage.MoveChannel(sourceGroup, channelID, targetGroup); err != nil {
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{
				OK:    false,
				Error: err.Error(),
			})
			return
		}

		if ym.dataDir != "" {
			sourceTokenPath := filepath.Join(ym.dataDir, "youtube", "group", sourceGroup, "account_"+channelID+".json")
			targetDir := filepath.Join(ym.dataDir, "youtube", "group", targetGroup)
			targetTokenPath := filepath.Join(targetDir, "account_"+channelID+".json")

			if _, err := os.Stat(sourceTokenPath); err == nil {
				os.MkdirAll(targetDir, 0755)
				if err := os.Rename(sourceTokenPath, targetTokenPath); err == nil {
					log.Printf("[MOVE] Moved token file from %s to %s", sourceGroup, targetGroup)
				}
			}
		}

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
