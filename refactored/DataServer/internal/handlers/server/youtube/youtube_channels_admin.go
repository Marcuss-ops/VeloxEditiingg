package youtube

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/integrations/youtube"
)

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
		if err := ym.service.DeleteChannel(channelID); err != nil {
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{
				OK:    false,
				Error: err.Error(),
			})
			return
		}
		// Belt-and-suspenders: also drop any legacy token-file locations the
		// service might have missed. Single source of truth = the canonical
		// secret path under dataDir; the other two are legacy fallbacks that
		// the cleanup script removes. Doing the sweep here too is cheap and
		// prevents stale OAuth tokens from hanging around after a delete.
		if ym.dataDir != "" {
			for _, p := range ym.legacyAccountTokenPaths(channelID) {
				if err := os.Remove(p); err == nil {
					log.Printf("[DEL] Removed legacy token file: %s", p)
				}
			}
		}
		ym.feedCache.Clear()

		c.JSON(http.StatusOK, youtube.APIResponse{
			OK:      true,
			Message: "Channel permanently deleted",
			Data: gin.H{
				"channel_id": channelID,
			},
		})
	}
}

// legacyAccountTokenPaths returns every legacy on-disk location an OAuth
// token for channelID might exist at. Used by DeleteChannelPermanentlyHandler
// to sweep every known copy when a channel is removed.
func (ym *YouTubeManager) legacyAccountTokenPaths(channelID string) []string {
	if ym.dataDir == "" || channelID == "" {
		return nil
	}
	fileName := "account_" + channelID + ".json"
	return []string{
		filepath.Join(ym.dataDir, "youtube", "tokens", fileName),
		filepath.Join(ym.dataDir, "secrets", "youtube", "tokens", fileName),
		filepath.Join(ym.dataDir, "youtube", "Token", fileName),
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
			oldPath := filepath.Join(ym.dataDir, "youtube", "Token", "account_"+channelID+".json")
			if _, err := os.Stat(oldPath); err == nil {
				canonicalDir := filepath.Join(ym.dataDir, "secrets", "youtube", "tokens")
				_ = os.MkdirAll(canonicalDir, 0755)
				canonicalPath := filepath.Join(canonicalDir, "account_"+channelID+".json")
				if err := os.Rename(oldPath, canonicalPath); err == nil {
					log.Printf("[MOVE] Consolidated token file for %s to canonical path", channelID)
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
		// Legacy token-file locations may still carry ACCOUNT_* per-group tokens
		// from before the consolidation migration. Move any that exist into the
		// canonical secrets/youtube/tokens/ path so there is a single source.
		legacyDirs := []string{
			filepath.Join(ym.dataDir, "youtube", "group", sourceGroup),
			filepath.Join(ym.dataDir, "youtube", "Token"),
		}
		canonicalDir := filepath.Join(ym.dataDir, "secrets", "youtube", "tokens")
		_ = os.MkdirAll(canonicalDir, 0755)
		fileName := "account_" + channelID + ".json"
		canonicalPath := filepath.Join(canonicalDir, fileName)
		for _, dir := range legacyDirs {
			src := filepath.Join(dir, fileName)
			if _, err := os.Stat(src); err != nil {
				continue
			}
			if err := os.Rename(src, canonicalPath); err == nil {
				log.Printf("[MOVE] Consolidated token file from %s to canonical path", dir)
				break
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
