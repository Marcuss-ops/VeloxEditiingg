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
// to sweep every known copy when a channel is removed, and by
// consolidateLegacyTokenFor to migrate stray copies into the canonical
// secrets/youtube/tokens/ path. Includes the per-group directory
// youtube/group/*/ because accounts were historically stored one-per-group
// and the startup migration enumerates those subdirs too.
func (ym *YouTubeManager) legacyAccountTokenPaths(channelID string) []string {
	if ym.dataDir == "" || channelID == "" {
		return nil
	}
	fileName := "account_" + channelID + ".json"
	paths := []string{
		filepath.Join(ym.dataDir, "youtube", "tokens", fileName),
		filepath.Join(ym.dataDir, "secrets", "youtube", "tokens", fileName),
		filepath.Join(ym.dataDir, "youtube", "Token", fileName),
	}
	groupRoot := filepath.Join(ym.dataDir, "youtube", "group")
	entries, err := os.ReadDir(groupRoot)
	if err != nil {
		return paths
	}
	for _, e := range entries {
		// Skip symlinks explicitly: e.IsDir() may follow them on some
		// platforms, and a malicious symlink here could redirect
		// os.Remove / os.Rename calls at this listing out of the data
		// root. We only want real subdirectories.
		if e.Type()&os.ModeSymlink != 0 || !e.Type().IsDir() {
			continue
		}
		paths = append(paths, filepath.Join(groupRoot, e.Name(), fileName))
	}
	return paths
}

var youtubeChannelIDRegex = regexp.MustCompile(`^UC[\w-]{21,22}$`)

// consolidateLegacyTokenFor safely consolidates any pre-migration token file
// for channelID into the canonical secrets/youtube/tokens/ path. If the
// canonical file already has content, the legacy copy is discarded instead
// of being os.Rename-d over the top of it (POSIX rename replaces the
// destination atomically, which would silently clobber a fresher canonical
// copy). No-op if ym.dataDir is empty.
func (ym *YouTubeManager) consolidateLegacyTokenFor(channelID string) {
	if ym.dataDir == "" || channelID == "" {
		return
	}
	canonicalPath := youtube.CanonicalOAuthTokenPath(ym.dataDir, channelID)
	if canonicalPath == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(canonicalPath), 0755); err != nil {
		log.Printf("[WARN] Cannot create canonical token dir for %s: %v", channelID, err)
		return
	}
	canonicalClean := filepath.Clean(canonicalPath)
	canonicalPopulated := func() bool {
		info, statErr := os.Stat(canonicalPath)
		return statErr == nil && info.Size() > 0
	}
	for _, src := range ym.legacyAccountTokenPaths(channelID) {
		if filepath.Clean(src) == canonicalClean {
			continue
		}
		if _, err := os.Stat(src); err != nil {
			continue
		}
		if canonicalPopulated() {
			_ = os.Remove(src)
			log.Printf("[MOVE] Legacy token file discarded (canonical already present): %s", src)
			continue
		}
		if renameErr := os.Rename(src, canonicalPath); renameErr == nil {
			log.Printf("[MOVE] Consolidated token file from %s to canonical path", src)
		}
	}
}

// MoveChannelToGroupHandler moves a channel to a target group. If the channel
// does not currently belong to any group, it is added to targetGroup as if it
// were a freshly-imported channel (used by drag-and-drop / bulk move from the
// UI). In either case, any pre-migration legacy token-file copies are
// consolidated into the canonical secrets/youtube/tokens/ path with a
// preserve-canonical guard so a stale legacy file cannot clobber the
// freshly-migrated canonical one.
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

		// Always consolidate legacy token copies into the canonical path
		// before any DB write, so service.DeleteChannel (called during a
		// later teardown) sees only the canonical file. The bootstrap
		// migration covers the same ground on startup; this branch is a
		// belt-and-suspenders guard against a write that races the
		// migration on a cold start.
		ym.consolidateLegacyTokenFor(channelID)

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

		if sourceGroup == targetGroup {
			ym.feedCache.Clear()
			c.JSON(http.StatusOK, youtube.APIResponse{
				OK:      true,
				Message: "Channel already in target group",
				Data: gin.H{
					"channel_id":   channelID,
					"source_group": sourceGroup,
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
