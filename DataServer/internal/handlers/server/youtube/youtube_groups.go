package youtube

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/integrations/youtube"
	ytService "velox-server/internal/services/youtube"
)

// YouTubeManager holds the dependencies for YouTube manager handlers
type YouTubeManager struct {
	svc *ytService.Service
}

// NewYouTubeManager creates a new YouTube manager handler instance.
func NewYouTubeManager(dataDir, apiKey string, existingStorage *youtube.Storage, ytIntegrationService *youtube.Service) *YouTubeManager {
	return &YouTubeManager{
		svc: ytService.New(dataDir, apiKey, existingStorage, ytIntegrationService),
	}
}

// CleanupOldData purges YouTube data older than the retention period
func (ym *YouTubeManager) CleanupOldData(retention time.Duration) int {
	return ym.svc.CleanupOldData(retention)
}

// CleanupCache removes expired entries from the API cache
func (ym *YouTubeManager) CleanupCache() int {
	return ym.svc.CleanupCache()
}

// DataRetentionCleanup performs a comprehensive cleanup of all YouTube cached data.
func (ym *YouTubeManager) DataRetentionCleanup() int {
	return ym.svc.DataRetentionCleanup()
}

// ListGroupsHandler returns all groups and their channels
func (ym *YouTubeManager) ListGroupsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		groups, trackedNiches := ym.svc.ListGroups()
		c.JSON(http.StatusOK, youtube.GroupsListResponse{
			OK:            true,
			Groups:        groups,
			TrackedNiches: trackedNiches,
		})
	}
}

// CreateGroupHandler creates a new group
func (ym *YouTubeManager) CreateGroupHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req youtube.CreateGroupRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{OK: false, Error: "Invalid request: " + err.Error()})
			return
		}

		name := strings.TrimSpace(req.Name)
		if name == "" {
			c.JSON(http.StatusBadRequest, youtube.APIResponse{OK: false, Error: "Group name cannot be empty"})
			return
		}

		if err := ym.svc.CreateGroup(name); err != nil {
			if err == youtube.ErrGroupExists {
				c.JSON(http.StatusConflict, youtube.APIResponse{OK: false, Error: "Group already exists"})
				return
			}
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{OK: false, Error: err.Error()})
			return
		}

		ym.svc.FeedCacheClear()
		c.JSON(http.StatusOK, youtube.APIResponse{OK: true, Message: "Group '" + name + "' created"})
	}
}

// DeleteGroupHandler deletes a group
func (ym *YouTubeManager) DeleteGroupHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		groupName := c.Param("group_name")
		if err := ym.svc.DeleteGroup(groupName); err != nil {
			if err == youtube.ErrGroupNotFound {
				c.JSON(http.StatusNotFound, youtube.APIResponse{OK: false, Error: "Group not found"})
				return
			}
			c.JSON(http.StatusInternalServerError, youtube.APIResponse{OK: false, Error: err.Error()})
			return
		}

		ym.svc.FeedCacheClear()
		c.JSON(http.StatusOK, youtube.APIResponse{OK: true, Message: "Group '" + groupName + "' deleted"})
	}
}

// ManagerStatsHandler returns the aggregate per-group channel + token-validity snapshot.
func (ym *YouTubeManager) ManagerStatsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		forceRefresh := isTruthyQuery(c.Query("refresh"))
		ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
		defer cancel()

		resp, cached, age, cacheHeader, err := ym.svc.GetCachedStats(forceRefresh, ctx)
		if err != nil {
			c.JSON(http.StatusInternalServerError, ytService.ManagerStatsResponse{
				OK:     false,
				Groups: map[string]ytService.ManagerGroupStats{},
				Error:  err.Error(),
			})
			return
		}

		c.Header("X-Cache", cacheHeader)
		if cached {
			c.Header("X-Cache-Age-Seconds", strconv.Itoa(age))
		}

		c.JSON(http.StatusOK, resp)
	}
}

func isTruthyQuery(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "t", "yes", "y":
		return true
	}
	return false
}
