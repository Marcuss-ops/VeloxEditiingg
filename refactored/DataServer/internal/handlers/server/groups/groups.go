package groups

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// Group represents a YouTube channel group
type Group struct {
	Name        string    `json:"name"`
	Channels    []Channel `json:"channels,omitempty"`
	CreatedAt   string    `json:"created_at,omitempty"`
	Translate   Translate `json:"translate,omitempty"`
	DefaultLang string    `json:"default_language,omitempty"`
	Privacy     string    `json:"privacy,omitempty"`
	AutoTags    bool      `json:"auto_tags,omitempty"`
	Schedule    Schedule  `json:"schedule,omitempty"`
}

// Channel represents a YouTube channel
type Channel struct {
	ID        string   `json:"id"`
	URL       string   `json:"url"`
	Title     string   `json:"title,omitempty"`
	Thumbnail string   `json:"thumbnail,omitempty"`
	Notes     string   `json:"notes,omitempty"`
	AddedAt   string   `json:"added_at,omitempty"`
	Keywords  []string `json:"keywords,omitempty"`
}

// Translate represents translation settings
type Translate struct {
	Enabled     bool     `json:"enabled,omitempty"`
	TargetLangs []string `json:"target_langs,omitempty"`
}

// Schedule represents schedule settings
type Schedule struct {
	Mode      string `json:"mode,omitempty"`
	PublishAt string `json:"publish_at,omitempty"`
}

// GroupsData holds the structure from youtube_manager.json
type GroupsData struct {
	Groups map[string]Group `json:"groups"`
}

// groupsCache holds cached groups data
type groupsCacheType struct {
	data     GroupsData
	lastLoad time.Time
	mu       sync.RWMutex
}

var groupsCache groupsCacheType
var groupsDataDir string

// GetGroupsHandler returns all groups
func GetGroupsHandler(c *gin.Context) {
	groups := getGroupsFromCache()
	if groups == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"ok":    false,
			"error": "Groups data not available",
		})
		return
	}

	// Convert map to slice
	result := make([]Group, 0, len(groups.Groups))
	for name, g := range groups.Groups {
		g.Name = name
		result = append(result, g)
	}

	c.JSON(http.StatusOK, result)
}

// GetGroupHandler returns a specific group
func GetGroupHandler(c *gin.Context) {
	name := c.Param("name")

	groups := getGroupsFromCache()
	if groups == nil {
		c.JSON(http.StatusNotFound, gin.H{
			"ok":    false,
			"error": "Group not found",
		})
		return
	}

	group, exists := groups.Groups[name]
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{
			"ok":    false,
			"error": "Group not found",
		})
		return
	}

	group.Name = name
	c.JSON(http.StatusOK, group)
}

// getGroupsFromCache returns cached groups data
func getGroupsFromCache() *GroupsData {
	groupsCache.mu.RLock()
	if !groupsCache.lastLoad.IsZero() && time.Since(groupsCache.lastLoad) < 30*time.Second {
		defer groupsCache.mu.RUnlock()
		return &groupsCache.data
	}
	groupsCache.mu.RUnlock()

	// Reload from disk
	loadGroupsFromDisk()

	groupsCache.mu.RLock()
	defer groupsCache.mu.RUnlock()
	return &groupsCache.data
}

// loadGroupsFromDisk loads groups from groups.json
func loadGroupsFromDisk() {
	if groupsDataDir == "" {
		return
	}

	filePath := filepath.Join(groupsDataDir, "youtube", "groups.json")
	data, err := os.ReadFile(filePath)
	if err != nil {
		return
	}

	// Try array format first (groups.json uses array)
	var groupsArray []Group
	if err := json.Unmarshal(data, &groupsArray); err == nil && len(groupsArray) > 0 {
		groupsData := GroupsData{Groups: make(map[string]Group)}
		for _, g := range groupsArray {
			if g.Name != "" {
				groupsData.Groups[g.Name] = g
			}
		}
		groupsCache.mu.Lock()
		groupsCache.data = groupsData
		groupsCache.lastLoad = time.Now()
		groupsCache.mu.Unlock()
		return
	}

	// Fallback to map format (legacy youtube_manager.json format)
	var groupsData GroupsData
	if err := json.Unmarshal(data, &groupsData); err != nil {
		return
	}

	groupsCache.mu.Lock()
	groupsCache.data = groupsData
	groupsCache.lastLoad = time.Now()
	groupsCache.mu.Unlock()
}

// InitGroupsCache initializes the groups cache
func InitGroupsCache(dataDirectory string) {
	groupsDataDir = dataDirectory
	loadGroupsFromDisk()
}
