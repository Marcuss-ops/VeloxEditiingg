package groups

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
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

var groupsStore *store.SQLiteStore

// InitGroupsStore sets the SQLite store for groups handlers.
func InitGroupsStore(db *store.SQLiteStore) {
	groupsStore = db
}

// GetGroupsHandler returns all groups from SQLite (canonical youtube_groups).
func GetGroupsHandler(c *gin.Context) {
	if groupsStore == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"ok":    false,
			"error": "Groups store not initialized",
		})
		return
	}

	// Try canonical tables first
	rows, err := groupsStore.ListYouTubeGroups()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"ok":    false,
			"error": err.Error(),
		})
		return
	}

	result := make([]Group, 0, len(rows))
	for _, row := range rows {
		g := Group{
			Name:    row.Name,
			Privacy: row.Privacy,
		}
		g.DefaultLang = row.Description

		// Load channel memberships from canonical group_channels if group ID is available
		if row.ID > 0 {
			channelIDs, err := groupsStore.ListGroupChannels(row.ID)
			if err == nil && len(channelIDs) > 0 {
				g.Channels = make([]Channel, len(channelIDs))
				for i, id := range channelIDs {
					g.Channels[i] = Channel{ID: id}
				}
			}
		}
		result = append(result, g)
	}

	c.JSON(http.StatusOK, result)
}

// GetGroupHandler returns a specific group from SQLite.
func GetGroupHandler(c *gin.Context) {
	name := c.Param("name")

	if groupsStore == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"ok":    false,
			"error": "Groups store not initialized",
		})
		return
	}

	// Try canonical tables first
	rows, err := groupsStore.ListYouTubeGroups()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"ok":    false,
			"error": err.Error(),
		})
		return
	}

	for _, row := range rows {
		if row.Name == name {
			g := Group{
				Name:    name,
				Privacy: row.Privacy,
			}
			g.DefaultLang = row.Description

			// Load channel memberships
			if row.ID > 0 {
				channelIDs, err := groupsStore.ListGroupChannels(row.ID)
				if err == nil && len(channelIDs) > 0 {
					g.Channels = make([]Channel, len(channelIDs))
					for i, id := range channelIDs {
						g.Channels[i] = Channel{ID: id}
					}
				}
			}

			c.JSON(http.StatusOK, g)
			return
		}
	}

	c.JSON(http.StatusNotFound, gin.H{
		"ok":    false,
		"error": "Group not found",
	})
}


