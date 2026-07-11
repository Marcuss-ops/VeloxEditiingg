// Package groups serves the canonical YouTube groups endpoints
// backed by the `youtube_groups` + `group_channels` SQLite tables.
//
// PR-DI-groups: constructor-based dependency injection. The previous
// design used a package-level *store.SQLiteStore mutator
// (InitGroupsStore) read at request time. Handlers now receives its
// dependency on the struct so composition-root wiring is explicit and
// tests construct their own dependency graph without touching global
// state.
package groups

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
)

// Group represents a YouTube channel group.
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

// Channel represents a YouTube channel.
type Channel struct {
	ID        string   `json:"id"`
	URL       string   `json:"url"`
	Title     string   `json:"title,omitempty"`
	Thumbnail string   `json:"thumbnail,omitempty"`
	Notes     string   `json:"notes,omitempty"`
	AddedAt   string   `json:"added_at,omitempty"`
	Keywords  []string `json:"keywords,omitempty"`
}

// Translate represents translation settings.
type Translate struct {
	Enabled     bool     `json:"enabled,omitempty"`
	TargetLangs []string `json:"target_langs,omitempty"`
}

// Schedule represents schedule settings.
type Schedule struct {
	Mode      string `json:"mode,omitempty"`
	PublishAt string `json:"publish_at,omitempty"`
}

// Handlers wires the canonical SQLite groups store to the HTTP layer.
// Constructed once at composition root and threaded through gin via
// RegisterRoutes, methods, or direct closure capture.
type Handlers struct {
	db *store.SQLiteStore
}

// NewHandlers returns a Handlers backed by the canonical SQLite groups
// store (youtube_groups + group_channels tables).
func NewHandlers(db *store.SQLiteStore) *Handlers {
	return &Handlers{db: db}
}

// RegisterRoutes mounts the two public groups endpoints on the given
// route group. Callers (composition root, tests) choose the path
// prefix; the canonical mount is "/api/v1/groups".
func (h *Handlers) RegisterRoutes(r gin.IRoutes) {
	r.GET("", h.GetGroups())
	r.GET("/:name", h.GetGroup())
}

// GetGroups returns GET <prefix>/ and lists every canonical YouTube
// group. Each group's channel membership is loaded lazily from the
// `group_channels` join table when a row has an associated ID.
func (h *Handlers) GetGroups() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.db == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"ok":    false,
				"error": "Groups store not configured",
			})
			return
		}

		rows, err := h.db.ListYouTubeGroups()
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

			if row.ID > 0 {
				channelIDs, err := h.db.ListGroupChannels(row.ID)
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
}

// GetGroup returns GET <prefix>/:name and fetches a single group by
// its canonical name. Returns 503 when the handler is not configured
// at construction time and 404 when no match exists.
func (h *Handlers) GetGroup() gin.HandlerFunc {
	return func(c *gin.Context) {
		name := c.Param("name")

		if h.db == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"ok":    false,
				"error": "Groups store not configured",
			})
			return
		}

		rows, err := h.db.ListYouTubeGroups()
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

				if row.ID > 0 {
					channelIDs, err := h.db.ListGroupChannels(row.ID)
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
}
