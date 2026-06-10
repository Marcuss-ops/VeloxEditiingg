package youtube

import (
	"log"
	"sync"
	"time"
	"velox-server/internal/integrations/youtube"
	"velox-server/internal/store"
	"github.com/gin-gonic/gin"
)

type privateVideosCacheEntry struct {
	Videos    []gin.H
	Timestamp time.Time
}

// YouTubeHandlers contains handlers for YouTube API endpoints
type YouTubeHandlers struct {
	service              *youtube.Service
	storage              *youtube.Storage // Shared storage with YouTubeManager for unified group data
	store                store.ProjectStore
	privateVideosCache   map[string]privateVideosCacheEntry
	privateVideosCacheMu sync.RWMutex
}

// NewYouTubeHandlers creates a new YouTubeHandlers instance
func NewYouTubeHandlers(cfg *youtube.ServiceConfig, storage *youtube.Storage, projectStore store.ProjectStore) (*YouTubeHandlers, error) {
	service, err := youtube.NewService(cfg)
	if err != nil {
		return nil, err
	}
	return &YouTubeHandlers{
		service:            service,
		storage:            storage,
		store:              projectStore,
		privateVideosCache: make(map[string]privateVideosCacheEntry),
	}, nil
}

// ClearPrivateVideosCache clears the cached private videos list (e.g. after upload)
func (h *YouTubeHandlers) ClearPrivateVideosCache() {
	h.privateVideosCacheMu.Lock()
	h.privateVideosCache = make(map[string]privateVideosCacheEntry)
	h.privateVideosCacheMu.Unlock()
	log.Printf("🧹 YouTubeHandlers: cleared private videos cache")
}

// GetService returns the underlying YouTube service for integration with other handlers
func (h *YouTubeHandlers) GetService() *youtube.Service {
	return h.service
}

// GetStorage returns the shared YouTube storage
func (h *YouTubeHandlers) GetStorage() *youtube.Storage {
	return h.storage
}

