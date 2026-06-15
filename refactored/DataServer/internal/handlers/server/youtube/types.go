package youtube

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/integrations/youtube"
)

type privateVideosCacheEntry struct {
	Videos    []gin.H
	Timestamp time.Time
}

// YouTubeHandlers contains handlers for YouTube API endpoints
type YouTubeHandlers struct {
	service              *youtube.Service
	storage              *youtube.Storage // Shared storage with YouTubeManager for unified group data
	privateVideosCache   map[string]privateVideosCacheEntry
	privateVideosCacheMu sync.RWMutex
}

// NewYouTubeHandlers creates a new YouTubeHandlers instance.
func NewYouTubeHandlers(service *youtube.Service, storage *youtube.Storage) (*YouTubeHandlers, error) {
	if service == nil {
		return nil, fmt.Errorf("YouTubeHandlers: no existing service provided, got nil")
	}
	return &YouTubeHandlers{
		service:            service,
		storage:            storage,
		privateVideosCache: make(map[string]privateVideosCacheEntry),
	}, nil
}

// ClearPrivateVideosCache clears the cached private videos list (e.g. after upload)
func (h *YouTubeHandlers) ClearPrivateVideosCache() {
	h.privateVideosCacheMu.Lock()
	h.privateVideosCache = make(map[string]privateVideosCacheEntry)
	h.privateVideosCacheMu.Unlock()
	log.Printf("[CLEANUP] YouTubeHandlers: cleared private videos cache")
}

// GetService returns the underlying YouTube service for integration with other handlers
func (h *YouTubeHandlers) GetService() *youtube.Service {
	return h.service
}

// GetStorage returns the shared YouTube storage
func (h *YouTubeHandlers) GetStorage() *youtube.Storage {
	return h.storage
}
