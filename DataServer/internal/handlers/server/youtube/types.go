package youtube

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/integrations/youtube"
)

// PrivateVideosCacheEntry holds cached private videos with timestamp.
type PrivateVideosCacheEntry struct {
	Videos    []gin.H
	Timestamp time.Time
}

// YouTubeHandlers contains handlers for YouTube API endpoints.
//
// PR-YT-REPO: the previous `storage *youtube.Storage` field is gone.
// The Storage facade was destroyed when YouTubeStore + StorageStore
// collapsed into youtube.Repository; storage-side methods
// (AddChannel, MoveChannel, UpdateChannelLanguage, etc.) are now
// methods on *youtube.Service. Handlers thread *Service only.
type YouTubeHandlers struct {
	service              *youtube.Service
	privateVideosCache   map[string]PrivateVideosCacheEntry
	privateVideosCacheMu sync.RWMutex
}

// NewYouTubeHandlers creates a new YouTubeHandlers instance.
//
// PR-YT-REPO: takes *Service only. The legacy (service, *Storage)
// signature is removed because the Storage facade no longer exists —
// any caller that needed Storage-backed reads (LoadData) now calls
// Service.LoadData() instead.
func NewYouTubeHandlers(service *youtube.Service) (*YouTubeHandlers, error) {
	if service == nil {
		return nil, fmt.Errorf("YouTubeHandlers: no existing service provided, got nil")
	}
	return &YouTubeHandlers{
		service:            service,
		privateVideosCache: make(map[string]PrivateVideosCacheEntry),
	}, nil
}

// ClearPrivateVideosCache clears the cached private videos list (e.g. after upload).
func (h *YouTubeHandlers) ClearPrivateVideosCache() {
	h.privateVideosCacheMu.Lock()
	h.privateVideosCache = make(map[string]PrivateVideosCacheEntry)
	h.privateVideosCacheMu.Unlock()
	log.Printf("[CLEANUP] YouTubeHandlers: cleared private videos cache")
}

// PrivVideosCache returns the private videos cache (for sub-package access).
func (h *YouTubeHandlers) PrivVideosCache() map[string]PrivateVideosCacheEntry {
	return h.privateVideosCache
}

// PrivVideosCacheMu returns the private videos cache mutex (for sub-package access).
func (h *YouTubeHandlers) PrivVideosCacheMu() *sync.RWMutex {
	return &h.privateVideosCacheMu
}

// GetService returns the underlying YouTube service for integration with other handlers.
func (h *YouTubeHandlers) GetService() *youtube.Service {
	return h.service
}
