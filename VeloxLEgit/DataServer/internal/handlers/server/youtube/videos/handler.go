package videos

import "velox-server/internal/integrations/youtube"

// Handler contains video-related HTTP handlers for the YouTube module.
type Handler struct {
	svc        *youtube.Service
	clearCache func()
}

// NewHandler creates a new videos Handler.
func NewHandler(svc *youtube.Service, clearCache func()) *Handler {
	return &Handler{svc: svc, clearCache: clearCache}
}
