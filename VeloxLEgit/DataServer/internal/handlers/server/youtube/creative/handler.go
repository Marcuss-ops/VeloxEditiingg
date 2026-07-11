package creative

import "velox-server/internal/integrations/youtube"

// Handler contains creative/AI-related HTTP handlers for the YouTube module.
type Handler struct {
	svc *youtube.Service
}

// NewHandler creates a new creative Handler.
func NewHandler(svc *youtube.Service) *Handler {
	return &Handler{svc: svc}
}
