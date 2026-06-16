// Package channels provides YouTube channel management handlers.
package channels

import (
	ytservice "velox-server/internal/integrations/youtube"
)

// Handler wraps service and storage for channel operations.
type Handler struct {
	service *ytservice.Service
	storage *ytservice.Storage
}

// NewHandler creates a channel handler.
func NewHandler(service *ytservice.Service, storage *ytservice.Storage) *Handler {
	return &Handler{
		service: service,
		storage: storage,
	}
}
