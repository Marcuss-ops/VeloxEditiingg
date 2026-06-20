package app

import (
	"log"

	"github.com/gin-gonic/gin"

	"velox-server/internal/handlers/remote/livestream"
	"velox-server/internal/integrations/youtube"
	"velox-server/internal/store"
)

// LivestreamModule provides livestream management endpoints.
///
/// PR15.1: ytService is now a concrete *youtube.Service pointer instead
/// of a lazy func() indirection. The YouTube module's Service is built
/// eagerly by NewYouTubeModule, so bootstrap can inject the pointer
/// directly. Storing a function made the lazy-init pattern mandatory
/// (and hid the same lifecycle bug as NewYouTubeModule).
type LivestreamModule struct {
	ytService *youtube.Service
	dbStore   *store.SQLiteStore
}

// NewLivestreamModule creates a new livestream module bound to the
// already-constructed YouTube integration service.
func NewLivestreamModule(ytService *youtube.Service, dbStore *store.SQLiteStore) *LivestreamModule {
	return &LivestreamModule{
		ytService: ytService,
		dbStore:   dbStore,
	}
}

// Name returns the module identifier.
func (m *LivestreamModule) Name() string {
	return "livestream"
}

// RegisterRoutes registers livestream API endpoints.

// PR15.1: short-circuit when ytService is nil. Bootstrap now injects a
// concrete *youtube.Service; if it's nil, the livestream module cannot
// operate (it would panic inside the handler constructor). Mirror the
// YouTube module's nil-mode contract: log + skip rather than panic.
func (m *LivestreamModule) RegisterRoutes(r *gin.Engine) {
	if m.ytService == nil {
		log.Printf("[LIVESTREAM] Skipping route registration - YouTube service not initialized")
		return
	}
	handlers := livestream.NewLivestreamHandlers(m.ytService, m.dbStore)

	v1 := r.Group("/api/v1")
	{
		v1.GET("/livestream", handlers.ListStreams)
		v1.POST("/livestream", handlers.CreateStream)
		v1.GET("/livestream/status", handlers.GetStatus)
		v1.GET("/livestream/:id", handlers.GetStream)
		v1.PUT("/livestream/:id", handlers.UpdateStream)
		v1.DELETE("/livestream/:id", handlers.DeleteStream)
		v1.POST("/livestream/:id/testing", handlers.StartTesting)
		v1.POST("/livestream/:id/live", handlers.GoLive)
		v1.POST("/livestream/:id/complete", handlers.EndStream)
	}

	log.Printf("[LIVESTREAM] Routes registered at /api/v1/livestream/*")
}
