// Package livestream provides livestream management routes as a self-contained module.
package livestream

import (
	"log"

	"github.com/gin-gonic/gin"
	"velox-server/internal/app"
	"velox-server/internal/handlers/remote/livestream"
	"velox-server/internal/integrations/youtube"
	"velox-server/internal/store"
)

// Module provides livestream management endpoints.
type Module struct {
	app.BaseModule
	ytService *youtube.Service
	dbStore   *store.SQLiteStore
}

// New creates a new livestream module.
func New(ytService *youtube.Service, dbStore *store.SQLiteStore) *Module {
	return &Module{
		ytService: ytService,
		dbStore:   dbStore,
	}
}

// Name returns the module identifier.
func (m *Module) Name() string {
	return "livestream"
}

// RegisterRoutes registers livestream API endpoints.
func (m *Module) RegisterRoutes(r *gin.Engine) {
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
