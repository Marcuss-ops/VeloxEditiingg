package health

import (
	"context"

	"github.com/gin-gonic/gin"
	"velox-server/internal/app"
)

// Module provides health check endpoints.
type Module struct {
	app.BaseModule
}

// New creates a new health module.
func New() *Module {
	return &Module{}
}

// Name returns the module identifier.
func (m *Module) Name() string {
	return "health"
}

// RegisterRoutes registers health check endpoints.
func (m *Module) RegisterRoutes(r *gin.Engine) {
	r.GET("/health", m.health)
	r.GET("/api/health", m.health)
	r.GET("/api/v1/health", m.health)
	r.GET("/metrics", m.metrics)
}

func (m *Module) health(c *gin.Context) {
	c.JSON(200, gin.H{"status": "healthy"})
}

func (m *Module) metrics(c *gin.Context) {
	c.String(200, "# metrics stub")
}

// Start initializes the module.
func (m *Module) Start(ctx context.Context) error {
	return nil
}

// Stop gracefully shuts down the module.
func (m *Module) Stop(ctx context.Context) error {
	return nil
}
