package health

import (
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
}

func (m *Module) health(c *gin.Context) {
	c.JSON(200, gin.H{"status": "healthy"})
}


