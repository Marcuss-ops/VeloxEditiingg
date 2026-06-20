package app

import (
	"github.com/gin-gonic/gin"
)

// HealthModule provides health check endpoints.
type HealthModule struct {
}

// NewHealthModule creates a new health module.
func NewHealthModule() *HealthModule {
	return &HealthModule{}
}

// Name returns the module identifier.
func (m *HealthModule) Name() string {
	return "health"
}

// RegisterRoutes registers health check endpoints.
func (m *HealthModule) RegisterRoutes(r *gin.Engine) {
	r.GET("/health", m.health)
	r.GET("/api/health", m.health)
}

func (m *HealthModule) health(c *gin.Context) {
	c.JSON(200, gin.H{"status": "healthy"})
}
