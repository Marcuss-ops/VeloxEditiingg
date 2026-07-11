package app

import (
	"github.com/gin-gonic/gin"
)

// Module defines the interface for a self-contained feature module.
// Each module owns its routes and dependencies.
type Module interface {
	// Name returns the unique identifier for this module.
	Name() string

	// RegisterRoutes registers all HTTP routes for this module on the given router.
	RegisterRoutes(r *gin.Engine)
}
