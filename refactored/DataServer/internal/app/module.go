package app

import (
	"context"

	"github.com/gin-gonic/gin"
)

// Module defines the interface for a self-contained feature module.
// Each module owns its routes, lifecycle, and dependencies.
type Module interface {
	// Name returns the unique identifier for this module.
	Name() string

	// RegisterRoutes registers all HTTP routes for this module on the given router.
	RegisterRoutes(r *gin.Engine)

	// Start initializes the module (e.g., background workers, caches).
	// Called once during server startup after all modules are registered.
	Start(ctx context.Context) error

	// Stop gracefully shuts down the module (e.g., drain connections, flush buffers).
	// Called once during server shutdown.
	Stop(ctx context.Context) error
}

// BaseModule provides default no-op implementations for Start and Stop.
// Embed this in modules that don't need lifecycle management.
type BaseModule struct{}

func (m *BaseModule) Start(ctx context.Context) error { return nil }
func (m *BaseModule) Stop(ctx context.Context) error  { return nil }
