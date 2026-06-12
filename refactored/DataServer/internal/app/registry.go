package app

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/gin-gonic/gin"
)

// Registry manages the lifecycle and route registration of all modules.
type Registry struct {
	modules []Module
	mu      sync.RWMutex
}

// NewRegistry creates a new empty module registry.
func NewRegistry() *Registry {
	return &Registry{}
}

// Register adds a module to the registry.
// Modules are registered in order and routes are applied in that same order.
func (r *Registry) Register(m Module) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.modules = append(r.modules, m)
	log.Printf("[MODULE] Registered: %s", m.Name())
}

// RegisterRoutes calls RegisterRoutes on all registered modules.
func (r *Registry) RegisterRoutes(router *gin.Engine) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, m := range r.modules {
		log.Printf("[MODULE] Registering routes for: %s", m.Name())
		m.RegisterRoutes(router)
	}
}

// Start initializes all modules in registration order.
func (r *Registry) Start(ctx context.Context) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, m := range r.modules {
		log.Printf("[MODULE] Starting: %s", m.Name())
		if err := m.Start(ctx); err != nil {
			return fmt.Errorf("module %s failed to start: %w", m.Name(), err)
		}
	}
	return nil
}

// Stop gracefully shuts down all modules in reverse order.
func (r *Registry) Stop(ctx context.Context) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var lastErr error
	for i := len(r.modules) - 1; i >= 0; i-- {
		m := r.modules[i]
		log.Printf("[MODULE] Stopping: %s", m.Name())
		if err := m.Stop(ctx); err != nil {
			log.Printf("[MODULE] Error stopping %s: %v", m.Name(), err)
			lastErr = err
		}
	}
	return lastErr
}

// List returns the names of all registered modules.
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, len(r.modules))
	for i, m := range r.modules {
		names[i] = m.Name()
	}
	return names
}

// Len returns the number of registered modules.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.modules)
}
