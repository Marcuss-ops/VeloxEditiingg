package app

import (
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
