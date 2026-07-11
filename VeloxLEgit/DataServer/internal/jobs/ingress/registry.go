package ingress

import (
	"context"
	"fmt"
	"sync"

	"velox-server/internal/costmodel"
)

type Builder func(
	ctx context.Context,
	raw map[string]any,
) (map[string]any, error)

type Definition struct {
	Kind            string
	ExecutorID      string
	ExecutorVersion int
	PipelineID      string
	Builder         Builder
	Requirements    costmodel.JobRequirements
}

type Registry struct {
	mu   sync.RWMutex
	defs map[string]Definition
}

func NewRegistry() *Registry {
	return &Registry{
		defs: make(map[string]Definition),
	}
}

func (r *Registry) Register(def Definition) error {
	if def.Kind == "" {
		return fmt.Errorf("job ingress: kind is required")
	}
	if def.Builder == nil {
		return fmt.Errorf("job ingress %q: builder is required", def.Kind)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.defs[def.Kind]; exists {
		return fmt.Errorf("job ingress %q already registered", def.Kind)
	}

	r.defs[def.Kind] = def
	return nil
}

func (r *Registry) MustRegister(def Definition) {
	if err := r.Register(def); err != nil {
		panic(err)
	}
}

func (r *Registry) Resolve(kind string) (Definition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	def, ok := r.defs[kind]
	return def, ok
}
