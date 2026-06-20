package pipeline

import (
	"fmt"
	"sort"
	"strings"
)

// Registry maps pipeline IDs to their compilers.
// It is safe for concurrent use after initialization.
type Registry struct {
	compilers map[string]Compiler
}

// NewRegistry creates an empty pipeline registry.
func NewRegistry() *Registry {
	return &Registry{
		compilers: make(map[string]Compiler),
	}
}

// Register adds a compiler to the registry.
// Panics if a compiler with the same ID is already registered.
func (r *Registry) Register(c Compiler) {
	id := c.ID()
	if id == "" {
		panic("pipeline: Register called with empty ID")
	}
	if _, exists := r.compilers[id]; exists {
		panic(fmt.Sprintf("pipeline: duplicate registration for %q", id))
	}
	r.compilers[id] = c
}

// Resolve returns the compiler for the given pipeline ID.
// Returns error if the pipeline is not registered.
func (r *Registry) Resolve(pipelineID string) (Compiler, error) {
	c, ok := r.compilers[pipelineID]
	if !ok {
		return nil, fmt.Errorf("pipeline: unknown pipeline %q", pipelineID)
	}
	return c, nil
}

// MustResolve returns the compiler or panics.
func (r *Registry) MustResolve(pipelineID string) Compiler {
	c, err := r.Resolve(pipelineID)
	if err != nil {
		panic(err.Error())
	}
	return c
}

// Has returns true if a compiler is registered for the given ID.
func (r *Registry) Has(pipelineID string) bool {
	_, ok := r.compilers[pipelineID]
	return ok
}

// IDs returns all registered pipeline IDs sorted alphabetically.
func (r *Registry) IDs() []string {
	ids := make([]string, 0, len(r.compilers))
	for id := range r.compilers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// DetectPipelineID infers the pipeline ID from job parameters.
// It checks for a "pipeline_id" parameter first, then falls back
// to heuristic detection based on available fields.
func DetectPipelineID(params map[string]interface{}) string {
	// Explicit override
	if pid, ok := params["pipeline_id"].(string); ok && pid != "" {
		return pid
	}

	// Heuristic detection
	hasImages := hasNonEmptySlice(params, "images") || hasNonEmptySlice(params, "scene_image_paths")
	hasClips := hasNonEmptySlice(params, "clips") || hasNonEmptySlice(params, "clip_segments") ||
		hasNonEmptySlice(params, "intro_clip_paths") || hasNonEmptySlice(params, "stock_clip_paths")
	hasScript := hasNonEmptyString(params, "script") || hasNonEmptyString(params, "script_text")
	hasEntities := hasNonEmptyString(params, "entity_style")

	if hasEntities && hasScript {
		return "entities.v1"
	}
	if hasImages && hasClips {
		return "hybrid.v1"
	}
	if hasImages {
		return "images.v1"
	}
	if hasClips {
		return "clips.v1"
	}

	return ""
}

func hasNonEmptyString(m map[string]interface{}, key string) bool {
	v, ok := m[key]
	if !ok {
		return false
	}
	s, ok := v.(string)
	return ok && strings.TrimSpace(s) != ""
}

func hasNonEmptySlice(m map[string]interface{}, key string) bool {
	v, ok := m[key]
	if !ok {
		return false
	}
	switch val := v.(type) {
	case []interface{}:
		return len(val) > 0
	case []string:
		return len(val) > 0
	}
	return false
}
