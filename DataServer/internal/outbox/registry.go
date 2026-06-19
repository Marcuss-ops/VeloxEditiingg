// Package outbox/registry — Handler interface + thread-safe Registry.
//
// Per PR 8: "Anche qui applicare la regola: un registry comune, nessuno
// switch duplicato in più package." — the registry is the ONLY mapping
// from event_type → handler in the codebase. New event types are added by
// calling Register at startup; no SQL change is needed because the
// outbox_events table no longer has a CHECK constraint on event_type.
package outbox

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// Handler is what a Registry maps an event_type to.
//
// EventType returns the canonical event name, e.g. "WORKFLOW_STEP_READY".
// Handle is invoked after the dispatcher atomically claims the event; it
// must be idempotent because the dispatcher may retry on transient errors.
//
// Returning a *HandlerError{Temporary: true} signals "retry later". Any
// other non-nil error is treated as permanent (event → FAILED).
type Handler interface {
	EventType() string
	Handle(ctx context.Context, e Event) error
}

// HandlerFunc is the convenience adapter for functions-as-handlers.
type HandlerFunc struct {
	Type  string
	Apply func(ctx context.Context, e Event) error
}

func (h HandlerFunc) EventType() string { return h.Type }
func (h HandlerFunc) Handle(ctx context.Context, e Event) error {
	return h.Apply(ctx, e)
}

// Registry is a thread-safe event_type → Handler map.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRegistry builds an empty registry.
func NewRegistry() *Registry {
	return &Registry{handlers: map[string]Handler{}}
}

// Register associates h.EventType() → h in the registry. Returns an
// error when the event type is already registered; production code
// should treat that as a configuration bug at startup.
func (r *Registry) Register(h Handler) error {
	if h == nil {
		return fmt.Errorf("outbox: nil handler")
	}
	t := h.EventType()
	if t == "" {
		return fmt.Errorf("outbox: handler %T has empty EventType()", h)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[t]; exists {
		return fmt.Errorf("outbox: handler already registered for %q", t)
	}
	r.handlers[t] = h
	return nil
}

// MustRegister panics on duplicate registrations; convenient at startup.
func (r *Registry) MustRegister(h Handler) {
	if err := r.Register(h); err != nil {
		panic(err)
	}
}

// Lookup returns the handler for a given event type or ErrNoHandler.
func (r *Registry) Lookup(eventType string) (Handler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[eventType]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrNoHandler, eventType)
	}
	return h, nil
}

// Types returns the sorted list of registered event types (for diagnostics).
func (r *Registry) Types() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	keys := make([]string, 0, len(r.handlers))
	for k := range r.handlers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Len returns the number of registered handlers.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.handlers)
}
