package publisher

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Registry resolves transport_id to a registered Transport. The worker creates
// one Registry at startup and reuses it across Attempts.
type Registry struct {
	mu    sync.RWMutex
	items map[string]Transport
}

// NewRegistry returns a Registry pre-populated with the in-tree transports.
func NewRegistry() *Registry {
	r := &Registry{items: make(map[string]Transport)}
	_ = r.Register(&MasterStreamTransport{
		HTTPClient: &http.Client{Timeout: 5 * time.Minute},
	})
	_ = r.Register(&ObjectStoreMultipartTransport{})
	return r
}

// Register adds a transport under its ID. It returns an error for a nil
// transport, an empty ID, or a duplicate ID.
func (r *Registry) Register(t Transport) error {
	if t == nil {
		return fmt.Errorf("publisher: nil transport")
	}
	id := t.ID()
	if id == "" {
		return fmt.Errorf("publisher: transport with empty ID")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.items[id]; exists {
		return fmt.Errorf("publisher: transport %q already registered", id)
	}
	r.items[id] = t
	return nil
}

// Resolve returns the Transport registered under transportID.
func (r *Registry) Resolve(transportID string) (Transport, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.items[transportID]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownTransport, transportID)
	}
	return t, nil
}

// IDs returns the registered transport IDs.
func (r *Registry) IDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.items))
	for k := range r.items {
		out = append(out, k)
	}
	return out
}
