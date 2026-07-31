package publication

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

var (
	// ErrProviderAlreadyRegistered identifies a duplicate provider registration.
	ErrProviderAlreadyRegistered = errors.New("publication: provider already registered")
	// ErrProviderNotFound identifies a provider lookup miss.
	ErrProviderNotFound = errors.New("publication: provider not found")
	// ErrInvalidProvider identifies an invalid provider name or nil adapter.
	ErrInvalidProvider = errors.New("publication: invalid provider")
)

// Provider is the shared, provider-neutral adapter contract. The registry does
// not know how a provider uploads bytes or stores state; it only gives the
// delivery layer a stable lookup and a provider-specific metadata validation
// hook. Provider implementations must not mutate the supplied snapshot.
type Provider interface {
	// Name returns the canonical provider key, such as "youtube" or "drive".
	Name() string
	// Validate checks provider-specific metadata and capability limits after
	// ResolvePublication has applied the shared precedence rules.
	Validate(ResolvedPublication) error
}

// ProviderRegistry is an immutable registry. Register returns a new value and
// never mutates the receiver, matching the shared rendercompiler registry
// convention and making a registry safe to share across concurrent deliveries.
type ProviderRegistry struct {
	providers map[string]Provider
}

// NewProviderRegistry creates an empty provider registry.
func NewProviderRegistry() ProviderRegistry {
	return ProviderRegistry{providers: make(map[string]Provider)}
}

// Register returns a copy containing provider. Duplicate names are rejected
// rather than silently replaced, because provider identity is part of the
// delivery contract and accidental replacement can route uploads incorrectly.
func (r ProviderRegistry) Register(provider Provider) (ProviderRegistry, error) {
	if isNilProvider(provider) {
		return ProviderRegistry{}, fmt.Errorf("%w: nil provider", ErrInvalidProvider)
	}
	name := strings.TrimSpace(provider.Name())
	if name == "" {
		return ProviderRegistry{}, fmt.Errorf("%w: provider name is required", ErrInvalidProvider)
	}
	if strings.ContainsAny(name, " ./\\") {
		return ProviderRegistry{}, fmt.Errorf("%w: provider name %q contains a reserved separator", ErrInvalidProvider, name)
	}
	if r.providers != nil {
		if _, exists := r.providers[name]; exists {
			return ProviderRegistry{}, fmt.Errorf("%w: %s", ErrProviderAlreadyRegistered, name)
		}
	}
	copy := make(map[string]Provider, len(r.providers)+1)
	for key, value := range r.providers {
		copy[key] = value
	}
	copy[name] = provider
	return ProviderRegistry{providers: copy}, nil
}

func isNilProvider(provider Provider) bool {
	if provider == nil {
		return true
	}
	value := reflect.ValueOf(provider)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// Resolve returns the provider registered under name.
func (r ProviderRegistry) Resolve(name string) (Provider, error) {
	name = strings.TrimSpace(name)
	provider, ok := r.providers[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrProviderNotFound, name)
	}
	return provider, nil
}

// Has reports whether name is registered.
func (r ProviderRegistry) Has(name string) bool {
	_, err := r.Resolve(name)
	return err == nil
}

// Names returns registered provider names in deterministic order.
func (r ProviderRegistry) Names() []string {
	out := make([]string, 0, len(r.providers))
	for name := range r.providers {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Len returns the number of registered providers.
func (r ProviderRegistry) Len() int {
	return len(r.providers)
}
