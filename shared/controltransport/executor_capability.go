package controltransport

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// ExecutorCapability is the canonical description advertised by a worker for
// one executable task implementation. It is shared by the worker, master,
// scheduler, health probes, and API projections.
type ExecutorCapability struct {
	ID            string   `json:"id"`
	Version       int      `json:"version"`
	ResourceClass string   `json:"resource_class"`
	TemporalMode  string   `json:"temporal_mode"`
	Deterministic bool     `json:"deterministic"`
	Cacheable     bool     `json:"cacheable"`
	SupportsAlpha bool     `json:"supports_alpha"`
	OutputTypes   []string `json:"output_types,omitempty"`
}

// Key is the canonical placement identity for an executor capability.
func (c ExecutorCapability) Key() string { return fmt.Sprintf("%s@%d", c.ID, c.Version) }

// Validate checks the identity fields required for placement.
func (c ExecutorCapability) Validate() error {
	if strings.TrimSpace(c.ID) == "" {
		return fmt.Errorf("executor capability id is required")
	}
	if strings.Contains(c.ID, "@") {
		return fmt.Errorf("executor capability id %q must not contain @", c.ID)
	}
	if c.Version <= 0 {
		return fmt.Errorf("executor capability %q version must be positive", c.ID)
	}
	return nil
}

// ExecutorRegistry is the canonical immutable-by-copy set of executor
// capabilities advertised by one worker. All accessors return copies or
// values, so consumers cannot mutate the worker's capability snapshot.
type ExecutorRegistry struct {
	executors []ExecutorCapability
}

// NewExecutorRegistry validates and stores capabilities in deterministic
// (ID, Version) order. Duplicate identities are rejected.
func NewExecutorRegistry(capabilities ...ExecutorCapability) (ExecutorRegistry, error) {
	out := ExecutorRegistry{executors: make([]ExecutorCapability, 0, len(capabilities))}
	seen := make(map[string]struct{}, len(capabilities))
	for _, capability := range capabilities {
		capability.ID = strings.TrimSpace(capability.ID)
		if err := capability.Validate(); err != nil {
			return ExecutorRegistry{}, err
		}
		if _, exists := seen[capability.Key()]; exists {
			return ExecutorRegistry{}, fmt.Errorf("duplicate executor capability %s", capability.Key())
		}
		seen[capability.Key()] = struct{}{}
		capability.OutputTypes = append([]string(nil), capability.OutputTypes...)
		out.executors = append(out.executors, capability)
	}
	sort.Slice(out.executors, func(i, j int) bool {
		if out.executors[i].ID != out.executors[j].ID {
			return out.executors[i].ID < out.executors[j].ID
		}
		return out.executors[i].Version < out.executors[j].Version
	})
	return out, nil
}

// NewExecutorRegistryFromLegacy is a compatibility constructor for callers
// that still hold the pre-typed capabilities map.
func NewExecutorRegistryFromLegacy(raw interface{}) (ExecutorRegistry, error) {
	return ExecutorRegistryFromLegacy(raw)
}

// EmptyExecutorRegistry returns a valid empty registry.
func EmptyExecutorRegistry() ExecutorRegistry {
	return ExecutorRegistry{executors: []ExecutorCapability{}}
}

// All returns a deterministic copy of every advertised capability.
func (r ExecutorRegistry) All() []ExecutorCapability {
	out := make([]ExecutorCapability, len(r.executors))
	copy(out, r.executors)
	for i := range out {
		out[i].OutputTypes = append([]string(nil), out[i].OutputTypes...)
	}
	return out
}

func (r ExecutorRegistry) Len() int { return len(r.executors) }

func (r ExecutorRegistry) IsEmpty() bool { return len(r.executors) == 0 }

func (r ExecutorRegistry) Has(id string, version int) bool {
	for _, capability := range r.executors {
		if capability.ID == id && capability.Version == version {
			return true
		}
	}
	return false
}

func (r ExecutorRegistry) HasID(id string) bool {
	for _, capability := range r.executors {
		if capability.ID == id {
			return true
		}
	}
	return false
}

// Without returns a copy of the registry without one executor identity.
// It is used when a worker rejects a capability after admission.
func (r ExecutorRegistry) Without(id string, version int) ExecutorRegistry {
	capabilities := make([]ExecutorCapability, 0, len(r.executors))
	for _, capability := range r.executors {
		if capability.ID == id && capability.Version == version {
			continue
		}
		capabilities = append(capabilities, capability)
	}
	out, err := NewExecutorRegistry(capabilities...)
	if err != nil {
		// The receiver was validated at construction time, so this is
		// unreachable unless the type gains an invalid internal state.
		// Preserve the current admission set rather than fail-open to an
		// empty registry and accidentally reject every task.
		return r
	}
	return out
}

func (r ExecutorRegistry) Primary() (ExecutorCapability, bool) {
	if len(r.executors) == 0 {
		return ExecutorCapability{}, false
	}
	return r.executors[0], true
}

// MarshalJSON emits the compact canonical executor array. The enclosing
// capability report remains responsible for the `executors` key.
func (r ExecutorRegistry) MarshalJSON() ([]byte, error) { return json.Marshal(r.executors) }

// UnmarshalJSON restores the canonical registry from its persisted executor
// array. Validation remains centralized in NewExecutorRegistry so database
// reloads cannot admit a malformed capability identity.
func (r *ExecutorRegistry) UnmarshalJSON(data []byte) error {
	if r == nil {
		return fmt.Errorf("cannot unmarshal executor registry into nil receiver")
	}
	var capabilities []ExecutorCapability
	if err := json.Unmarshal(data, &capabilities); err != nil {
		return err
	}
	decoded, err := NewExecutorRegistry(capabilities...)
	if err != nil {
		return err
	}
	*r = decoded
	return nil
}

// ExecutorRegistryFromLegacy decodes the pre-typed capabilities map. It is
// intentionally the only compatibility adapter used by migration tests and
// persistence boundaries that already know the payload is legacy.
func ExecutorRegistryFromLegacy(raw interface{}) (ExecutorRegistry, error) {
	if raw == nil {
		return EmptyExecutorRegistry(), nil
	}
	if typed, ok := raw.(ExecutorRegistry); ok {
		return typed, nil
	}
	if typed, ok := raw.([]ExecutorCapability); ok {
		return NewExecutorRegistry(typed...)
	}
	var executors interface{}
	switch value := raw.(type) {
	case map[string]interface{}:
		executors = value["executors"]
	default:
		executors = raw
	}
	if executors == nil {
		return EmptyExecutorRegistry(), nil
	}
	items := make([]ExecutorCapability, 0)
	switch list := executors.(type) {
	case []ExecutorCapability:
		items = append(items, list...)
	case []interface{}:
		for i, item := range list {
			m, ok := item.(map[string]interface{})
			if !ok {
				return ExecutorRegistry{}, fmt.Errorf("executors[%d] must be an object, got %T", i, item)
			}
			capability, err := capabilityFromMap(m)
			if err != nil {
				return ExecutorRegistry{}, fmt.Errorf("executors[%d]: %w", i, err)
			}
			items = append(items, capability)
		}
	case []map[string]interface{}:
		for i, m := range list {
			capability, err := capabilityFromMap(m)
			if err != nil {
				return ExecutorRegistry{}, fmt.Errorf("executors[%d]: %w", i, err)
			}
			items = append(items, capability)
		}
	default:
		return ExecutorRegistry{}, fmt.Errorf("executors must be an array, got %T", executors)
	}
	return NewExecutorRegistry(items...)
}

// ExecutorRegistryFromLegacyStrict decodes a capability report at an
// authenticated registration/heartbeat boundary. Missing executor metadata
// is an admission error, not an empty-capability fallback.
func ExecutorRegistryFromLegacyStrict(raw interface{}) (ExecutorRegistry, error) {
	m, ok := raw.(map[string]interface{})
	if !ok || m == nil {
		return ExecutorRegistry{}, fmt.Errorf("capability report must be an object")
	}
	if _, ok := m["executors"]; !ok {
		return ExecutorRegistry{}, fmt.Errorf("capability report executors array is required")
	}
	return ExecutorRegistryFromLegacy(m)
}

func capabilityFromMap(m map[string]interface{}) (ExecutorCapability, error) {
	capability := ExecutorCapability{}
	capability.ID, _ = m["id"].(string)
	capability.Version = integerValue(m["version"])
	capability.ResourceClass, _ = m["resource_class"].(string)
	capability.TemporalMode, _ = m["temporal_mode"].(string)
	capability.Deterministic, _ = m["deterministic"].(bool)
	capability.Cacheable, _ = m["cacheable"].(bool)
	capability.SupportsAlpha, _ = m["supports_alpha"].(bool)
	if values, ok := m["output_types"].([]interface{}); ok {
		for _, value := range values {
			if s, ok := value.(string); ok {
				capability.OutputTypes = append(capability.OutputTypes, s)
			}
		}
	}
	if values, ok := m["output_types"].([]string); ok {
		capability.OutputTypes = append(capability.OutputTypes, values...)
	}
	return capability, capability.Validate()
}

func integerValue(value interface{}) int {
	switch number := value.(type) {
	case int:
		return number
	case int32:
		return int(number)
	case int64:
		return int(number)
	case float32:
		return int(number)
	case float64:
		return int(number)
	case json.Number:
		var numberInt int
		_, _ = fmt.Sscan(string(number), &numberInt)
		return numberInt
	default:
		return 0
	}
}
