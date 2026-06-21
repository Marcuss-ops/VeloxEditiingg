package executor

import (
	"fmt"
	"sort"
	"sync"
)

// Registry is the canonical, thread-safe catalog of executors on a worker.
//
// It is the SINGLE source of truth for which task types a worker can
// run. Worker hello (PR-3.5) reads from Registry.Descriptors() so the
// capability payload stays in lock-step with what the taskrunner can
// actually resolve.
//
// Concurrency: Register mutates state under a write lock; Resolve,
// Has, IDs, Descriptors, All, and Len take a read lock so they can be
// called concurrently from taskrunners without contention on Register.
type Registry struct {
	mu        sync.RWMutex
	executors map[string]Executor
}

// NewRegistry returns an empty Registry ready for Register calls.
// Worker bootstrap calls NewRegistry and immediately populates it.
func NewRegistry() *Registry {
	return &Registry{
		executors: make(map[string]Executor),
	}
}

// Register adds an executor to the registry. It validates the
// descriptor, then stores the executor under its (id, version) key.
// Returns ErrExecutorExists if the same key is already present.
// Returns ErrInvalidDescriptor (wrapped) for empty IDs, IDs containing
// "@", non-positive versions, or unknown ResourceClass/TemporalMode.
func (r *Registry) Register(e Executor) error {
	if r == nil {
		return fmt.Errorf("executor: nil registry")
	}
	if e == nil {
		return fmt.Errorf("%w: nil executor", ErrInvalidDescriptor)
	}
	desc := e.Descriptor()
	if err := desc.Validate(); err != nil {
		return err
	}

	k := desc.Key()
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.executors[k]; exists {
		return fmt.Errorf("%w: %s", ErrExecutorExists, k)
	}
	r.executors[k] = e
	return nil
}

// MustRegister panics on validation or duplicate errors. Convenient for
// worker bootstrap where duplicates are always a bug.
func (r *Registry) MustRegister(e Executor) {
	if err := r.Register(e); err != nil {
		panic(fmt.Sprintf("executor: MustRegister failed: %v", err))
	}
}

// Resolve returns the executor registered under (id, version).
// Returns ErrExecutorNotFound if no executor matches.
func (r *Registry) Resolve(id string, version int) (Executor, error) {
	if r == nil {
		return nil, fmt.Errorf("executor: nil registry")
	}
	k := fmt.Sprintf("%s@%d", id, version)
	r.mu.RLock()
	defer r.mu.RUnlock()
	if e, ok := r.executors[k]; ok {
		return e, nil
	}
	return nil, fmt.Errorf("%w: %s", ErrExecutorNotFound, k)
}

// Has returns true if an executor with the given (id, version) is
// registered. Useful for capability pre-checks without surfacing an
// error.
func (r *Registry) Has(id string, version int) bool {
	if r == nil {
		return false
	}
	k := fmt.Sprintf("%s@%d", id, version)
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.executors[k]
	return ok
}

// IDs returns all registered executor (id, version) keys as strings
// in lexicographic order. Deterministic — REQUIRED by PR-3.5 so the
// worker hello payload matches across boots.
func (r *Registry) IDs() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.executors))
	for k := range r.executors {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Descriptors returns every registered Descriptor in deterministic
// (ID, Version) order. PR-3.5 reads this to publish worker capabilities.
// Returned copies cannot mutate internal state.
func (r *Registry) Descriptors() []Descriptor {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Descriptor, 0, len(r.executors))
	for _, e := range r.executors {
		out = append(out, e.Descriptor())
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ID != out[j].ID {
			return out[i].ID < out[j].ID
		}
		return out[i].Version < out[j].Version
	})
	return out
}

// All returns every registered Executor. Order matches Descriptors().
// Returned interface references are the live pointers — do not mutate
// their internal state via type assertions.
func (r *Registry) All() []Executor {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Executor, 0, len(r.executors))
	for _, e := range r.executors {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		di, dj := out[i].Descriptor(), out[j].Descriptor()
		if di.ID != dj.ID {
			return di.ID < dj.ID
		}
		return di.Version < dj.Version
	})
	return out
}

// Len returns the number of registered executors.
func (r *Registry) Len() int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.executors)
}
