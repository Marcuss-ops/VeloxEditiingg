// Package registry/capability.go
//
// Verdetto P0 #2, #3, #4 (Blocco 1). CapabilityRegistry aggregates
// readiness probes for the master subsystem surfaces that the
// artifact.commit.v1 gRPC method depends on. The gRPC handler consults
// CapabilityRegistry.Readyz() at method-dispatch time; if the
// registry is not fully ready the method returns ErrCapabilityNotReady
// to the worker over the wire (NOT a panic, NOT a silent zero —
// the worker gRPC layer sees a typed error and can retry/defer).
//
// Three capability surfaces are wired:
//
//   - coordinator — the completion.Coordinator (CommitAttempt,
//                   CompleteUpload, ReconcileAttempt). The coordinator
//                   is healthy iff *sql.DB ping succeeds AND the
//                   uow factory is non-nil.
//   - spool       — the artifact spool / chunked-upload staging dir.
//                   Healthy iff staging dir exists and is writable.
//   - transport   — the (typed) transport registry used by the
//                   forward-and-publish pipeline. Healthy iff the
//                   registry is non-nil AND at least one transport
//                   is registered (a registry with zero registered
//                   transports cannot publish through any path, so
//                   artefact.commit.v1 must refuse).
//
// Adding a new capability is one Register call; the registry stays
// extensible without changing the gRPC handler.
package registry

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

// ErrCapabilityNotReady is the sentinel returned by CapabilityRegistry.Readyz
// when at least one registered capability probe fails. The error
// message lists every failing capability name so an operator can
// spot-check the /ready endpoint log without diving into code.
var ErrCapabilityNotReady = errors.New("registry: not all capabilities ready")

// Probe is a per-capability readiness check. Returning nil means
// "ready"; returning a non-nil error means "not ready" and the
// error message is folded into the registry's aggregated message.
type Probe struct {
	Name  string
	Check func() error
}

// CapabilityRegistry is the canonical readiness aggregator for the
// artifact.commit.v1 prerequisites. It is concurrency-safe so the
// gRPC handler (read-heavy) and the bootstrap wiring (write-once)
// can both touch it without an external lock.
type CapabilityRegistry struct {
	mu     sync.RWMutex
	probes map[string]Probe
}

// NewCapabilityRegistry constructs an empty registry. Probes register
// themselves via Register before the gRPC handler begins dispatching.
func NewCapabilityRegistry() *CapabilityRegistry {
	return &CapabilityRegistry{
		probes: make(map[string]Probe),
	}
}

// Register binds a named Probe into the registry. Overwriting an
// existing name is permitted (used by tests + hot-swap paths). A
// probe with an empty name or nil Check is rejected — the
// artifact.commit.v1 gate is fail-closed, so any malformed
// registration blocks the method until it is fixed.
func (r *CapabilityRegistry) Register(p Probe) error {
	if r == nil {
		return fmt.Errorf("registry: nil registry")
	}
	if p.Name == "" {
		return fmt.Errorf("registry: probe Name is required")
	}
	if p.Check == nil {
		return fmt.Errorf("registry: probe %q has nil Check", p.Name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.probes[p.Name] = p
	return nil
}

// Unregister removes a probe (used by tests + late-binding reset
// paths). A no-op if the name is not currently registered.
func (r *CapabilityRegistry) Unregister(name string) {
	if r == nil || name == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.probes, name)
}

// Names returns the sorted list of currently-registered probes for
// diagnostic surfaces (e.g. /ready probe logging).
func (r *CapabilityRegistry) Names() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.probes))
	for k := range r.probes {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Readyz iterates every registered probe and aggregates failures
// into a single returned error. Returns nil ONLY when each probe
// reports nil — this is the gate the gRPC handler uses to decide
// whether to publish artifact.commit.v1 methods.
//
// Order is sorted by probe name so the returned error message is
// deterministic across runs (useful for log diffs across releases).
func (r *CapabilityRegistry) Readyz() error {
	if r == nil {
		return fmt.Errorf("%w: registry is nil", ErrCapabilityNotReady)
	}
	r.mu.RLock()
	names := make([]string, 0, len(r.probes))
	for k := range r.probes {
		names = append(names, k)
	}
	sort.Strings(names)
	probes := make(map[string]Probe, len(r.probes))
	for k, v := range r.probes {
		probes[k] = v
	}
	r.mu.RUnlock()

	var failing []string
	for _, n := range names {
		p := probes[n]
		if err := p.Check(); err != nil {
			failing = append(failing, fmt.Sprintf("%s(%v)", n, err))
		}
	}
	if len(failing) == 0 {
		return nil
	}
	return fmt.Errorf("%w: failing=%v", ErrCapabilityNotReady, failing)
}
