// Package reconcile defines the canonical reconciliation framework for
// the Velox master.
//
// A Reconciler is a bounded, idempotent background pass that closes the
// gap between the durable lifecycle state and the real world (a worker
// stream that stopped, an upload that never started, a delivery gate
// that never closed). The framework is deliberately tiny: one interface,
// one registry, one report. All SQL lives in the store package — the
// implementations in internal/store implement this interface and the
// registry composes them in bootstrap.
//
// Contract (see AGENTS.md §6 capability state machine):
//
//   - Every registered reconciler MUST do real work. A registry entry
//     that returns nil without mutating anything is a hidden noop and
//     is forbidden in production wiring.
//   - A reconciler MUST be idempotent: re-running the same pass must
//     not change state a second time. All mutations are CAS-guarded on
//     the current status so a concurrent writer (or a second
//     reconciler instance) cannot double-apply.
//   - A reconciler MUST NEVER resurrect or modify a terminal job
//     (SUCCEEDED / FAILED / CANCELLED). Terminal states are immutable
//     except through an explicit administrative workflow.
//
// The registry runs entries in registration order; one failing entry
// does not stop the remaining entries, and the aggregate Report carries
// the per-entry error so the supervisor can restart with backoff.
package reconcile

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// Reconciler is one canonical, idempotent reconciliation pass.
//
// Implementations receive the wall-clock `now` they must reason against
// (the supervisor passes time.Now().UTC()) so the pass is deterministic
// and testable with a fixed clock.
type Reconciler interface {
	Reconcile(ctx context.Context, now time.Time) error
}

// Canonical registry entry names. The bootstrap registers these entries;
// tests and the operator surface may build their own registries with the
// same names.
const (
	// NameAwaitingArtifact terminalizes jobs stuck in AWAITING_ARTIFACT
	// with no active attempt, no READY artifact, and no active transfer.
	NameAwaitingArtifact = "AWAITING_ARTIFACT"
	// NameDeliveryPending rolls up jobs stuck in DELIVERING whose
	// deliveries are all terminal and terminalizes deliveries that
	// exhausted their retry budget while the runner was down.
	NameDeliveryPending = "DELIVERY_PENDING"
	// NameWorkerLost partitions workers whose heartbeat stream has
	// stopped entirely (the recovery-side counterpart of the
	// heartbeat-path detector).
	NameWorkerLost = "WORKER_LOST"
	// NameStaleExecution applies bounded, idempotent recovery for stale
	// leases, orphaned tasks/attempts, committed artifacts, unconfirmed
	// spool declarations, and offline workers.
	NameStaleExecution = "STALE_EXECUTION"
)

// EntryReport is the per-entry outcome of one registry pass.
type EntryReport struct {
	Name string `json:"name"`
	// DurationMS is the wall-clock duration of the entry's Reconcile in
	// milliseconds (an explicit int64 so the JSON contract is truthful —
	// a raw time.Duration would marshal as nanoseconds).
	DurationMS int64         `json:"duration_ms"`
	Duration   time.Duration `json:"-"`
	Err        error         `json:"-"`
}

// Report aggregates the outcome of one registry pass.
type Report struct {
	GeneratedAt time.Time     `json:"generated_at"`
	Entries     []EntryReport `json:"entries"`
}

// Registry is the canonical ReconciliationRegistry: a fixed set of
// named Reconciler entries run in registration order.
type Registry struct {
	mu    sync.RWMutex
	order []string
	items map[string]Reconciler
}

// NewRegistry returns an empty registry. Production wiring must
// register every entry it needs before the first Reconcile call; a
// production registry that ends up empty is a wiring error and should
// fail the bootstrap, never silently pass.
func NewRegistry() *Registry {
	return &Registry{items: make(map[string]Reconciler)}
}

// Register adds a named reconciler. Registering the same name twice is
// an error (a duplicate entry would silently shadow the first one).
func (r *Registry) Register(name string, rec Reconciler) error {
	if name == "" {
		return fmt.Errorf("reconcile: registry entry name is empty")
	}
	if rec == nil {
		return fmt.Errorf("reconcile: registry entry %q is nil", name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.items[name]; exists {
		return fmt.Errorf("reconcile: registry entry %q already registered", name)
	}
	r.items[name] = rec
	r.order = append(r.order, name)
	return nil
}

// Names returns the registered entry names in registration order.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, len(r.order))
	copy(out, r.order)
	return out
}

// Reconcile runs every registered entry in registration order. One
// entry failing does not stop the remaining entries; the returned
// Report carries the per-entry error so the caller can decide whether
// to surface it (supervisor restart policy) or continue on the next
// tick.
func (r *Registry) Reconcile(ctx context.Context, now time.Time) Report {
	r.mu.RLock()
	order := make([]string, len(r.order))
	copy(order, r.order)
	items := make(map[string]Reconciler, len(r.items))
	for k, v := range r.items {
		items[k] = v
	}
	r.mu.RUnlock()

	report := Report{GeneratedAt: now}
	for _, name := range order {
		entry := EntryReport{Name: name}
		started := time.Now()
		entry.Err = items[name].Reconcile(ctx, now)
		entry.Duration = time.Since(started)
		entry.DurationMS = entry.Duration.Milliseconds()
		report.Entries = append(report.Entries, entry)
	}
	return report
}

// Err returns the first non-nil error in the report, or nil.
func (r Report) Err() error {
	for _, entry := range r.Entries {
		if entry.Err != nil {
			return entry.Err
		}
	}
	return nil
}

// RunPeriodically runs the registry once immediately, then on every tick,
// until ctx is cancelled. One failing entry never stops the remaining
// entries, and per-entry failures are logged so a failing reconciler retries
// on the next tick without masking the other passes.
func RunPeriodically(ctx context.Context, r *Registry, tick time.Duration) error {
	runOnce := func() {
		report := r.Reconcile(ctx, time.Now().UTC())
		for _, entry := range report.Entries {
			if entry.Err != nil {
				log.Printf("[RECONCILIATION] entry %s failed after %s: %v", entry.Name, entry.Duration, entry.Err)
			}
		}
	}
	// Immediate first pass so a job stuck in AWAITING_ARTIFACT / DELIVERING
	// does not wait a full tick after a master restart.
	runOnce()
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			runOnce()
		}
	}
}
