// Package fleet — Step 4/15 fleet controller abstraction: the
// OperationExecutor interface and a default in-process registry
// that future steps replace operation-kind-by-operation-kind.
//
// The ExecutorRegistry is the SINGLE mapping from operation kind
// (drain | resume | restart | update | rollback | quarantine | smoke)
// to a concrete OperationExecutor. Step 4/15 ships only the
// NoopOperationExecutor; Step 7+ lands concrete Ansible/SSH-backed
// executors that register themselves on boot, replacing the noop
// entry without changing any producer-side call sites.
//
// Architectural rule (PR §4): HTTP handlers NEVER call executors
// directly. The HTTP layer publishes Operations via
// FleetController.PublishOperation and the tick goroutine routes
// them through the registry. This means swapping a noop for a
// real executor at boot is a SINGLE call to Register — no HTTP
// handler churn.
package fleet

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"velox-server/internal/store"
)

// OperationKind is the canonical enum of admin-side fleet
// operations. Future admin mutation handlers (POST /drain,
// POST /restart, etc.) MUST publish Operations restricted to
// this set — the schema CHECK constraint rejects anything else.
//
// String-typed rather than int-typed so the audit log, the JSON
// envelope, and the SQL CHECK constraint agree on the canonical
// vocabulary without enum-table mappings.
const (
	OperationKindDrain      = "drain"
	OperationKindResume     = "resume"
	OperationKindRestart    = "restart"
	OperationKindUpdate     = "update"
	OperationKindRollback   = "rollback"
	OperationKindQuarantine = "quarantine"
	OperationKindSmoke      = "smoke"
)

// AllOperationKinds is the canonical complete-enum set. Drives
// both the schema CHECK source-of-truth (mirrored in
// sqlite/104_fleet_operations.sql and postgres/014) AND the
// boot-time register list for the default NoopOperationExecutor
// — adding a kind here without the matching CHECK migrations is
// a silent schema drift, so the test
// TestExecutorRegistry_RegistersAllKinds (controller_test.go)
// pins the invariant.
//
// The set is stable for Step 4/15. Future kinds (e.g.
// "rotate_secret", "drain_cluster") land as additive migrations.
var AllOperationKinds = []string{
	OperationKindDrain,
	OperationKindResume,
	OperationKindRestart,
	OperationKindUpdate,
	OperationKindRollback,
	OperationKindQuarantine,
	OperationKindSmoke,
}

// ErrNoExecutorForKind is returned by ExecutorRegistry.Lookup
// when no executor has been registered for the requested kind.
// SHOULD never happen in production because the FleetModule
// registers the NoopOperationExecutor at boot for every kind in
// AllOperationKinds; reachability means a misconfigured boot.
// Surface as an HTTP 500-class error at the API layer.
var ErrNoExecutorForKind = errors.New("fleet: no executor registered for operation kind")

// OperationExecutor runs one Operation. Implementations MUST:
//
//   - be idempotent against repeated invocations. The partial
//     UNIQUE INDEX in fleet_operations suppresses repeated
//     publishes of (worker_id, op), but executors MUST tolerate
//     the duplicate defense firing (a worker restart that re-runs
//     the same operation must not double-execute).
//   - return nil within the context deadline on success.
//   - return a non-nil error on failure — the controller
//     transitions the row to FAILED and persists the error
//     string so the audit dashboard can render the cause.
//   - NOT spawn long-running background work outside the
//     returned context. Operations share a single tick goroutine;
//     a slow goroutine blocks every other queued operation.
//     Use the context.WithTimeout provided by the controller.
//   - treat the op's Payload as the canonical input. The Kind
//     determines the payload shape (e.g. update: digest,
//     smoke: timeout). Empty payload is the canonical
//     "no-args" marker and means "sensible defaults".
//
// OperationExecutor does NOT take a callback or send events:
// the FleetController reads the error return value directly.
// Future steps may add a Notification hook if an operation's
// completion must reach an internal subscriber; until then the
// registry path stays narrow.
type OperationExecutor interface {
	Execute(ctx context.Context, op *store.Operation) error
}

// ExecutorRegistry maps OperationKind → OperationExecutor.
// Thread-safe — the FleetController reads on every tick.
type ExecutorRegistry struct {
	mu        sync.RWMutex
	executors map[string]OperationExecutor
}

// NewExecutorRegistry returns a registry with the NoopOperationExecutor
// pre-registered for every kind in AllOperationKinds. Concrete
// executors (Ansible, SSH, smoke-test runners) overwrite the
// entry on boot via Register; the boot sequence stacks over the
// noop-defaults.
//
// Returns a non-nil registry with non-nil default for every
// canonical kind. Sub-tests pin the invariant via
// TestExecutorRegistry_RegistersAllKinds.
func NewExecutorRegistry() *ExecutorRegistry {
	r := &ExecutorRegistry{executors: make(map[string]OperationExecutor)}
	noop := &NoopOperationExecutor{}
	for _, kind := range AllOperationKinds {
		r.executors[kind] = noop
	}
	return r
}

// Register overwrites the executor entry for a single kind. The
// canonical use is the boot sequence:
//
//	fleet.NewExecutorRegistry()
//	          .Register(fleet.OperationKindUpdate, &UpdateExecutor{...})
//	          .Register(fleet.OperationKindDrain, &DrainExecutor{...})
//
// Register is the ONLY way to bind a new executor. Returns nil
// on success, an error if `exec` is nil or `kind` is not in
// AllOperationKinds (a typo here would lead to the noop
// default kicking in silently and the operator seeing a fake
// "succeeded" outcome — the eager error saves the dashboard
// the debug trip).
func (r *ExecutorRegistry) Register(kind string, exec OperationExecutor) error {
	if exec == nil {
		return errors.New("fleet: executor cannot be nil")
	}
	if !IsKnownKind(kind) {
		return fmt.Errorf("fleet: unknown operation kind %q", kind)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.executors[kind] = exec
	return nil
}

// Lookup returns the executor for a kind. Returns
// ErrNoExecutorForKind if absent (no kind in AllOperationKinds
// has no default — the path is internal-only unless a future
// step drops a kind without updating the registry).
func (r *ExecutorRegistry) Lookup(kind string) (OperationExecutor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	exec, ok := r.executors[kind]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrNoExecutorForKind, kind)
	}
	return exec, nil
}

// HasKind is a no-error read used by tests and the audit
// endpoint to confirm the kind enum surface stays in lockstep
// with the registry contents (a missing kind means the boot
// sequence removed a default — a kin to ErrNoExecutorForKind).
func (r *ExecutorRegistry) HasKind(kind string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.executors[kind]
	return ok
}

// Kinds returns the registered kinds, alphabetically sorted for
// stable diagnostics. Used by the boot self-check
// (TestExecutorRegistry_RegistersAllKinds in controller_test.go)
// and any debug surface.
func (r *ExecutorRegistry) Kinds() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.executors))
	for k := range r.executors {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// IsKnownKind is true when `kind` is in the canonical
// AllOperationKinds set. Pure helper; no locks.
func IsKnownKind(kind string) bool {
	for _, k := range AllOperationKinds {
		if k == kind {
			return true
		}
	}
	return false
}

// NoopOperationExecutor is the default executor registered for
// every OperationKind at boot. It immediately returns nil
// (success). The FleetController transitions the row
// QUEUED → RUNNING → SUCCEEDED without any side effect, which
// proves the full lifecycle path works end-to-end without an
// SSH/Ansible dependency.
//
// Future steps (e.g. Step 7) replace this entry with a concrete
// executor at boot (Registry.Register(kind, ansibleExec)). The
// registry mutation is the ONLY way to bind a new executor;
// the controller does not implement a plugin loader.
//
// NoopOperationExecutor is concurrency-safe by virtue of having
// no state.
type NoopOperationExecutor struct{}

// Execute returns nil unconditionally. The Kind and Payload
// arguments are intentionally ignored here — concrete executors
// in Step 7+ consume them.
func (NoopOperationExecutor) Execute(_ context.Context, _ *store.Operation) error {
	return nil
}
