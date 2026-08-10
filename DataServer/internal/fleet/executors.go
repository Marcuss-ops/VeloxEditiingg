// Package fleet — fleet operation execution contracts.
//
// The ExecutorRegistry is the SINGLE mapping from operation kind
// (drain | resume | restart | update | rollback | quarantine | smoke)
// to a concrete OperationExecutor. Production registries start empty
// and are populated explicitly during bootstrap. Test-only executor
// helpers are kept in _test.go files and cannot enter the production binary.
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
	"reflect"
	"sort"
	"strings"
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

// AllOperationKinds is the canonical complete-enum set. It drives
// the schema CHECK source-of-truth (mirrored in sqlite/104_fleet_operations.sql).
// Production must register concrete executors explicitly;
// this list is not a default-registration list.
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

// ErrExecutorNotConfigured is the stable operator-facing marker for a
// production operation that has no concrete executor. It must be persisted
// on the fleet operation row instead of allowing a false SUCCEEDED outcome.
var ErrExecutorNotConfigured = errors.New("EXECUTOR_NOT_CONFIGURED")

// ErrNoExecutorForKind is retained as the detailed lookup sentinel for
// callers that need to distinguish a missing kind from executor failures.
var ErrNoExecutorForKind = errors.New("fleet: no executor registered for operation kind")

// ErrNoopExecutorNotAllowed prevents a no-op executor from being wired
// into a production registry. The no-op implementation itself exists only
// in test files.
var ErrNoopExecutorNotAllowed = errors.New("fleet: noop executor is only allowed in test/dev registries")

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

// NewExecutorRegistry returns an empty production registry. Every concrete
// executor must be registered explicitly by bootstrap. A missing binding is
// therefore observable and fails the operation instead of succeeding through
// an implicit no-op.
func NewExecutorRegistry() *ExecutorRegistry {
	return &ExecutorRegistry{executors: make(map[string]OperationExecutor)}
}

// Register overwrites the executor entry for a single kind. The
// canonical production use is the boot sequence:
//
//	fleet.NewExecutorRegistry()
//	          .Register(fleet.OperationKindUpdate, &UpdateExecutor{...})
//	          .Register(fleet.OperationKindDrain, &DrainExecutor{...})
//
// Register is the ONLY way to bind a new executor. Returns nil
// on success, an error if `exec` is nil or `kind` is not in
// AllOperationKinds. Production registries reject no-op executors
// so a wiring typo cannot create a fake "succeeded" outcome.
func (r *ExecutorRegistry) Register(kind string, exec OperationExecutor) error {
	if r == nil {
		return errors.New("fleet: nil executor registry")
	}
	if isNilExecutor(exec) {
		return errors.New("fleet: executor cannot be nil")
	}
	if !IsKnownKind(kind) {
		return fmt.Errorf("fleet: unknown operation kind %q", kind)
	}
	if isNoopExecutor(exec) {
		return ErrNoopExecutorNotAllowed
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.executors == nil {
		r.executors = make(map[string]OperationExecutor)
	}
	r.executors[kind] = exec
	return nil
}

// Lookup returns the executor for a kind. Missing bindings are reported with
// both ErrExecutorNotConfigured (stable operational marker) and
// ErrNoExecutorForKind (detailed lookup sentinel).
func (r *ExecutorRegistry) Lookup(kind string) (OperationExecutor, error) {
	if r == nil {
		return nil, fmt.Errorf("%w: %w: %q", ErrExecutorNotConfigured, ErrNoExecutorForKind, kind)
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	exec, ok := r.executors[kind]
	if !ok || isNilExecutor(exec) {
		return nil, fmt.Errorf("%w: %w: %q", ErrExecutorNotConfigured, ErrNoExecutorForKind, kind)
	}
	return exec, nil
}

// ValidateRequiredExecutors verifies the concrete executor bindings needed by
// the current production composition. With no arguments it validates the
// canonical production set; callers may pass a narrower set in tests or when
// a capability is deliberately disabled. Noop bindings are never accepted.
func (r *ExecutorRegistry) ValidateRequiredExecutors(required ...string) error {
	if len(required) == 0 {
		required = ProductionRequiredOperationKinds
	}
	missing := make([]string, 0, len(required))
	for _, kind := range required {
		if !IsKnownKind(kind) {
			return fmt.Errorf("fleet: unknown required operation kind %q", kind)
		}
		exec, err := r.Lookup(kind)
		if err != nil || isNoopExecutor(exec) {
			missing = append(missing, kind)
			continue
		}
		if validator, ok := exec.(interface{ ValidateProductionBackends() error }); ok {
			if err := validator.ValidateProductionBackends(); err != nil {
				return fmt.Errorf("%w: %s: %v", ErrExecutorNotConfigured, kind, err)
			}
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: missing concrete executors for %s", ErrExecutorNotConfigured, strings.Join(missing, ", "))
	}
	return nil
}

// HasKind is a no-error read used by tests and readiness reporting
// to inspect the explicitly wired registry contents.
func (r *ExecutorRegistry) HasKind(kind string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.executors[kind]
	return ok
}

// Kinds returns the registered kinds, alphabetically sorted for
// stable diagnostics and boot/readiness reporting.
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

type noopExecutorMarker interface {
	isNoopExecutor()
}

func isNoopExecutor(exec OperationExecutor) bool {
	_, ok := exec.(noopExecutorMarker)
	return ok
}

func isNilExecutor(exec OperationExecutor) bool {
	if exec == nil {
		return true
	}
	value := reflect.ValueOf(exec)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// ProductionRequiredOperationKinds are the concrete capabilities currently
// promised by the production fleet composition. Other enum values remain
// valid for persistence and fail at dispatch with EXECUTOR_NOT_CONFIGURED
// until their capability is explicitly wired.
var ProductionRequiredOperationKinds = []string{
	OperationKindDrain,
	OperationKindResume,
	OperationKindUpdate,
	OperationKindQuarantine,
	OperationKindSmoke,
}
