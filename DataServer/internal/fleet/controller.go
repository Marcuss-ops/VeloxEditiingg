// Package fleet controller.go — FleetController: the in-process
// implementation of the Step 4/15 fleet-operator abstraction.
//
// The FleetController owns the QUEUED → RUNNING → SUCCEEDED /
// FAILED lifecycle for the fleet_operations ledger. The HTTP
// layer never executes anything; admin mutations (drain / resume
// / etc.) PUBLISH an Operation via FleetController.PublishOperation
// and return 202 Accepted with operation_id at the time those
// mutations land (later steps). The HTTP layer's only
// responsibility at Step 4/15 is the AUDIT surface
// (GET /api/v1/admin/operations + /{id}).
//
// Lifecycle pattern mirrors outbox/dispatcher.go:
//
//   - Start(ctx) launches a single background goroutine running
//     a tick loop (single ticker; same shape as outbox/dispatcher).
//   - Stop() closes a cancel channel; the goroutine returns nil.
//   - Done() exposes a chan struct{} that closes when the
//     goroutine has fully exited — cmd/server/runUntilShutdown's
//     graceful teardown waits on this with a 15-second budget per
//     bootstrap.go:runUntilShutdown.
//
// Concurrency: the controller's Start/Stop/PublishOperation
// paths share a sync.Mutex so the field group stays consistent.
// Tick is single-threaded and may run concurrently with
// PublishOperation; the database row's status guard (MarkRunning
// checks status='QUEUED', MarkSucceeded checks status='RUNNING')
// prevents duplicate transition.
//
// File layout:
//   - controller.go             types, constructor, publish, audit, ID gen
//   - controller_lifecycle.go   Run / Start / Stop / Done
//   - controller_tick.go        Tick dispatch loop + processOne
//   - controller_persistence.go terminal persistence + stale reconciliation
package fleet

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"

	"velox-server/internal/store"
)

// Default tick / op-timeout knobs. Operators override via the
// constructor for production deployments where 1s tick is too
// aggressive or 10min op timeout is too short. The defaults are
// intentionally generous: a tick slower than the noop executor
// proves the lifecycle still works (no heartbeat loss under
// idle fleet) and an op timeout long enough to absorb a slow
// ssh handshake.
const (
	DefaultTickInterval = 1 * time.Second
	DefaultOpTimeout    = 10 * time.Minute

	// terminalPersistTimeout is independent from the executor budget. An
	// executor that reaches its deadline must still be able to persist the
	// terminal operation state; reusing the expired executor context would
	// leave the durable row RUNNING until stale reconciliation.
	terminalPersistTimeout = 5 * time.Second
)

// ErrAlreadyRunning is returned by Start when the controller is
// already running. Defensive — production boot should never
// trigger it, but the gate pins the contract for tests that
// call Start twice.
var ErrAlreadyRunning = errors.New("fleet: controller already running")

// ErrStoreNotConfigured means the fleet capability was requested without its
// durable operation ledger. Running without the ledger would turn a queued
// operation into an untracked external side effect, so the controller fails
// closed before starting its ticker.
var ErrStoreNotConfigured = errors.New("fleet: operation store is not configured")

// ErrNotRunning only documents the inverse; Stop is silent on a
// never-started controller (returns nil), so listing this
// sentinel here is informational and keeps the "running" axis
// explicit.

// FleetStore is the slice of *store.SQLiteStore the controller
// actually uses. Defined as an interface so tests can swap in a
// stub store without standing up a SQLite handle or running the
// migration sweep.
//
// Production: pass *store.SQLiteStore directly (it satisfies
// the interface via its methods on the receiver; the interface
// is met by Go's structural typing — no explicit declaration
// anywhere).
type FleetStore interface {
	InsertOperation(ctx context.Context, op *store.Operation) error
	ListQueuedOperations(ctx context.Context, limit int) ([]store.Operation, error)
	ListOperations(ctx context.Context, workerID, statusFilter string, limit int) ([]store.Operation, error)
	GetOperation(ctx context.Context, operationID string) (*store.Operation, error)
	MarkRunning(ctx context.Context, id string, startedAt time.Time) (bool, error)
	MarkSucceeded(ctx context.Context, id string, finishedAt time.Time) error
	MarkFailed(ctx context.Context, id string, finishedAt time.Time, errMsg string) error

	// Deployment read model + verified close, used by the post-restart
	// reconciler (resumeStaleUpdate): a stale RUNNING update is decided from
	// durable state (worker_deployment_state + MarkVerifiedSucceeded) instead
	// of being blindly marked FAILED. Both are implemented by
	// *store.SQLiteStore; tests stub them.
	GetWorkerDeploymentState(ctx context.Context, workerID string) (*store.WorkerDeploymentState, error)
	MarkVerifiedSucceeded(ctx context.Context, deploymentID, observedDigest string, finishedAt time.Time) error
}

// FleetController owns the queue + lifecycle. The struct is
// mutably initialised by the constructor; concurrent Start/Stop/
// PublishOperation/AuditGet/AuditList use the same mutex so the
// field group stays consistent.
//
// Dependency injection: store (the fleet_operations reader/writer),
// executors (per-kind runner), runtime knobs (tick + op timeout).
// Tests pass a fresh in-memory store per test case; production
// wires the live booted store + the populated ExecutorRegistry.
type FleetController struct {
	store     FleetStore
	executors *ExecutorRegistry
	tickI     time.Duration
	opTimeout time.Duration

	mu      sync.Mutex
	running bool
	cancel  chan struct{}
	done    chan struct{}

	// operationIDFn is a seam for tests to inject deterministic
	// IDs (e.g. to assert a particular row exists without
	// scanning for it). Production uses NewOperationID by default.
	operationIDFn func() string
}

// NewFleetController builds the controller but does NOT start
// the tick goroutine. Call Start(ctx) to begin processing.
//
// Constructor applies default fallbacks for zero-valued knobs so
// callers passing CONFIG defaults do not have to remember every
// field:
//
//	tickI     <= 0  → DefaultTickInterval
//	opTimeout <= 0  → DefaultOpTimeout
func NewFleetController(
	s FleetStore,
	execs *ExecutorRegistry,
	tickInterval time.Duration,
	opTimeout time.Duration,
) *FleetController {
	if tickInterval <= 0 {
		tickInterval = DefaultTickInterval
	}
	if opTimeout <= 0 {
		opTimeout = DefaultOpTimeout
	}
	return &FleetController{
		store:         s,
		executors:     execs,
		tickI:         tickInterval,
		opTimeout:     opTimeout,
		operationIDFn: NewOperationID,
	}
}

// PublishOperation persists a new Operation in QUEUED state.
// Side-effect: fills in empty OperationID (UUIDv4 via
// NewOperationID) and empty QueuedAt (time.Now().UTC()). The
// caller passes the audit-meaningful fields (WorkerID, Op,
// RequestedBy, Reason, Payload) and the controller handles the
// envelope plumbing.
//
// Returns nil on success, ErrOperationInFlight if the partial
// UNIQUE INDEX trips on (worker_id, op), or any underlying SQL
// error verbatim (the audit dashboard's 5xx path renders the
// raw error string with a "transient, retry later" framing).
func (c *FleetController) PublishOperation(ctx context.Context, op *store.Operation) error {
	if c == nil {
		return errors.New("fleet: nil controller")
	}
	if c.store == nil {
		return ErrStoreNotConfigured
	}
	if op == nil {
		return errors.New("fleet: nil operation")
	}
	if op.OperationID == "" {
		op.OperationID = c.operationIDFn()
	}
	if op.QueuedAt.IsZero() {
		op.QueuedAt = time.Now().UTC()
	}
	op.Status = store.OperationStatusQueued

	return c.store.InsertOperation(ctx, op)
}

// AuditList is the audit-read path used by the admin handler.
// Thin wrapper around the underlying store so the handler does
// NOT need to thread the store as a second dependency.
//
// limit <= 0 means "no cap"; the handler passes the canonical
// 200-row cap.
func (c *FleetController) AuditList(ctx context.Context, workerID, statusFilter string, limit int) ([]store.Operation, error) {
	if c == nil {
		return nil, errors.New("fleet: nil controller")
	}
	if c.store == nil {
		return nil, ErrStoreNotConfigured
	}
	return c.store.ListOperations(ctx, workerID, statusFilter, limit)
}

// AuditGet fetches one row by ID. Thin wrapper. Maps to
// ErrOperationNotFound on miss (handler turns it into 404).
func (c *FleetController) AuditGet(ctx context.Context, operationID string) (*store.Operation, error) {
	if c == nil {
		return nil, errors.New("fleet: nil controller")
	}
	if c.store == nil {
		return nil, ErrStoreNotConfigured
	}
	return c.store.GetOperation(ctx, operationID)
}

// NewOperationID generates a UUIDv4-formatted operation_id per
// RFC 4122 §4.4. Cryptographically random via crypto/rand so
// concurrent admins cannot guess / collide. Used by
// FleetController.PublishOperation when the caller passes an
// empty OperationID.
//
// Fallback path: crypto/rand.Read failures are extraordinarily
// rare on Linux (getrandom(2) only fails on EFAULT or
// operational surprises); the fallback encodes the wallclock
// nanos in a "op-wallclock-..." ID so production NEVER panics
// on a transient entropy failure. The audit dashboard tags
// fallback IDs with a post-hoc script in the rare scenario.
func NewOperationID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("op-wallclock-%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
