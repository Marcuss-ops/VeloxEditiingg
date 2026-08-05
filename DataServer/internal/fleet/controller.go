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
package fleet

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
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
)

// ErrAlreadyRunning is returned by Start when the controller is
// already running. Defensive — production boot should never
// trigger it, but the gate pins the contract for tests that
// call Start twice.
var ErrAlreadyRunning = errors.New("fleet: controller already running")

// ErrNotRunning only documents the inverse; Stop is silent on a
// never-started controller (returns nil), so listing this
// sentinel here is informational and keeps the "running" axis
// explicit.

// FleetStore is the slice of *store.SQLiteStore (or a future
// PostgresStore wrapper) the controller actually uses. Defined
// as an interface so tests can swap in a stub store without
// standing up a SQLite handle or running the migration sweep.
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
	MarkRunning(ctx context.Context, id string, startedAt time.Time) error
	MarkSucceeded(ctx context.Context, id string, finishedAt time.Time) error
	MarkFailed(ctx context.Context, id string, finishedAt time.Time, errMsg string) error
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

// Run blocks until ctx is cancelled or Stop() is called. Matches
// the supervisor.Runner.Run signature so the production boot
// wires the FleetController directly via the supervisor (no
// goroutine spawning inside the Run callback — the supervisor
// owns the goroutine).
//
// On EITHER clean-exit path (ctx-done or Stop) Run returns nil:
// this is critical so the supervisor does not treat graceful
// shutdown as a transient failure (otherwise the supervisor
// would backoff-and-retry on SIGINT, which is wrong).
//
// Production wiring (cmd/server/bootstrap_composition.go)
// registers Run as a ClassRestartable runner; tests drive Tick
// directly when they need deterministic control over the
// dispatch surface.
func (c *FleetController) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.tickI)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.cancel:
			return nil
		case <-ticker.C:
			c.Tick(ctx)
		}
	}
}

// Start launches Run in a goroutine and exposes Done() for
// shutdown synchronisation. Idempotent: a second Start returns
// ErrAlreadyRunning.
//
// Tests that want explicit lifecycle hooks (Start/Stop/Done)
// drive the controller here; production boot prefers Run
// directly via the supervisor because the supervisor owns the
// goroutine and the run-done channel.
func (c *FleetController) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return ErrAlreadyRunning
	}
	c.running = true
	c.cancel = make(chan struct{})
	c.done = make(chan struct{})
	c.mu.Unlock()

	go func() {
		defer close(c.done)
		_ = c.Run(ctx)
	}()
	return nil
}

// Tick is exposed for tests; production code lets the inner
// tick goroutine drive it via the Start/Stop pair.
//
// Process: list QUEUED rows ordered by queued_at ASC (FIFO so
// an admin's "drain now, then update 5s later" doesn't get
// answered in reverse), MarkRunning each, then Execute via the
// per-kind entry in the ExecutorRegistry. On success
// MarkSucceeded; on error MarkFailed with the error string
// captured. The tick is bound to ctx so a slow executor cannot
// pin the loop beyond ctx-Done.
//
// Errors inside Tick are logged and the loop returns to the
// next iteration — they are NOT propagated up because the
// inner runLoop would exit on any non-nil return, leaving
// QUEUED rows stranded. Defensive iteration is the
// fail-loud-but-recover contract.
func (c *FleetController) Tick(ctx context.Context) {
	queued, err := c.store.ListQueuedOperations(ctx, 0)
	if err != nil {
		// Transient error: log and let the next tick retry.
		// The operator's audit surface can also see this
		// indirectly: a QUEUED row whose status never
		// transitions is "stuck"; the dedicated
		// "stuck-QUEUED" alert is a Step 7+ concern.
		log.Printf("[FLEET] list queued operations: %v", err)
		return
	}
	for i := range queued {
		op := &queued[i]
		// Per-operation timeout guards against an SSH/ansible
		// hung session pinning RUNNING indefinitely. Resume owns
		// a real Level D smoke and must not block this single fleet
		// dispatcher tick while ffmpeg/Drive work runs; it is
		// dispatched asynchronously and completes its own ledger row.
		execCtx, cancel := context.WithTimeout(ctx, c.opTimeout)
		if op.Op == OperationKindResume {
			go func(op *store.Operation, execCtx context.Context, cancel context.CancelFunc) {
				defer cancel()
				c.processOne(execCtx, op)
			}(op, execCtx, cancel)
			continue
		}
		c.processOne(execCtx, op)
		cancel()
	}
}

// processOne runs the QUEUED → RUNNING → terminal transition
// for a single operation. Each step's WHERE-clause guard means
// a duplicate processOne call (e.g. tick re-fires after a
// transient error) is a silent no-op — the row stays in its
// current state and the executor does not run twice.
func (c *FleetController) processOne(ctx context.Context, op *store.Operation) {
	now := time.Now().UTC()
	if err := c.store.MarkRunning(ctx, op.OperationID, now); err != nil {
		// MarkRunning failure is rare (DB locked, connection
		// drop mid-transaction). Log and let the next tick
		// try again — the row is still QUEUED.
		log.Printf("[FLEET] mark running %s: %v", op.OperationID, err)
		return
	}
	exec, err := c.executors.Lookup(op.Op)
	if err != nil {
		// Lookup failure: no executor registered. Mark FAILED
		// with the error string. Audit dashboard surfaces
		// "no_executor_registered_for_kind_X" so a misconfig
		// never silently noops.
		c.store.MarkFailed(ctx, op.OperationID, time.Now().UTC(), err.Error())
		return
	}
	if err := exec.Execute(ctx, op); err != nil {
		c.store.MarkFailed(ctx, op.OperationID, time.Now().UTC(), err.Error())
		return
	}
	if err := c.store.MarkSucceeded(ctx, op.OperationID, time.Now().UTC()); err != nil {
		log.Printf("[FLEET] mark succeeded %s: %v", op.OperationID, err)
	}
}

// Stop signals the tick goroutine to exit. Idempotent: a second
// call on an already-stopped controller returns nil (the cancel
// channel is one-shot; a second close would panic). The
// caller-facing contract is "you may call Stop at most once
// per Start".
func (c *FleetController) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.running {
		return
	}
	close(c.cancel)
	c.running = false
}

// Done returns a channel that closes when the tick goroutine
// has fully exited. runUntilShutdown's graceful teardown waits
// on this with a 15-second budget per bootstrap.go:runUntilShutdown.
//
// For a never-started controller, Done returns a pre-closed
// channel so the caller does NOT need to special-case nil.
// (init via the Start path is the production case; never-started
// is a test fixture.)
func (c *FleetController) Done() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return c.done
}

// AuditList is the audit-read path used by the admin handler.
// Thin wrapper around the underlying store so the handler does
// NOT need to thread the store as a second dependency.
//
// limit <= 0 means "no cap"; the handler passes the canonical
// 200-row cap.
func (c *FleetController) AuditList(ctx context.Context, workerID, statusFilter string, limit int) ([]store.Operation, error) {
	return c.store.ListOperations(ctx, workerID, statusFilter, limit)
}

// AuditGet fetches one row by ID. Thin wrapper. Maps to
// ErrOperationNotFound on miss (handler turns it into 404).
func (c *FleetController) AuditGet(ctx context.Context, operationID string) (*store.Operation, error) {
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
