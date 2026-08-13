package fleet

// controller_tick.go: the dispatch loop (Tick) + per-operation transition
// (processOne) for the FleetController. Split out of controller.go.

import (
	"context"
	"fmt"
	"log"
	"time"

	"velox-server/internal/store"
)

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
	if c == nil || c.store == nil {
		log.Printf("[FLEET] tick skipped: %v", ErrStoreNotConfigured)
		return
	}
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
	c.reconcileStaleRunning(ctx)
}

// processOne runs the QUEUED → RUNNING → terminal transition
// for a single operation. Each step's WHERE-clause guard means
// a duplicate processOne call (e.g. tick re-fires after a
// transient error) is a silent no-op — the row stays in its
// current state and the executor does not run twice.
func (c *FleetController) processOne(ctx context.Context, op *store.Operation) {
	now := time.Now().UTC()
	claimed, err := c.store.MarkRunning(ctx, op.OperationID, now)
	if err != nil {
		// MarkRunning failure is rare (DB locked, connection
		// drop mid-transaction). Log and let the next tick
		// try again — the row is still QUEUED.
		log.Printf("[FLEET] mark running %s: %v", op.OperationID, err)
		return
	}
	if !claimed {
		// The SQL status guard rejected this claim because another
		// dispatcher already owns the row or it is already terminal.
		// Never replay the external executor on a no-op claim.
		return
	}
	var exec OperationExecutor
	if c.executors == nil {
		err = fmt.Errorf("%w: registry is nil", ErrExecutorNotConfigured)
	} else {
		exec, err = c.executors.Lookup(op.Op)
	}
	if err != nil {
		// Lookup failure: no concrete executor registered. Persist the
		// stable EXECUTOR_NOT_CONFIGURED marker so a misconfigured boot
		// can never become a false SUCCEEDED operation.
		c.persistFailed(op.OperationID, err.Error())
		return
	}
	if err := exec.Execute(ctx, op); err != nil {
		c.persistFailed(op.OperationID, err.Error())
		return
	}
	c.persistSucceeded(op.OperationID)
}
