// Package store / sqlite_task_lease_renew.go — lease renewal.
// Extracted from sqlite_task_lease.go: the RenewLease CAS.
package store

import (
	"context"
	"fmt"
	"time"

	"velox-server/internal/taskgraph"
)

// RenewLease extends a currently-leased or running task's deadline
// (PR-03 / fix/task-lease-renewal-protocol). CAS tuple:
//
//	task_id=? AND worker_id=? AND lease_id=?
//	AND status IN ('LEASED', 'RUNNING') AND revision=?
//
// Acceptance of BOTH states is intentional: a worker progressed to
// RUNNING after TaskLeaseGranted is acknowledged and a task longer
// than the 30-min TTL must renew without first being reaped.
//
// The CAS intentionally does NOT gate on attempt_id: AcceptTaskAtomic
// is the sole writer of attempt_id on tasks, and a worker cannot hold
// two different attempt_ids for the same task concurrently. The
// (worker_id, lease_id) tuple already binds the renewal to the
// canonical attempt implicitly. The TOCTOU race against reaper-reset
// is closed by (worker_id, lease_id) gates alone — a stale worker on
// (W1, L1) cannot match a freshly re-stamped row with (W2, L2).
//
// revision is intentionally NOT bumped (see the interface comment):
// renewal is idempotent on its own (task_id, worker_id, lease_id, revision)
// tuple.
func (r *SQLiteTaskRepository) RenewLease(ctx context.Context, id, workerID, leaseID string, expiry time.Time, revision int) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("task repository: store not initialized")
	}
	if id == "" || workerID == "" || leaseID == "" {
		return fmt.Errorf("task repository: RenewLease requires task_id, worker_id, lease_id")
	}
	if expiry.IsZero() {
		return fmt.Errorf("task repository: RenewLease requires a non-zero expiry")
	}

	now := time.Now().UTC().Format(time.RFC3339)
	res, err := r.store.db.ExecContext(ctx,
		`UPDATE tasks
		 SET lease_expires_at = ?, updated_at = ?
		 WHERE task_id = ?
		   AND worker_id = ? AND lease_id = ? AND revision = ?
		   AND status IN ('LEASED', 'RUNNING')`,
		expiry.UTC().Format(time.RFC3339), now,
		id, workerID, leaseID, revision,
	)
	if err != nil {
		return wrapDBInfrastructure("task renew lease", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return wrapDBInfrastructure("task renew lease rows", err)
	}
	if n == 0 {
		return fmt.Errorf("task renew lease %s: %w", id, taskgraph.ErrTransitionConflict)
	}
	return nil
}
