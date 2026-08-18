// Package store provides the SQLite persistence layer for forwarding state.
package store

import (
	"context"
	"fmt"
	"time"

	"velox-server/internal/jobs"
	"velox-server/internal/taskgraph"
)

// Atomic forwarding operations spanning creator_forwardings and job/task creation.

// ── Atomic Enqueue + Forward ───────────────────────────────────────────

// AtomicForwardAndEnqueue combines the Job+Task+TaskSpec creation AND the
// forwarding status update into a single SQLite transaction. This guarantees
// that a crash between the enqueue and the FORWARDED marking cannot leave a
// forwarded Job with the forwarding row still in FORWARDING, or vice versa.
//
// The transaction:
//  1. CAS: READY_TO_FORWARD → FORWARDING (claim the row)
//  2. INSERT Job, Task, TaskSpec (same semantics as CreateJobWithTask)
//  3. CAS: FORWARDING → FORWARDED (set target_job_id = job.ID)
//
// If the initial CAS fails (another runner claimed the row), the
// transaction rolls back and ErrTransitionConflict is returned without
// any side effects.
//
// This method stays on SQLiteStore (rather than moving to the
// forwardingstore leaf) because step 2 is a cross-domain write: it delegates
// Job+Task+TaskSpec creation to the atomic creator, which owns delivery-plan,
// publication-state and task-requirements SQL outside the forwarding domain.
func (s *SQLiteStore) AtomicForwardAndEnqueue(
	ctx context.Context,
	forwardingID string,
	job *jobs.Job,
	taskSpec *taskgraph.TaskSpec,
	priority int,
	runnerID string,
	leaseID string,
) error {
	if forwardingID == "" || job == nil || job.ID == "" {
		return fmt.Errorf("store: AtomicForwardAndEnqueue: missing required fields")
	}
	if (runnerID == "") != (leaseID == "") {
		return fmt.Errorf("store: AtomicForwardAndEnqueue: runner_id and lease_id must be both empty or both present")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return wrapDBInfrastructure("AtomicForwardAndEnqueue begin", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := nowRFC3339()

	// 1. CAS: READY_TO_FORWARD → FORWARDING
	claimResult, err := tx.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'FORWARDING', updated_at = ?
		 WHERE forwarding_id = ?
		   AND status = 'READY_TO_FORWARD'
		   AND (? = '' OR (locked_by = ? AND lease_id = ? AND lease_expires_at > ?))`,
		now, forwardingID, runnerID, runnerID, leaseID, now,
	)
	if err != nil {
		return wrapDBInfrastructure("AtomicForwardAndEnqueue claim", err)
	}
	affected, rowsErr := readRowsAffected(claimResult, "AtomicForwardAndEnqueue claim")
	if rowsErr != nil {
		return rowsErr
	}
	if affected == 0 {
		return ErrTransitionConflict
	}

	// 2. Delegate Job+Task+TaskSpec creation to the canonical single-writer
	//    path (CreateJobWithTaskTx) so the SQL lives in exactly one place.
	creator := NewAtomicJobTaskCreator(s)
	if err := creator.CreateJobWithTaskTx(ctx, tx, job, taskSpec, priority); err != nil {
		// CreateJobWithTaskTx classifies its own SQL failures at the
		// store boundary. Preserve its validation and domain errors
		// unchanged instead of treating every callback error as SQL.
		return err
	}

	// 3. CAS: FORWARDING → FORWARDED
	forwardResult, err := tx.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'FORWARDED', target_job_id = ?,
		     locked_by = '', lease_id = '', lease_expires_at = '',
		     forwarded_at = ?, updated_at = ?
		 WHERE forwarding_id = ?
		   AND status = 'FORWARDING'
		   AND (? = '' OR (locked_by = ? AND lease_id = ? AND lease_expires_at > ?))`,
		job.ID, now, now, forwardingID, runnerID, runnerID, leaseID, now,
	)
	if err != nil {
		return wrapDBInfrastructure("AtomicForwardAndEnqueue forward", err)
	}
	affected, rowsErr = readRowsAffected(forwardResult, "AtomicForwardAndEnqueue forward")
	if rowsErr != nil {
		return rowsErr
	}
	if affected == 0 {
		return fmt.Errorf("store: AtomicForwardAndEnqueue: FORWARDING→FORWARDED CAS failed")
	}

	if err := tx.Commit(); err != nil {
		return wrapDBInfrastructure("AtomicForwardAndEnqueue commit", err)
	}
	return nil
}

// MarkCreatorForwardingReadySync transitions a PENDING/POLLING forwarding to
// READY_TO_FORWARD WITHOUT a (locked_by, lease_id) CAS (the synchronous
// handler path). SQL lives in the forwardingstore leaf.
func (s *SQLiteStore) MarkCreatorForwardingReadySync(ctx context.Context, forwardingID, payloadJSON, payloadSHA256 string) error {
	return s.forwarding.MarkCreatorForwardingReadySync(ctx, forwardingID, payloadJSON, payloadSHA256)
}

// MarkCreatorForwardingEnqueueRetry moves a forwarding that failed to enqueue
// (FORWARDING or READY_TO_FORWARD) to RETRY_WAIT with a backoff delay.
func (s *SQLiteStore) MarkCreatorForwardingEnqueueRetry(ctx context.Context, forwardingID, runnerID, leaseID, errorCode, errorMsg string, nextAttemptAt time.Time) error {
	return s.forwarding.MarkCreatorForwardingEnqueueRetry(ctx, forwardingID, runnerID, leaseID, errorCode, errorMsg, nextAttemptAt)
}
