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
// READY_TO_FORWARD WITHOUT a (locked_by, lease_id) CAS. This is the
// synchronous handler path: the HTTP request INSERTed a fresh PENDING row
// (no lease) and immediately needs to promote it for the atomic enqueue step.
//
// Diff vs MarkCreatorForwardingReadyToForward: the latter is the legitimate
// runner lease-holder promotion (CAS on qualifier+lease_id pair). The sync
// path has no lease — using a CAS that requires one would never match. The
// sync method therefore matches the forwarding ID, a promotable status, and
// the absence of ownership fields. If a runner claims the row between INSERT
// and promotion, the sync path gets ErrTransitionConflict and cannot clear or
// overwrite the runner's ownership.
//
// Returns ErrTransitionConflict if the row is not in a promotable state
// (already READY_TO_FORWARD, FORWARDED, FAILED, BLOCKED, etc.).
func (s *SQLiteStore) MarkCreatorForwardingReadySync(ctx context.Context, forwardingID, payloadJSON, payloadSHA256 string) error {
	if forwardingID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingReadySync: empty forwarding_id")
	}
	now := nowRFC3339()
	result, err := s.db.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'READY_TO_FORWARD',
		     source_status = 'completed',
		     payload_json = ?, payload_sha256 = ?,
			    locked_by = '', lease_id = '', lease_expires_at = '',
			    updated_at = ?
		 WHERE forwarding_id = ?
		   AND status IN ('PENDING', 'POLLING')
		   AND COALESCE(locked_by, '') = ''
		   AND COALESCE(lease_id, '') = ''
		   AND COALESCE(lease_expires_at, '') = ''`,
		payloadJSON, payloadSHA256, now, forwardingID,
	)
	if err != nil {
		return wrapDBInfrastructure("MarkCreatorForwardingReadySync exec", err)
	}
	affected, rowsErr := readRowsAffected(result, "MarkCreatorForwardingReadySync")
	if rowsErr != nil {
		return rowsErr
	}
	if affected == 0 {
		return ErrTransitionConflict
	}
	return nil
}

// MarkCreatorForwardingEnqueueRetry moves a forwarding that failed to enqueue
// (FORWARDING or READY_TO_FORWARD) to RETRY_WAIT with a backoff delay.
// This is the enqueue-phase analog of MarkCreatorForwardingRetry (which
// handles the POLLING phase). The transition is owned by the active lease;
// a stale runner must not be able to rewrite a row claimed by another runner.
func (s *SQLiteStore) MarkCreatorForwardingEnqueueRetry(ctx context.Context, forwardingID, runnerID, leaseID, errorCode, errorMsg string, nextAttemptAt time.Time) error {
	if forwardingID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: MarkCreatorForwardingEnqueueRetry: missing forwarding_id, runner_id or lease_id")
	}

	now := nowRFC3339()
	nextISO := nextAttemptAt.UTC().Format(time.RFC3339)
	result, err := s.db.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET status = 'RETRY_WAIT',
		     locked_by = '', lease_id = '', lease_expires_at = '',
		     next_attempt_at = ?,
			 last_error_code = ?, last_error_message = ?,
		     updated_at = ?
		 WHERE forwarding_id = ?
		   AND status IN ('FORWARDING', 'READY_TO_FORWARD')
		   AND locked_by = ?
		   AND lease_id = ?
		   AND lease_expires_at > ?`,
		nextISO, nullIfEmpty(errorCode), nullIfEmpty(errorMsg), now,
		forwardingID, runnerID, leaseID, now,
	)
	if err != nil {
		return wrapDBInfrastructure("MarkCreatorForwardingEnqueueRetry exec", err)
	}
	affected, rowsErr := readRowsAffected(result, "MarkCreatorForwardingEnqueueRetry")
	if rowsErr != nil {
		return rowsErr
	}
	if affected == 0 {
		return ErrTransitionConflict
	}
	return nil
}
