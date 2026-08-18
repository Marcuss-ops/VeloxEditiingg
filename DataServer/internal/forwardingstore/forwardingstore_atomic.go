package forwardingstore

import (
	"context"
	"fmt"

	"velox-server/internal/jobs"
	"velox-server/internal/storecore"
	"velox-server/internal/taskgraph"
)

// AtomicForwardAndEnqueue combines the Job+Task+TaskSpec creation AND the
// forwarding status update into a single SQLite transaction. This guarantees
// that a crash between the enqueue and the FORWARDED marking cannot leave a
// forwarded Job with the forwarding row still in FORWARDING, or vice versa.
//
// The transaction:
//  1. CAS: READY_TO_FORWARD → FORWARDING (claim the row)
//  2. Job+Task+TaskSpec creation via the injected JobTaskTxCreator (the
//     canonical single-writer path; the SQL lives in store.AtomicJobTaskCreator)
//  3. CAS: FORWARDING → FORWARDED (set target_job_id = job.ID)
//
// If the initial CAS fails (another runner claimed the row), the
// transaction rolls back and ErrTransitionConflict is returned without
// any side effects.
//
// This used to live on store.SQLiteStore; it moved here once the
// cross-domain step 2 was injectable. store.SQLiteStore keeps a thin
// delegating facade so existing callers keep their method name.
func (s *SQLiteForwardingStore) AtomicForwardAndEnqueue(
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
	if s.jobTaskCreator == nil {
		return fmt.Errorf("store: AtomicForwardAndEnqueue: job/task creator not wired")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storecore.WrapDBInfrastructure("AtomicForwardAndEnqueue begin", err)
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
		return storecore.WrapDBInfrastructure("AtomicForwardAndEnqueue claim", err)
	}
	affected, rowsErr := storecore.ReadRowsAffected(claimResult, "AtomicForwardAndEnqueue claim")
	if rowsErr != nil {
		return rowsErr
	}
	if affected == 0 {
		return storecore.ErrTransitionConflict
	}

	// 2. Delegate Job+Task+TaskSpec creation to the injected canonical
	//    single-writer path. It classifies its own SQL failures, so preserve
	//    its validation and domain errors unchanged.
	if err := s.jobTaskCreator.CreateJobWithTaskTx(ctx, tx, job, taskSpec, priority); err != nil {
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
		return storecore.WrapDBInfrastructure("AtomicForwardAndEnqueue forward", err)
	}
	affected, rowsErr = storecore.ReadRowsAffected(forwardResult, "AtomicForwardAndEnqueue forward")
	if rowsErr != nil {
		return rowsErr
	}
	if affected == 0 {
		return fmt.Errorf("store: AtomicForwardAndEnqueue: FORWARDING→FORWARDED CAS failed")
	}

	if err := tx.Commit(); err != nil {
		return storecore.WrapDBInfrastructure("AtomicForwardAndEnqueue commit", err)
	}
	return nil
}
