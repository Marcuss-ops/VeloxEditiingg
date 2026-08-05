package store

// sqlite_task_atomic_accept.go: AcceptTaskAtomic — the atomic
// LEASED → RUNNING Task promotion paired with the PENDING → RUNNING
// TaskAttempt UPDATE, all in ONE transaction. Split out of
// sqlite_task_atomic.go.

import (
	"context"
	"fmt"
	"time"

	"velox-server/internal/jobs"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"

	sharedtelemetry "velox-shared/telemetry"
)

// AcceptTaskAtomic atomically transitions a Task from LEASED → RUNNING
// AND UPDATES the matching PENDING TaskAttempt to RUNNING in the SAME
// transaction. The single legal entry point for promoting a worker
// offer to a running execution. Returns taskgraph.ErrTransitionConflict
// if the Task CAS does not match (stale lease or revision); the
// rolled-back DB is indistinguishable from a never-called AcceptTaskAtomic.
//
// PR-2 (canonical-attempt-identity) CHANGED this method:
//   - Pre-PR-2 INSERTed the TaskAttempt row (because Claim did NOT pre-create one).
//   - Post-PR-2 the PENDING TaskAttempt row was inserted by ClaimNextWithAttemptAtomic
//     at Claim time, so AcceptTaskAtomic now UPDATEs status PENDING → RUNNING.
//   - The CAS tuple gains attempt_id + attempt_number on the Task row stamp
//     so a replay / stale-acceptance is bounded by both Task CAS and Attempt CAS.
//
// §9.5 closes the desync surface in handleTaskAccepted where a
// crash between h.taskRepo.Start and h.taskAttemptRepo.Create could
// leave a Task in RUNNING with no active Attempt. POST-PR-2 the PENDING
// attempt row is created atomically with the LEASED CAS at Claim time,
// so the §9.5 invariant holds at the moment of TaskOffer send.
func (r *SQLiteTaskRepository) AcceptTaskAtomic(ctx context.Context, attempt *taskattempts.TaskAttempt, revision int) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("task repository: store not initialized")
	}
	if attempt == nil {
		return fmt.Errorf("task repository: AcceptTaskAtomic requires a non-nil attempt")
	}
	if attempt.TaskID == "" || attempt.WorkerID == "" || attempt.LeaseID == "" || attempt.ID == "" {
		return fmt.Errorf("task repository: AcceptTaskAtomic requires task_id, worker_id, lease_id, attempt_id (canonical from Claim)")
	}

	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("task accept atomic begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	now := time.Now().UTC().Format(time.RFC3339)

	// 1. Task CAS: LEASED → RUNNING with worker_id + lease_id + revision.
	// PR-2 also asserts (attempt_id, attempt_number) match the canonical
	// row stamped at Claim time, so a re-entry with a mismatched attempt
	// surfaces as ErrTransitionConflict instead of silently advancing the
	// wrong attempt.
	taskRes, err := tx.ExecContext(ctx,
		`UPDATE tasks
		 SET status = 'RUNNING', started_at = ?, revision = revision + 1,
		     attempt_count = ?, updated_at = ?
		 WHERE task_id = ? AND status = 'LEASED'
		   AND worker_id = ? AND lease_id = ? AND revision = ?
		   AND attempt_id = ? AND attempt_number = ?`,
		now, attempt.AttemptNumber, now,
		attempt.TaskID, attempt.WorkerID, attempt.LeaseID, revision,
		attempt.ID, attempt.AttemptNumber,
	)
	if err != nil {
		return fmt.Errorf("task accept atomic task cas: %w", err)
	}
	if n, _ := taskRes.RowsAffected(); n != 1 {
		return fmt.Errorf("task accept atomic %s (canonical attempt mismatch?): %w", attempt.TaskID, taskgraph.ErrTransitionConflict)
	}

	// 2. Attempt UPDATE: PENDING → RUNNING in the same tx. The CAS tuple
	// enforces (id, task_id, attempt_number, worker_id, lease_id, PENDING);
	// any collision surfaces ErrTransitionConflict (attempt_row CAS gate
	// matches the audit §9.5 invariant on Task RUNNING ⇒ Attempt RUNNING).
	attRes, err := tx.ExecContext(ctx,
		`UPDATE task_attempts
		 SET status = 'RUNNING', updated_at = ?
		 WHERE id = ? AND task_id = ? AND attempt_number = ?
		   AND worker_id = ? AND lease_id = ?
		   AND status = 'PENDING'`,
		now, attempt.ID, attempt.TaskID, attempt.AttemptNumber,
		attempt.WorkerID, attempt.LeaseID,
	)
	if err != nil {
		return fmt.Errorf("task accept atomic attempt cas: %w", err)
	}
	if n, _ := attRes.RowsAffected(); n != 1 {
		// Either: attempt row missing (reject — a §9.5 desync since
		// ClaimNextWithAttemptAtomic would have created it) OR attempt
		// is already RUNNING (replay-safe no-op: but in that case the
		// UPDATE should have hit 1 row, so we land here only on
		// genuinely-missing rows).
		return fmt.Errorf("task accept atomic attempt %s not PENDING or missing (canonical drift): %w",
			attempt.ID, taskgraph.ErrTransitionConflict)
	}
	if err := persistMasterExecutionEventTx(ctx, tx, masterExecutionEvent{
		AttemptID: attempt.ID, JobID: attempt.JobID, TaskID: attempt.TaskID,
		WorkerID: attempt.WorkerID, WorkerSessionID: attempt.WorkerSessionID,
		SnapshotID: attempt.WorkerSnapshotID, LeaseID: attempt.LeaseID,
		Scope: sharedtelemetry.ScopeTask, Component: "master.offer", Action: "accept_to_start", Phase: "queue",
		StartedAt: time.Now().UTC(), CompletedAt: time.Now().UTC(),
	}); err != nil {
		return fmt.Errorf("task accept master telemetry: %w", err)
	}

	// 3. Job roll-up: once the worker acceptance is persisted, the parent
	// Job must become RUNNING in the same transaction so artifact upload
	// admission sees a consistent lifecycle state. We intentionally keep the
	// BeginUpload gate strict and only promote promotable Job states here.
	jobRes, err := tx.ExecContext(ctx,
		`UPDATE jobs
		 SET status = 'RUNNING',
		     started_at = COALESCE(started_at, ?),
		     updated_at = ?,
		     revision = CASE
		         WHEN status IN ('PENDING', 'RETRY_WAIT') THEN revision + 1
		         ELSE revision
		     END
		 WHERE job_id = ?
		   AND status IN ('PENDING', 'RETRY_WAIT', 'RUNNING')`,
		now, now, attempt.JobID,
	)
	if err != nil {
		return fmt.Errorf("task accept atomic job cas: %w", err)
	}
	if n, _ := jobRes.RowsAffected(); n != 1 {
		return fmt.Errorf("task accept atomic job %s not promotable to %s: %w",
			attempt.JobID, jobs.StatusRunning, taskgraph.ErrTransitionConflict)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("task accept atomic commit: %w", err)
	}
	committed = true
	attempt.CreatedAt, _ = time.Parse(time.RFC3339, now)
	attempt.UpdatedAt = attempt.CreatedAt
	attempt.Status = taskattempts.AttemptStatusRunning
	return nil
}
