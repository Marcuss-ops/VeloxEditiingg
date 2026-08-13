package store

// sqlite_task_atomic_transition.go: TransitionTaskToTerminalAtomic —
// the atomic Task + TaskAttempt terminal transition in ONE transaction.
// Split out of sqlite_task_atomic.go.

import (
	"context"
	"fmt"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// TransitionTaskToTerminalAtomic marks a Task AND its matching active
// TaskAttempt as terminal (SUCCEEDED / FAILED / CANCELLED) in a SINGLE
// transaction. The Task CAS gates on worker_id + lease_id + non-terminal
// state; the Attempt CAS gates on worker_id + lease_id + non-terminal
// status. Either both rows advance to terminal, or neither does.
//
// Idempotency semantics:
//   - Task CAS hits 0 rows ⇒ ErrTransitionConflict (stale or already terminal).
//   - Attempt CAS hits 0 rows BUT there is already a terminal attempt
//     for this (task_id, worker_id, lease_id) ⇒ commit (replay-safe).
//   - Attempt CAS hits 0 rows AND no attempt exists for this task_id
//     AT ALL ⇒ rollback with ErrStaleReport. This guard prevents the
//     transition from "improving" a Task that was already desynced from
//     its attempt into Task terminal + no attempt, violating §9.5 more
//     deeply than the pre-state.
//
// §9.5 closes the desync surface in handleTaskResult where a
// crash between h.taskRepo.SetStatus|Fail and h.taskAttemptRepo.CompleteFinal
// could permanently strand Task terminal + Attempt RUNNING.
func (r *SQLiteTaskRepository) TransitionTaskToTerminalAtomic(
	ctx context.Context,
	taskID, workerID, leaseID string,
	taskStatus taskgraph.Status,
	attemptStatus taskattempts.AttemptStatus,
	errorCode, errorMessage string,
) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("task repository: store not initialized")
	}
	if taskID == "" || workerID == "" || leaseID == "" {
		return fmt.Errorf("task repository: TransitionTaskToTerminalAtomic requires task_id, worker_id, lease_id")
	}
	if !taskStatus.IsTerminal() {
		return fmt.Errorf("task repository: TransitionTaskToTerminalAtomic requires terminal task status, got %s", taskStatus)
	}
	if !attemptStatus.IsTerminal() {
		return fmt.Errorf("task repository: TransitionTaskToTerminalAtomic requires terminal attempt status, got %s", attemptStatus)
	}

	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("task terminal atomic begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	now := nowRFC3339()

	// 1. Task CAS: any non-terminal → taskStatus (gated on worker + lease).
	taskRes, err := tx.ExecContext(ctx,
		`UPDATE tasks
		 SET status = ?, completed_at = ?, revision = revision + 1, updated_at = ?
		 WHERE task_id = ? AND status IN ('LEASED', 'RUNNING', 'READY')
		   AND worker_id = ? AND lease_id = ?`,
		string(taskStatus), now, now,
		taskID, workerID, leaseID,
	)
	if err != nil {
		return fmt.Errorf("task terminal atomic task cas: %w", err)
	}
	n, rowsErr := readRowsAffected(taskRes, "task terminal atomic task cas")
	if rowsErr != nil {
		return rowsErr
	}
	if n != 1 {
		return fmt.Errorf("task terminal atomic %s: %w", taskID, taskgraph.ErrTransitionConflict)
	}

	// 2. Attempt CAS: non-terminal → attemptStatus (gated on worker + lease).
	attRes, err := tx.ExecContext(ctx,
		`UPDATE task_attempts
		 SET status = ?, completed_at = ?, error_code = ?, error_message = ?,
		     report_version = report_version + 1, updated_at = ?
		 WHERE task_id = ?
		   AND worker_id = ? AND lease_id = ?
		   AND status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
		string(attemptStatus), now, errorCode, errorMessage, now,
		taskID, workerID, leaseID,
	)
	if err != nil {
		return fmt.Errorf("task terminal atomic attempt cas: %w", err)
	}
	attemptRows, rowsErr := readRowsAffected(attRes, "task terminal atomic attempt cas")
	if rowsErr != nil {
		return rowsErr
	}
	if attemptRows == 0 {
		// Either the attempt is already terminal (replay-safe) OR no
		// attempt exists at all for this (task_id, worker_id, lease_id).
		// Probe defensively to distinguish — a "missing attempt" stuck
		// Task in RUNNING would already be a §9.5 breach, and we must
		// NOT commit Task → terminal on top of that without an attempt
		// row, or §9.5 deepens from "no-Attempt" to "Task terminal +
		// no Attempt".
		var existingTerminal int
		probeErr := tx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM task_attempts
			 WHERE task_id = ? AND worker_id = ? AND lease_id = ?
			   AND status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
			taskID, workerID, leaseID,
		).Scan(&existingTerminal)
		if probeErr != nil {
			return fmt.Errorf("task terminal atomic attempt probe: %w", probeErr)
		}
		if existingTerminal == 0 {
			// No active AND no terminal attempt for this (task, worker,
			// lease) exists. The Task was either never accepted via
			// AcceptTaskAtomic, or its attempt row was lost. Either
			// way we cannot commit Task → terminal without an attempt.
			// Roll back and surface ErrStaleReport for the caller to
			// log / re-derive.
			return fmt.Errorf("task terminal atomic %s: missing attempt row for worker=%s lease=%s (§9.5 invariant guard): %w",
				taskID, workerID, leaseID, taskattempts.ErrStaleReport)
		}
		// existingTerminal > 0: replay-safe (a previous complete
		// already produced a terminal attempt); commit Task terminal
		// in the same retry.
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("task terminal atomic commit: %w", err)
	}
	committed = true
	return nil
}
