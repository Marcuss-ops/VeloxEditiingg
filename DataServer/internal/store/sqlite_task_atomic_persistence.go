package store

// sqlite_task_atomic_persistence.go: CAS and invariant-probe helpers used by
// IngestTaskResultAtomic. The persistence writers live in
// sqlite_task_atomic_persistence_helpers.go; both files receive the
// coordinator-owned transaction and never open or close one themselves.

import (
	"context"
	"database/sql"
	"fmt"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// ingestTaskCAS performs the Task-side CAS for IngestTaskResultAtomic.
// See IngestTaskResultAtomic for the full invariant documentation.
func ingestTaskCAS(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand, now string) error {
	alreadyTerminalForThisAttempt, probeErr := probeTaskAlreadyTerminalForAttempt(ctx, tx, cmd.TaskID, cmd.WorkerID, cmd.LeaseID, cmd.AttemptID)
	if probeErr != nil {
		return probeErr
	}
	if alreadyTerminalForThisAttempt {
		return nil
	}

	var (
		taskRes sql.Result
		errCas  error
	)
	if cmd.TaskStatus == taskgraph.StatusSucceeded {
		taskRes, errCas = tx.ExecContext(ctx,
			`UPDATE tasks
			 SET winning_attempt_terminal_pending = 1,
			     completed_at = ?, updated_at = ?
			 WHERE task_id = ? AND status = 'RUNNING'
			   AND attempt_id = ? AND worker_id = ? AND lease_id = ?`,
			now, now,
			cmd.TaskID, cmd.AttemptID, cmd.WorkerID, cmd.LeaseID,
		)
	} else {
		taskRes, errCas = tx.ExecContext(ctx,
			`UPDATE tasks
			 SET status = ?, completed_at = ?, revision = revision + 1, updated_at = ?
			 WHERE task_id = ? AND status IN ('LEASED', 'RUNNING', 'READY')
			   AND worker_id = ? AND lease_id = ?`,
			string(cmd.TaskStatus), now, now,
			cmd.TaskID, cmd.WorkerID, cmd.LeaseID,
		)
	}
	if errCas != nil {
		return fmt.Errorf("task ingest atomic task cas: %w", errCas)
	}
	if n, _ := taskRes.RowsAffected(); n != 1 {
		return fmt.Errorf("task ingest atomic %s: %w", cmd.TaskID, taskgraph.ErrTransitionConflict)
	}
	return nil
}

// probeTaskAlreadyTerminalForAttempt returns true when the task is already
// terminal for the same attempt_id. In that case the downstream
// metric/cache/cost/artifact/event writes must still be replay-safe, so the
// lifecycle CAS is skipped.
func probeTaskAlreadyTerminalForAttempt(ctx context.Context, tx *sql.Tx, taskID, workerID, leaseID, attemptID string) (bool, error) {
	var cs, ca string
	probeErr := tx.QueryRowContext(ctx,
		`SELECT status, COALESCE(attempt_id, '') FROM tasks WHERE task_id = ? AND worker_id = ? AND lease_id = ?`,
		taskID, workerID, leaseID,
	).Scan(&cs, &ca)
	if probeErr == sql.ErrNoRows {
		return false, fmt.Errorf("task ingest atomic %s: %w", taskID, taskgraph.ErrTransitionConflict)
	}
	if probeErr != nil {
		return false, fmt.Errorf("task ingest atomic probe: %w", probeErr)
	}
	switch cs {
	case "SUCCEEDED", "FAILED", "CANCELLED":
		return ca == attemptID, nil
	default:
		return false, nil
	}
}

// ingestAttemptCAS performs the Attempt-side CAS for IngestTaskResultAtomic.
// It returns nil when the attempt is already terminal (replay-safe) and
// ErrStaleReport when no attempt row exists at all.
func ingestAttemptCAS(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand, now string) error {
	attRes, err := tx.ExecContext(ctx,
		`UPDATE task_attempts
		 SET status = ?, completed_at = ?, error_code = ?, error_message = ?,
		     report_version = report_version + 1, updated_at = ?
		 WHERE task_id = ?
		   AND worker_id = ? AND lease_id = ?
		   AND status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
		string(cmd.AttemptStatus), now, cmd.ErrorCode, cmd.ErrorMsg, now,
		cmd.TaskID, cmd.WorkerID, cmd.LeaseID,
	)
	if err != nil {
		return fmt.Errorf("task ingest atomic attempt cas: %w", err)
	}
	attemptRows, _ := attRes.RowsAffected()
	if attemptRows == 0 {
		return handleAttemptCASMiss(ctx, tx, cmd.TaskID, cmd.WorkerID, cmd.LeaseID)
	}
	return nil
}

// handleAttemptCASMiss distinguishes a replay-safe already-terminal attempt
// from a missing attempt row. The latter is a §9.5 invariant violation.
func handleAttemptCASMiss(ctx context.Context, tx *sql.Tx, taskID, workerID, leaseID string) error {
	existingTerminal, err := countTerminalAttempts(ctx, tx, taskID, workerID, leaseID)
	if err != nil {
		return fmt.Errorf("task ingest atomic attempt probe: %w", err)
	}
	if existingTerminal == 0 {
		return fmt.Errorf("task ingest atomic %s: missing attempt row for worker=%s lease=%s (§9.5 invariant guard): %w",
			taskID, workerID, leaseID, taskattempts.ErrStaleReport)
	}
	return nil
}

// countTerminalAttempts returns the number of terminal attempts for the
// identity tuple.
func countTerminalAttempts(ctx context.Context, tx *sql.Tx, taskID, workerID, leaseID string) (int, error) {
	var existingTerminal int
	probeErr := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_attempts
		 WHERE task_id = ? AND worker_id = ? AND lease_id = ?
		   AND status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
		taskID, workerID, leaseID,
	).Scan(&existingTerminal)
	if probeErr != nil {
		return 0, probeErr
	}
	return existingTerminal, nil
}
