package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"velox-server/internal/taskgraph"
	"velox-shared/contract"
)

// schedulePreparedWindowRetry runs before the failed attempt transaction is
// committed. Thus the parent never sees a transient FAILED child between
// attempt completion and requeue, and a crash cannot split those operations.
func schedulePreparedWindowRetry(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand, now time.Time) (bool, error) {
	if cmd.TaskStatus != taskgraph.StatusFailed || !retryablePreparedWindowFailure(cmd.ErrorCode, cmd.ErrorMsg) {
		return false, nil
	}
	var attempt, maxRetries int
	err := tx.QueryRowContext(ctx, `SELECT t.attempt_number, COALESCE(j.max_retries,0)
		FROM tasks t JOIN jobs j ON j.job_id=t.job_id
		WHERE t.task_id=? AND t.job_id=? AND t.executor_id=? AND t.status='FAILED'
		  AND t.attempt_id=? AND t.worker_id=? AND t.lease_id=?
		  AND UPPER(j.status) NOT IN ('FAILED','SUCCEEDED','CANCELLED','CANCELED')
		  AND EXISTS (SELECT 1 FROM tasks p JOIN task_specs s ON s.task_id=p.task_id,
		    json_each(COALESCE(p.depends_on,'[]')) dep
		    WHERE p.job_id=t.job_id AND dep.value=t.task_id
		      AND p.status IN ('PENDING','READY')
		      AND json_extract(s.payload_json,'$.runtime_finalize_status')='WAITING_PREPARATION')`,
		cmd.TaskID, cmd.JobID, contract.VideoPrepareWindowExecutorID, cmd.AttemptID, cmd.WorkerID, cmd.LeaseID).Scan(&attempt, &maxRetries)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("prepared-window retry inspect: %w", err)
	}
	if maxRetries <= 0 {
		maxRetries = 3
	}
	if attempt < 1 || attempt >= maxRetries+1 {
		return false, nil
	}
	shift := attempt - 1
	if shift > 5 {
		shift = 5
	}
	retryAt := now.UTC().Add(2 * time.Second * time.Duration(1<<shift)).Format(time.RFC3339Nano)
	updatedAt := now.UTC().Format(time.RFC3339Nano)
	res, err := tx.ExecContext(ctx, `UPDATE tasks SET status='PENDING',worker_id='',lease_id='',lease_expires_at=NULL,
		attempt_id=NULL,ready_at=?,started_at=NULL,completed_at=NULL,winning_attempt_terminal_pending=0,
		winning_attempt_id=NULL,winning_attempt_committed_at=NULL,revision=revision+1,updated_at=?
		WHERE task_id=? AND status='FAILED' AND attempt_id=? AND worker_id=? AND lease_id=?`,
		retryAt, updatedAt, cmd.TaskID, cmd.AttemptID, cmd.WorkerID, cmd.LeaseID)
	if err != nil {
		return false, fmt.Errorf("prepared-window retry schedule: %w", err)
	}
	count, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("prepared-window retry rows: %w", err)
	}
	if count != 1 {
		return false, fmt.Errorf("prepared-window retry: task changed during scheduling")
	}
	return true, nil
}
