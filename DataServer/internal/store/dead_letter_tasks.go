package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"velox-server/internal/taskgraph"
)

// persistDeadLetter is called inside the task-result transaction. The unique
// attempt key makes report replays idempotent while keeping every historical
// failed attempt available in task_attempts.
func persistDeadLetter(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand, now time.Time) error {
	if cmd.TaskStatus != taskgraph.StatusFailed {
		return nil
	}
	failureClass := cmd.FailureClass
	if failureClass == "" {
		failureClass = "TASK_FAILURE"
	}
	retryable := 0
	if cmd.Metrics.ErrorRetryable {
		retryable = 1
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO dead_letter_tasks (
			id, job_id, task_id, last_attempt_id, failure_class, error_code,
			retryable, payload_snapshot_json, first_failed_at, last_failed_at,
			replay_count, status
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'OPEN')
		ON CONFLICT(last_attempt_id) DO UPDATE SET
			last_failed_at=excluded.last_failed_at,
			error_code=excluded.error_code,
			retryable=excluded.retryable,
			payload_snapshot_json=excluded.payload_snapshot_json`,
		"dlq_"+cmd.AttemptID, cmd.JobID, cmd.TaskID, cmd.AttemptID,
		failureClass, cmd.ErrorCode, retryable, cmd.RawReportJSON,
		now.UTC().Format(time.RFC3339), now.UTC().Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("dead letter persist: %w", err)
	}
	return nil
}
