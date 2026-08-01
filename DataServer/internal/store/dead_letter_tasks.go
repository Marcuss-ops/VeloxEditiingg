package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"velox-server/internal/deadletters"
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

// GetDeadLetter returns one operator-visible failure.
func (s *SQLiteStore) GetDeadLetter(ctx context.Context, id string) (*deadletters.Task, error) {
	var d deadletters.Task
	var first, last string
	err := s.db.QueryRowContext(ctx, `SELECT id, job_id, task_id, last_attempt_id, failure_class, error_code, retryable, payload_snapshot_json, first_failed_at, last_failed_at, replay_count, status FROM dead_letter_tasks WHERE id=?`, id).Scan(
		&d.ID, &d.JobID, &d.TaskID, &d.LastAttemptID, &d.FailureClass, &d.ErrorCode, &d.Retryable, &d.PayloadSnapshotJSON, &first, &last, &d.ReplayCount, &d.Status)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("dead letter get: %w", err)
	}
	d.FirstFailedAt, _ = time.Parse(time.RFC3339, first)
	d.LastFailedAt, _ = time.Parse(time.RFC3339, last)
	return &d, nil
}

// ListDeadLetters lists the newest failures, optionally filtered by status.
func (s *SQLiteStore) ListDeadLetters(ctx context.Context, status string, limit int) ([]deadletters.Task, error) {
	if limit <= 0 {
		limit = 100
	}
	query := `SELECT id, job_id, task_id, last_attempt_id, failure_class, error_code, retryable, payload_snapshot_json, first_failed_at, last_failed_at, replay_count, status FROM dead_letter_tasks`
	args := []any{}
	if status != "" {
		query += ` WHERE status=?`
		args = append(args, status)
	}
	query += ` ORDER BY last_failed_at DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("dead letter list: %w", err)
	}
	defer rows.Close()
	var out []deadletters.Task
	for rows.Next() {
		var d deadletters.Task
		var first, last string
		if err := rows.Scan(&d.ID, &d.JobID, &d.TaskID, &d.LastAttemptID, &d.FailureClass, &d.ErrorCode, &d.Retryable, &d.PayloadSnapshotJSON, &first, &last, &d.ReplayCount, &d.Status); err != nil {
			return nil, fmt.Errorf("dead letter scan: %w", err)
		}
		d.FirstFailedAt, _ = time.Parse(time.RFC3339, first)
		d.LastFailedAt, _ = time.Parse(time.RFC3339, last)
		out = append(out, d)
	}
	return out, rows.Err()
}

// ReplayDeadLetter resets the failed task to READY. The next atomic claim
// creates a fresh TaskAttempt; existing attempts and reports are untouched.
func (s *SQLiteStore) ReplayDeadLetter(ctx context.Context, id string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("dead letter replay begin: %w", err)
	}
	defer tx.Rollback()
	var taskID string
	if err := tx.QueryRowContext(ctx, `SELECT task_id FROM dead_letter_tasks WHERE id=? AND status IN ('OPEN','REPLAY_PENDING')`, id).Scan(&taskID); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("dead letter replay %s: not open", id)
		}
		return fmt.Errorf("dead letter replay load: %w", err)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := tx.ExecContext(ctx, `UPDATE tasks SET status='READY', worker_id='', lease_id='', lease_expires_at=NULL, attempt_id=NULL, attempt_number=0, revision=revision+1, updated_at=? WHERE task_id=? AND status='FAILED'`, now, taskID)
	if err != nil {
		return fmt.Errorf("dead letter replay task: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("dead letter replay %s: task is not FAILED", id)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE dead_letter_tasks SET status='REPLAY_PENDING', replay_count=replay_count+1 WHERE id=?`, id); err != nil {
		return fmt.Errorf("dead letter replay mark: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("dead letter replay commit: %w", err)
	}
	return nil
}

func (s *SQLiteStore) ResolveDeadLetter(ctx context.Context, id string) error {
	return s.updateDeadLetterStatus(ctx, id, deadletters.StatusResolved)
}
func (s *SQLiteStore) CancelDeadLetter(ctx context.Context, id string) error {
	return s.updateDeadLetterStatus(ctx, id, deadletters.StatusCancelled)
}

func (s *SQLiteStore) updateDeadLetterStatus(ctx context.Context, id, status string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE dead_letter_tasks SET status=? WHERE id=? AND status IN ('OPEN','REPLAY_PENDING')`, status, id)
	if err != nil {
		return fmt.Errorf("dead letter status: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("dead letter %s: no mutable row", id)
	}
	return nil
}
