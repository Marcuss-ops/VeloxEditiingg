package spool

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// TaskResultOutboxEntry is one durable worker TaskResult awaiting a
// master acknowledgement. The composite key makes replay idempotent:
// the same task/attempt/report cannot create a second local row.
type TaskResultOutboxEntry struct {
	TaskID        string
	AttemptID     string
	ReportHash    string
	Payload       []byte
	AttemptCount  int
	NextAttemptAt time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// UpsertTaskResult stores a TaskResult before it is sent. Repeating the
// same key is a no-op and preserves the retry counters already recorded.
func (s *Store) UpsertTaskResult(ctx context.Context, taskID, attemptID, reportHash string, payload []byte) error {
	if s == nil || s.db == nil {
		return errors.New("spool.UpsertTaskResult: store is nil")
	}
	if taskID == "" || attemptID == "" || reportHash == "" || len(payload) == 0 {
		return fmt.Errorf("spool.UpsertTaskResult: task_id, attempt_id, report_hash and payload are required")
	}
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO task_result_outbox
		    (task_id, attempt_id, report_hash, payload, attempt_count, next_attempt_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, 0, ?, ?, ?)
		ON CONFLICT(task_id, attempt_id, report_hash) DO NOTHING`,
		taskID, attemptID, reportHash, payload,
		now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("spool.UpsertTaskResult: %w", err)
	}
	return nil
}

// ListDueTaskResults returns durable reports whose retry time has arrived.
func (s *Store) ListDueTaskResults(ctx context.Context, now time.Time, limit int) ([]TaskResultOutboxEntry, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("spool.ListDueTaskResults: store is nil")
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT task_id, attempt_id, report_hash, payload, attempt_count,
		       next_attempt_at, created_at, updated_at
		  FROM task_result_outbox
		 WHERE next_attempt_at <= ?
		 ORDER BY next_attempt_at ASC, created_at ASC
		 LIMIT ?`, now.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, fmt.Errorf("spool.ListDueTaskResults: %w", err)
	}
	defer rows.Close()
	var entries []TaskResultOutboxEntry
	for rows.Next() {
		var e TaskResultOutboxEntry
		var nextAt, createdAt, updatedAt string
		if err := rows.Scan(&e.TaskID, &e.AttemptID, &e.ReportHash, &e.Payload, &e.AttemptCount, &nextAt, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("spool.ListDueTaskResults: scan: %w", err)
		}
		if e.NextAttemptAt, err = parseRFC3339Nano(nextAt); err != nil {
			return nil, fmt.Errorf("spool.ListDueTaskResults: next_attempt_at: %w", err)
		}
		if e.CreatedAt, err = parseRFC3339Nano(createdAt); err != nil {
			return nil, fmt.Errorf("spool.ListDueTaskResults: created_at: %w", err)
		}
		if e.UpdatedAt, err = parseRFC3339Nano(updatedAt); err != nil {
			return nil, fmt.Errorf("spool.ListDueTaskResults: updated_at: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// ClaimTaskResultAttempt atomically claims a due row for one send. The
// expected attempt count is part of the predicate, so concurrent replay
// loops cannot both transmit the same due entry.
func (s *Store) ClaimTaskResultAttempt(ctx context.Context, taskID, attemptID, reportHash string, expectedAttemptCount int, now, nextAttemptAt time.Time) (bool, error) {
	if s == nil || s.db == nil {
		return false, errors.New("spool.ClaimTaskResultAttempt: store is nil")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	updatedAt := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := s.db.ExecContext(ctx, `
		UPDATE task_result_outbox
		   SET attempt_count = attempt_count + 1, next_attempt_at = ?, updated_at = ?
		 WHERE task_id = ? AND attempt_id = ? AND report_hash = ? AND attempt_count = ? AND next_attempt_at <= ?`,
		nextAttemptAt.UTC().Format(time.RFC3339Nano), updatedAt, taskID, attemptID, reportHash, expectedAttemptCount, now.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return false, fmt.Errorf("spool.ClaimTaskResultAttempt: %w", err)
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// MarkTaskResultAttempt advances retry metadata before a send. It is kept
// for callers that already own the row; new replay code should use the
// atomic ClaimTaskResultAttempt method.
func (s *Store) MarkTaskResultAttempt(ctx context.Context, taskID, attemptID, reportHash string, nextAttemptAt time.Time) error {
	if s == nil || s.db == nil {
		return errors.New("spool.MarkTaskResultAttempt: store is nil")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := s.db.ExecContext(ctx, `
		UPDATE task_result_outbox
		   SET attempt_count = attempt_count + 1, next_attempt_at = ?, updated_at = ?
		 WHERE task_id = ? AND attempt_id = ? AND report_hash = ?`,
		nextAttemptAt.UTC().Format(time.RFC3339Nano), now, taskID, attemptID, reportHash)
	if err != nil {
		return fmt.Errorf("spool.MarkTaskResultAttempt: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrNotFound
	}
	return nil
}

// ListTaskResultsForAttempt returns all durable report variants for identity
// validation. Normally there is one row; retaining the report hash in the
// query keeps conflict/replay diagnostics intact.
func (s *Store) ListTaskResultsForAttempt(ctx context.Context, taskID, attemptID string) ([]TaskResultOutboxEntry, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("spool.ListTaskResultsForAttempt: store is nil")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT task_id, attempt_id, report_hash, payload, attempt_count,
		       next_attempt_at, created_at, updated_at
		  FROM task_result_outbox
		 WHERE task_id = ? AND attempt_id = ?
		 ORDER BY created_at ASC`, taskID, attemptID)
	if err != nil {
		return nil, fmt.Errorf("spool.ListTaskResultsForAttempt: %w", err)
	}
	defer rows.Close()
	var entries []TaskResultOutboxEntry
	for rows.Next() {
		var e TaskResultOutboxEntry
		var nextAt, createdAt, updatedAt string
		if err := rows.Scan(&e.TaskID, &e.AttemptID, &e.ReportHash, &e.Payload, &e.AttemptCount, &nextAt, &createdAt, &updatedAt); err != nil {
			return nil, fmt.Errorf("spool.ListTaskResultsForAttempt: scan: %w", err)
		}
		if e.NextAttemptAt, err = parseRFC3339Nano(nextAt); err != nil {
			return nil, fmt.Errorf("spool.ListTaskResultsForAttempt: next_attempt_at: %w", err)
		}
		if e.CreatedAt, err = parseRFC3339Nano(createdAt); err != nil {
			return nil, fmt.Errorf("spool.ListTaskResultsForAttempt: created_at: %w", err)
		}
		if e.UpdatedAt, err = parseRFC3339Nano(updatedAt); err != nil {
			return nil, fmt.Errorf("spool.ListTaskResultsForAttempt: updated_at: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// DeleteTaskResult removes a row only after a matching TaskResultAck.
func (s *Store) DeleteTaskResult(ctx context.Context, taskID, attemptID, reportHash string) error {
	if s == nil || s.db == nil {
		return errors.New("spool.DeleteTaskResult: store is nil")
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM task_result_outbox WHERE task_id = ? AND attempt_id = ? AND report_hash = ?`, taskID, attemptID, reportHash); err != nil {
		return fmt.Errorf("spool.DeleteTaskResult: %w", err)
	}
	return nil
}

// DeleteTaskResultsForAttempt is the restart-safe ACK path. TaskResultAck
// intentionally does not echo report_hash; a master ACK for this canonical
// attempt therefore clears any local replay of that attempt.
func (s *Store) DeleteTaskResultsForAttempt(ctx context.Context, taskID, attemptID string) (bool, error) {
	if s == nil || s.db == nil {
		return false, errors.New("spool.DeleteTaskResultsForAttempt: store is nil")
	}
	res, err := s.db.ExecContext(ctx, `DELETE FROM task_result_outbox WHERE task_id = ? AND attempt_id = ?`, taskID, attemptID)
	if err != nil {
		return false, fmt.Errorf("spool.DeleteTaskResultsForAttempt: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("spool.DeleteTaskResultsForAttempt: rows affected: %w", err)
	}
	return n > 0, nil
}

// PendingTaskResultCount is used by tests and operator diagnostics.
func (s *Store) PendingTaskResultCount(ctx context.Context) (int, error) {
	if s == nil || s.db == nil {
		return 0, errors.New("spool.PendingTaskResultCount: store is nil")
	}
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM task_result_outbox`).Scan(&count); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("spool.PendingTaskResultCount: %w", err)
	}
	return count, nil
}
