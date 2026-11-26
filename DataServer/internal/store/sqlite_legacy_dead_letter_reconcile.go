package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// LegacyVideoDeadLetterReconcile summarizes the terminal legacy video
// dead-letter rows that can be removed from the active queue without deleting
// their task, attempt, payload, or task-spec history.
type LegacyVideoDeadLetterReconcile struct {
	Open      int  `json:"open"`
	Resolved  int  `json:"resolved"`
	Cancelled int  `json:"cancelled"`
	Applied   bool `json:"applied"`
}

// ReconcileLegacyVideoDeadLetters closes only non-retryable dead-letter rows
// for terminal video.prepare.window.v1 and video.assemble.slice.v1 tasks.
// Rows whose task succeeded are RESOLVED even when the referenced attempt
// failed; this records that a later attempt completed the task. Failed or
// cancelled terminal tasks are CANCELLED in the queue, while their complete
// immutable history remains stored. Any live, retryable, or malformed row
// aborts the whole operation.
func (s *SQLiteStore) ReconcileLegacyVideoDeadLetters(ctx context.Context, apply bool) (LegacyVideoDeadLetterReconcile, error) {
	var summary LegacyVideoDeadLetterReconcile
	if s == nil || s.db == nil {
		return summary, fmt.Errorf("legacy dead-letter reconcile: store is required")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: !apply})
	if err != nil {
		return summary, fmt.Errorf("legacy dead-letter reconcile begin: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT d.retryable, t.status, a.status, j.status
		FROM dead_letter_tasks d
		JOIN tasks t ON t.task_id=d.task_id
		JOIN task_attempts a ON a.id=d.last_attempt_id
		JOIN jobs j ON j.job_id=d.job_id
		WHERE d.status='OPEN'
		  AND t.executor_id IN ('video.prepare.window.v1','video.assemble.slice.v1')
	`)
	if err != nil {
		return summary, fmt.Errorf("legacy dead-letter reconcile inspect: %w", err)
	}
	for rows.Next() {
		var retryable int
		var taskStatus, attemptStatus, jobStatus string
		if err := rows.Scan(&retryable, &taskStatus, &attemptStatus, &jobStatus); err != nil {
			rows.Close()
			return summary, fmt.Errorf("legacy dead-letter reconcile scan: %w", err)
		}
		summary.Open++
		taskStatus = strings.ToUpper(strings.TrimSpace(taskStatus))
		attemptStatus = strings.ToUpper(strings.TrimSpace(attemptStatus))
		jobStatus = strings.ToUpper(strings.TrimSpace(jobStatus))
		jobTerminal := jobStatus == "SUCCEEDED" || jobStatus == "FAILED" || jobStatus == "CANCELLED" || jobStatus == "CANCELED"
		if retryable != 0 || !jobTerminal {
			rows.Close()
			return LegacyVideoDeadLetterReconcile{}, fmt.Errorf("legacy dead-letter reconcile: found retryable or live row (retryable=%d task=%s attempt=%s job=%s)", retryable, taskStatus, attemptStatus, jobStatus)
		}
		switch {
		case taskStatus == "SUCCEEDED":
			if attemptStatus != "FAILED" && attemptStatus != "SUCCEEDED" {
				rows.Close()
				return LegacyVideoDeadLetterReconcile{}, fmt.Errorf("legacy dead-letter reconcile: succeeded task has nonterminal attempt %s", attemptStatus)
			}
			summary.Resolved++
		case (taskStatus == "FAILED" || taskStatus == "CANCELLED" || taskStatus == "CANCELED") && attemptStatus == "FAILED":
			summary.Cancelled++
		default:
			rows.Close()
			return LegacyVideoDeadLetterReconcile{}, fmt.Errorf("legacy dead-letter reconcile: unsupported state task=%s attempt=%s job=%s", taskStatus, attemptStatus, jobStatus)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return LegacyVideoDeadLetterReconcile{}, fmt.Errorf("legacy dead-letter reconcile rows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return LegacyVideoDeadLetterReconcile{}, fmt.Errorf("legacy dead-letter reconcile close rows: %w", err)
	}
	if !apply {
		if err := tx.Commit(); err != nil {
			return LegacyVideoDeadLetterReconcile{}, fmt.Errorf("legacy dead-letter reconcile dry-run commit: %w", err)
		}
		return summary, nil
	}

	res, err := tx.ExecContext(ctx, `
		UPDATE dead_letter_tasks
		SET status = CASE
			WHEN EXISTS (
				SELECT 1 FROM tasks t
				WHERE t.task_id=dead_letter_tasks.task_id AND t.status='SUCCEEDED'
			) THEN 'RESOLVED'
			ELSE 'CANCELLED'
		END
		WHERE status='OPEN' AND retryable=0
		  AND EXISTS (
			SELECT 1 FROM tasks t
			JOIN task_attempts a ON a.id=dead_letter_tasks.last_attempt_id AND a.task_id=t.task_id
			JOIN jobs j ON j.job_id=dead_letter_tasks.job_id AND j.job_id=t.job_id
			WHERE t.task_id=dead_letter_tasks.task_id
			  AND t.executor_id IN ('video.prepare.window.v1','video.assemble.slice.v1')
			  AND j.status IN ('SUCCEEDED','FAILED','CANCELLED','CANCELED')
			  AND ((t.status='SUCCEEDED' AND a.status IN ('FAILED','SUCCEEDED'))
			    OR (t.status IN ('FAILED','CANCELLED','CANCELED') AND a.status='FAILED'))
		  )
	`)
	if err != nil {
		return LegacyVideoDeadLetterReconcile{}, fmt.Errorf("legacy dead-letter reconcile update: %w", err)
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return LegacyVideoDeadLetterReconcile{}, fmt.Errorf("legacy dead-letter reconcile rows affected: %w", err)
	}
	if changed != int64(summary.Open) {
		return LegacyVideoDeadLetterReconcile{}, fmt.Errorf("legacy dead-letter reconcile: inspected %d rows but updated %d; aborting", summary.Open, changed)
	}
	if err := tx.Commit(); err != nil {
		return LegacyVideoDeadLetterReconcile{}, fmt.Errorf("legacy dead-letter reconcile commit: %w", err)
	}
	summary.Applied = true
	return summary, nil
}
