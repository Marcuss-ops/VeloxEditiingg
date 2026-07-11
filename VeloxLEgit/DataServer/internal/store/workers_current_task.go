// Package store — RW-PROD-005 LoadCurrentTask SQL implementation.
//
// The SQL query lives here (the canonical data layer) so that the
// single-writer invariant enforced by check-db-access.sh is obeyed.
// The handler package (internal/handlers/server/api) calls
// LoadCurrentTaskRow and adapts the result into its own TaskSummary
// type for JSON serialization.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// CurrentTaskRow is the raw row shape returned by LoadCurrentTaskRow.
// The api package adapts this into its own TaskSummary type with JSON
// tags for the HTTP response.
type CurrentTaskRow struct {
	TaskID    string
	JobID     string
	Executor  string
	Status    string
	StartedAt string
}

const currentTaskQuery = `
SELECT
    ta.task_id, ta.job_id, ta.attempt_id,
    ta.started_at,
    t.executor, t.executor_version
FROM task_attempts ta
LEFT JOIN tasks t ON t.task_id = ta.task_id
WHERE ta.worker_id = ?
  AND ta.status = 'RUNNING'
ORDER BY ta.started_at DESC
LIMIT 1
`

// LoadCurrentTaskRow returns the most-recently-started RUNNING TaskAttempt
// for `workerID`. Returns (nil, nil) when no RUNNING attempt exists.
//
// The query is keyed on ta.task_id + ta.started_at DESC LIMIT 1 so
// concurrent RUNNING rows (which should not exist by spec — a
// worker has at most one task RUNNING at any moment) degrade to the
// most-recently-started as the tiebreaker.
func LoadCurrentTaskRow(db *sql.DB, ctx context.Context, workerID string) (*CurrentTaskRow, error) {
	if db == nil {
		return nil, nil
	}
	row := db.QueryRowContext(ctx, currentTaskQuery, workerID)
	var (
		ts              CurrentTaskRow
		startedRaw      sql.NullString
		executor        sql.NullString
		executorVersion sql.NullInt32
	)
	err := row.Scan(&ts.TaskID, &ts.JobID, new(sql.NullString), &startedRaw, &executor, &executorVersion)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("LoadCurrentTaskRow scan: %w", err)
	}
	if startedRaw.Valid && startedRaw.String != "" {
		if t, perr := time.Parse(time.RFC3339Nano, startedRaw.String); perr == nil {
			ts.StartedAt = t.UTC().Format(time.RFC3339)
		} else {
			ts.StartedAt = startedRaw.String
		}
	}
	if executor.Valid {
		execStr := executor.String
		if executorVersion.Valid && executorVersion.Int32 > 0 {
			execStr = fmt.Sprintf("%s@%d", execStr, executorVersion.Int32)
		}
		ts.Executor = execStr
	}
	ts.Status = "RUNNING"
	return &ts, nil
}
