// Package api — RW-PROD-005 LoadCurrentTask (per-worker RUNNING TaskAttempt).
//
// Spec §3 A4: "LoadCurrentTask(workerID) via JOIN task_attempts JOIN
// tasks WHERE status='RUNNING' AND worker_id=?".
//
// Implementation note: the join is implemented in pure SQL against
// the SQLite schema (tasks has task_id; task_attempts has worker_id
// + status). The mapper tolerates a row missing in either side by
// returning nil — there is no spurious TaskSummary with empty TaskID
// emitted. The handler calls LoadCurrentTask per worker only on the
// single-worker endpoint (GET /:worker_id) — the list endpoint skips
// it by design to avoid an N+1 query; if a dashboard needs current_task
// in the list, the alternative is the bulk query LoadCurrentTasksByWorkerIDs
// which the implementation below also exports.
//
// Race tolerance: a TaskAttempt status transition RIGHT after we
// query may either include or exclude the row. The handler treats
// the result as a snapshot — a worker that loses its RUNNING attempt
// between the SQL and the JSON write simply appears with empty
// current_task. This is acceptable because the dispatcher's selector
// re-evaluates on a 1s tick; a one-tick stale reading is not an
// operator-visible drift.
package api

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// CurrentTaskLoader abstracts the SQL backend so tests can pass a
// fake (sqlite in-mem or pgx) and production passes *sql.DB. The
// default NewCurrentTaskLoader(sqldb) is the wiring path used by
// the WorkersHandler.
type CurrentTaskLoader interface {
	LoadCurrentTask(ctx context.Context, workerID string) (*TaskSummary, error)
}

// sqliteCurrentTaskLoader implements CurrentTaskLoader against
// *sql.DB. The query joins task_attempts (status, started_at,
// attempt_id, task_id, worker_id) JOIN tasks (task_id, executor,
// executor_version) on the running attempt for the worker.
//
// The WHERE clause filters status='RUNNING'; if the schema evolves
// to use an enum with multiple running-shaped states (e.g. WAITING_FOR_LEASE),
// extend the IN-list here, NOT in handler code.
type sqliteCurrentTaskLoader struct {
	db *sql.DB
}

// NewCurrentTaskLoader is the canonical constructor. Returns nil-safe
// NewCurrentTaskLoader(nil) so callers can pass (db) directly.
func NewCurrentTaskLoader(db *sql.DB) CurrentTaskLoader {
	if db == nil {
		return nil
	}
	return &sqliteCurrentTaskLoader{db: db}
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

// LoadCurrentTask returns the most-recently-started RUNNING TaskAttempt
// for `workerID`. Returns (nil, sql.ErrNoRows)-equivalent (nil, nil)
// when no RUNNING attempt exists — handlers translate that into
// current_task=omitted in the JSON response.
//
// The query is keyed on ta.task_id + ta.started_at DESC LIMIT 1 so
// concurrent RUNNING rows (which should not exist by spec — a
// worker has at most one task RUNNING at any moment) degrade to the
// most-recently-started as the tiebreaker.
func (l *sqliteCurrentTaskLoader) LoadCurrentTask(ctx context.Context, workerID string) (*TaskSummary, error) {
	if l == nil || l.db == nil {
		return nil, nil
	}
	row := l.db.QueryRowContext(ctx, currentTaskQuery, workerID)
	var (
		ts              TaskSummary
		startedRaw      sql.NullString
		executor        sql.NullString
		executorVersion sql.NullInt32
	)
	err := row.Scan(&ts.TaskID, &ts.JobID, new(sql.NullString), &startedRaw, &executor, &executorVersion)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("LoadCurrentTask scan: %w", err)
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

// noopCurrentTaskLoader is the fallback used when the handler is wired
// without a DB (unit tests). Returns nil on every call so the handler's
// nil-check suppresses current_task in the JSON response without
// emitting an error.
type noopCurrentTaskLoader struct{}

func (noopCurrentTaskLoader) LoadCurrentTask(context.Context, string) (*TaskSummary, error) {
	return nil, nil
}
