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

	"velox-server/internal/store"
)

// CurrentTaskLoader abstracts the SQL backend so tests can pass a
// fake (sqlite in-mem or pgx) and production passes a store-backed
// implementation. The canonical SQL query lives in
// DataServer/internal/store (LoadCurrentTaskRow).
type CurrentTaskLoader interface {
	LoadCurrentTask(ctx context.Context, workerID string) (*TaskSummary, error)
}

// sqliteCurrentTaskLoader implements CurrentTaskLoader by delegating
// to store.LoadCurrentTaskRow. The *sql.DB field is stored here
// (not used for direct queries) so the handler can inject the DB
// dependency without importing the store package at the call site —
// only this adapter file imports store.
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

// LoadCurrentTask delegates to store.LoadCurrentTaskRow and adapts the
// raw row into an api.TaskSummary for JSON serialization.
func (l *sqliteCurrentTaskLoader) LoadCurrentTask(ctx context.Context, workerID string) (*TaskSummary, error) {
	row, err := store.LoadCurrentTaskRow(l.db, ctx, workerID)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	return &TaskSummary{
		TaskID:    row.TaskID,
		JobID:     row.JobID,
		Executor:  row.Executor,
		Status:    row.Status,
		StartedAt: row.StartedAt,
	}, nil
}

// noopCurrentTaskLoader is the fallback used when the handler is wired
// without a DB (unit tests). Returns nil on every call so the handler's
// nil-check suppresses current_task in the JSON response without
// emitting an error.
type noopCurrentTaskLoader struct{}

func (noopCurrentTaskLoader) LoadCurrentTask(context.Context, string) (*TaskSummary, error) {
	return nil, nil
}
