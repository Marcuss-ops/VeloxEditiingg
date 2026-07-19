// Package api — production *sql.DB-backed adapters for the per-worker
// read endpoints (metrics / sessions / events).
//
// workers_read_adapters.go wires the three Reader interfaces
// (MetricsReader, SessionsReader, EventsReader) declared in the
// respective handler files to the canonical store-layer helpers:
//
//	store.ListWorkerMetrics
//	store.ListWorkerSessions
//	store.ListWorkerEvents
//
// Keeping the adapter in a dedicated file means:
//   - the handler files compile without an explicit database/sql
//     import, so the test surface (which passes fake Readers) does
//     not need to drag *sql.DB into the test binary.
//   - a future Postgres backend just adds a sibling workers_read_adapters_pg.go
//     that implements the same three interfaces against *sql.DB or
//     pgxpool.Pool, without touching the handler logic.
//
// The adapter holds a *sql.DB (not *SQLiteStore) so the read path
// can be exercised by a plain in-memory SQLite open or a Postgres
// connection without pulling the SQLite god-object.
package api

import (
	"context"
	"database/sql"

	"velox-server/internal/store"
)

// SQLDBReader bundles the three production adapters. Construction
// is a single NewSQLDBReader(db) call; downstream handlers hold
// the inner fields directly. Nil-safe: NewSQLDBReader(nil) returns
// nil so callers can short-circuit route registration.
type SQLDBReader struct {
	Metrics  MetricsReader
	Sessions SessionsReader
	Events   EventsReader
}

// NewSQLDBReader wires the three Readers against the given *sql.DB.
// Returns nil when db is nil so callers can short-circuit route
// registration without nil-checking every reader individually.
func NewSQLDBReader(db *sql.DB) *SQLDBReader {
	if db == nil {
		return nil
	}
	r := &sqlDBReader{db: db}
	return &SQLDBReader{
		Metrics:  r,
		Sessions: r,
		Events:   r,
	}
}

// sqlDBReader is the concrete adapter. It implements all three
// Reader interfaces by delegating to the store helpers.
type sqlDBReader struct {
	db *sql.DB
}

// ListWorkerMetrics implements MetricsReader via store.ListWorkerMetrics.
func (r *sqlDBReader) ListWorkerMetrics(ctx context.Context, workerID, since string, limit int) ([]store.WorkerMetricSampleRow, error) {
	return store.ListWorkerMetrics(ctx, r.db, workerID, since, limit)
}

// ListWorkerSessions implements SessionsReader via store.ListWorkerSessions.
func (r *sqlDBReader) ListWorkerSessions(ctx context.Context, workerID string, includeRevoked bool, limit int) ([]store.WorkerSessionRow, error) {
	return store.ListWorkerSessions(ctx, r.db, workerID, includeRevoked, limit)
}

// ListWorkerEvents implements EventsReader via store.ListWorkerEvents.
func (r *sqlDBReader) ListWorkerEvents(ctx context.Context, workerID, eventType, since string, limit int) ([]store.WorkerEventRow, error) {
	return store.ListWorkerEvents(ctx, r.db, workerID, eventType, since, limit)
}
