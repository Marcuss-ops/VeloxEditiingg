package repository

import (
	"context"
	"database/sql"

	"velox-server/internal/jobs"
)

// Dialect encapsulates SQL-dialect differences plus optional audit hooks;
// SQLite is currently the only implementation.
type Dialect interface {
	// Placeholder returns "?" (SQLite).
	Placeholder(n int) string

	// Placeholders returns a comma-separated list of n placeholders.
	Placeholders(n int) string

	// CoalesceStatus returns the status column expression for predicates.
	CoalesceStatus() string

	// ConflictError returns the transition CAS-miss sentinel.
	ConflictError() error

	// ProjectionColumns returns the comma-separated column list for
	// Reader (Get/List) queries.
	ProjectionColumns() string

	// ScanJob scans and deserializes one row into *jobs.Job.
	ScanJob(row interface{ Scan(...interface{}) error }) (*jobs.Job, error)

	// ListByStatus queries jobs by status using dialect-specific SQL.
	// Returns all jobs when statuses is empty.
	ListByStatus(ctx context.Context, db *sql.DB, statuses []string, limit int) ([]jobs.Job, error)

	// GetCounts returns aggregate counts grouped by status.
	GetCounts(ctx context.Context, db *sql.DB) (jobs.Counts, error)

	// ── Audit hooks (job_history / job_events / outbox_events) ─────────

	InsertHistoryTx(ctx context.Context, tx *sql.Tx, jobID, status, workerID, message string) error
	InsertEventTx(ctx context.Context, tx *sql.Tx, jobID, eventType string, payload map[string]interface{}) error
	EmitOutboxTx(ctx context.Context, tx *sql.Tx, aggregateType, aggregateID, eventType string, payload []byte) error
}
