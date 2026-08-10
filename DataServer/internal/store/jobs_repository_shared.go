// Package store / jobs_repository_shared.go
//
// Writer AND Reader implementation used by SQLiteJobRepository. The
// Dialect interface encapsulates SQL-dialect differences plus optional
// audit hooks; SQLite is currently the only implementation.
//
// Job-level ClaimNext / ClaimNextForProfile were REMOVED in favor of
// task-level ClaimNextWithAttemptAtomic (PR-2 / canonical-attempt-identity).
// The shared Writer (SetStatus / Fail / Cancel) and Reader (Get / List)
// are the remaining domain surface.
//
// This file keeps the Dialect contract, the base repository shape and
// the Reader (Get / List / Counts) + internal getJob projection. The
// Writer transitions (SetStatus / Fail / FailWithCode / Cancel / Delete)
// live in the sibling file jobs_repository_transitions.go.

package store

import (
	"context"
	"database/sql"
	"fmt"

	"velox-server/internal/jobs"
)

// ── Dialect ──────────────────────────────────────────────────────────────

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

// ── baseJobRepository ───────────────────────────────────────────────────

type baseJobRepository struct {
	db      *sql.DB
	dialect Dialect
}

// ── jobs.Reader ─────────────────────────────────────────────────────────

func (b *baseJobRepository) Get(ctx context.Context, id string) (*jobs.Job, error) {
	if id == "" {
		return nil, fmt.Errorf("job repository: empty jobID")
	}
	p := b.dialect
	row := b.db.QueryRowContext(ctx,
		`SELECT `+p.ProjectionColumns()+` FROM jobs WHERE job_id = `+p.Placeholder(1), id)
	j, err := p.ScanJob(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	return j, nil
}

func (b *baseJobRepository) List(ctx context.Context, filter jobs.Filter) ([]jobs.Job, error) {
	statuses := make([]string, len(filter.Statuses))
	for i, s := range filter.Statuses {
		statuses[i] = string(s)
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 1000
	}
	return b.dialect.ListByStatus(ctx, b.db, statuses, limit)
}

func (b *baseJobRepository) Counts(ctx context.Context) (jobs.Counts, error) {
	return b.dialect.GetCounts(ctx, b.db)
}

// getJob is the internal projection used by SetStatus and Fail (which
// need to read the job before mutating it).  Uses the same narrow
// projection as the shared List method.
func (b *baseJobRepository) getJob(ctx context.Context, id string) (*jobs.Job, error) {
	if id == "" {
		return nil, fmt.Errorf("job repository: empty jobID")
	}
	p := b.dialect
	row := b.db.QueryRowContext(ctx,
		`SELECT job_id, COALESCE(status,''), COALESCE(video_name,''), COALESCE(project_id,''),
		        COALESCE(revision,0), COALESCE(max_retries,0),
		        COALESCE(created_at,''), COALESCE(updated_at,''),
		        COALESCE(started_at,''), COALESCE(completed_at,''),
		        COALESCE(run_id,''), COALESCE(request_json,'')
		 FROM jobs WHERE job_id = `+p.Placeholder(1), id)
	var (
		jID, status, videoName, projectID                                string
		createdAt, updatedAt, startedAt, completedAt, runID, requestJSON string
		rev, maxRet                                                      int
	)
	if err := row.Scan(&jID, &status, &videoName, &projectID, &rev, &maxRet,
		&createdAt, &updatedAt, &startedAt, &completedAt, &runID, &requestJSON); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("get job: %w", err)
	}
	return &jobs.Job{
		ID:          jID,
		Status:      jobs.Status(status),
		VideoName:   videoName,
		ProjectID:   projectID,
		Revision:    rev,
		MaxRetries:  maxRet,
		CreatedAt:   parseTimeOrZero(createdAt),
		UpdatedAt:   parseTimeOrZero(updatedAt),
		StartedAt:   parseTimeOrZero(startedAt),
		CompletedAt: parseTimeOrZero(completedAt),
		RunID:       runID,
		Payload:     requestJSON,
	}, nil
}
