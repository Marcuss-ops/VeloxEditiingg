// Package store / jobs_repository_shared.go
//
// Shared Writer AND Reader implementation used by both SQLiteJobRepository
// and PostgresJobRepository.  The Dialect interface encapsulates every
// SQL-dialect difference plus optional audit hooks.
//
// Job-level ClaimNext / ClaimNextForProfile were REMOVED in favor of
// task-level ClaimNextWithAttemptAtomic (PR-2 / canonical-attempt-identity).
// The shared Writer (SetStatus / Fail / Cancel) and Reader (Get / List)
// are the remaining domain surface.

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"velox-server/internal/jobs"
)

// ── Dialect ──────────────────────────────────────────────────────────────

type Dialect interface {
	// Placeholder returns "?" (SQLite) or "$n" (Postgres).
	Placeholder(n int) string

	// Placeholders returns a comma-separated list of n placeholders.
	Placeholders(n int) string

	// CoalesceStatus returns the status column expression for predicates.
	// SQLite: "UPPER(status)"   Postgres: "UPPER(COALESCE(status, ''))"
	CoalesceStatus() string

	// ConflictError returns the transition CAS-miss sentinel.
	ConflictError() error

	// ProjectionColumns returns the comma-separated column list for
	// Reader (Get/List) queries.  SQLite includes Requirements columns;
	// Postgres uses a narrow Phase 2 projection.
	ProjectionColumns() string

	// ScanJob scans and deserializes one row into *jobs.Job.
	ScanJob(row interface{ Scan(...interface{}) error }) (*jobs.Job, error)

	// ListByStatus queries jobs by status using dialect-specific SQL
	// (IN clause for SQLite, = ANY($1::text[]) for Postgres).
	// Returns all jobs when statuses is empty (SQLite); nil when empty
	// (Postgres).
	ListByStatus(ctx context.Context, db *sql.DB, statuses []string, limit int) ([]jobs.Job, error)

	// GetCounts returns aggregate counts grouped by status.
	GetCounts(ctx context.Context, db *sql.DB) (jobs.Counts, error)

	// ── Optional audit hooks (no-ops on backends that don't support
	//     job_history / job_events / outbox_events) ─────────────────────

	InsertHistoryTx(ctx context.Context, tx *sql.Tx, jobID, status, workerID, message string) error
	InsertEventTx(ctx context.Context, tx *sql.Tx, jobID, eventType string, payload map[string]interface{}) error
	EmitOutboxTx(ctx context.Context, tx *sql.Tx, aggregateType, aggregateID, eventType string, payload []byte) error
}

// ── baseJobRepository ───────────────────────────────────────────────────

type baseJobRepository struct {
	db      *sql.DB
	dialect Dialect
}

func toAny(ss []string) []interface{} {
	out := make([]interface{}, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
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

// ── jobs.Writer ─────────────────────────────────────────────────────────

func (b *baseJobRepository) SetStatus(ctx context.Context, id string, from, to jobs.Status) error {
	sj, err := b.getJob(ctx, id)
	if err != nil {
		return fmt.Errorf("setstatus: get job %s: %w", id, err)
	}
	p := b.dialect
	now := nowStrISO()
	res, err := b.db.ExecContext(ctx,
		`UPDATE jobs
		   SET status = `+p.Placeholder(1)+`,
		       updated_at = `+p.Placeholder(2)+`,
		       revision = COALESCE(revision, 0) + 1
		 WHERE job_id = `+p.Placeholder(3)+`
		   AND `+p.CoalesceStatus()+` = `+p.Placeholder(4)+`
		   AND COALESCE(revision, 0) = `+p.Placeholder(5),
		string(to), now, id, string(from), sj.Revision)
	if err != nil {
		return fmt.Errorf("setstatus exec: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("setstatus %s: %w", id, p.ConflictError())
	}
	return nil
}

func (b *baseJobRepository) Fail(ctx context.Context, id, reason string) error {
	if id == "" {
		return fmt.Errorf("job repository: empty jobID in Fail")
	}
	p := b.dialect

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("fail begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Idempotency check: reject terminal jobs.
	var current string
	row := tx.QueryRowContext(ctx,
		`SELECT `+p.CoalesceStatus()+` FROM jobs WHERE job_id = `+p.Placeholder(1), id)
	if err := row.Scan(&current); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("fail %s: not found", id)
		}
		return fmt.Errorf("fail status: %w", err)
	}
	switch current {
	case "SUCCEEDED", "FAILED", "CANCELLED":
		return fmt.Errorf("fail: job %s is already terminal (%s)", id, current)
	}

	now := nowStrISO()
	res, err := tx.ExecContext(ctx,
		`UPDATE jobs
		   SET status = 'FAILED',
		       updated_at = `+p.Placeholder(1)+`,
		       revision = COALESCE(revision, 0) + 1,
		       error_message = `+p.Placeholder(2)+`,
		       failed_at = `+p.Placeholder(3)+`,
		       failed_by = ''
		 WHERE job_id = `+p.Placeholder(4)+`
		   AND `+p.CoalesceStatus()+` NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
		now, reason, now, id)
	if err != nil {
		return fmt.Errorf("fail update: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("fail %s: %w", id, p.ConflictError())
	}

	_ = p.InsertHistoryTx(ctx, tx, id, "FAILED", "" /* workerID */, "Job failed: "+reason)
	_ = p.InsertEventTx(ctx, tx, id, "job_failed", map[string]interface{}{
		"error": reason,
	})

	payload, _ := json.Marshal(map[string]interface{}{
		"job_id": id, "error": reason,
	})
	_ = p.EmitOutboxTx(ctx, tx, "job", id, "JOB_FAILED", payload)

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("fail commit: %w", err)
	}
	return nil
}

func (b *baseJobRepository) Cancel(ctx context.Context, id, reason string, revision int) error {
	if id == "" {
		return fmt.Errorf("job repository: empty jobID in Cancel")
	}
	p := b.dialect
	now := nowStrISO()

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("cancel begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Idempotency check.
	var current string
	row := tx.QueryRowContext(ctx,
		`SELECT `+p.CoalesceStatus()+` FROM jobs WHERE job_id = `+p.Placeholder(1), id)
	if err := row.Scan(&current); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("cancel %s: not found", id)
		}
		return fmt.Errorf("cancel status: %w", err)
	}
	switch current {
	case "CANCELLED":
		return tx.Commit()
	case "SUCCEEDED", "FAILED":
		return fmt.Errorf("cancel %s: cannot cancel terminal job (%s)", id, current)
	}

	var res sql.Result
	if revision >= 0 {
		res, err = tx.ExecContext(ctx,
			`UPDATE jobs
			   SET status = 'CANCELLED',
			       updated_at = `+p.Placeholder(1)+`,
			       revision = COALESCE(revision, 0) + 1,
			       claimed_at = '',
			       assigned_at = ''
			 WHERE job_id = `+p.Placeholder(2)+`
			   AND `+p.CoalesceStatus()+` NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
			   AND COALESCE(revision, 0) = `+p.Placeholder(3),
			now, id, revision)
	} else {
		res, err = tx.ExecContext(ctx,
			`UPDATE jobs
			   SET status = 'CANCELLED',
			       updated_at = `+p.Placeholder(1)+`,
			       revision = COALESCE(revision, 0) + 1,
			       claimed_at = '',
			       assigned_at = ''
			 WHERE job_id = `+p.Placeholder(2)+`
			   AND `+p.CoalesceStatus()+` NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
			now, id)
	}
	if err != nil {
		return fmt.Errorf("cancel update: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("cancel %s: %w", id, p.ConflictError())
	}

	_ = p.InsertHistoryTx(ctx, tx, id, "CANCELLED", "" /* workerID */, "Cancelled: "+reason)
	_ = p.InsertEventTx(ctx, tx, id, "job_cancelled", map[string]interface{}{"reason": reason})

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("cancel commit: %w", err)
	}
	return nil
}

func (b *baseJobRepository) Delete(ctx context.Context, id string) error {
	if id == "" {
		return fmt.Errorf("job repository: empty jobID in Delete")
	}
	p := b.dialect
	if _, err := b.db.ExecContext(ctx, `DELETE FROM jobs WHERE job_id = `+p.Placeholder(1), id); err != nil {
		return fmt.Errorf("delete %s: %w", id, err)
	}
	return nil
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
		jID, status, videoName, projectID                                                                      string
		createdAt, updatedAt, startedAt, completedAt, runID, requestJSON                                        string
		rev, maxRet                                                                                             int
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

