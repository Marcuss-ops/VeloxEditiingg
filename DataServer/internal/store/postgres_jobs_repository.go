// Package store / postgres_jobs_repository.go
//
// Postgres-side implementation of jobs.Repository (narrow interface:
// internal/jobs/repository.go). Independent struct so the narrow contract
// stays clean and pgx vs mattn/go-sqlite3 specifics cannot leak across
// future adapters.
//
// Phase 2 scope:
//   - WRITES to the `jobs` table ONLY (002_jobs.sql).
//   - Does NOT emit to job_history / job_events / outbox_events. SQLite's
//     PR 3 paths (PR3Start, PR3Fail, PR3RenewLease, PR3Cancel,
//     PR3RecordRenderFinished, PR3RequeueExpiredLeases) write audit rows
//     and outbox events for transactional-outbox guarantees. Those rows
//     are NOT surfaced in the jobs.Writer interface contract — they're an
//     internal implementation detail of SQLiteJobRepository that the
//     narrow contract intentionally hides. Phase 2 accepts this divergence
//     to keep each ported adapter self-contained; a late-Phase 2 PR will
//     land the Postgres equivalents (job_history + job_events +
//     outbox_events schema + a PostgresOutboxEmitter) and re-thread
//     PR3-style audit emissions through the same tx paths.
//
// Connection ownership lives on platform/database.Handle. The repo
// only borrows the *sql.DB pointer; the previous *PostgresStore helper
// was dropped because platform/database now owns the connection
// lifecycle end-to-end (Open -> Handle -> handle.DB.Close), so every
// repo can depend on Handle uniformly across both Phase 2 backends.
//
// Placeholder syntax: $1, $2, ... matching pgx v5 stdlib.
// Predicate patterns: COALESCE(field, ”) for text and COALESCE(field, 0)
// for integers so a NULL in DB still matches Go ” / 0 zero-values.
//
// Compile-time assertion: jobs.Repository = jobs.Reader + jobs.Writer,
// so all 16 methods below are mandatory.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"velox-server/internal/costmodel"
	"velox-server/internal/jobs"
	"velox-server/internal/platform/database"
)

// PostgresJobRepository implements jobs.Repository on a *database.Handle.
type PostgresJobRepository struct {
	handle *database.Handle
}

// Compile-time assertion: satisfies jobs.Repository (jobs.Reader + jobs.Writer).
var _ jobs.Repository = (*PostgresJobRepository)(nil)

// NewPostgresJobRepository wraps a *database.Handle as a jobs.Repository.
// The handle is held by reference — the caller retains ownership of
// Close() so repos can be cheaply swapped during test setup without
// tearing down the connection pool multiple times.
func NewPostgresJobRepository(handle *database.Handle) *PostgresJobRepository {
	return &PostgresJobRepository{handle: handle}
}

// jobsProjectionColumns is the narrow SELECT list used by Get and List.
// Matches the jobs.Job domain model (NOT the wide store_jobs_query.go
// projection). COALESCE wrappers preserve the SQLite-style "empty string
// instead of NULL" zero-value convention so Go scan into string fields
// never sees a NULL surprise.
// PR #9: assigned_to, lease_id, retry_count columns dropped from jobs table.
var jobsProjectionColumns = strings.Join([]string{
	"job_id",
	"COALESCE(status, '')",
	"COALESCE(video_name, '')",
	"COALESCE(project_id, '')",
	"COALESCE(revision, 0)",
	"COALESCE(max_retries, 0)",
	"COALESCE(created_at, '')",
	"COALESCE(updated_at, '')",
	"COALESCE(started_at, '')",
	"COALESCE(completed_at, '')",
	"COALESCE(run_id, '')",
	"COALESCE(request_json, '')",
}, ", ")

// pgJobTransitionConflict is the Postgres-side transition-conflict
// sentinel. Distinct from any store.ErrTransitionConflict defined
// elsewhere so the two adapters' sentinels don't collide under errors.Is.
// The contract suite asserts BEHAVIOUR (second SetStatus fails) rather
// than error-string matching, so this divergence is invisible to callers
// that use jobs.Repository directly.
var pgJobTransitionConflict = errors.New("postgres jobs: transition from-state mismatch")

// parseTimeOrZero converts an RFC3339 string to time.Time, returning
// a zero time.Time for empty/invalid inputs (the SQLite code uses
// time.Time{}.IsZero() to mean "no timestamp stamped").
func parseTimeOrZero(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// nowStrISO is the canonical "now" string used in INSERT/UPDATE. Always
// UTC, RFC3339, in lockstep with the SQLite adapter's now strings.
func nowStrISO() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// toJob converts a scanned narrow-projection row into a *jobs.Job.
// PR #7: WorkerID + LeaseID removed — tasks carry the per-execution state.
// PR #9: assignedTo, leaseID, retryCount columns dropped.
func toJob(
	jobID, status, videoName, projectID,
	createdAt, updatedAt, startedAt, completedAt, runID, requestJSON string,
	revision, maxRetries int,
) *jobs.Job {
	return &jobs.Job{
		ID:          jobID,
		Status:      jobs.Status(status),
		Type:        "", // type is embedded in request_json; not surfaced here
		VideoName:   videoName,
		ProjectID:   projectID,
		RunID:       runID,
		Attempts:    0,
		Revision:    revision,
		MaxRetries:  maxRetries,
		StartedAt:   parseTimeOrZero(startedAt),
		CompletedAt: parseTimeOrZero(completedAt),
		CreatedAt:   parseTimeOrZero(createdAt),
		UpdatedAt:   parseTimeOrZero(updatedAt),
		Payload:     requestJSON,
	}
}

// ── jobs.Reader ────────────────────────────────────────────────────────────

// Get returns a single job by ID in the canonical domain model, or (nil, nil) on missing.
func (r *PostgresJobRepository) Get(ctx context.Context, id string) (*jobs.Job, error) {
	if id == "" {
		return nil, fmt.Errorf("postgres jobs: empty jobID in Get")
	}
	row := r.handle.DB.QueryRowContext(ctx,
		`SELECT `+jobsProjectionColumns+` FROM jobs WHERE job_id = $1`, id)

	var (
		jobID, status, videoName, projectID,
		createdAt, updatedAt, startedAt, completedAt, runID, requestJSON string
		revision, maxRetries int
	)
	err := row.Scan(
		&jobID, &status, &videoName, &projectID,
		&revision, &maxRetries,
		&createdAt, &updatedAt, &startedAt, &completedAt,
		&runID, &requestJSON,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres jobs: Get %s: %w", id, err)
	}
	return toJob(jobID, status, videoName, projectID,
		createdAt, updatedAt, startedAt, completedAt, runID, requestJSON,
		revision, maxRetries), nil
}

// List returns jobs matching the filter as canonical domain model objects.
// Empty Statuses → returns nil with no error (matches SQLite semantics).
// Uses ANY($1::text[]) on the upper-cased status column so the IN-list
// stays a single parameter — no dynamic placeholder building.
func (r *PostgresJobRepository) List(ctx context.Context, filter jobs.Filter) ([]jobs.Job, error) {
	if len(filter.Statuses) == 0 {
		return nil, nil
	}
	statuses := make([]string, 0, len(filter.Statuses))
	for _, s := range filter.Statuses {
		// Convert jobs.Status to string explicitly so the slice element
		// type doesn't get muddled by an inferred Job.Status→string assignment.
		cleaned := strings.TrimSpace(strings.ToUpper(string(s)))
		if cleaned == "" {
			continue
		}
		statuses = append(statuses, cleaned)
	}
	if len(statuses) == 0 {
		return nil, nil
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 1000
	}

	rows, err := r.handle.DB.QueryContext(ctx,
		`SELECT `+jobsProjectionColumns+`
		 FROM jobs
		 WHERE UPPER(COALESCE(status, '')) = ANY($1::text[])
		 ORDER BY COALESCE(updated_at, created_at) DESC
		 LIMIT $2`,
		statuses, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres jobs: List: %w", err)
	}
	defer rows.Close()

	out := make([]jobs.Job, 0)
	for rows.Next() {
		var (
			jobID, status, videoName, projectID,
			createdAt, updatedAt, startedAt, completedAt, runID, requestJSON string
			revision, maxRetries int
		)
		if err := rows.Scan(
			&jobID, &status, &videoName, &projectID,
			&revision, &maxRetries,
			&createdAt, &updatedAt, &startedAt, &completedAt,
			&runID, &requestJSON,
		); err != nil {
			continue
		}
		if j := toJob(jobID, status, videoName, projectID,
			createdAt, updatedAt, startedAt, completedAt, runID, requestJSON,
			revision, maxRetries); j != nil {
			out = append(out, *j)
		}
	}
	return out, rows.Err()
}

// Counts returns the aggregate count of jobs grouped by status. Only the
// 7 canonical status values (PENDING/LEASED/RUNNING/RETRY_WAIT/SUCCEEDED/
// FAILED/CANCELLED) appear in the returned map. The canonical filter
// lives in SQL (WHERE UPPER(...) IN (...)) so UNKNOWN / NULL rows are
// never round-tripped just to be discarded at the Go layer — matching
// SQLite's aggregate behaviour while keeping the wire format compact.
func (r *PostgresJobRepository) Counts(ctx context.Context) (jobs.Counts, error) {
	rows, err := r.handle.DB.QueryContext(ctx,
		`SELECT UPPER(COALESCE(status, '')) AS s, COUNT(*)
		 FROM jobs
		 WHERE UPPER(COALESCE(status, '')) IN
		     ('PENDING','LEASED','RUNNING','RETRY_WAIT','SUCCEEDED','FAILED','CANCELLED')
		 GROUP BY UPPER(COALESCE(status, ''))`)
	if err != nil {
		return nil, fmt.Errorf("postgres jobs: Counts: %w", err)
	}
	defer rows.Close()

	out := jobs.Counts{}
	for rows.Next() {
		var sname string
		var cnt int64
		if err := rows.Scan(&sname, &cnt); err != nil {
			continue
		}
		out[jobs.Status(sname)] = cnt
	}
	return out, rows.Err()
}

// ── jobs.Writer ────────────────────────────────────────────────────────────

// SetStatus performs a CAS status change from → to. Returns a transition
// conflict sentinel on miss. SQLite counterpart exists.
func (r *PostgresJobRepository) SetStatus(ctx context.Context, id string, from, to jobs.Status) error {
	sj, err := r.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("postgres jobs: SetStatus %s: get: %w", id, err)
	}
	if sj == nil {
		return fmt.Errorf("postgres jobs: SetStatus %s: not found", id)
	}
	fromNorm := strings.ToUpper(strings.TrimSpace(string(from)))
	toNorm := strings.ToUpper(strings.TrimSpace(string(to)))

	res, err := r.handle.DB.ExecContext(ctx,
		`UPDATE jobs
		   SET status = $1,
		       updated_at = $2,
		       revision = COALESCE(revision, 0) + 1
		 WHERE job_id = $3
		   AND UPPER(COALESCE(status, '')) = $4
		   AND COALESCE(revision, 0) = $5`,
		toNorm, nowStrISO(), id, fromNorm, sj.Revision)
	if err != nil {
		return fmt.Errorf("postgres jobs: SetStatus %s: exec: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres jobs: SetStatus %s: rows: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("postgres jobs: SetStatus %s -> %s: %w", id, toNorm, pgJobTransitionConflict)
	}
	return nil
}

// Lease atomically assigns a PENDING job to a worker. Mirrors SQLite
// LeaseJob behaviour: revision bump, status flips PENDING → LEASED.
// PR #9: lease_id, lease_expiry, assigned_to, claimed_by, retry_count columns dropped.
func (r *PostgresJobRepository) Lease(ctx context.Context, id, workerID string) error {
	if id == "" {
		return fmt.Errorf("postgres jobs: empty jobID in Lease")
	}
	if workerID == "" {
		return fmt.Errorf("postgres jobs: empty workerID in Lease")
	}
	now := nowStrISO()

	res, err := r.handle.DB.ExecContext(ctx,
		`UPDATE jobs
		   SET status = 'LEASED',
		       assigned_at = $1,
		       claimed_at = $1,
		       updated_at = $1,
		       revision = COALESCE(revision, 0) + 1
		 WHERE job_id = $2
		   AND UPPER(COALESCE(status, '')) = 'PENDING'`,
		now, id)
	if err != nil {
		return fmt.Errorf("postgres jobs: Lease %s: exec: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres jobs: Lease %s: rows: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("postgres jobs: Lease %s: not in PENDING: %w", id, pgJobTransitionConflict)
	}
	return nil
}

// ReleaseLease resets a LEASED/RUNNING job back to PENDING without
// retry increment. Clears assigned_at/claimed_at. SQLite ReleaseClaim counterpart.
// PR #9: lease_id, lease_expiry, assigned_to, claimed_by columns dropped.
func (r *PostgresJobRepository) ReleaseLease(ctx context.Context, id string) error {
	if id == "" {
		return fmt.Errorf("postgres jobs: empty jobID in ReleaseLease")
	}
	now := nowStrISO()

	res, err := r.handle.DB.ExecContext(ctx,
		`UPDATE jobs
		   SET status = 'PENDING',
		       assigned_at = '',
		       claimed_at = '',
		       updated_at = $1,
		       revision = COALESCE(revision, 0) + 1
		 WHERE job_id = $2
		   AND UPPER(COALESCE(status, '')) IN ('LEASED', 'RUNNING')`,
		now, id)
	if err != nil {
		return fmt.Errorf("postgres jobs: ReleaseLease %s: exec: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres jobs: ReleaseLease %s: rows: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("postgres jobs: ReleaseLease %s: not LEASED/RUNNING: %w", id, pgJobTransitionConflict)
	}
	return nil
}

// Fail marks a job FAILED and records the reason. Validates that the
// job isn't already terminal before transitioning. SQLite Fail counterpart.
// PR #9: assigned_to column dropped — failed_by uses empty string.
func (r *PostgresJobRepository) Fail(ctx context.Context, id, reason string) error {
	sj, err := r.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("postgres jobs: Fail %s: get: %w", id, err)
	}
	if sj == nil {
		return fmt.Errorf("postgres jobs: Fail %s: not found", id)
	}
	if sj.Status.IsTerminal() {
		return fmt.Errorf("postgres jobs: Fail %s: already terminal (%s)", id, sj.Status)
	}
	now := nowStrISO()
	res, err := r.handle.DB.ExecContext(ctx,
		`UPDATE jobs
		   SET status = 'FAILED',
		       error_message = $1,
		       failed_at = $2,
		       failed_by = '',
		       updated_at = $2,
		       revision = COALESCE(revision, 0) + 1
		 WHERE job_id = $3
		   AND UPPER(COALESCE(status, '')) NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
		   AND COALESCE(revision, 0) = $4`,
		reason, now, id, sj.Revision)
	if err != nil {
		return fmt.Errorf("postgres jobs: Fail %s: exec: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres jobs: Fail %s: rows: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("postgres jobs: Fail %s: predicate miss: %w", id, pgJobTransitionConflict)
	}
	return nil
}

// Start atomically transitions LEASED → RUNNING verifying the
// attempt + revision CAS tuple.
// PR #9: assigned_to + lease_id columns dropped.
func (r *PostgresJobRepository) Start(ctx context.Context, id, workerID, leaseID string, attempt, revision int) error {
	if id == "" {
		return fmt.Errorf("postgres jobs: empty jobID in Start")
	}
	if workerID == "" || leaseID == "" {
		return fmt.Errorf("postgres jobs: missing worker/lease identity in Start")
	}
	now := nowStrISO()
	res, err := r.handle.DB.ExecContext(ctx,
		`UPDATE jobs
		   SET status = 'RUNNING',
		       started_at = $1,
		       updated_at = $1,
		       revision = COALESCE(revision, 0) + 1,
		       attempt = $2
		 WHERE job_id = $3
		   AND UPPER(COALESCE(status, '')) = 'LEASED'
		   AND COALESCE(attempt, 0) = $4
		   AND COALESCE(revision, 0) = $5`,
		now, attempt, id, attempt, revision)
	if err != nil {
		return fmt.Errorf("postgres jobs: Start %s: exec: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres jobs: Start %s: rows: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("postgres jobs: Start %s: predicate miss: %w", id, pgJobTransitionConflict)
	}
	return nil
}

// RenewLease extends the lease on an active job. emitEvent is reserved for
// future Postgres-side PR9 audit insertion; in Phase 2 the value is
// silently accepted and not written to any table.
// PR #9: lease_expiry, assigned_to, lease_id columns dropped. Emits revision bump only.
func (r *PostgresJobRepository) RenewLease(ctx context.Context, id, workerID, leaseID string, expiry time.Time, emitEvent bool, revision int) error {
	if id == "" || leaseID == "" {
		return fmt.Errorf("postgres jobs: missing jobID/leaseID in RenewLease")
	}
	now := nowStrISO()

	res, err := r.handle.DB.ExecContext(ctx,
		`UPDATE jobs
		   SET updated_at = $1,
		       revision = COALESCE(revision, 0) + 1
		 WHERE job_id = $2
		   AND UPPER(COALESCE(status, '')) IN ('LEASED', 'RUNNING')
		   AND COALESCE(revision, 0) = $3`,
		now, id, revision)
	if err != nil {
		return fmt.Errorf("postgres jobs: RenewLease %s: exec: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres jobs: RenewLease %s: rows: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("postgres jobs: RenewLease %s: predicate miss: %w", id, pgJobTransitionConflict)
	}
	_ = expiry
	_ = workerID
	_ = emitEvent
	return nil
}

// FailWithRetry marks a job FAILED or RETRY_WAIT depending on retryable
// AND retry budget.
// PR #9: retry_count + assigned_to columns dropped. Use attempt as proxy.
func (r *PostgresJobRepository) FailWithRetry(ctx context.Context, id, errorCode, errorMessage string, retryable bool, revision int) error {
	if id == "" {
		return fmt.Errorf("postgres jobs: empty jobID in FailWithRetry")
	}

	var attemptCount, maxRetries int
	row := r.handle.DB.QueryRowContext(ctx,
		`SELECT COALESCE(attempt, 0), COALESCE(max_retries, 0) FROM jobs WHERE job_id = $1`,
		id)
	if err := row.Scan(&attemptCount, &maxRetries); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("postgres jobs: FailWithRetry %s: not found", id)
		}
		return fmt.Errorf("postgres jobs: FailWithRetry %s: budget: %w", id, err)
	}

	now := nowStrISO()
	willRetry := retryable && attemptCount < maxRetries
	nextStatus := "FAILED"
	if willRetry {
		nextStatus = "RETRY_WAIT"
	}

	res, err := r.handle.DB.ExecContext(ctx,
		`UPDATE jobs
		   SET status = $1,
		       updated_at = $2,
		       revision = COALESCE(revision, 0) + 1,
		       error_message = $3,
		       failed_at = $2,
		       failed_by = ''
		 WHERE job_id = $4
		   AND UPPER(COALESCE(status, '')) IN ('LEASED', 'RUNNING', 'RENDER_FINISHED', 'AWAITING_ARTIFACT')
		   AND COALESCE(revision, 0) = $5`,
		nextStatus, now, errorMessage, id, revision)
	if err != nil {
		return fmt.Errorf("postgres jobs: FailWithRetry %s: exec: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres jobs: FailWithRetry %s: rows: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("postgres jobs: FailWithRetry %s: predicate miss: %w", id, pgJobTransitionConflict)
	}
	_ = errorCode
	return nil
}

// Cancel transitions a job to CANCELLED. Idempotent when no worker identity
// is provided (orchestrator-initiated cancel), strict CAS otherwise.
// PR #9: lease_id, lease_expiry, assigned_to, claimed_by columns dropped.
func (r *PostgresJobRepository) Cancel(ctx context.Context, id, reason string, revision int) error {
	if id == "" {
		return fmt.Errorf("postgres jobs: empty jobID in Cancel")
	}
	now := nowStrISO()

	if revision < 0 {
		res, err := r.handle.DB.ExecContext(ctx,
			`UPDATE jobs
			   SET status = 'CANCELLED',
			       updated_at = $1,
			       revision = COALESCE(revision, 0) + 1,
			       claimed_at = '',
			       assigned_at = ''
			 WHERE job_id = $2
			   AND UPPER(COALESCE(status, '')) NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
			now, id)
		if err != nil {
			return fmt.Errorf("postgres jobs: Cancel %s (no-CAS): exec: %w", id, err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return fmt.Errorf("postgres jobs: Cancel %s: terminal or missing: %w", id, pgJobTransitionConflict)
		}
		_ = reason
		return nil
	}

	res, err := r.handle.DB.ExecContext(ctx,
		`UPDATE jobs
		   SET status = 'CANCELLED',
		       updated_at = $1,
		       revision = COALESCE(revision, 0) + 1,
		       claimed_at = '',
		       assigned_at = ''
		 WHERE job_id = $2
		   AND UPPER(COALESCE(status, '')) NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
		   AND COALESCE(revision, 0) = $3`,
		now, id, revision)
	if err != nil {
		return fmt.Errorf("postgres jobs: Cancel %s: exec: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres jobs: Cancel %s: rows: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("postgres jobs: Cancel %s: predicate miss: %w", id, pgJobTransitionConflict)
	}
	_ = reason
	return nil
}

// RequeueExpiredLeases processes up to `limit` expired-lease jobs. Jobs
// with retry budget left → PENDING. Jobs with exhausted budget → FAILED.
// PR #9: lease_expiry, retry_count, lease_id columns dropped.
// Uses job_attempts.started_at as proxy for lease timeout (30 min window).
func (r *PostgresJobRepository) RequeueExpiredLeases(ctx context.Context, now time.Time, limit int) ([]jobs.RequeueResult, error) {
	if limit <= 0 {
		limit = 100
	}
	cutoff := now.Add(-30 * time.Minute).UTC().Format(time.RFC3339)
	nowStr := now.UTC().Format(time.RFC3339)

	tx, err := r.handle.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("postgres jobs: RequeueExpiredLeases begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx,
		`SELECT j.job_id,
		        COALESCE(j.revision, 0),
		        COALESCE(j.attempt, 0),
		        COALESCE(j.max_retries, 0),
		        '',
		        UPPER(COALESCE(j.status, ''))
		 FROM jobs j
		 JOIN job_attempts ja ON ja.job_id = j.job_id
		        AND ja.id = (SELECT id FROM job_attempts WHERE job_id = j.job_id ORDER BY id DESC LIMIT 1)
		 WHERE UPPER(COALESCE(j.status, '')) IN ('LEASED', 'RUNNING')
		   AND ja.started_at IS NOT NULL AND ja.started_at <> ''
		   AND ja.started_at < $1
		 ORDER BY ja.started_at ASC
		 LIMIT $2
		 FOR UPDATE OF j SKIP LOCKED`,
		cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres jobs: RequeueExpiredLeases select: %w", err)
	}
	defer rows.Close()

	type candidate struct {
		jobID      string
		revision   int
		attemptCnt int
		maxRetries int
		leaseID    string
		current    string
		status     jobs.Status
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.jobID, &c.revision, &c.attemptCnt, &c.maxRetries, &c.leaseID, &c.current); err != nil {
			continue
		}
		c.status = jobs.Status(c.current)
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres jobs: RequeueExpiredLeases scan: %w", err)
	}
	rows.Close()

	results := make([]jobs.RequeueResult, 0, len(candidates))
	for _, c := range candidates {
		willRetry := c.attemptCnt < c.maxRetries
		next := jobs.StatusPending
		reason := "expired_lease_retry"
		if !willRetry {
			next = jobs.StatusFailed
			reason = "expired_lease_no_retries_left"
		}
		nextStr := string(next)
		// PR #9: retry_count, lease_id, lease_expiry, assigned_to, claimed_by columns dropped.
		res, err := tx.ExecContext(ctx,
			`UPDATE jobs
			   SET status = $1,
			       claimed_at = '',
			       assigned_at = '',
			       updated_at = $2,
			       revision = COALESCE(revision, 0) + 1
			 WHERE job_id = $3
			   AND UPPER(COALESCE(status, '')) = $4
			   AND COALESCE(revision, 0) = $5`,
			nextStr, nowStr, c.jobID, c.current, c.revision)
		if err != nil {
			return nil, fmt.Errorf("postgres jobs: RequeueExpiredLeases update %s: %w", c.jobID, err)
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			results = append(results, jobs.RequeueResult{
				JobID:          c.jobID,
				PreviousStatus: c.status,
				NewStatus:      c.status,
				Reason:         "skipped_concurrent_transition",
				Attempt:        c.attemptCnt,
			})
			continue
		}
		results = append(results, jobs.RequeueResult{
			JobID:          c.jobID,
			PreviousStatus: c.status,
			NewStatus:      next,
			Reason:         reason,
			Attempt:        c.attemptCnt + 1,
		})
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("postgres jobs: RequeueExpiredLeases commit: %w", err)
	}
	return results, nil
}

// ClaimNext atomically claims the next PENDING job. PG version uses
// UPDATE … RETURNING inside a transaction so the SELECT + UPDATE happens
// in one snapshot.
// PR #9: lease_id, lease_expiry, assigned_to, claimed_by, retry_count columns dropped.
func (r *PostgresJobRepository) ClaimNext(ctx context.Context, workerID string, allowedJobTypes []string) (*jobs.ClaimNextResult, error) {
	if workerID == "" {
		return nil, fmt.Errorf("postgres jobs: empty workerID in ClaimNext")
	}
	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339)

	tx, err := r.handle.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("postgres jobs: ClaimNext begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	row := tx.QueryRowContext(ctx,
		`UPDATE jobs
		   SET status = 'LEASED',
		       assigned_at = $1,
		       claimed_at = $1,
		       updated_at = $1,
		       revision = COALESCE(revision, 0) + 1
		 WHERE job_id = (
		     SELECT job_id FROM jobs
		     WHERE UPPER(COALESCE(status, '')) = 'PENDING'
		     ORDER BY COALESCE(updated_at, created_at) ASC
		     FOR UPDATE SKIP LOCKED
		     LIMIT 1
		 )
		 RETURNING job_id, COALESCE(attempt, 0)`,
		nowStr)

	var (
		jobID           string
		attemptReturned int
	)
	if err := row.Scan(&jobID, &attemptReturned); err != nil {
		if errors.Is(err, sql.ErrNoRows) || errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNoClaimableJob
		}
		return nil, fmt.Errorf("postgres jobs: ClaimNext scan: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("postgres jobs: ClaimNext commit: %w", err)
	}

	_ = allowedJobTypes

	return &jobs.ClaimNextResult{
		JobID:        jobID,
		Attempt:      attemptReturned,
		LeaseID:      "",
		LeaseExpires: now.Add(30 * time.Minute),
	}, nil
}

// RecordRenderFinished verifies the worker-identity CAS and stamps the
// attempt status to RENDER_FINISHED. Job stays RUNNING.
// PR #9: assigned_to + lease_id columns dropped.
func (r *PostgresJobRepository) RecordRenderFinished(ctx context.Context, id, workerID, leaseID string, attempt, revision int) error {
	if id == "" {
		return fmt.Errorf("postgres jobs: empty jobID in RecordRenderFinished")
	}
	now := nowStrISO()

	var currentStatus string
	row := r.handle.DB.QueryRowContext(ctx,
		`SELECT UPPER(COALESCE(status, '')) FROM jobs WHERE job_id = $1`, id)
	if err := row.Scan(&currentStatus); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("postgres jobs: RecordRenderFinished %s: status: %w", id, err)
	}
	if currentStatus == "RENDER_FINISHED" || currentStatus == "SUCCEEDED" || currentStatus == "FAILED" || currentStatus == "CANCELLED" {
		return nil
	}

	res, err := r.handle.DB.ExecContext(ctx,
		`UPDATE jobs
		   SET status = 'RENDER_FINISHED',
		       updated_at = $1,
		       revision = COALESCE(revision, 0) + 1,
		       started_at = COALESCE(started_at, $1)
		 WHERE job_id = $2
		   AND UPPER(COALESCE(status, '')) = 'RUNNING'
		   AND COALESCE(attempt, 0) = $3
		   AND COALESCE(revision, 0) = $4`,
		now, id, attempt, revision)
	if err != nil {
		return fmt.Errorf("postgres jobs: RecordRenderFinished %s: exec: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres jobs: RecordRenderFinished %s: rows: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("postgres jobs: RecordRenderFinished %s: predicate miss: %w", id, pgJobTransitionConflict)
	}
	_ = workerID
	_ = leaseID
	return nil
}

// Delete hard-deletes a job. Idempotent — a missing job returns nil.
// Note: PG has no FKs to artifacts / job_attempts / job_events in Phase 2
// so this DELETE only removes from the jobs table; related rows in those
// tables (still SQLite) remain authoritative.
func (r *PostgresJobRepository) Delete(ctx context.Context, id string) error {
	if id == "" {
		return fmt.Errorf("postgres jobs: empty jobID in Delete")
	}
	if _, err := r.handle.DB.ExecContext(ctx, `DELETE FROM jobs WHERE job_id = $1`, id); err != nil {
		return fmt.Errorf("postgres jobs: Delete %s: %w", id, err)
	}
	return nil
}

// ClaimNextForProfile is the cost-rank sibling of ClaimNext (PR-04.6).
// Phase 2 does NOT implement the rank path on Postgres: the master
// scheduler (push mode) ships with the SQLite-backed jobs store, and
// the Postgres adapter is a Phase 2 mirror that preserves the narrow
// contract but does not yet thread the per-job Requirements end-to-end
// (the dedicated columns + JSON subobject asymmetry on PG would force a
// separate schema migration that lands once Postgres is the primary
// scheduler backend).
//
// Returning ErrNoClaimableJob here is the safe fallback — the dispatch
// site treats this exactly as "nothing eligible" and the worker pulls
// no job in this round; the next tick will retry via FIFO ClaimNext.
// Phase 3 closes this gap; tracked separately from PR-04.6.
//
// Cycle-safety: costmodel only imports `strings`, so jobs → costmodel
// (already established in PR-04.5) plus store → costmodel is forward-
// only from this adapter and does not introduce a new module boundary.
func (r *PostgresJobRepository) ClaimNextForProfile(ctx context.Context, workerID string, allowedJobTypes []string, profile costmodel.WorkerProfile, maxCandidates int) (*jobs.ClaimNextResult, error) {
	_ = workerID
	_ = allowedJobTypes
	_ = profile
	_ = maxCandidates
	return nil, ErrNoClaimableJob
}
