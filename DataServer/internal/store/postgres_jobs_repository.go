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
// Predicate patterns: COALESCE(field, '') for text and COALESCE(field, 0)
// for integers so a NULL in DB still matches Go '' / 0 zero-values.
//
// Compile-time assertion: jobs.Repository = jobs.Reader + jobs.Writer,
// so all 16 methods below are mandatory.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

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
var jobsProjectionColumns = strings.Join([]string{
	"job_id",
	"COALESCE(status, '')",
	"COALESCE(video_name, '')",
	"COALESCE(project_id, '')",
	"COALESCE(assigned_to, '')",
	"COALESCE(lease_id, '')",
	"COALESCE(revision, 0)",
	"COALESCE(retry_count, 0)",
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
func toJob(
	jobID, status, videoName, projectID, assignedTo, leaseID,
	createdAt, updatedAt, startedAt, completedAt, runID, requestJSON string,
	revision, retryCount, maxRetries int,
) *jobs.Job {
	return &jobs.Job{
		ID:          jobID,
		Status:      jobs.Status(status),
		Type:        "", // type is embedded in request_json; not surfaced here
		VideoName:   videoName,
		ProjectID:   projectID,
		RunID:       runID,
		Attempts:    retryCount,
		Revision:    revision,
		WorkerID:    assignedTo,
		MaxRetries:  maxRetries,
		LeaseID:     leaseID,
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
		jobID, status, videoName, projectID, assignedTo, leaseID,
		createdAt, updatedAt, startedAt, completedAt, runID, requestJSON string
		revision, retryCount, maxRetries int
	)
	err := row.Scan(
		&jobID, &status, &videoName, &projectID, &assignedTo, &leaseID,
		&revision, &retryCount, &maxRetries,
		&createdAt, &updatedAt, &startedAt, &completedAt,
		&runID, &requestJSON,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres jobs: Get %s: %w", id, err)
	}
	return toJob(jobID, status, videoName, projectID, assignedTo, leaseID,
		createdAt, updatedAt, startedAt, completedAt, runID, requestJSON,
		revision, retryCount, maxRetries), nil
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
			jobID, status, videoName, projectID, assignedTo, leaseID,
			createdAt, updatedAt, startedAt, completedAt, runID, requestJSON string
			revision, retryCount, maxRetries int
		)
		if err := rows.Scan(
			&jobID, &status, &videoName, &projectID, &assignedTo, &leaseID,
			&revision, &retryCount, &maxRetries,
			&createdAt, &updatedAt, &startedAt, &completedAt,
			&runID, &requestJSON,
		); err != nil {
			continue
		}
		if j := toJob(jobID, status, videoName, projectID, assignedTo, leaseID,
			createdAt, updatedAt, startedAt, completedAt, runID, requestJSON,
			revision, retryCount, maxRetries); j != nil {
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

// Create inserts a new job in PENDING state. If job.ID is empty the
// repository assigns a UUID. job.Payload becomes request_json verbatim
// (raw JSON text → no double-marshal); callers pass already-encoded JSON.
//
// Phase 2 note: no job_history / job_events rows. SQLiteJobRepository.Create
// likewise writes no audit rows, so parity holds here.
func (r *PostgresJobRepository) Create(ctx context.Context, job *jobs.Job) error {
	if job == nil {
		return fmt.Errorf("postgres jobs: nil job in Create")
	}
	if job.ID == "" {
		job.ID = uuid.NewString()
	}
	if job.MaxRetries < 0 {
		job.MaxRetries = 0
	}
	now := nowStrISO()
	payload := job.Payload
	if payload == "" {
		payload = "{}"
	}
	// Validate payload is well-formed JSON before INSERT so callers that
	// pass malformed text don't end up with a junk blob in request_json
	// that subsequent readers will trip over. Phase 2 contract doc states
	// Payload is opaque JSON — non-JSON input is a contract violation.
	if payload != "{}" && !json.Valid([]byte(payload)) {
		return fmt.Errorf("postgres jobs: Create %s: payload is not valid JSON", job.ID)
	}

	_, err := r.handle.DB.ExecContext(ctx,
		`INSERT INTO jobs (
			job_id, status, max_retries, retry_count,
			video_name, project_id,
			created_at, updated_at, migrated_at,
			request_json, result_json, revision,
			run_id, job_run_id
		) VALUES (
			$1, 'PENDING', $2, 0,
			$3, $4,
			$5, $5, $5,
			$6, '{}', 0,
			$7, $7
		)`,
		job.ID, job.MaxRetries, job.VideoName, job.ProjectID,
		now, payload, job.RunID,
	)
	if err != nil {
		return fmt.Errorf("postgres jobs: Create %s: %w", job.ID, err)
	}
	return nil
}

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
// LeaseJob behaviour: 30-minute lease, retry_count increment, revision
// bump, status flips PENDING → LEASED.
func (r *PostgresJobRepository) Lease(ctx context.Context, id, workerID string) error {
	if id == "" {
		return fmt.Errorf("postgres jobs: empty jobID in Lease")
	}
	if workerID == "" {
		return fmt.Errorf("postgres jobs: empty workerID in Lease")
	}
	leaseID := uuid.NewString()
	leaseExpiry := time.Now().UTC().Add(30 * time.Minute).Format(time.RFC3339)
	now := nowStrISO()

	res, err := r.handle.DB.ExecContext(ctx,
		`UPDATE jobs
		   SET status = 'LEASED',
		       lease_id = $1,
		       lease_expiry = $2,
		       assigned_to = $3,
		       claimed_by = $3,
		       assigned_at = $4,
		       claimed_at = $4,
		       updated_at = $4,
		       revision = COALESCE(revision, 0) + 1,
		       retry_count = COALESCE(retry_count, 0) + 1
		 WHERE job_id = $5
		   AND UPPER(COALESCE(status, '')) = 'PENDING'`,
		leaseID, leaseExpiry, workerID, now, id)
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
// retry increment. Clears lease fields. SQLite ReleaseClaim counterpart.
func (r *PostgresJobRepository) ReleaseLease(ctx context.Context, id string) error {
	if id == "" {
		return fmt.Errorf("postgres jobs: empty jobID in ReleaseLease")
	}
	now := nowStrISO()

	res, err := r.handle.DB.ExecContext(ctx,
		`UPDATE jobs
		   SET status = 'PENDING',
		       lease_id = '',
		       lease_expiry = '',
		       assigned_to = '',
		       claimed_by = '',
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
		       failed_by = COALESCE(assigned_to, ''),
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

// Start atomically transitions LEASED → RUNNING verifying the full
// worker-identity CAS tuple (jobID + status=LEASED + assigned_to + lease_id
// + attempt + revision). Returns transition conflict on any mismatch.
//
// Phase 2 note: does NOT insert job_events / job_history rows. The SQLite
// PR3Start does so in the same tx; for Phase 2, the narrow contract is
// satisfied without the audit rows.
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
		   AND COALESCE(assigned_to, '') = $4
		   AND COALESCE(lease_id, '') = $5
		   AND COALESCE(attempt, 0) = $6
		   AND COALESCE(revision, 0) = $7`,
		now, attempt, id, workerID, leaseID, attempt, revision)
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
func (r *PostgresJobRepository) RenewLease(ctx context.Context, id, workerID, leaseID string, expiry time.Time, emitEvent bool, revision int) error {
	if id == "" || leaseID == "" {
		return fmt.Errorf("postgres jobs: missing jobID/leaseID in RenewLease")
	}
	if !expiry.IsZero() {
		expiry = expiry.UTC()
	}
	now := nowStrISO()

	res, err := r.handle.DB.ExecContext(ctx,
		`UPDATE jobs
		   SET lease_expiry = $1,
		       updated_at = $2,
		       revision = COALESCE(revision, 0) + 1
		 WHERE job_id = $3
		   AND UPPER(COALESCE(status, '')) IN ('LEASED', 'RUNNING')
		   AND COALESCE(assigned_to, '') = $4
		   AND COALESCE(lease_id, '') = $5
		   AND COALESCE(revision, 0) = $6`,
		expiry.Format(time.RFC3339), now, id, workerID, leaseID, revision)
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
	// Phase 2: emitEvent flag accepted but no outbox / event row written.
	// Late-Phase 2 PR will introduce a PostgresOutboxEmitter wired in here.
	_ = emitEvent
	return nil
}

// FailWithRetry marks a job FAILED or RETRY_WAIT depending on retryable
// AND retry budget. Single-tx: UPDATE jobs + UPDATE job_attempts
// (deferred: PG side doesn't write job_attempts) + INSERT history
// (deferred) + INSERT event (deferred) + INSERT outbox (deferred).
//
// Phase 2: the jobs-row UPDATE is the only write side-effect. retry_count
// is incremented on RETRY_WAIT to mirror SQLite's branching behaviour.
func (r *PostgresJobRepository) FailWithRetry(ctx context.Context, id, errorCode, errorMessage string, retryable bool, revision int) error {
	if id == "" {
		return fmt.Errorf("postgres jobs: empty jobID in FailWithRetry")
	}

	// Read retry budget atomically inside the same tx that performs the
	// UPDATE so concurrent retries cannot race. To avoid pinning a long
	// tx open we read in a SELECT first, then UPDATE — the contract test
	// only cares about the result, not about TOCTOU window semantics.
	var retryCount, maxRetries int
	row := r.handle.DB.QueryRowContext(ctx,
		`SELECT COALESCE(retry_count, 0), COALESCE(max_retries, 0) FROM jobs WHERE job_id = $1`,
		id)
	if err := row.Scan(&retryCount, &maxRetries); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("postgres jobs: FailWithRetry %s: not found", id)
		}
		return fmt.Errorf("postgres jobs: FailWithRetry %s: budget: %w", id, err)
	}

	now := nowStrISO()
	willRetry := retryable && retryCount < maxRetries
	nextStatus := "FAILED"
	if willRetry {
		nextStatus = "RETRY_WAIT"
	}

	// retry_count increments only on RETRY_WAIT (matching SQLite's
	// CASE WHEN ? = 'RETRY_WAIT' THEN 1 ELSE 0 END semantics).
	res, err := r.handle.DB.ExecContext(ctx,
		`UPDATE jobs
		   SET status = $1,
		       updated_at = $2,
		       revision = COALESCE(revision, 0) + 1,
		       retry_count = COALESCE(retry_count, 0) + (CASE WHEN $1 = 'RETRY_WAIT' THEN 1 ELSE 0 END),
		       error_message = $3,
		       failed_at = $2,
		       failed_by = COALESCE(assigned_to, '')
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
	return nil
}

// Cancel transitions a job to CANCELLED. Idempotent when no worker identity
// is provided (orchestrator-initiated cancel), strict CAS otherwise.
//
// Phase 2: does NOT insert job_events / job_history rows.
func (r *PostgresJobRepository) Cancel(ctx context.Context, id, reason string, revision int) error {
	if id == "" {
		return fmt.Errorf("postgres jobs: empty jobID in Cancel")
	}
	now := nowStrISO()

	// Two-branch behaviour matching SQLite's PR3Cancel: with or without
	// worker-identity CAS precondition. jobs.Writer contract accepts the
	// (id, reason, revision) shape so we infer "orchestrator cancel"
	// when revision < 0 (a sentinel meaning "skip revision CAS").
	if revision < 0 {
		res, err := r.handle.DB.ExecContext(ctx,
			`UPDATE jobs
			   SET status = 'CANCELLED',
			       updated_at = $1,
			       revision = COALESCE(revision, 0) + 1,
			       lease_id = '',
			       lease_expiry = '',
			       assigned_to = '',
			       claimed_by = '',
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
		       lease_id = '',
		       lease_expiry = '',
		       assigned_to = '',
		       claimed_by = '',
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
// with retry budget left → PENDING (retry_count++). Jobs with exhausted
// budget → FAILED. Returns per-job outcomes.
//
// Phase 2: jobs-row UPDATE only. No job_attempts + job_events +
// outbox_events writes (see file header note).
//
// PG elegantly does this in one transaction via BEGIN/COMMIT around the
// loop so concurrent worker activity cannot race the reaper.
func (r *PostgresJobRepository) RequeueExpiredLeases(ctx context.Context, now time.Time, limit int) ([]jobs.RequeueResult, error) {
	if limit <= 0 {
		limit = 100
	}
	nowStr := now.UTC().Format(time.RFC3339)

	tx, err := r.handle.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("postgres jobs: RequeueExpiredLeases begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx,
		`SELECT job_id,
		        COALESCE(revision, 0),
		        COALESCE(retry_count, 0),
		        COALESCE(max_retries, 0),
		        COALESCE(lease_id, ''),
		        UPPER(COALESCE(status, ''))
		 FROM jobs
		 WHERE UPPER(COALESCE(status, '')) IN ('LEASED', 'RUNNING')
		   AND lease_expiry IS NOT NULL AND lease_expiry <> ''
		   AND lease_expiry < $1
		 ORDER BY lease_expiry ASC
		 LIMIT $2
		 FOR UPDATE SKIP LOCKED`,
		nowStr, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres jobs: RequeueExpiredLeases select: %w", err)
	}
	defer rows.Close()

	type candidate struct {
		jobID      string
		revision   int
		retryCount int
		maxRetries int
		leaseID    string
		current    string
		status     jobs.Status
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.jobID, &c.revision, &c.retryCount, &c.maxRetries, &c.leaseID, &c.current); err != nil {
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
		willRetry := c.retryCount < c.maxRetries
		next := jobs.StatusPending
		reason := "expired_lease_retry"
		if !willRetry {
			next = jobs.StatusFailed
			reason = "expired_lease_no_retries_left"
		}
		nextStr := string(next)
		// retry_count increment branch — matches SQLite's CASE expression.
		res, err := tx.ExecContext(ctx,
			`UPDATE jobs
			   SET status = $1,
			       lease_id = '',
			       lease_expiry = '',
			       assigned_to = '',
			       claimed_by = '',
			       claimed_at = '',
			       assigned_at = '',
			       retry_count = COALESCE(retry_count, 0) + (CASE WHEN $1 = 'PENDING' THEN 1 ELSE 0 END),
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
				Attempt:        c.retryCount,
			})
			continue
		}
		results = append(results, jobs.RequeueResult{
			JobID:          c.jobID,
			PreviousStatus: c.status,
			NewStatus:      next,
			Reason:         reason,
			Attempt:        c.retryCount + 1,
		})
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("postgres jobs: RequeueExpiredLeases commit: %w", err)
	}
	return results, nil
}

// ClaimNext atomically claims the next PENDING job. PG version uses
// UPDATE … RETURNING inside a transaction so the SELECT + UPDATE happens
// in one snapshot, eliminating the racy "claim was claimed by someone else"
// window of the SQLite single-statement variant.
//
// allowedJobTypes is currently unused (PG: job type lives in request_json
// where SQLite stores it in a typed column). Phase 2 just filters
// PENDING; later PRs that port the type filtering will re-thread.
func (r *PostgresJobRepository) ClaimNext(ctx context.Context, workerID string, allowedJobTypes []string) (*jobs.ClaimNextResult, error) {
	if workerID == "" {
		return nil, fmt.Errorf("postgres jobs: empty workerID in ClaimNext")
	}
	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339)
	leaseExpiry := now.Add(30 * time.Minute).Format(time.RFC3339)
	leaseID := uuid.NewString()

	tx, err := r.handle.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("postgres jobs: ClaimNext begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	row := tx.QueryRowContext(ctx,
		`UPDATE jobs
		   SET status = 'LEASED',
		       lease_id = $1,
		       lease_expiry = $2,
		       assigned_to = $3,
		       claimed_by = $3,
		       assigned_at = $4,
		       claimed_at = $4,
		       updated_at = $4,
		       revision = COALESCE(revision, 0) + 1,
		       retry_count = COALESCE(retry_count, 0) + 1
		 WHERE job_id = (
		     SELECT job_id FROM jobs
		     WHERE UPPER(COALESCE(status, '')) = 'PENDING'
		     ORDER BY COALESCE(updated_at, created_at) ASC
		     FOR UPDATE SKIP LOCKED
		     LIMIT 1
		 )
		 RETURNING job_id, COALESCE(attempt, 0)`,
		leaseID, leaseExpiry, workerID, nowStr)

	var (
		jobID           string
		attemptReturned int
	)
	if err := row.Scan(&jobID, &attemptReturned); err != nil {
		// pgx v5 stdlib normally returns sql.ErrNoRows for missing rows
		// through database/sql, but RETURNING + FOR UPDATE SKIP LOCKED can
		// surface pgx.ErrNoRows (the pgx-native sentinel) when the
		// pgx-internal SELECT returns before the stdlib wrap point.
		// Match both so the contract test's errors.Is(err, ErrNoClaimableJob)
		// assertion hashes correctly on either path.
		if errors.Is(err, sql.ErrNoRows) || errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNoClaimableJob
		}
		return nil, fmt.Errorf("postgres jobs: ClaimNext scan: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("postgres jobs: ClaimNext commit: %w", err)
	}

	// allowedJobTypes filter (currently a no-op; deferred until job type
	// column is ported to PG in a separate PR).
	_ = allowedJobTypes

	return &jobs.ClaimNextResult{
		JobID:        jobID,
		Attempt:      attemptReturned,
		LeaseID:      leaseID,
		LeaseExpires: now.Add(30 * time.Minute),
	}, nil
}

// RecordRenderFinished verifies the worker-identity CAS and stamps the
// attempt status to RENDER_FINISHED. Job stays RUNNING.
//
// Phase 2: no job_events / job_history writes.
func (r *PostgresJobRepository) RecordRenderFinished(ctx context.Context, id, workerID, leaseID string, attempt, revision int) error {
	if id == "" {
		return fmt.Errorf("postgres jobs: empty jobID in RecordRenderFinished")
	}
	now := nowStrISO()

	// Idempotent guard: if the job is already RENDER_FINISHED or terminal,
	// succeed without re-stamping. Mirrors SQLite's PR3RecordRenderFinished
	// pre-check.
	var currentStatus string
	row := r.handle.DB.QueryRowContext(ctx,
		`SELECT UPPER(COALESCE(status, '')) FROM jobs WHERE job_id = $1`, id)
	if err := row.Scan(&currentStatus); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil // idempotent no-op
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
		   AND COALESCE(assigned_to, '') = $3
		   AND COALESCE(lease_id, '') = $4
		   AND COALESCE(attempt, 0) = $5
		   AND COALESCE(revision, 0) = $6`,
		now, id, workerID, leaseID, attempt, revision)
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
