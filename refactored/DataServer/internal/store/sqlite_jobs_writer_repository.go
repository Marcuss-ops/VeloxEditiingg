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
)

// SQLiteJobRepository implements JobRepository against *SQLiteStore (spec §5).
//
// The atomicity contract is enforced via either an internal BeginTx (for
// CreateJob / Transition / ClaimNext) or via delegation to *SQLiteStore
// methods that already own their own transactions (ClaimNextPendingJob).
type SQLiteJobRepository struct {
	store *SQLiteStore
}

// NewSQLiteJobRepository wraps a SQLiteStore as a JobRepository.
func NewSQLiteJobRepository(store *SQLiteStore) *SQLiteJobRepository {
	return &SQLiteJobRepository{store: store}
}

// jobProjectionColumns lists the columns MaterializedBy JobRepository reads.
// Centralised so GetJob and ListByStatus stay in lock-step.
var jobProjectionColumns = []string{
	"job_id",
	"status",
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
}

// scanJob reads one row in jobProjectionColumns order into a *Job.
func scanJob(row interface {
	Scan(...interface{}) error
}) (*Job, error) {
	var j Job
	err := row.Scan(
		&j.JobID, &j.Status, &j.VideoName, &j.ProjectID,
		&j.AssignedTo, &j.LeaseID,
		&j.Revision, &j.RetryCount, &j.MaxRetries,
		&j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.CompletedAt,
	)
	if err != nil {
		return nil, err
	}
	return &j, nil
}

// CreateJob inserts a job in PENDING state atomically. If params.JobID is
// empty the repository assigns a UUID.
func (r *SQLiteJobRepository) CreateJob(ctx context.Context, params CreateJobParams) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("job repository: store not initialized")
	}
	if params.MaxRetries < 0 {
		params.MaxRetries = 0
	}
	if params.JobID == "" {
		params.JobID = uuid.NewString()
	}

	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("create job begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339)
	requestJSON := "{}"
	if len(params.Payload) > 0 {
		if b, err := json.Marshal(params.Payload); err == nil {
			requestJSON = string(b)
		}
	}

	res, err := tx.ExecContext(ctx,
		`INSERT INTO jobs (
			job_id, status, max_retries, retry_count,
			video_name, project_id,
			created_at, updated_at,
			request_json, result_json, revision, raw_json
		) VALUES (?, 'PENDING', ?, 0, ?, ?, ?, ?, ?, '{}', 0, '{}')`,
		params.JobID, params.MaxRetries, params.VideoName, params.ProjectID,
		now, now, requestJSON,
	)
	if err != nil {
		return fmt.Errorf("create job exec: %w", err)
	}
	if _, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("create job rows: %w", err)
	}

	return tx.Commit()
}

// GetJob returns one job projection, or (nil, nil) if missing.
func (r *SQLiteJobRepository) GetJob(ctx context.Context, jobID string) (*Job, error) {
	if jobID == "" {
		return nil, fmt.Errorf("job repository: empty jobID")
	}
	row := r.store.db.QueryRowContext(ctx,
		`SELECT `+strings.Join(jobProjectionColumns, ",")+` FROM jobs WHERE job_id = ?`,
		jobID)
	j, err := scanJob(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	return j, nil
}

// ClaimNext delegates to the well-tested ClaimNextPendingJob, which already
// owns its own transaction. Wrapping it preserves the spec §5 single-method
// atomicity contract while keeping the complex CAS update SQL in one place.
func (r *SQLiteJobRepository) ClaimNext(ctx context.Context, claim ClaimParams) (*ClaimResult, error) {
	if claim.WorkerID == "" {
		return nil, fmt.Errorf("job repository: claim with empty workerID")
	}
	now := claim.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	resultJSON, ok, err := r.store.ClaimNextPendingJob(claim.WorkerID, claim.AllowedJobTypes, now)
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	if !ok {
		return nil, ErrNoClaimableJob
	}

	out := &ClaimResult{ResultJSON: append([]byte(nil), resultJSON...)}
	var parsed map[string]interface{}
	if err := json.Unmarshal(resultJSON, &parsed); err != nil {
		return nil, fmt.Errorf("claim result unmarshal: %w", err)
	}
	if id, ok := parsed["job_id"].(string); ok {
		out.JobID = id
	}
	if lease, ok := parsed["lease_id"].(string); ok {
		out.LeaseID = lease
	}
	switch a := parsed["attempt"].(type) {
	case float64:
		out.Attempt = int(a)
	case int:
		out.Attempt = a
	case int64:
		out.Attempt = int(a)
	}
	if leaseStr, ok := parsed["lease_expiry"].(string); ok && leaseStr != "" {
		if t, perr := time.Parse(time.RFC3339, leaseStr); perr == nil {
			out.LeaseExpires = t
		}
	}
	return out, nil
}

// Transition wraps *SQLiteStore.TransitionJobStatus with our typed signature
// and surfaces the spec's ErrTransitionConflict via errors.Is. The
// underlying store wraps ErrTransitionConflict with %w, so errors.Is works
// reliably even if the surrounding message text changes in the future
// (replaces a previous strings.Contains-based check that was brittle).
func (r *SQLiteJobRepository) Transition(ctx context.Context, t TransitionParams) error {
	if t.JobID == "" {
		return fmt.Errorf("job repository: empty jobID")
	}
	_, err := r.store.TransitionJobStatus(ctx, t.JobID, string(t.ExpectedStatus), string(t.NewStatus), t.Revision)
	if err != nil {
		if errors.Is(err, ErrTransitionConflict) {
			return ErrTransitionConflict
		}
		return fmt.Errorf("transition: %w", err)
	}
	return nil
}

// ListByStatus returns up to limit jobs in any of the supplied statuses.
//
// Empty statuses returns nil with no error (semantically: "no filter, no
// result"). limit <= 0 is treated as 1000.
func (r *SQLiteJobRepository) ListByStatus(ctx context.Context, statuses []JobStatus, limit int) ([]Job, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = 1000
	}
	placeholders := strings.Repeat(",?", len(statuses))[1:]
	args := make([]interface{}, len(statuses)+1)
	for i, s := range statuses {
		args[i] = string(s)
	}
	args[len(statuses)] = limit
	query := fmt.Sprintf(
		`SELECT %s FROM jobs WHERE UPPER(status) IN (%s) ORDER BY updated_at DESC LIMIT ?`,
		strings.Join(jobProjectionColumns, ","),
		placeholders,
	)
	rows, err := r.store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list by status: %w", err)
	}
	defer rows.Close()
	var jobs []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			continue
		}
		jobs = append(jobs, *j)
	}
	return jobs, rows.Err()
}

// RenewLease extends the lease on an active job atomically.
// Validates internally that the job is in LEASED, RUNNING, or PROCESSING
// status (the renewable states) via a single UPDATE with WHERE clause.
// Returns ErrTransitionConflict if no rows matched.
func (r *SQLiteJobRepository) RenewLease(ctx context.Context, params RenewLeaseParams) error {
	if params.JobID == "" {
		return fmt.Errorf("job repository: empty jobID in RenewLease")
	}
	if params.LeaseID == "" {
		return fmt.Errorf("job repository: empty leaseID in RenewLease")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	leaseExpiry := params.LeaseExpiry.UTC().Format(time.RFC3339)
	result, err := r.store.db.ExecContext(ctx,
		`UPDATE jobs
		 SET lease_id = ?,
		     lease_expiry = ?,
		     updated_at = ?,
		     revision = revision + 1,
		     attempt = CASE WHEN attempt = 0 THEN retry_count ELSE attempt END
		 WHERE job_id = ?
		   AND UPPER(status) IN ('LEASED', 'RUNNING')`,
		params.LeaseID, leaseExpiry, now, params.JobID,
	)
	if err != nil {
		return fmt.Errorf("renew lease exec: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("renew lease rows: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("renew lease %s: %w", params.JobID, ErrTransitionConflict)
	}
	return nil
}

// StartJob performs the atomic LEASED → RUNNING transition.
//
// All four identity fields (worker_id, lease_id, attempt, revision) AND the
// precondition status='LEASED' are evaluated in a single CAS UPDATE so a
// stale message from a zombie session cannot promote a job that is no longer
// his. The started_at timestamp is recorded and revision is bumped to make
// the state observable to subsequent Transition calls.
//
// Returns ErrTransitionConflict when no row matches — callers should reject
// the JobAccepted at the gRPC layer with a "stale lease" signal.
func (r *SQLiteJobRepository) StartJob(ctx context.Context, params StartJobParams) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("job repository: store not initialized")
	}
	if params.JobID == "" || params.WorkerID == "" || params.LeaseID == "" {
		return fmt.Errorf("job repository: StartJob requires jobID+workerID+leaseID")
	}
	if params.ExpectedRevision < 0 {
		params.ExpectedRevision = 0
	}
	if params.Now.IsZero() {
		params.Now = time.Now().UTC()
	}
	nowRFC := params.Now.Format(time.RFC3339)

	res, err := r.store.db.ExecContext(ctx,
		`UPDATE jobs
		 SET status         = 'RUNNING',
		     started_at     = COALESCE(started_at, ?),
		     updated_at     = ?,
		     revision       = revision + 1,
		     attempt        = CASE WHEN attempt = 0 THEN ? ELSE attempt END
		 WHERE job_id        = ?
		   AND assigned_to   = ?
		   AND lease_id      = ?
		   AND COALESCE(attempt, 0) = ?
		   AND revision      = ?
		   AND UPPER(status) = 'LEASED'`,
		nowRFC, nowRFC,
		params.Attempt,
		params.JobID, params.WorkerID, params.LeaseID,
		params.Attempt, params.ExpectedRevision,
	)
	if err != nil {
		return fmt.Errorf("start job exec: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("start job rows: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("start job %s: %w (worker=%s lease=%s attempt=%d rev=%d)",
			params.JobID, ErrTransitionConflict,
			params.WorkerID, params.LeaseID, params.Attempt, params.ExpectedRevision)
	}
	return nil
}

// Compile-time interface check.
var _ JobRepository = (*SQLiteJobRepository)(nil)
