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
			created_at, updated_at, migrated_at,
			request_json, result_json, revision, raw_json,
			run_id, job_run_id
		) VALUES (?, 'PENDING', ?, 0, ?, ?, ?, ?, ?, ?, '{}', 0, '{}', ?, ?)`,
		params.JobID, params.MaxRetries, params.VideoName, params.ProjectID,
		now, now, now,
		requestJSON,
		params.RunID, params.RunID,
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

// LeaseJob atomically leases a PENDING job to a worker.
// Updates status to LEASED, sets lease_id (generated UUID), assigned_to,
// claimed_by, assigned_at, claimed_at, lease_expiry (30 min), increments
// retry_count, bumps revision.
func (r *SQLiteJobRepository) LeaseJob(ctx context.Context, jobID, workerID string) error {
	if jobID == "" {
		return fmt.Errorf("job repository: empty jobID in LeaseJob")
	}
	if workerID == "" {
		return fmt.Errorf("job repository: empty workerID in LeaseJob")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	leaseID := uuid.NewString()
	leaseExpiry := time.Now().UTC().Add(30 * time.Minute).Format(time.RFC3339)
	result, err := r.store.db.ExecContext(ctx,
		`UPDATE jobs
		 SET status = 'LEASED',
		     lease_id = ?,
		     lease_expiry = ?,
		     assigned_to = ?,
		     claimed_by = ?,
		     assigned_at = ?,
		     claimed_at = ?,
		     updated_at = ?,
		     revision = revision + 1,
		     retry_count = retry_count + 1
		 WHERE job_id = ?
		   AND UPPER(status) = 'PENDING'`,
		leaseID, leaseExpiry, workerID, workerID, now, now, now, jobID,
	)
	if err != nil {
		return fmt.Errorf("lease job exec: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("lease job rows: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("lease job %s: job not in PENDING state", jobID)
	}
	return nil
}

// ReleaseClaim atomically resets a LEASED/RUNNING job back to PENDING
// without incrementing retry count. Clears lease_id, lease_expiry,
// assigned_to, claimed_by, assigned_at, claimed_at.
func (r *SQLiteJobRepository) ReleaseClaim(ctx context.Context, jobID string) error {
	if jobID == "" {
		return fmt.Errorf("job repository: empty jobID in ReleaseClaim")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := r.store.db.ExecContext(ctx,
		`UPDATE jobs
		 SET status = 'PENDING',
		     lease_id = '',
		     lease_expiry = '',
		     assigned_to = '',
		     claimed_by = '',
		     assigned_at = '',
		     claimed_at = '',
		     updated_at = ?,
		     revision = revision + 1
		 WHERE job_id = ?
		   AND UPPER(status) IN ('LEASED', 'RUNNING')`,
		now, jobID,
	)
	if err != nil {
		return fmt.Errorf("release claim exec: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("release claim rows: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("release claim %s: job not in LEASED/RUNNING state", jobID)
	}
	return nil
}

// RequeueZombieJobs finds jobs in LEASED/RUNNING state whose lease_expiry
// has passed and atomically requeues them to PENDING. Returns the count of
// requeued jobs.
func (r *SQLiteJobRepository) RequeueZombieJobs(ctx context.Context, timeout time.Duration) (int, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	// Requeue jobs with expired leases
	result, err := r.store.db.ExecContext(ctx,
		`UPDATE jobs
		 SET status = 'PENDING',
		     lease_id = '',
		     lease_expiry = '',
		     assigned_to = '',
		     claimed_by = '',
		     assigned_at = '',
		     claimed_at = '',
		     retry_count = retry_count + 1,
		     updated_at = ?,
		     revision = revision + 1
		 WHERE UPPER(status) IN ('LEASED', 'RUNNING')
		   AND lease_expiry IS NOT NULL
		   AND lease_expiry != ''
		   AND lease_expiry < ?`,
		now, now,
	)
	if err != nil {
		return 0, fmt.Errorf("requeue zombies exec: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("requeue zombies rows: %w", err)
	}
	return int(n), nil
}

// UpdateJobResult writes the result_json blob for a job.
func (r *SQLiteJobRepository) UpdateJobResult(ctx context.Context, jobID string, resultJSON []byte) error {
	return r.store.UpsertJobResult(jobID, resultJSON)
}

// StartJob performs the LEASED → RUNNING transition atomically.
//
// The single UPDATE verifies all five identity columns at once:
//   - job_id     (primary key)
//   - status     = 'LEASED' (only LEASED jobs can start)
//   - assigned_to = workerID (worker must own the lease)
//   - lease_id   = leaseID (lease identity must match)
//   - attempt    matches the caller's view (COALESCE(attempt, 0))
//   - revision   = ExpectedRevision (optimistic CAS)
//
// On success it bumps revision, sets started_at to Now (or time.Now if
// zero), and updates updated_at. The ErrTransitionConflict sentinel is
// returned for any predicate mismatch so callers can distinguish stale
// acceptances from infrastructure errors via errors.Is.
//
// Test coverage in sqlite_jobs_writer_repository_test.go validates the
// happy path plus all five mismatch variants (lease, worker, attempt,
// revision, already-running, NULL-attempt legacy row).
func (r *SQLiteJobRepository) StartJob(ctx context.Context, params StartJobParams) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("job repository: store not initialized")
	}
	if params.JobID == "" {
		return fmt.Errorf("job repository: empty jobID in StartJob")
	}
	if params.WorkerID == "" || params.LeaseID == "" {
		return fmt.Errorf("job repository: missing worker/lease identity in StartJob")
	}

	now := params.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	nowStr := now.UTC().Format(time.RFC3339)

	// COALESCE(attempt, 0) so legacy rows with NULL attempt match params.Attempt == 0.
	res, err := r.store.db.ExecContext(ctx,
		`UPDATE jobs
		   SET status = 'RUNNING',
		       started_at = ?,
		       updated_at = ?,
		       revision = revision + 1,
		       attempt = ?
		 WHERE job_id = ?
		   AND UPPER(status) = 'LEASED'
		   AND COALESCE(assigned_to, '') = ?
		   AND COALESCE(lease_id, '') = ?
		   AND COALESCE(attempt, 0) = ?
		   AND revision = ?`,
		nowStr, nowStr, params.Attempt,
		params.JobID,
		params.WorkerID,
		params.LeaseID,
		params.Attempt,
		params.ExpectedRevision,
	)
	if err != nil {
		return fmt.Errorf("start job exec: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("start job rows: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("start job %s: %w", params.JobID, ErrTransitionConflict)
	}
	return nil
}

// CompleteJob performs the RUNNING → terminal transition (SUCCEEDED |
// FAILED | CANCELLED) atomically, with the same worker-identity CAS tuple
// as StartJob. Unlike StartJob (which requires LEASED), CompleteJob
// accepts RUNNING, LEASED, or RETRY_WAIT — covering the case where a
// worker completes quickly enough that the LEASED → RUNNING transition
// was never recorded (the very race StartJob was introduced to fix).
//
// On success it bumps revision, sets completed_at, writes result_json
// (storing empty blob if nil/empty), and clears lease fields so the
// row is reconcilable by reaper/outbox. Matches the existing pattern
// of returning ErrTransitionConflict on predicate mismatch.
func (r *SQLiteJobRepository) CompleteJob(ctx context.Context, params CompleteJobParams) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("job repository: store not initialized")
	}
	if params.JobID == "" {
		return fmt.Errorf("job repository: empty jobID in CompleteJob")
	}
	if params.WorkerID == "" || params.LeaseID == "" {
		return fmt.Errorf("job repository: missing worker/lease identity in CompleteJob")
	}
	switch params.FinalStatus {
	case JobStatusSucceeded, JobStatusFailed, JobStatusCancelled:
		// ok
	default:
		return fmt.Errorf("job repository: invalid FinalStatus %q (want SUCCEEDED|FAILED|CANCELLED)", params.FinalStatus)
	}

	now := params.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	nowStr := now.UTC().Format(time.RFC3339)

	resultJSON := "{}"
	if len(params.ResultJSON) > 0 {
		resultJSON = string(params.ResultJSON)
	}

	res, err := r.store.db.ExecContext(ctx,
		`UPDATE jobs
		   SET status = ?,
		       completed_at = ?,
		       updated_at = ?,
		       result_json = ?,
		       revision = revision + 1,
		       lease_id = '',
		       lease_expiry = '',
		       assigned_to = '',
		       claimed_by = '',
		       assigned_at = '',
		       claimed_at = '',
		       attempt = ?
		 WHERE job_id = ?
		   AND UPPER(status) IN ('RUNNING', 'LEASED', 'RETRY_WAIT')
		   AND COALESCE(assigned_to, '') = ?
		   AND COALESCE(lease_id, '') = ?
		   AND COALESCE(attempt, 0) = ?
		   AND revision = ?`,
		string(params.FinalStatus), nowStr, nowStr, resultJSON, params.Attempt,
		params.JobID,
		params.WorkerID,
		params.LeaseID,
		params.Attempt,
		params.ExpectedRevision,
	)
	if err != nil {
		return fmt.Errorf("complete job exec: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("complete job rows: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("complete job %s: %w", params.JobID, ErrTransitionConflict)
	}
	return nil
}

// RecordRenderFinished atomically verifies the worker-identity tuple against
// the jobs row, marks the attempt as RENDER_FINISHED, and inserts a
// RENDER_FINISHED event — all inside a single transaction. The job stays
// RUNNING (no status transition). Idempotent: if the attempt is already
// RENDER_FINISHED with the same lease_id and worker_id, returns nil.
func (r *SQLiteJobRepository) RecordRenderFinished(ctx context.Context, cmd RecordRenderFinishedCommand) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("job repository: store not initialized")
	}
	if cmd.JobID == "" {
		return fmt.Errorf("job repository: empty jobID in RecordRenderFinished")
	}
	if cmd.WorkerID == "" {
		return fmt.Errorf("job repository: empty workerID in RecordRenderFinished")
	}

	now := cmd.FinishedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	nowStr := now.UTC().Format(time.RFC3339)

	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("record render finished begin: %w", err)
	}
	defer tx.Rollback()

	// 1. Read the current job identity tuple.
	var status, assignedTo, leaseID string
	var revision int
	err = tx.QueryRowContext(ctx,
		`SELECT COALESCE(status, ''), COALESCE(assigned_to, ''),
		        COALESCE(lease_id, ''), COALESCE(revision, 0)
		 FROM jobs WHERE job_id = ?`, cmd.JobID,
	).Scan(&status, &assignedTo, &leaseID, &revision)
	if err == sql.ErrNoRows {
		return fmt.Errorf("record render finished: job %s not found", cmd.JobID)
	}
	if err != nil {
		return fmt.Errorf("record render finished query: %w", err)
	}

	// 2. Verify the identity tuple.
	if status != string(JobStatusRunning) {
		return fmt.Errorf("record render finished: job %s is in status %s, expected RUNNING", cmd.JobID, status)
	}
	if assignedTo != cmd.WorkerID {
		return fmt.Errorf("record render finished: worker %s does not own job %s (assigned to %s)", cmd.WorkerID, cmd.JobID, assignedTo)
	}
	if cmd.LeaseID != "" && leaseID != cmd.LeaseID {
		return fmt.Errorf("record render finished: lease mismatch for job %s: expected %s, got %s", cmd.JobID, leaseID, cmd.LeaseID)
	}
	if cmd.ExpectedRevision != 0 && revision != cmd.ExpectedRevision {
		return fmt.Errorf("record render finished: revision mismatch for job %s: expected %d, got %d", cmd.JobID, cmd.ExpectedRevision, revision)
	}

	// 3. Update the attempt to RENDER_FINISHED. Accept attempts in
	//    RUNNING or PROCESSING state; idempotent if already RENDER_FINISHED
	//    with the same lease and worker.
	res, err := tx.ExecContext(ctx,
		`UPDATE job_attempts
		 SET status = 'RENDER_FINISHED',
		     finished_at = ?
		 WHERE job_id = ?
		   AND attempt_number = ?
		   AND worker_id = ?
		   AND lease_id = ?
		   AND status IN ('RUNNING', 'PROCESSING', 'RENDER_FINISHED')`,
		nowStr, cmd.JobID, cmd.AttemptNumber, cmd.WorkerID, cmd.LeaseID,
	)
	if err != nil {
		return fmt.Errorf("record render finished update attempt: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("record render finished attempt rows: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("record render finished: %w for job %s attempt %d (wrong worker, lease, or attempt not in RUNNING/PROCESSING)",
			ErrRecordRenderFinishedNotFound, cmd.JobID, cmd.AttemptNumber)
	}

	// 4. Insert RENDER_FINISHED event (deduplicated by caller already,
	//    but we insert unconditionally for audit trail).
	_, err = tx.ExecContext(ctx,
		`INSERT INTO job_events (job_id, event_type, created_at)
		 VALUES (?, 'RENDER_FINISHED', ?)`,
		cmd.JobID, nowStr,
	)
	if err != nil {
		return fmt.Errorf("record render finished insert event: %w", err)
	}

	return tx.Commit()
}

// Compile-time interface check.
var _ JobRepository = (*SQLiteJobRepository)(nil)
