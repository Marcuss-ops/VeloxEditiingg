package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"velox-server/internal/costmodel"
	"velox-server/internal/jobs"
)

// SQLiteJobRepository implements jobs.Repository against *SQLiteStore.
//
// It satisfies both jobs.Reader and jobs.Writer, making it the single
// canonical job persistence implementation (PR15.5: adapter inlined).
// The atomicity contract is enforced via either an internal BeginTx (for
// CreateJob / Transition / claimNext) or via delegation to *SQLiteStore
// methods that already own their own transactions (ClaimNextPendingJob).
type SQLiteJobRepository struct {
	store *SQLiteStore
}

// Compile-time assertion.
var _ jobs.Repository = (*SQLiteJobRepository)(nil)

// NewSQLiteJobRepository wraps a SQLiteStore as a jobs.Repository.
func NewSQLiteJobRepository(store *SQLiteStore) *SQLiteJobRepository {
	return &SQLiteJobRepository{store: store}
}

// NewJobsRepository returns the canonical jobs.Repository backed by the given
// SQLiteJobRepository. Since PR15.5 inlined the adapter, this is a convenience
// that returns its argument (now already a jobs.Repository).
func NewJobsRepository(repo *SQLiteJobRepository) jobs.Repository {
	return repo
}

// jobProjectionColumns lists the columns MaterializedBy JobRepository reads.
// Centralised so GetJob and ListByStatus stay in lock-step.
//
// PR-04.5: appends the two dedicated columns added by migration 039 so
// RequiredResourceClass + RequiredTemporalMode are surfaced in the
// DB-row projection. They're consumed by toJobsJob to reconstruct the
// canonical jobs.Job.Requirements.
var jobProjectionColumns = []string{
	"job_id",
	"status",
	"COALESCE(video_name, '')",
	"COALESCE(project_id, '')",
	"COALESCE(revision, 0)",
	"COALESCE(max_retries, 0)",
	"COALESCE(created_at, '')",
	"COALESCE(updated_at, '')",
	"COALESCE(started_at, '')",
	"COALESCE(completed_at, '')",
	"COALESCE(run_id, '')",
	"COALESCE(request_json, '{}')",
	"COALESCE(job_required_resource_class, '')",
	"COALESCE(job_required_temporal_mode, '')",
	"COALESCE(job_required_deterministic, 0)",
	"COALESCE(job_required_cacheable, 0)",
	"COALESCE(job_required_min_bandwidth_mbps, 0.0)",
}

// scanJob reads one row in jobProjectionColumns order into a *JobRecord.
func scanJob(row interface {
	Scan(...interface{}) error
}) (*JobRecord, error) {
	var j JobRecord
	err := row.Scan(
		&j.JobID, &j.Status, &j.VideoName, &j.ProjectID,
		&j.Revision, &j.MaxRetries,
		&j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.CompletedAt,
		&j.RunID, &j.PayloadJSON,
		&j.RequiredResourceClass, &j.RequiredTemporalMode,
		&j.RequiredDeterministic, &j.RequiredCacheable, &j.RequiredMinBandwidthMbps,
	)
	if err != nil {
		return nil, err
	}
	return &j, nil
}

// GetJob returns one job projection, or (nil, nil) if missing.
func (r *SQLiteJobRepository) GetJob(ctx context.Context, jobID string) (*JobRecord, error) {
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

// claimNext delegates to the well-tested ClaimNextPendingJob, which already
// owns its own transaction. Wrapping it preserves the spec §5 single-method
// atomicity contract while keeping the complex CAS update SQL in one place.
func (r *SQLiteJobRepository) claimNext(ctx context.Context, claim ClaimParams) (*ClaimResult, error) {
	if claim.WorkerID == "" {
		return nil, fmt.Errorf("job repository: claim with empty workerID")
	}
	now := claim.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	resultJSON, claimedReq, ok, err := r.store.ClaimNextPendingJob(claim.WorkerID, claim.AllowedJobTypes, now)
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	if !ok {
		return nil, ErrNoClaimableJob
	}

	out := &ClaimResult{ResultJSON: append([]byte(nil), resultJSON...), Requirements: claimedReq}
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
	// PR #6: Requirements returned from dedicated columns by the claim path.
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
func (r *SQLiteJobRepository) ListByStatus(ctx context.Context, statuses []JobStatus, limit int) ([]JobRecord, error) {
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
	var results []JobRecord
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			continue
		}
		results = append(results, *j)
	}
	return results, rows.Err()
}

// LeaseJob atomically leases a PENDING job to a worker.
// Updates status to LEASED, sets assigned_at, claimed_at, bumps revision.
// PR #9: lease_id, lease_expiry, assigned_to, claimed_by, retry_count columns
// dropped — lease identity flows through result_json + job_attempts.
func (r *SQLiteJobRepository) LeaseJob(ctx context.Context, jobID, workerID string) error {
	if jobID == "" {
		return fmt.Errorf("job repository: empty jobID in LeaseJob")
	}
	if workerID == "" {
		return fmt.Errorf("job repository: empty workerID in LeaseJob")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := r.store.db.ExecContext(ctx,
		`UPDATE jobs
		 SET status = 'LEASED',
		     assigned_at = ?,
		     claimed_at = ?,
		     updated_at = ?,
		     revision = revision + 1
		 WHERE job_id = ?
		   AND UPPER(status) = 'PENDING'`,
		now, now, now, jobID,
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
// without incrementing retry count. Clears assigned_at, claimed_at.
// PR #9: lease_id, lease_expiry, assigned_to, claimed_by columns dropped.
func (r *SQLiteJobRepository) ReleaseClaim(ctx context.Context, jobID string) error {
	if jobID == "" {
		return fmt.Errorf("job repository: empty jobID in ReleaseClaim")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := r.store.db.ExecContext(ctx,
		`UPDATE jobs
		 SET status = 'PENDING',
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

// StartJob performs the LEASED → RUNNING transition atomically.
//
// PR #9: assigned_to and lease_id columns dropped from jobs table.
// The single UPDATE verifies:
//   - job_id     (primary key)
//   - status     = 'LEASED' (only LEASED jobs can start)
//   - attempt    matches the caller's view (COALESCE(attempt, 0))
//   - revision   = ExpectedRevision (optimistic CAS)
//
// On success it bumps revision, sets started_at to Now (or time.Now if
// zero), and updates updated_at. The ErrTransitionConflict sentinel is
// returned for any predicate mismatch so callers can distinguish stale
// acceptances from infrastructure errors via errors.Is.
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
		   AND COALESCE(attempt, 0) = ?
		   AND revision = ?`,
		nowStr, nowStr, params.Attempt,
		params.JobID,
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
// FAILED | CANCELLED) atomically.
//
// PR #9: assigned_to, lease_id, lease_expiry, claimed_by columns dropped.
// On success it bumps revision, sets completed_at, writes result_json
// (storing empty blob if nil/empty), and clears assigned_at/claimed_at.
// Matches the existing pattern of returning ErrTransitionConflict on
// predicate mismatch.
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
		       assigned_at = '',
		       claimed_at = '',
		       attempt = ?
		 WHERE job_id = ?
		   AND UPPER(status) IN ('RUNNING', 'LEASED', 'RETRY_WAIT')
		   AND COALESCE(attempt, 0) = ?
		   AND revision = ?`,
		string(params.FinalStatus), nowStr, nowStr, resultJSON, params.Attempt,
		params.JobID,
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

// ── Mappers (jobs domain ↔ store row) ─────────────────────────────────────

// toJobsJob converts a store.JobRecord (DB projection) into a canonical jobs.Job.
//
// PR #6: Requirements are read from dedicated columns only; no JSON fallback.
// PR #7: WorkerID + LeaseID removed — tasks carry the per-execution state now.
// PR #9: RetryCount removed — job_attempts count is authoritative.
func toJobsJob(sj *JobRecord) *jobs.Job {
	if sj == nil {
		return nil
	}
	createdAt, _ := time.Parse(time.RFC3339, sj.CreatedAt)
	updatedAt, _ := time.Parse(time.RFC3339, sj.UpdatedAt)
	startedAt, _ := time.Parse(time.RFC3339, sj.StartedAt)
	completedAt, _ := time.Parse(time.RFC3339, sj.CompletedAt)
	return &jobs.Job{
		ID:          sj.JobID,
		Type:        "",
		Status:      jobs.Status(sj.Status),
		Attempts:    0,
		Revision:    sj.Revision,
		VideoName:   sj.VideoName,
		ProjectID:   sj.ProjectID,
		RunID:       sj.RunID,
		MaxRetries:  sj.MaxRetries,
		StartedAt:   startedAt,
		CompletedAt: completedAt,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
		Payload:     sj.PayloadJSON,
		Requirements: costmodel.JobRequirements{
			ResourceClass:    costmodel.ResourceClass(strings.TrimSpace(sj.RequiredResourceClass)),
			TemporalMode:     costmodel.TemporalMode(strings.TrimSpace(sj.RequiredTemporalMode)),
			Deterministic:    sj.RequiredDeterministic,
			Cacheable:        sj.RequiredCacheable,
			MinBandwidthMbps: sj.RequiredMinBandwidthMbps,
		},
	}
}

// boolToInt converts a bool to an int for SQLite (0/1).
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func toJobsFilter(f jobs.Filter) ([]JobStatus, int) {
	statuses := make([]JobStatus, len(f.Statuses))
	for i, s := range f.Statuses {
		statuses[i] = JobStatus(s)
	}
	return statuses, f.Limit
}

func toJobsCounts(raw map[string]int64) jobs.Counts {
	if raw == nil {
		return nil
	}
	out := make(jobs.Counts, len(raw))
	for k, v := range raw {
		out[jobs.Status(k)] = v
	}
	return out
}

// ── jobs.Reader ────────────────────────────────────────────────────────────

// Get returns a single job by ID in the canonical domain model, or (nil, nil) on missing.
func (r *SQLiteJobRepository) Get(ctx context.Context, id string) (*jobs.Job, error) {
	sj, err := r.GetJob(ctx, id)
	if err != nil {
		return nil, err
	}
	return toJobsJob(sj), nil
}

// List returns jobs matching the filter as canonical domain model objects.
func (r *SQLiteJobRepository) List(ctx context.Context, filter jobs.Filter) ([]jobs.Job, error) {
	statuses, limit := toJobsFilter(filter)
	storeJobs, err := r.ListByStatus(ctx, statuses, limit)
	if err != nil {
		return nil, err
	}
	out := make([]jobs.Job, 0, len(storeJobs))
	for _, sj := range storeJobs {
		if jj := toJobsJob(&sj); jj != nil {
			out = append(out, *jj)
		}
	}
	return out, nil
}

// Counts returns the aggregate count of jobs grouped by status.
func (r *SQLiteJobRepository) Counts(ctx context.Context) (jobs.Counts, error) {
	if r.store == nil {
		return nil, fmt.Errorf("job repository: store not initialized")
	}
	raw, err := r.store.JobCounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("counts: %w", err)
	}
	return toJobsCounts(raw), nil
}

// ── jobs.Writer ────────────────────────────────────────────────────────────

// SetStatus performs a CAS status change from → to. Returns ErrTransitionConflict
// if the precondition does not hold.
func (r *SQLiteJobRepository) SetStatus(ctx context.Context, id string, from, to jobs.Status) error {
	sj, err := r.GetJob(ctx, id)
	if err != nil {
		return fmt.Errorf("setstatus: get job %s: %w", id, err)
	}
	if sj == nil {
		return fmt.Errorf("setstatus: job %s not found", id)
	}
	return r.Transition(ctx, TransitionParams{
		JobID:          id,
		ExpectedStatus: JobStatus(from),
		NewStatus:      JobStatus(to),
		Revision:       sj.Revision,
	})
}

// Lease atomically assigns a PENDING job to a worker.
func (r *SQLiteJobRepository) Lease(ctx context.Context, id, workerID string) error {
	return r.LeaseJob(ctx, id, workerID)
}

// Fail marks a job FAILED and records the reason.
func (r *SQLiteJobRepository) Fail(ctx context.Context, id, reason string) error {
	sj, err := r.GetJob(ctx, id)
	if err != nil {
		return fmt.Errorf("fail: get job %s: %w", id, err)
	}
	if sj == nil {
		return fmt.Errorf("fail: job %s not found", id)
	}
	if sj.Status.IsTerminal() {
		return fmt.Errorf("fail: job %s is already terminal (%s)", id, sj.Status)
	}
	log.Printf("[JOBS] failing job %s: reason=%q", id, reason)
	return r.Transition(ctx, TransitionParams{
		JobID:          id,
		ExpectedStatus: sj.Status,
		NewStatus:      JobStatusFailed,
		Revision:       sj.Revision,
	})
}

// Start atomically transitions LEASED → RUNNING with full CAS tuple + history + event.
func (r *SQLiteJobRepository) Start(ctx context.Context, id, workerID, leaseID string, attempt, revision int) error {
	return r.PR3Start(ctx, StartCommand{
		JobID:            id,
		WorkerID:         workerID,
		LeaseID:          leaseID,
		Attempt:          attempt,
		ExpectedRevision: revision,
	})
}

// RenewLease extends the lease on an active job with CAS tuple + optional event.
func (r *SQLiteJobRepository) RenewLease(ctx context.Context, id, workerID, leaseID string, expiry time.Time, emitEvent bool, revision int) error {
	return r.PR3RenewLease(ctx, RenewLeaseCommand{
		JobID:            id,
		WorkerID:         workerID,
		LeaseID:          leaseID,
		LeaseExpiry:      expiry,
		EmitEvent:        emitEvent,
		ExpectedRevision: revision,
	})
}

// FailWithRetry marks a job FAILED or RETRY_WAIT depending on retry budget.
func (r *SQLiteJobRepository) FailWithRetry(ctx context.Context, id, errorCode, errorMessage string, retryable bool, revision int) error {
	return r.PR3Fail(ctx, FailCommand{
		JobID:            id,
		ErrorCode:        errorCode,
		ErrorMessage:     errorMessage,
		Retryable:        retryable,
		ExpectedRevision: revision,
	})
}

// Cancel transitions a job to CANCELLED. Idempotent on terminal states.
func (r *SQLiteJobRepository) Cancel(ctx context.Context, id, reason string, revision int) error {
	return r.PR3Cancel(ctx, CancelCommand{
		JobID:            id,
		Reason:           reason,
		ExpectedRevision: revision,
	})
}

// RequeueExpiredLeases processes expired leases, returning PENDING or FAILED per job.
func (r *SQLiteJobRepository) RequeueExpiredLeases(ctx context.Context, now time.Time, limit int) ([]jobs.RequeueResult, error) {
	results, err := r.PR3RequeueExpiredLeases(ctx, now, limit)
	if err != nil {
		return nil, err
	}
	out := make([]jobs.RequeueResult, len(results))
	for i, r := range results {
		out[i] = jobs.RequeueResult{
			JobID:          r.JobID,
			PreviousStatus: jobs.Status(r.PreviousStatus),
			NewStatus:      jobs.Status(r.NewStatus),
			Reason:         r.Reason,
			Attempt:        r.Attempt,
		}
	}
	return out, nil
}

// ClaimNext atomically claims the next PENDING job for a worker.
func (r *SQLiteJobRepository) ClaimNext(ctx context.Context, workerID string, allowedJobTypes []string) (*jobs.ClaimNextResult, error) {
	result, err := r.claimNext(ctx, ClaimParams{
		WorkerID:        workerID,
		AllowedJobTypes: allowedJobTypes,
	})
	if err != nil {
		if errors.Is(err, ErrNoClaimableJob) {
			return nil, err
		}
		return nil, fmt.Errorf("claim next: %w", err)
	}
	return &jobs.ClaimNextResult{
		JobID:        result.JobID,
		Attempt:      result.Attempt,
		LeaseID:      result.LeaseID,
		LeaseExpires: result.LeaseExpires,
		Requirements: result.Requirements,
	}, nil
}

// ClaimNextForProfile is the cost-rank sibling of ClaimNext
// (PR-04.6). Instead of FIFO across PENDING jobs, it loads up to
// maxCandidates PENDING jobs whose job_type matches
// allowedJobTypes, scores each against the supplied
// costmodel.WorkerProfile using costmodel.Score, filters
// Eligible=true, and CAS-claims the lowest-scored (best-fit)
// candidate. Race-safe: if the CAS fails for the top-scored
// candidate (another worker raced the row), the runner-up is
// tried in Score-sorted order.
//
// Returns ErrNoClaimableJob when nothing is eligible OR every CAS
// attempt failed. maxCandidates is clamped to [1, 100]; default 20.
func (r *SQLiteJobRepository) ClaimNextForProfile(
	ctx context.Context,
	workerID string,
	allowedJobTypes []string,
	profile costmodel.WorkerProfile,
	maxCandidates int,
) (*jobs.ClaimNextResult, error) {
	resultJSON, claimedReq, ok, err := r.store.ClaimNextPendingJobForWorker(ctx, workerID, allowedJobTypes, profile, maxCandidates, time.Time{})
	if err != nil {
		if errors.Is(err, ErrNoClaimableJob) {
			return nil, err
		}
		return nil, fmt.Errorf("claim next for profile: %w", err)
	}
	if !ok {
		return nil, ErrNoClaimableJob
	}
	// Parse result_json (the rank path emits the same shape as the
	// FIFO path's ClaimNextPendingJob so this parser is byte-for-byte
	// equivalent to the lowercase `claimNext` parsing block).
	out := &jobs.ClaimNextResult{Requirements: claimedReq}
	var parsed map[string]interface{}
	if err := json.Unmarshal(resultJSON, &parsed); err == nil {
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
		// PR #6: Requirements populated from dedicated columns by the claim path.
	}
	return out, nil
}

// ReleaseLease resets a LEASED/RUNNING job back to PENDING without retry increment.
func (r *SQLiteJobRepository) ReleaseLease(ctx context.Context, id string) error {
	return r.ReleaseClaim(ctx, id)
}

// RecordRenderFinished verifies the worker-identity tuple, marks attempt as
// RENDER_FINISHED, inserts event. Job stays RUNNING.
func (r *SQLiteJobRepository) RecordRenderFinished(ctx context.Context, id, workerID, leaseID string, attempt, revision int) error {
	return r.PR3RecordRenderFinished(ctx, RecordRenderFinishedCommand{
		JobID:            id,
		WorkerID:         workerID,
		LeaseID:          leaseID,
		AttemptNumber:    attempt,
		ExpectedRevision: revision,
	})
}

// Delete hard-deletes a job and its supplementary rows from persistence.
func (r *SQLiteJobRepository) Delete(ctx context.Context, id string) error {
	if r.store == nil {
		return fmt.Errorf("job repository: store not initialized")
	}
	return r.store.DeleteJob(id)
}
