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

	"github.com/google/uuid"

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
	"COALESCE(request_json, '{}')",
	"COALESCE(job_required_resource_class, '')",
	"COALESCE(job_required_temporal_mode, '')",
}

// scanJob reads one row in jobProjectionColumns order into a *JobRecord.
func scanJob(row interface {
	Scan(...interface{}) error
}) (*JobRecord, error) {
	var j JobRecord
	err := row.Scan(
		&j.JobID, &j.Status, &j.VideoName, &j.ProjectID,
		&j.AssignedTo, &j.LeaseID,
		&j.Revision, &j.RetryCount, &j.MaxRetries,
		&j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.CompletedAt,
		&j.RunID, &j.PayloadJSON,
		&j.RequiredResourceClass, &j.RequiredTemporalMode,
	)
	if err != nil {
		return nil, err
	}
	return &j, nil
}

// requirementsJSONKey is the JSON-only sub-object key inside request_json
// where the per-job requirements are mirrored. PR-04.5 keeps the redundant
// copy so legacy workers that introspect request_json continue to find
// Requirements under the well-known key.
const requirementsJSONKey = "_requirements"

// attachRequirementsToPayload mutates payload in-place by setting (or
// overwriting) the `_requirements` sub-object. The resulting map is
// always serializable. Empty (default) requirements are still embedded
// so the on-disk shape is stable across writers.
func attachRequirementsToPayload(payload map[string]interface{}, req costmodel.JobRequirements) map[string]interface{} {
	if payload == nil {
		payload = make(map[string]interface{})
	}
	payload[requirementsJSONKey] = map[string]interface{}{
		"resource_class": string(req.ResourceClass),
		"temporal_mode":  string(req.TemporalMode),
		"deterministic":  req.Deterministic,
		"cacheable":      req.Cacheable,
	}
	return payload
}

// requirementsFromPayload reads the `_requirements` sub-object out of a
// parsed payload map. Used by toJobsJob as a fallback when the dedicated
// columns are blank (pre-PR-04.5 rows).
func requirementsFromPayload(payload map[string]interface{}) costmodel.JobRequirements {
	if payload == nil {
		return costmodel.DefaultRequirements()
	}
	raw, ok := payload[requirementsJSONKey].(map[string]interface{})
	if !ok || raw == nil {
		return costmodel.DefaultRequirements()
	}
	req := costmodel.DefaultRequirements()
	if v, ok := raw["resource_class"].(string); ok {
		req.ResourceClass = costmodel.ResourceClass(strings.TrimSpace(v))
	}
	if v, ok := raw["temporal_mode"].(string); ok {
		req.TemporalMode = costmodel.TemporalMode(strings.TrimSpace(v))
	}
	if v, ok := raw["deterministic"].(bool); ok {
		req.Deterministic = v
	}
	if v, ok := raw["cacheable"].(bool); ok {
		req.Cacheable = v
	}
	return req
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
	// PR-04.5: embed the per-job Requirements twice \u2014 once as a
	// `_requirements` sub-object inside the request_json blob (legacy
	// readers can introspect this without a schema change), and once on
	// the dedicated columns added by migration 039 (canonical,
	// query-time filterable). The two writes are independent so a row
	// with pre-PR-04.5 schema (no columns yet) still gets the in-blob
	// copy \u2014 the get path reads columns first, falls back to JSON.
	payloadWithReq := attachRequirementsToPayload(params.Payload, params.Requirements)
	requestJSON := "{}"
	if len(payloadWithReq) > 0 {
		if b, err := json.Marshal(payloadWithReq); err == nil {
			requestJSON = string(b)
		}
	}

	res, err := tx.ExecContext(ctx,
		`INSERT INTO jobs (
			job_id, status, max_retries, retry_count,
			video_name, project_id,
			created_at, updated_at, migrated_at,
			request_json, result_json, revision,
			run_id, job_run_id,
			job_required_resource_class, job_required_temporal_mode
		) VALUES (?, 'PENDING', ?, 0, ?, ?, ?, ?, ?, ?, '{}', 0, ?, ?, ?, ?)`,
		params.JobID, params.MaxRetries, params.VideoName, params.ProjectID,
		now, now, now,
		requestJSON,
		params.RunID, params.RunID,
		string(params.Requirements.ResourceClass),
		string(params.Requirements.TemporalMode),
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

// ── Mappers (jobs domain ↔ store row) ─────────────────────────────────────

// toJobsJob converts a store.JobRecord (DB projection) into a canonical jobs.Job.
//
// PR-04.5: Requirements is reconstructed from the dedicated columns
// (added by migration 039) first, with a fallback to the
// `_requirements` sub-object inside request_json. Pre-PR-04.5 rows
// (no columns, no JSON sub-object) fold into JobRequirements{} =
// DefaultRequirements = permissive, preserving legacy routing.
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
		Status:      jobs.Status(sj.Status),
		VideoName:   sj.VideoName,
		ProjectID:   sj.ProjectID,
		RunID:       sj.RunID,
		Attempts:    sj.RetryCount,
		Revision:    sj.Revision,
		WorkerID:    sj.AssignedTo,
		MaxRetries:  sj.MaxRetries,
		LeaseID:     sj.LeaseID,
		StartedAt:   startedAt,
		CompletedAt: completedAt,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
		Payload:     sj.PayloadJSON,
		Requirements: reconstructRequirements(sj),
	}
}

// reconstructRequirements reads the per-job Requirements from the
// dedicated columns first; falls back to the JSON sub-object inside
// request_json when the columns are blank (pre-PR-04.5 rows); defaults
// to JobRequirements{} when both are blank (= DefaultRequirements = permissive).
func reconstructRequirements(sj *JobRecord) costmodel.JobRequirements {
	if sj == nil {
		return costmodel.DefaultRequirements()
	}
	rc := strings.TrimSpace(sj.RequiredResourceClass)
	tm := strings.TrimSpace(sj.RequiredTemporalMode)
	if rc == "" && tm == "" {
		// Fallback to JSON sub-object (legacy rows).
		parsed := jobs.ParsePayloadJSON(sj.PayloadJSON)
		jsonReq := requirementsFromPayload(parsed)
		if jsonReq.ResourceClass != "" || jsonReq.TemporalMode != "" || jsonReq.Deterministic || jsonReq.Cacheable {
			return jsonReq
		}
		return costmodel.DefaultRequirements()
	}
	// Columns present: authoritative for resource_class and temporal_mode.
	// Deterministic + Cacheable are JSON-only \u2014 pick them up from the blob
	// to fill the rank-side two booleans (PR-04.6 consumes them).
	parsed := jobs.ParsePayloadJSON(sj.PayloadJSON)
	jsonReq := requirementsFromPayload(parsed)
	return costmodel.JobRequirements{
		ResourceClass: costmodel.ResourceClass(rc),
		TemporalMode:  costmodel.TemporalMode(tm),
		Deterministic: jsonReq.Deterministic,
		Cacheable:     jsonReq.Cacheable,
	}
}

func toStoreParams(j *jobs.Job) CreateJobParams {
	if j == nil {
		return CreateJobParams{}
	}
	return CreateJobParams{
		JobID:       j.ID,
		VideoName:   j.VideoName,
		ProjectID:   j.ProjectID,
		RunID:       j.RunID,
		MaxRetries:  j.MaxRetries,
		Requirements: j.Requirements,
	}
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

// Create inserts a new job in PENDING state. If job.ID is empty the repository assigns one.
func (r *SQLiteJobRepository) Create(ctx context.Context, job *jobs.Job) error {
	if job == nil {
		return fmt.Errorf("job repository: nil job")
	}
	return r.CreateJob(ctx, toStoreParams(job))
}

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
	}, nil
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
