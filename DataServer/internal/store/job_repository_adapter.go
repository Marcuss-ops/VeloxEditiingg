package store

import (
	"context"
	"fmt"
	"log"
	"time"

	"velox-server/internal/jobs"
)

// ── Mappers ────────────────────────────────────────────────────────────────

// toJobsJob converts a store.JobRecord (DB projection) into a canonical jobs.Job.
func toJobsJob(sj *Job) *jobs.Job {
	if sj == nil {
		return nil
	}
	createdAt, _ := time.Parse(time.RFC3339, sj.CreatedAt)
	updatedAt, _ := time.Parse(time.RFC3339, sj.UpdatedAt)
	startedAt, _ := time.Parse(time.RFC3339, sj.StartedAt)
	completedAt, _ := time.Parse(time.RFC3339, sj.CompletedAt)
	return &jobs.Job{
		ID:          sj.JobID,
		Status:      jobs.Status(sj.Status), // type alias, zero-cost
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
		Payload:     sj.PayloadJSON, // opaque persistence blob; typed fields above are authoritative
	}
}

// toStoreParams converts a jobs.Job into a CreateJobParams for persistence.
func toStoreParams(j *jobs.Job) CreateJobParams {
	if j == nil {
		return CreateJobParams{}
	}
	return CreateJobParams{
		JobID:      j.ID,
		VideoName:  j.VideoName,
		ProjectID:  j.ProjectID,
		RunID:      j.RunID,
		MaxRetries: j.MaxRetries,
	}
}

// toJobsFilter converts a jobs.Filter into the (statuses, limit) pair
// expected by ListByStatus.
func toJobsFilter(f jobs.Filter) ([]JobStatus, int) {
	statuses := make([]JobStatus, len(f.Statuses))
	for i, s := range f.Statuses {
		statuses[i] = JobStatus(s) // type alias, zero-cost
	}
	return statuses, f.Limit
}

// toJobsCounts converts a map[string]int64 (from SQLiteStore.JobCounts)
// into canonical jobs.Counts.
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

// ── jobs.Reader implementation ─────────────────────────────────────────────

// Get returns a single job by ID, or (nil, nil) on missing.
// Implements jobs.Reader.
func (r *SQLiteJobRepository) Get(ctx context.Context, id string) (*jobs.Job, error) {
	sj, err := r.GetJob(ctx, id)
	if err != nil {
		return nil, err
	}
	return toJobsJob(sj), nil
}

// List returns jobs matching the filter.
// Implements jobs.Reader.
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
// Implements jobs.Reader.
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

// ── jobs.Writer implementation ─────────────────────────────────────────────

// Create inserts a new job in PENDING state.
// Implements jobs.Writer.
func (r *SQLiteJobRepository) Create(ctx context.Context, job *jobs.Job) error {
	if job == nil {
		return fmt.Errorf("job repository: nil job")
	}
	params := toStoreParams(job)
	return r.CreateJob(ctx, params)
}

// SetStatus performs a CAS status change from → to.
// Reads the current job first to obtain the revision counter, then passes
// it through to the underlying CAS transition so the optimistic lock
// catches concurrent mutations.
// Implements jobs.Writer.
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
// Implements jobs.Writer.
func (r *SQLiteJobRepository) Lease(ctx context.Context, id, workerID string) error {
	return r.LeaseJob(ctx, id, workerID)
}

// Fail marks a job FAILED and records the reason.
// Reads the current status and revision first, then performs a CAS
// transition to FAILED with the current revision as the optimistic lock.
// The reason is logged for operator visibility; TODO(PR 3): persist to
// result_json or job_events.
// Implements jobs.Writer.
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

// ── Compile-time assertion ─────────────────────────────────────────────────

// SQLiteJobRepository now satisfies the canonical jobs.Repository interface.
// This is the single concrete implementation; any future backend (Postgres)
// must also satisfy this interface.
var _ jobs.Repository = (*SQLiteJobRepository)(nil)
