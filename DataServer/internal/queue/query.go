// Package queue provides job queue management with SQLite persistence.
// QueryService (Batch 3) is reduced to a thin wrapper over the canonical
// jobs.Reader: the legacy map-based eventStore surface has been dropped, and
// the four methods that depended on it (GetNextJobID, DeleteJob,
// UpdateJobLogs, CleanupOldJobs) have been deleted outright.
package queue

import (
	"context"
	"fmt"

	"velox-server/internal/jobs"
)

// QueryService provides read-only access to job data via the canonical
// jobs.Reader interface. All eventStore-coupled reads have been dropped
// (Batch 3). Write/mutation operations live on LifecycleService.
type QueryService struct {
	reader jobs.Reader
}

// NewQueryService constructs a QueryService backed by the canonical
// jobs.Reader. The reader is mandatory.
func NewQueryService(reader jobs.Reader) *QueryService {
	return &QueryService{reader: reader}
}

// GetJob retrieves a job by ID via the canonical domain reader.
func (q *QueryService) GetJob(ctx context.Context, jobID string) (*Job, error) {
	j, err := q.reader.Get(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("job not found: %s: %w", jobID, err)
	}
	if j == nil {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}
	return jobs.ToQueueItem(j), nil
}

// GetJobPayload returns the job payload with enriched fields.
// Collapsed onto jobs.ToPayloadMap (Ondata 4 Strategy B Phase 2 sweep).
func (q *QueryService) GetJobPayload(ctx context.Context, jobID string) (map[string]interface{}, error) {
	j, err := q.reader.Get(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("job not found: %s: %w", jobID, err)
	}
	if j == nil {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}
	return jobs.ToPayloadMap(j), nil
}

// GetJobAttempt returns the current retry count.
func (q *QueryService) GetJobAttempt(ctx context.Context, jobID string) (int, error) {
	j, err := q.reader.Get(ctx, jobID)
	if err != nil {
		return 0, fmt.Errorf("job not found: %s: %w", jobID, err)
	}
	if j == nil {
		return 0, fmt.Errorf("job not found: %s", jobID)
	}
	return j.Attempts, nil
}

// GetJobAsMap returns a job as a map for flexible field access.
// Collapsed onto jobs.ToFlatMap (Ondata 4 Strategy B Phase 2 sweep).
func (q *QueryService) GetJobAsMap(ctx context.Context, jobID string) (map[string]interface{}, error) {
	j, err := q.reader.Get(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("job not found: %s: %w", jobID, err)
	}
	if j == nil {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}
	return jobs.ToFlatMap(j), nil
}

// listJobs calls the canonical jobs.Reader.List with the converted filter.
// statuses is a slice of canonical jobs.Status names. Empty slice means
// "no filter" (return all jobs).
func (q *QueryService) listJobs(ctx context.Context, statuses []jobs.Status) ([]*Job, error) {
	domainJobs, err := q.reader.List(ctx, jobs.Filter{Statuses: statuses, Limit: 1000})
	if err != nil {
		return nil, err
	}
	result := make([]*Job, 0, len(domainJobs))
	for i := range domainJobs {
		result = append(result, jobs.ToQueueItem(&domainJobs[i]))
	}
	return result, nil
}

// GetJobsByStatus returns all jobs with a given status via the canonical reader.
func (q *QueryService) GetJobsByStatus(ctx context.Context, status JobStatus) ([]*Job, error) {
	return q.listJobs(ctx, []jobs.Status{jobs.Status(status)})
}

// GetPendingJobs returns all pending jobs via the canonical reader.
func (q *QueryService) GetPendingJobs(ctx context.Context) ([]*Job, error) {
	return q.listJobs(ctx, []jobs.Status{StatusPending})
}

// GetRunningJobs returns all running jobs via the canonical reader.
func (q *QueryService) GetRunningJobs(ctx context.Context) ([]*Job, error) {
	return q.listJobs(ctx, []jobs.Status{StatusRunning})
}

// GetAllJobs returns all jobs (no status filter) via the canonical reader.
func (q *QueryService) GetAllJobs(ctx context.Context) ([]*Job, error) {
	return q.listJobs(ctx, nil)
}

// Stats returns aggregate job counts grouped by status.
// Collapsed onto jobs.FormatStats (Ondata 4 Strategy B Phase 2 sweep).
func (q *QueryService) Stats(ctx context.Context) (map[string]int64, error) {
	counts, err := q.reader.Counts(ctx)
	if err != nil {
		return nil, err
	}
	return jobs.FormatStats(counts), nil
}
