// Package queue provides job queue management with SQLite persistence.
//
// PR15.2 (Ondata 4 Strategy B completion): QueryService is now a THIN
// shim over the canonical jobs.Reader + jobs/view.go free functions.
// All previously-duplicated helpers (parsePayloadJSON, domainJobToQueueJob,
// the inner listJobs conversion loop) have been deleted. The wire shape
// produced for HTTP/JSON consumers is preserved verbatim by jobs.ToQueueItem,
// jobs.ToPayloadMap, jobs.ToFlatMap, jobs.FormatStats.
//
// Write / mutation operations continue to live on LifecycleService.
package queue

import (
	"context"
	"fmt"

	"velox-server/internal/jobs"
)

// QueryService provides read-only access to job data via the canonical
// jobs.Reader surface. PR15.2: it is a thin shim that delegates every
// method to jobs.Reader + jobs/view.go free functions. Earlier duplicate
// implementations of parsePayloadJSON / domainJobToQueueJob have been
// removed — the canonical helpers in jobs/view.go are the single source.
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
func (q *QueryService) GetJobPayload(ctx context.Context, jobID string) (map[string]interface{}, error) {
	j, err := q.reader.Get(ctx, jobID)
	if err != nil {
		return nil, err
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
		return 0, err
	}
	if j == nil {
		return 0, fmt.Errorf("job not found: %s", jobID)
	}
	return j.Attempts, nil
}

// GetJobAsMap returns a job as a flattened map for flexible field access.
func (q *QueryService) GetJobAsMap(ctx context.Context, jobID string) (map[string]interface{}, error) {
	j, err := q.reader.Get(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if j == nil {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}
	return jobs.ToFlatMap(j), nil
}

// listJobs is a small helper that projects a slice of domain Job into
// transport QueueItem. Kept inlined here because the four status-specific
// list methods (GetPendingJobs / GetRunningJobs / GetAllJobs /
// GetJobsByStatus) all need it; the canonical filter lives on jobs.Reader.
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

// GetJobsByStatus returns all jobs with a given status.
func (q *QueryService) GetJobsByStatus(ctx context.Context, status JobStatus) ([]*Job, error) {
	return q.listJobs(ctx, []jobs.Status{jobs.Status(status)})
}

// GetPendingJobs returns all pending jobs.
func (q *QueryService) GetPendingJobs(ctx context.Context) ([]*Job, error) {
	return q.listJobs(ctx, []jobs.Status{StatusPending})
}

// GetRunningJobs returns all running jobs.
func (q *QueryService) GetRunningJobs(ctx context.Context) ([]*Job, error) {
	return q.listJobs(ctx, []jobs.Status{StatusRunning})
}

// GetAllJobs returns all jobs (no status filter).
func (q *QueryService) GetAllJobs(ctx context.Context) ([]*Job, error) {
	return q.listJobs(ctx, nil)
}

// Stats returns aggregate job counts grouped by status.
// The returned map's keys are canonical jobs.Status string
// representations (matches pre-Batch-3 wire format produced earlier).
func (q *QueryService) Stats(ctx context.Context) (map[string]int64, error) {
	counts, err := q.reader.Counts(ctx)
	if err != nil {
		return nil, err
	}
	return jobs.FormatStats(counts), nil
}
