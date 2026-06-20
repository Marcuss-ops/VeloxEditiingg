// Package queue provides job queue management with SQLite persistence.
// QueryService (Batch 3) is reduced to a thin wrapper over the canonical
// jobs.Reader: the legacy map-based eventStore surface has been dropped, and
// the four methods that depended on it (GetNextJobID, DeleteJob,
// UpdateJobLogs, CleanupOldJobs) have been deleted outright.
package queue

import (
	"context"
	"encoding/json"
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
	return domainJobToQueueJob(j), nil
}

// parsePayloadJSON converts a raw JSON payload string into a map.
func parsePayloadJSON(raw string) map[string]interface{} {
	if raw == "" || raw == "{}" {
		return make(map[string]interface{})
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return make(map[string]interface{})
	}
	return m
}

// domainJobToQueueJob converts a canonical jobs.Job into a queue.Job
// (scheduling/transport projection). Fields not present in the domain
// model (history, logs, slot_data, PayloadJSON) are left zero-valued;
// callers that need the full MapToJob hydration should go through the
// eventStore legacy path (which has been dropped; use SQLiteStore direct).
func domainJobToQueueJob(j *jobs.Job) *Job {
	if j == nil {
		return nil
	}
	return &Job{
		JobID:       j.ID,
		Status:      JobStatus(j.Status),
		VideoName:   j.VideoName,
		ProjectID:   j.ProjectID,
		WorkerName:  j.WorkerID,
		AssignedTo:  j.WorkerID,
		LeaseID:     j.LeaseID,
		RetryCount:  j.Attempts,
		Attempt:     j.Attempts,
		MaxRetries:  j.MaxRetries,
		RunID:       j.RunID,
		CreatedAt:   j.CreatedAt,
		UpdatedAt:   j.UpdatedAt,
		StartedAt:   j.StartedAt,
		CompletedAt: j.CompletedAt,
		Payload:     parsePayloadJSON(j.Payload),
	}
}

// GetJobPayload returns the job payload with enriched fields.
func (q *QueryService) GetJobPayload(ctx context.Context, jobID string) (map[string]interface{}, error) {
	job, err := q.GetJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	payload := make(map[string]interface{})
	if job.Payload != nil {
		for k, v := range job.Payload {
			payload[k] = v
		}
	}
	payload["job_id"] = job.JobID
	payload["job_run_id"] = job.RunID
	payload["run_id"] = job.RunID
	payload["status"] = string(job.Status)
	payload["video_name"] = job.VideoName
	payload["project_id"] = job.ProjectID
	if job.LeaseID != "" {
		payload["lease_id"] = job.LeaseID
	}
	if job.LeaseExpiry != nil {
		payload["lease_expiry"] = job.LeaseExpiry
	}
	return payload, nil
}

// GetJobAttempt returns the current retry count.
func (q *QueryService) GetJobAttempt(ctx context.Context, jobID string) (int, error) {
	job, err := q.GetJob(ctx, jobID)
	if err != nil {
		return 0, err
	}
	return job.RetryCount, nil
}

// GetJobAsMap returns a job as a map for flexible field access.
func (q *QueryService) GetJobAsMap(ctx context.Context, jobID string) (map[string]interface{}, error) {
	job, err := q.GetJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	result := make(map[string]interface{})
	result["job_id"] = job.JobID
	result["status"] = string(job.Status)
	result["video_name"] = job.VideoName
	result["project_id"] = job.ProjectID
	result["created_at"] = job.CreatedAt
	result["updated_at"] = job.UpdatedAt
	result["started_at"] = job.StartedAt
	result["completed_at"] = job.CompletedAt
	result["assigned_to"] = job.AssignedTo
	result["claimed_by"] = job.ClaimedBy
	result["claimed_at"] = job.ClaimedAt
	result["lease_id"] = job.LeaseID
	result["lease_expiry"] = job.LeaseExpiry
	result["worker_name"] = job.WorkerName
	result["retry_count"] = job.RetryCount
	result["attempt"] = job.Attempt
	result["max_retries"] = job.MaxRetries
	result["last_error"] = job.LastError
	result["error_message"] = job.ErrorMessage
	result["run_id"] = job.RunID
	result["job_run_id"] = job.RunID
	if len(job.Logs) > 0 {
		result["logs"] = job.Logs
	}
	if len(job.History) > 0 {
		result["history"] = job.History
	}
	if job.Payload != nil {
		for k, v := range job.Payload {
			if _, exists := result[k]; !exists {
				result[k] = v
			}
		}
	}
	return result, nil
}

// list Jobs calls the canonical jobs.Reader.List with the converted filter.
// statuses is a slice of canonical jobs.Status names. Empty slice means
// "no filter" (return all jobs).
func (q *QueryService) listJobs(ctx context.Context, statuses []jobs.Status) ([]*Job, error) {
	domainJobs, err := q.reader.List(ctx, jobs.Filter{Statuses: statuses, Limit: 1000})
	if err != nil {
		return nil, err
	}
	result := make([]*Job, 0, len(domainJobs))
	for i := range domainJobs {
		result = append(result, domainJobToQueueJob(&domainJobs[i]))
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

// Stats returns aggregate job counts grouped by status. The returned map's
// keys are canonical jobs.Status string representations, matching the
// pre-Batch-3 (string-keyed) shape so HTTP/JSON consumers continue to work
// without breaking changes.
func (q *QueryService) Stats(ctx context.Context) (map[string]int64, error) {
	counts, err := q.reader.Counts(ctx)
	if err != nil {
		return nil, err
	}
	res := make(map[string]int64, len(counts))
	for k, v := range counts {
		res[string(k)] = v
	}
	return res, nil
}
