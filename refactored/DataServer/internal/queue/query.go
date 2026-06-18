// Package queue provides job queue management with SQLite persistence
package queue

import (
	"context"
	"fmt"
	"time"

	"velox-server/internal/store"
)

// QueryService provides read-only access to job data.
// Uses EventStore (which embeds dbStore) for map-based reads.
type QueryService struct {
	eventStore store.EventStore
}

// NewQueryService creates a new query service.
func NewQueryService(eventStore store.EventStore) *QueryService {
	return &QueryService{eventStore: eventStore}
}

// GetJob retrieves a job by ID.
func (q *QueryService) GetJob(ctx context.Context, jobID string) (*Job, error) {
	m, err := q.eventStore.GetJob(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}
	return MapToJob(m), nil
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

// GetJobsByStatus returns all jobs with a given status.
func (q *QueryService) GetJobsByStatus(ctx context.Context, status JobStatus) ([]*Job, error) {
	return q.listJobs(ctx, []string{string(status)})
}

// GetPendingJobs returns all pending jobs.
func (q *QueryService) GetPendingJobs(ctx context.Context) ([]*Job, error) {
	return q.listJobs(ctx, []string{string(StatusPending)})
}

// GetRunningJobs returns all running jobs.
func (q *QueryService) GetRunningJobs(ctx context.Context) ([]*Job, error) {
	return q.listJobs(ctx, []string{string(StatusRunning)})
}

func (q *QueryService) listJobs(ctx context.Context, statuses []string) ([]*Job, error) {
	jobs, err := q.eventStore.ListJobsByStatus(statuses, 1000)
	if err != nil {
		return nil, err
	}
	result := make([]*Job, 0, len(jobs))
	for _, m := range jobs {
		result = append(result, MapToJob(m))
	}
	return result, nil
}

// GetAllJobs returns all active jobs.
func (q *QueryService) GetAllJobs(ctx context.Context) (map[string]*Job, error) {
	activeJobs, err := q.eventStore.GetActiveJobs()
	if err != nil {
		return nil, err
	}
	result := make(map[string]*Job)
	for id, m := range activeJobs {
		result[id] = MapToJob(m)
	}
	return result, nil
}

// Stats returns queue statistics.
func (q *QueryService) Stats(ctx context.Context) (map[string]int64, error) {
	return q.eventStore.JobCounts(ctx)
}

// DeleteJob removes a job.
func (q *QueryService) DeleteJob(ctx context.Context, jobID string) error {
	return q.eventStore.DeleteJob(jobID)
}

// GetNextJobID returns the next pending job ID.
func (q *QueryService) GetNextJobID(ctx context.Context) (string, error) {
	jobs, err := q.eventStore.ListJobsByStatus([]string{"PENDING"}, 1)
	if err != nil {
		return "", err
	}
	if len(jobs) == 0 {
		return "", nil
	}
	if id, ok := jobs[0]["job_id"].(string); ok {
		return id, nil
	}
	return "", nil
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
	result["video_uploaded"] = job.VideoUploaded
	result["master_video_path"] = job.MasterVideoPath
	result["artifact_id"] = job.ArtifactID
	result["output_sha256"] = job.OutputSHA256
	result["upload_idempotency_key"] = job.IdempotencyKey
	result["output_video_id"] = job.OutputVideoID
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

// UpdateJobLogs persists worker log entries.
func (q *QueryService) UpdateJobLogs(ctx context.Context, jobID string, logs []JobLogEntry) error {
	for _, entry := range logs {
		if err := q.eventStore.AddJobLog(jobID, entry.Message, entry.WorkerID, entry.IsError); err != nil {
			return fmt.Errorf("failed to add job log: %w", err)
		}
	}
	return nil
}

// CleanupOldJobs removes completed/error jobs older than specified age.
func (q *QueryService) CleanupOldJobs(ctx context.Context, age time.Duration) (int, error) {
	cutoff := time.Now().Add(-age)
	count, err := q.eventStore.ArchiveOldJobs(cutoff)
	if err != nil {
		return 0, err
	}
	return int(count), nil
}
