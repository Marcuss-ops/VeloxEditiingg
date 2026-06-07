// Package queue provides job queue management with SQLite persistence
package queue

import (
	"context"
	"fmt"

	"velox-server/internal/store"
)

// GetJob retrieves a job by ID
func GetJob(ctx context.Context, jobID string, dbStore *store.SQLiteStore, activeJobs map[string]*Job) (*Job, error) {
	// Check active cache first
	if job, ok := activeJobs[jobID]; ok {
		return job, nil
	}

	// Not in cache - check SQLite for completed/error jobs
	m, err := dbStore.GetJob(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("job not found: %s", jobID)
	}

	return MapToJob(m), nil
}

// GetJobPayload returns the job payload
func GetJobPayload(ctx context.Context, jobID string, dbStore *store.SQLiteStore, activeJobs map[string]*Job) (map[string]interface{}, error) {
	job, err := GetJob(ctx, jobID, dbStore, activeJobs)
	if err != nil {
		return nil, err
	}

	// Build payload from job
	payload := make(map[string]interface{})
	if job.Payload != nil {
		for k, v := range job.Payload {
			payload[k] = v
		}
	}

	// Add job fields
	payload["job_id"] = job.JobID
	payload["job_run_id"] = job.RunID
	payload["run_id"] = job.RunID
	payload["status"] = string(job.Status)
	payload["video_name"] = job.VideoName
	payload["project_id"] = job.ProjectID

	return payload, nil
}

// GetJobAttempt returns the current retry count
func GetJobAttempt(ctx context.Context, jobID string, dbStore *store.SQLiteStore, activeJobs map[string]*Job) (int, error) {
	job, err := GetJob(ctx, jobID, dbStore, activeJobs)
	if err != nil {
		return 0, err
	}
	return job.RetryCount, nil
}

// GetJobsByStatus returns all jobs with a given status
func GetJobsByStatus(ctx context.Context, status JobStatus, dbStore *store.SQLiteStore, activeJobs map[string]*Job) ([]*Job, error) {
	var result []*Job

	// Check active cache first for active statuses
	if status == StatusPending || status == StatusProcessing {
		for _, job := range activeJobs {
			if job.Status == status {
				result = append(result, job)
			}
		}
		return result, nil
	}

	// For completed/error, query SQLite directly
	statuses := []string{string(status)}
	jobs, err := dbStore.ListJobsByStatus(statuses, 1000)
	if err != nil {
		return nil, err
	}

	for _, m := range jobs {
		result = append(result, MapToJob(m))
	}

	return result, nil
}

// GetPendingJobs returns all pending jobs
func GetPendingJobs(ctx context.Context, dbStore *store.SQLiteStore, activeJobs map[string]*Job) ([]*Job, error) {
	return GetJobsByStatus(ctx, StatusPending, dbStore, activeJobs)
}

// GetProcessingJobs returns all processing jobs
func GetProcessingJobs(ctx context.Context, dbStore *store.SQLiteStore, activeJobs map[string]*Job) ([]*Job, error) {
	return GetJobsByStatus(ctx, StatusProcessing, dbStore, activeJobs)
}

// GetAllJobs returns all jobs (limited to recent active + query for historical)
func GetAllJobs(ctx context.Context, activeJobs map[string]*Job) (map[string]*Job, error) {
	// Return copy of active jobs
	result := make(map[string]*Job)
	for id, job := range activeJobs {
		result[id] = job
	}

	return result, nil
}

// Stats returns queue statistics (uses SQLite for accurate counts)
func Stats(ctx context.Context, dbStore *store.SQLiteStore) (map[string]int64, error) {
	return dbStore.JobCounts(ctx)
}

// GetJobAsMap returns a job as a map for flexible field access
func GetJobAsMap(ctx context.Context, jobID string, dbStore *store.SQLiteStore, activeJobs map[string]*Job) (map[string]interface{}, error) {
	job, err := GetJob(ctx, jobID, dbStore, activeJobs)
	if err != nil {
		return nil, err
	}

	// Build map from job struct
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
	result["worker_name"] = job.WorkerName
	result["retry_count"] = job.RetryCount
	result["max_retries"] = job.MaxRetries
	result["last_error"] = job.LastError
	result["error_message"] = job.ErrorMessage
	result["video_uploaded"] = job.VideoUploaded
	result["master_video_path"] = job.MasterVideoPath
	result["output_video_id"] = job.OutputVideoID
	result["run_id"] = job.RunID
	result["job_run_id"] = job.RunID
	if len(job.Logs) > 0 {
		result["logs"] = job.Logs
	}
	if len(job.History) > 0 {
		result["history"] = job.History
	}

	// Add payload fields
	if job.Payload != nil {
		for k, v := range job.Payload {
			if _, exists := result[k]; !exists {
				result[k] = v
			}
		}
	}

	return result, nil
}
