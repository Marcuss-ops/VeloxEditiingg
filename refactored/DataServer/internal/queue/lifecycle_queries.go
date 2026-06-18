package queue

import (
	"context"
	"fmt"

	"velox-server/internal/store"
)

// GetJobsByStatus returns all jobs with a given status via JobRepository.
func (l *LifecycleService) GetJobsByStatus(ctx context.Context, status JobStatus) ([]*Job, error) {
	storeJobs, err := l.repo.ListByStatus(ctx, []store.JobStatus{toStoreJobStatus(status)}, 1000)
	if err != nil {
		return nil, fmt.Errorf("job repo list by status: %w", err)
	}
	result := make([]*Job, 0, len(storeJobs))
	for _, sj := range storeJobs {
		// Build a minimal queue.Job from the store.Job projection.
		job := &Job{
			JobID:      sj.JobID,
			Status:     JobStatus(sj.Status),
			VideoName:  sj.VideoName,
			ProjectID:  sj.ProjectID,
			CreatedAt:  sj.CreatedAt,
			UpdatedAt:  sj.UpdatedAt,
			StartedAt:  sj.StartedAt,
			CompletedAt: sj.CompletedAt,
			RetryCount: sj.RetryCount,
			MaxRetries: sj.MaxRetries,
		}
		result = append(result, job)
	}
	return result, nil
}

// GetNextJobID returns the next pending job ID.
func (l *LifecycleService) GetNextJobID(ctx context.Context) (string, error) {
	jobs, err := l.repo.ListByStatus(ctx, []store.JobStatus{store.JobStatusPending}, 1)
	if err != nil {
		return "", err
	}
	if len(jobs) == 0 {
		return "", nil
	}
	return jobs[0].JobID, nil
}

// toStoreJobStatus maps a queue.JobStatus to the equivalent store.JobStatus.
func toStoreJobStatus(s JobStatus) store.JobStatus {
	return store.JobStatus(s)
}
