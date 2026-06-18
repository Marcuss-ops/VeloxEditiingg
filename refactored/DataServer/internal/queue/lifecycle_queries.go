package queue

import (
	"context"
	"fmt"
	"log"

	"velox-server/internal/store"
)

// GetJobsByStatus returns all jobs with a given status via JobRepository.
func (l *LifecycleService) GetJobsByStatus(ctx context.Context, status JobStatus) ([]*Job, error) {
	storeJobs, err := l.jobRepo.ListByStatus(ctx, []store.JobStatus{toStoreJobStatus(status)}, 1000)
	if err != nil {
		return nil, fmt.Errorf("job repo list by status: %w", err)
	}
	result := make([]*Job, 0, len(storeJobs))
	for _, sj := range storeJobs {
		m, err := l.eventStore.GetJob(ctx, sj.JobID)
		if err != nil {
			log.Printf("GetJobsByStatus: GetJob(%s) failed after ListByStatus returned it: %v", sj.JobID, err)
			continue
		}
		result = append(result, MapToJob(m))
	}
	return result, nil
}

// GetNextJobID returns the next pending job ID.
func (l *LifecycleService) GetNextJobID(ctx context.Context) (string, error) {
	jobs, err := l.eventStore.ListJobsByStatus([]string{"PENDING"}, 1)
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

// toStoreJobStatus maps a queue.JobStatus to the equivalent store.JobStatus.
func toStoreJobStatus(s JobStatus) store.JobStatus {
	return store.JobStatus(s)
}

// getIntField extracts an integer field from a job map, returning 0 if not found.
func getIntField(m map[string]interface{}, key string) int {
	if m == nil {
		return 0
	}
	v, ok := m[key]
	if !ok || v == nil {
		return 0
	}
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}
