package queue

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// RenewJobLease extends the current lease for an active job.
func (q *FileQueue) RenewJobLease(ctx context.Context, jobID, workerID, leaseID string, leaseExpiry time.Time) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, ok := q.activeJobs[jobID]
	if !ok {
		m, err := q.dbStore.GetJob(ctx, jobID)
		if err != nil {
			return fmt.Errorf("job not found: %s", jobID)
		}
		job = MapToJob(m)
		q.activeJobs[jobID] = job
	}
	if job == nil {
		return fmt.Errorf("job not found: %s", jobID)
	}
	if strings.TrimSpace(job.LeaseID) != "" && !strings.EqualFold(strings.TrimSpace(job.LeaseID), strings.TrimSpace(leaseID)) {
		return fmt.Errorf("lease mismatch for job %s", jobID)
	}
	if strings.TrimSpace(job.AssignedTo) != "" && !strings.EqualFold(strings.TrimSpace(job.AssignedTo), strings.TrimSpace(workerID)) {
		return fmt.Errorf("worker mismatch for job %s", jobID)
	}
	if !isValidJobStatusTransition(job.Status, StatusProcessing) && job.Status != StatusProcessing {
		return fmt.Errorf("job %s is not renewable in state %s", jobID, job.Status)
	}

	now := NowUnix()
	nowISO := NowISO()
	job.Status = StatusProcessing
	job.LeaseID = strings.TrimSpace(leaseID)
	job.LeaseExpiry = leaseExpiry.UTC().Format(time.RFC3339)
	job.UpdatedAt = now
	if job.Attempt == 0 {
		job.Attempt = job.RetryCount
	}
	job.History = append(job.History, JobHistoryEntry{
		Status:    "PROCESSING",
		Timestamp: nowISO,
		WorkerID:  workerID,
		Message:   "Lease renewed",
	})

	if err := PersistJob(job, q.dbStore); err != nil {
		return err
	}
	return nil
}
