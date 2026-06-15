package queue

import (
	"context"
	"fmt"
	"log"
	"time"
)

// RequeueZombieJobs finds jobs with expired leases and requeues them
func (q *FileQueue) RequeueZombieJobs(ctx context.Context, timeout time.Duration) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	requeued := 0

	for id, job := range q.activeJobs {
		if job.Status != StatusProcessing {
			continue
		}

		var assignedTime time.Time
		switch v := job.AssignedAt.(type) {
		case string:
			assignedTime, _ = time.Parse(time.RFC3339, v)
		case float64:
			assignedTime = time.Unix(int64(v), 0)
		}

		if assignedTime.IsZero() {
			continue
		}

		// Check lease expiry
		leaseExpired := false
		if job.LeaseExpiry != nil {
			if leaseStr, ok := job.LeaseExpiry.(string); ok && leaseStr != "" {
				if leaseTime, err := time.Parse(time.RFC3339, leaseStr); err == nil && now.After(leaseTime) {
					leaseExpired = true
				}
			}
		}

		if now.Sub(assignedTime) > timeout || leaseExpired {
			nowISOVal := NowISO()
			reason := fmt.Sprintf("Zombie: no heartbeat for %v", now.Sub(assignedTime))
			if leaseExpired {
				reason = "Lease expired"
			}
			job.Status = StatusPending
			job.LastError = reason
			job.LastErrorAt = now.Unix()
			job.AssignedTo = ""
			job.AssignedAt = nil
			job.ClaimedBy = ""
			job.ClaimedAt = ""
			job.LeaseExpiry = nil
			job.RetryCount++

			job.History = append(job.History, JobHistoryEntry{
				Status:    "PENDING",
				Timestamp: nowISOVal,
				Message:   "Requeued after zombie timeout",
			})

			if err := PersistJob(job, q.dbStore); err != nil {
				log.Printf("[WARN] Failed to persist zombie requeue for %s: %v", id, err)
				continue
			}

			requeued++
		}
	}

	return requeued, nil
}
