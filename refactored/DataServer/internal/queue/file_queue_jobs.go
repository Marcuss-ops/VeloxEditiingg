package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// SubmitJob adds a new job to the queue
func (q *FileQueue) SubmitJob(ctx context.Context, jobID string, payload map[string]interface{}) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := NowUnix()
	nowISOVal := NowISO()

	job := &Job{
		JobID:      jobID,
		Status:     StatusPending,
		CreatedAt:  now,
		UpdatedAt:  now,
		RetryCount: 0,
		MaxRetries: q.maxRetries,
		History: []JobHistoryEntry{{
			Status:    "PENDING",
			Timestamp: nowISOVal,
			Message:   "Job created",
		}},
		Payload: payload,
	}

	if s, ok := payload["video_name"].(string); ok {
		job.VideoName = s
	}
	if s, ok := payload["project_id"].(string); ok {
		job.ProjectID = s
	}
	if s, ok := payload["project_name"].(string); ok && job.ProjectID == "" {
		job.ProjectID = s
	}
	if s, ok := payload["youtube_group"].(string); ok && job.ProjectID == "" {
		job.ProjectID = s
	}
	if s, ok := payload["output_video_id"].(string); ok {
		job.OutputVideoID = s
	}
	if s, ok := payload["job_fingerprint"].(string); ok {
		job.JobFingerprint = s
	}
	if s, ok := payload["job_run_id"].(string); ok && s != "" {
		job.RunID = s
	} else if s, ok := payload["run_id"].(string); ok && s != "" {
		job.RunID = s
	}
	if m, ok := payload["slot_data"].(map[string]interface{}); ok {
		job.SlotData = m
	}

	if err := PersistJob(job, q.dbStore); err != nil {
		return err
	}

	q.activeJobs[jobID] = job

	q.logEvent(jobID, "created", map[string]interface{}{
		"project_id": job.ProjectID,
		"video_name": job.VideoName,
	})

	return nil
}

// ClaimNextJob atomically claims the next pending job for a worker.
func (q *FileQueue) ClaimNextJob(ctx context.Context, workerID string, allowedJobTypes []string) (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	rawJSON, ok, err := q.dbStore.ClaimNextPendingJob(workerID, allowedJobTypes, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	var payload map[string]any
	if err := json.Unmarshal(rawJSON, &payload); err != nil {
		return nil, fmt.Errorf("failed to decode claimed job: %w", err)
	}

	job := MapToJob(payload)
	q.activeJobs[job.JobID] = job
	q.logEvent(job.JobID, "claimed", map[string]interface{}{
		"worker_id": workerID,
	})
	return job, nil
}

// CompleteJob marks a job as completed (idempotent).
// If the job is already COMPLETED (e.g. via UploadCompletedVideo or SubmitResult),
// the call succeeds without overwriting any fields — this prevents clobbering
// master_video_path, drive_url, and other metadata set by those earlier steps.
func (q *FileQueue) CompleteJob(ctx context.Context, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, ok := q.activeJobs[jobID]
	if !ok {
		// Job already completed / removed from active cache → load from SQLite
		m, dbErr := q.dbStore.GetJob(ctx, jobID)
		if dbErr != nil {
			return fmt.Errorf("job not found: %s", jobID)
		}
		job = MapToJob(m)
		if job.Status == StatusCompleted {
			// Already COMPLETED — idempotent, nothing to do
			return nil
		}
		if job.Status != StatusPending && job.Status != StatusProcessing {
			return fmt.Errorf("job %s in unexpected state %s", jobID, job.Status)
		}
		// Job was PENDING/PROCESSING in SQLite but not in active cache → add it back
		q.activeJobs[jobID] = job
	}

	now := NowUnix()
	nowISOVal := NowISO()

	job.Status = StatusCompleted
	job.CompletedAt = nowISOVal
	job.UpdatedAt = now
	job.LastError = ""
	job.LastErrorAt = nil
	job.ErrorMessage = ""
	job.FailedAt = nil
	job.FailedBy = ""

	job.History = append(job.History, JobHistoryEntry{
		Status:    "COMPLETED",
		Timestamp: nowISOVal,
		WorkerID:  job.AssignedTo,
		Message:   "Job completed successfully",
	})

	if err := PersistJob(job, q.dbStore); err != nil {
		return err
	}

	delete(q.activeJobs, jobID)

	q.logEvent(jobID, "completed", map[string]interface{}{
		"worker_id": job.AssignedTo,
	})

	return nil
}

// FailJob marks a job as failed, optionally requeueing for retry
func (q *FileQueue) FailJob(ctx context.Context, jobID, errMsg string, requeue bool) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, ok := q.activeJobs[jobID]
	if !ok {
		return fmt.Errorf("job not found: %s", jobID)
	}

	now := NowUnix()
	nowISOVal := NowISO()
	workerID := job.AssignedTo

	if requeue && job.RetryCount < job.MaxRetries {
		job.Status = StatusPending
		job.LastError = errMsg
		job.LastErrorAt = now
		job.AssignedTo = ""
		job.AssignedAt = nil
		job.ClaimedBy = ""
		job.ClaimedAt = ""
		job.ProcessingAt = nil

		job.History = append(job.History, JobHistoryEntry{
			Status:    "PENDING",
			Timestamp: nowISOVal,
			WorkerID:  workerID,
			Message:   fmt.Sprintf("Job requeued after failure: %s", errMsg),
		})

		if err := PersistJob(job, q.dbStore); err != nil {
			return err
		}
	} else {
		job.Status = StatusError
		job.ErrorMessage = errMsg
		job.LastError = errMsg
		job.LastErrorAt = now
		job.FailedAt = nowISOVal
		job.FailedBy = workerID

		job.History = append(job.History, JobHistoryEntry{
			Status:    "ERROR",
			Timestamp: nowISOVal,
			WorkerID:  workerID,
			Message:   fmt.Sprintf("Job failed: %s", errMsg),
		})

		if err := PersistJob(job, q.dbStore); err != nil {
			return err
		}

		delete(q.activeJobs, jobID)
	}

	job.UpdatedAt = now

	q.logEvent(jobID, "failed", map[string]interface{}{
		"worker_id": workerID,
		"error":     errMsg,
		"requeued":  requeue && job.RetryCount < job.MaxRetries,
	})

	return nil
}

// LeaseJob claims a job for a worker with atomic check-and-set.
func (q *FileQueue) LeaseJob(ctx context.Context, jobID, workerID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, ok := q.activeJobs[jobID]
	if !ok {
		m, err := q.dbStore.GetJob(ctx, jobID)
		if err != nil {
			return fmt.Errorf("job not found: %s", jobID)
		}
		job = MapToJob(m)
		if job.Status == StatusPending || job.Status == StatusProcessing {
			q.activeJobs[jobID] = job
		}
	}
	if job.Status != StatusPending {
		return fmt.Errorf("job %s is not pending", jobID)
	}
	if job.ClaimedBy != "" || job.AssignedTo != "" {
		return fmt.Errorf("job %s already claimed by %s", jobID, job.ClaimedBy)
	}

	now := NowUnix()
	nowISOVal := NowISO()

	job.Status = StatusProcessing
	job.AssignedTo = workerID
	job.AssignedAt = nowISOVal
	job.ClaimedBy = workerID
	job.ClaimedAt = nowISOVal
	job.LeaseExpiry = time.Now().UTC().Add(30 * time.Minute).Format(time.RFC3339)
	job.UpdatedAt = now
	job.RetryCount++

	job.History = append(job.History, JobHistoryEntry{
		Status:    "PROCESSING",
		Timestamp: nowISOVal,
		WorkerID:  workerID,
		Message:   fmt.Sprintf("Job assigned to worker %s", workerID),
	})

	if err := PersistJob(job, q.dbStore); err != nil {
		return err
	}

	q.logEvent(jobID, "claimed", map[string]interface{}{
		"worker_id": workerID,
	})

	return nil
}
