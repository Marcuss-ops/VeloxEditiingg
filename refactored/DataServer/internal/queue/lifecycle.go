// Package queue provides job queue management with SQLite persistence
package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"velox-server/internal/store"
)

// LifecycleService validates and executes job status transitions.
type LifecycleService struct {
	repo       store.JobRepository
	clock      Clock
	jobRepo    store.JobRepository
	eventStore store.EventStore
}

// NewLegacyLifecycleService creates a new legacy lifecycle service with the
// dual-dep shape: JobRepository + EventStore. Both are mandatory.
func NewLegacyLifecycleService(repo store.JobRepository, eventStore store.EventStore) (*LifecycleService, error) {
	if repo == nil {
		return nil, errors.New("job repository is required")
	}
	if eventStore == nil {
		return nil, errors.New("event store is required")
	}
	return &LifecycleService{jobRepo: repo, eventStore: eventStore, repo: repo}, nil
}

// Validate checks whether a transition from one status to another is allowed.
func (l *LifecycleService) Validate(from, to JobStatus) error {
	if !isValidJobStatusTransition(from, to) {
		return fmt.Errorf("invalid transition: %s → %s", from, to)
	}
	return nil
}

// RecordRenderFinished records that a worker has completed rendering.
func (l *LifecycleService) RecordRenderFinished(ctx context.Context, cmd store.RecordRenderFinishedCommand) error {
	if l.repo != nil {
		return l.repo.PR3RecordRenderFinished(ctx, cmd)
	}
	return l.jobRepo.PR3RecordRenderFinished(ctx, cmd)
}

// FailJob marks a job as FAILED or RETRY_WAIT using CAS.
func (l *LifecycleService) FailJob(ctx context.Context, jobID, errMsg, workerID string, requeue bool, maxRetries int) error {
	sj, err := l.jobRepo.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("job not found: %s", jobID)
	}

	nowISO := NowISO()

	if requeue && sj.RetryCount < maxRetries {
		if err := l.Validate(JobStatus(sj.Status), StatusRetryWait); err != nil {
			return err
		}
		if err := l.jobRepo.Transition(ctx, store.TransitionParams{
			JobID:          jobID,
			ExpectedStatus: sj.Status,
			NewStatus:      store.JobStatusRetryWait,
			Revision:       sj.Revision,
		}); err != nil {
			return fmt.Errorf("CAS transition to RETRY_WAIT failed: %w", err)
		}

		l.eventStore.UpdateJobSupplementary(jobID, map[string]interface{}{
			"last_error":   errMsg,
			"assigned_to":  "",
			"claimed_by":   "",
			"lease_id":     "",
			"lease_expiry": nil,
		})
		l.eventStore.LogJobEvent(jobID, "job_retry_wait", map[string]interface{}{
			"worker_id": workerID,
			"error":     errMsg,
			"revision":  sj.Revision + 1,
		})
	} else {
		if err := l.Validate(JobStatus(sj.Status), StatusFailed); err != nil {
			return err
		}
		if err := l.jobRepo.Transition(ctx, store.TransitionParams{
			JobID:          jobID,
			ExpectedStatus: sj.Status,
			NewStatus:      store.JobStatusFailed,
			Revision:       sj.Revision,
		}); err != nil {
			return fmt.Errorf("CAS transition to FAILED failed: %w", err)
		}

		l.eventStore.UpdateJobSupplementary(jobID, map[string]interface{}{
			"error_message": errMsg,
			"last_error":    errMsg,
			"failed_at":     nowISO,
			"failed_by":     workerID,
			"lease_id":      "",
			"lease_expiry":  nil,
		})
		l.eventStore.LogJobEvent(jobID, "job_failed", map[string]interface{}{
			"worker_id": workerID,
			"error":     errMsg,
			"revision":  sj.Revision + 1,
		})
	}
	return nil
}

// RequeueZombieJobs finds jobs with expired leases and requeues them.
func (l *LifecycleService) RequeueZombieJobs(ctx context.Context, timeout time.Duration) (int, error) {
	return l.jobRepo.RequeueZombieJobs(ctx, timeout)
}

// RenewLease extends the lease for an active job via JobRepository.
func (l *LifecycleService) RenewLease(ctx context.Context, jobID, workerID, leaseID string, leaseExpiry time.Time) error {
	if err := l.jobRepo.RenewLease(ctx, store.RenewLeaseParams{
		JobID:       jobID,
		WorkerID:    workerID,
		LeaseID:     leaseID,
		LeaseExpiry: leaseExpiry.UTC(),
	}); err != nil {
		return fmt.Errorf("renew lease: %w", err)
	}
	nowISO := NowISO()
	l.eventStore.LogJobEvent(jobID, "lease_renewed", map[string]interface{}{
		"worker_id": workerID,
		"lease_id":  leaseID,
		"timestamp": nowISO,
	})
	return nil
}

// SubmitJob creates a new job via the JobRepository.
func (l *LifecycleService) SubmitJob(ctx context.Context, jobID string, payload map[string]interface{}, maxRetries int) (*Job, error) {
	now := NowUnix()
	nowISO := NowISO()

	job := &Job{
		JobID:      jobID,
		Status:     StatusPending,
		CreatedAt:  now,
		UpdatedAt:  now,
		RetryCount: 0,
		MaxRetries: maxRetries,
		History: []JobHistoryEntry{{
			Status:    "PENDING",
			Timestamp: nowISO,
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

	params := store.CreateJobParams{
		JobID:      jobID,
		Payload:    payload,
		VideoName:  job.VideoName,
		ProjectID:  job.ProjectID,
		RunID:      job.RunID,
		MaxRetries: maxRetries,
	}
	if err := l.jobRepo.CreateJob(ctx, params); err != nil {
		return nil, fmt.Errorf("job repo create: %w", err)
	}
	_ = l.eventStore.AddJobHistory(jobID, "PENDING", "", "Job created", nil)
	if err := PersistJobRequest(jobID, payload, l.eventStore); err != nil {
		return nil, fmt.Errorf("failed to persist request_json: %w", err)
	}
	return job, nil
}

// TransitionToRunning transitions a job from LEASED to RUNNING using CAS.
func (l *LifecycleService) TransitionToRunning(ctx context.Context, jobID string) error {
	sj, err := l.jobRepo.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("job not found: %s", jobID)
	}

	if sj.Status == store.JobStatusRunning {
		return nil // idempotent
	}

	if err := l.Validate(JobStatus(sj.Status), StatusRunning); err != nil {
		return err
	}

	nowISO := NowISO()
	if err := l.jobRepo.Transition(ctx, store.TransitionParams{
		JobID:          jobID,
		ExpectedStatus: sj.Status,
		NewStatus:      store.JobStatusRunning,
		Revision:       sj.Revision,
	}); err != nil {
		return fmt.Errorf("CAS transition LEASED→RUNNING failed: %w", err)
	}

	l.eventStore.UpdateJobSupplementary(jobID, map[string]interface{}{
		"started_at": nowISO,
	})
	l.eventStore.LogJobEvent(jobID, "job_running", map[string]interface{}{
		"worker_id": sj.AssignedTo,
		"revision":  sj.Revision + 1,
	})
	return nil
}

// LeaseJob leases a PENDING job to a worker via JobRepository.
func (l *LifecycleService) LeaseJob(ctx context.Context, jobID, workerID string) error {
	return l.jobRepo.LeaseJob(ctx, jobID, workerID)
}

// ReleaseClaim releases a LEASED/RUNNING job back to PENDING via JobRepository.
func (l *LifecycleService) ReleaseClaim(ctx context.Context, jobID string) error {
	if err := l.jobRepo.ReleaseClaim(ctx, jobID); err != nil {
		return err
	}
	l.eventStore.LogJobEvent(jobID, "claim_released", map[string]interface{}{
		"reason": "send_failure",
	})
	return nil
}
