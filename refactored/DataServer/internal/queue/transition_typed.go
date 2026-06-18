// Package queue / transition_typed.go
//
// Typed method signatures on LifecycleService so callers can use a
// stable surface instead of constructing field-mutation maps. Each method
// delegates to an existing method on the same service or on the underlying
// eventStore so the canonical implementation is unchanged; this layer is
// strictly about ergonomics + a single audit surface for the orchestrator.
//
// Naming follows the orchestrator's mental model:
//   ClaimJob       — atomic SELECT-and-mark the next pending job
//   StartJob       — claim-and-flip a specific job to RUNNING (LEASE)
//   RenewLease     — extend an active lease without flipping status
//   RecordProgress — append a progress marker (event log only)
//   RequestCancel  — transition to CANCELLED
//   FailAttempt    — terminal failure for the current attempt
//   ScheduleRetry  — fail-and-leave-on-RETRY_WAIT
//   FinalizeArtifact — StageArtifact-READY from the verifier
//   CompleteJobTx  — atomic SUCCEEDED + close attempt + outbox
package queue

import (
	"context"
)

// ClaimJob atomically claims the next pending job for a worker.
func (l *LifecycleService) ClaimJob(ctx context.Context, workerID string, allowedJobTypes []string) (*Job, error) {
	return l.ClaimNextJob(ctx, workerID, allowedJobTypes)
}

// StartJob claims a specific job for a worker, flipping status LEASED.
func (l *LifecycleService) StartJob(ctx context.Context, jobID, workerID string) error {
	return l.LeaseJob(ctx, jobID, workerID)
}

// RecordProgress appends a progress marker for a job.
func (l *LifecycleService) RecordProgress(ctx context.Context, jobID, workerID string, pct int, message string) (string, error) {
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	now := NowISO()
	payload := map[string]interface{}{
		"worker_id": workerID,
		"progress":  pct,
		"message":   message,
	}
	if err := l.eventStore.LogJobEvent(jobID, "job_progress", payload); err != nil {
		return "", err
	}
	return now, nil
}

// RequestCancel transitions a job to CANCELLED.
func (l *LifecycleService) RequestCancel(ctx context.Context, jobID string) error {
	m, err := l.eventStore.GetJob(ctx, jobID)
	if err != nil {
		return err
	}
	job := MapToJob(m)
	if job.Status == StatusCancelled || job.Status == StatusSucceeded || job.Status == StatusFailed {
		return nil // idempotent — already terminal
	}
	if err := l.Validate(job.Status, StatusCancelled); err != nil {
		return err
	}
	revision := getIntField(m, "revision")
	if _, err := l.eventStore.TransitionJobStatus(ctx, jobID, string(job.Status), string(StatusCancelled), revision); err != nil {
		return err
	}
	l.eventStore.LogJobEvent(jobID, "job_cancelled", map[string]interface{}{})
	return nil
}

// FailAttempt marks the current attempt as failed without retrying.
func (l *LifecycleService) FailAttempt(ctx context.Context, jobID, errMsg, workerID string, maxRetries int) error {
	if maxRetries < 0 {
		maxRetries = 0
	}
	return l.FailJob(ctx, jobID, errMsg, workerID, false, maxRetries)
}

// ScheduleRetry marks the current attempt as failed but leaves the job in RETRY_WAIT.
func (l *LifecycleService) ScheduleRetry(ctx context.Context, jobID, errMsg, workerID string, maxRetries int) error {
	if maxRetries < 0 {
		maxRetries = 0
	}
	return l.FailJob(ctx, jobID, errMsg, workerID, true, maxRetries)
}

// FinalizeArtifact moves an artifact from VERIFYING to READY.
func (l *LifecycleService) FinalizeArtifact(ctx context.Context, artifactID string) error {
	return l.eventStore.UpdateArtifactStatus(ctx, artifactID, "READY")
}

// CompleteJobTx performs atomic SUCCEEDED + close attempt + outbox.
func (l *LifecycleService) CompleteJobTx(ctx context.Context, jobID string, attemptID int64, outboxPayload string) error {
	return l.eventStore.CompleteJobTx(ctx, jobID, attemptID, outboxPayload)
}

// maxRetries default helper.
func (l *LifecycleService) maxRetries() int {
	return 3
}
