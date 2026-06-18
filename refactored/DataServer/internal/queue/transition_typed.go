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
	"fmt"
	"time"

	"velox-server/internal/store"
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
// expectedLeaseID != "" → second-line CAS on jobs.lease_id (PR-2, gRPC
// strict artifact-success-gate path). expectedRevision > 0 → second-line
// CAS on jobs.revision. LifecycleService callers (non-strict) pass
// "" / 0 to opt out.
func (l *LifecycleService) CompleteJobTx(ctx context.Context, jobID string, attemptID int64, outboxPayload string, expectedLeaseID string, expectedRevision int) error {
	return l.eventStore.CompleteJobTx(ctx, jobID, attemptID, outboxPayload, expectedLeaseID, expectedRevision)
}

// TransitionService provides typed transition methods on top of LifecycleService.
// It wraps LifecycleService to provide a stable API surface for callers like
// joblifecycle.Service and grpcserver.Handler.
type TransitionService struct {
	lc *LifecycleService
}

// NewTransitionService creates a new TransitionService.
func NewTransitionService(lc *LifecycleService) *TransitionService {
	return &TransitionService{lc: lc}
}

// SetLifecycleService wires the lifecycle service for deferred initialization.
func (ts *TransitionService) SetLifecycleService(lc *LifecycleService) {
	ts.lc = lc
}

// CompleteJob completes a job (delegates to LifecycleService).
func (ts *TransitionService) CompleteJob(ctx context.Context, jobID string) error {
	return ts.lc.CompleteJob(ctx, jobID)
}

// RecordRenderFinished records render completion without changing job status.
// Called by the gRPC handler when a worker reports success.
func (ts *TransitionService) RecordRenderFinished(ctx context.Context, cmd store.RecordRenderFinishedCommand) error {
	return ts.lc.RecordRenderFinished(ctx, cmd)
}

// RenewLease extends a lease (delegates to LifecycleService).
func (ts *TransitionService) RenewLease(ctx context.Context, jobID, workerID, leaseID string, leaseExpiry time.Time) error {
	return ts.lc.RenewLease(ctx, jobID, workerID, leaseID, leaseExpiry)
}

// FailJob marks a job as failed (delegates to LifecycleService).
func (ts *TransitionService) FailJob(ctx context.Context, jobID, errMsg, workerID string, requeue bool, maxRetries int) error {
	return ts.lc.FailJob(ctx, jobID, errMsg, workerID, requeue, maxRetries)
}

// GetJob retrieves a job by ID (delegates to eventStore).
func (ts *TransitionService) GetJob(ctx context.Context, jobID string) (*Job, error) {
	m, err := ts.lc.eventStore.GetJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	job := MapToJob(m)
	return job, nil
}

// Validate checks whether a transition is allowed (delegates to LifecycleService).
func (ts *TransitionService) Validate(from, to JobStatus) error {
	return ts.lc.Validate(from, to)
}

// ReleaseClaim releases a claimed job (delegates to LifecycleService).
func (ts *TransitionService) ReleaseClaim(ctx context.Context, jobID string) error {
	return ts.lc.ReleaseClaim(ctx, jobID)
}

// maxRetries default helper.
func (l *LifecycleService) maxRetries() int {
	return 3
}

// StartJobWithLease is the typed LEASED → RUNNING transition used by the
// gRPC handler when a JobAccepted arrives. It delegates to the wired
// JobRepository so the full identity (workerID, leaseID, attempt, revision)
// is checked atomically in a single CAS UPDATE (spec §5 single-method
// atomicity).
//
// Returning ErrTransitionConflict means one of: the lease was lost (expired
// or reassigned), the worker identity does not match, or the job's status
// is no longer LEASED. Callers should reject the JobAccepted at the gRPC
// layer with a "stale lease" signal so the worker drops its pending state.
func (ts *TransitionService) StartJobWithLease(ctx context.Context, params store.StartJobParams) error {
	if ts == nil {
		return fmt.Errorf("transition service: nil receiver")
	}
	if ts.lc == nil {
		return fmt.Errorf("transition service: lifecycle service not wired")
	}
	return ts.lc.jobRepo.StartJob(ctx, params)
}
