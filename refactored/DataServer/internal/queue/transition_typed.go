// Package queue / transition_typed.go
//
// PR1e — typed method signatures on TransitionService so callers can use a
// stable surface instead of constructing field-mutation maps. Each method
// delegates to an existing method on the same service or on the underlying
// SQLiteStore so the canonical implementation is unchanged; this layer is
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

	"velox-server/internal/dbutil"
	"velox-server/internal/store"
)

// ClaimJob atomically claims the next pending job for a worker.
//
// Underlying method: ClaimNextJob. This wrapper is a rename for the
// orchestrator's vocabulary (claim = select-and-mark). Returns nil, nil
// when no pending job is available.
func (ts *TransitionService) ClaimJob(ctx context.Context, workerID string, allowedJobTypes []string) (*Job, error) {
	return ts.ClaimNextJob(ctx, workerID, allowedJobTypes)
}

// StartJob claims a specific job for a worker, flipping status LEASED.
//
// Underlying method: LeaseJob. The transition PENDING→LEASED already
// validates via the state machine. Returns nil if the job is already in
// a terminal state (cf. LeaseJob semantics).
func (ts *TransitionService) StartJob(ctx context.Context, jobID, workerID string) error {
	return ts.LeaseJob(ctx, jobID, workerID)
}

// Note: RenewLease is intentionally NOT reimplemented here because
// TransitionService already exposes a canonically-typed RenewLease(ctx,
// jobID, workerID, leaseID, leaseExpiry time.Time) in transition.go —
// orchestrator callers already use that. The vocabulary mapping is
// documented in CLAUDE-orcdocs and the TODO list.

// RecordProgress appends a progress marker for a job by writing a
// job_events row with the (progress_pct, message) payload. Pct is clamped
// to [0, 100]. Returns the event timestamp on success.
//
// This is an event-log write only — it does NOT change the job status.
// Callers persist progress without flipping state.
func (ts *TransitionService) RecordProgress(ctx context.Context, jobID, workerID string, pct int, message string) (string, error) {
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
	if err := ts.dbStore.LogJobEvent(jobID, "job_progress", payload); err != nil {
		return "", err
	}
	return now, nil
}

// RequestCancel transitions a job to CANCELLED.
//
// Underlying: a typed wrapper around TransitionJobStatus (CAS on revision).
// Idempotent: a job already in CANCELLED/SUCCEEDED returns nil.
func (ts *TransitionService) RequestCancel(ctx context.Context, jobID string) error {
	m, err := ts.dbStore.GetJob(ctx, jobID)
	if err != nil {
		return err
	}
	job := MapToJob(m)
	if job.Status == StatusCancelled || job.Status == StatusSucceeded || job.Status == StatusFailed {
		return nil // idempotent — already terminal
	}
	if err := ts.Validate(job.Status, StatusCancelled); err != nil {
		return err
	}
	revision := dbutil.IntFromMap(m, "revision")
	if _, err := ts.dbStore.TransitionJobStatus(ctx, jobID, string(job.Status), string(StatusCancelled), revision); err != nil {
		return err
	}
	ts.dbStore.LogJobEvent(jobID, "job_cancelled", map[string]interface{}{})
	return nil
}

// FailAttempt marks the current attempt as failed without retrying.
//
// Underlying method: FailJob with requeue=false. Writes terminal FAILED
// status with the error message and the worker that failed the attempt.
// maxRetries is threaded through by the caller (orchestrator reads
// cfg.Workers.MaxJobAttempts via the orchestrator config).
func (ts *TransitionService) FailAttempt(ctx context.Context, jobID, errMsg, workerID string, maxRetries int) error {
	if maxRetries < 0 {
		maxRetries = 0
	}
	return ts.FailJob(ctx, jobID, errMsg, workerID, false, maxRetries)
}

// ScheduleRetry marks the current attempt as failed but leaves the job in
// RETRY_WAIT for the next worker to claim.
//
// Underlying method: FailJob with requeue=true. The structured retry
// counter is incremented as part of FailJob. maxRetries is the configured
// retry ceiling — orchestrator callers thread their own.
func (ts *TransitionService) ScheduleRetry(ctx context.Context, jobID, errMsg, workerID string, maxRetries int) error {
	if maxRetries < 0 {
		maxRetries = 0
	}
	return ts.FailJob(ctx, jobID, errMsg, workerID, true, maxRetries)
}

// FinalizeArtifact moves an artifact from VERIFYING to READY after the
// master has re-hashed the bytes. The actual SHA recomputation is not done
// here — ArtifactFinalizationService.FinalizeRender already performed that
// off-line. This method is the typed handle the orchestrator calls when
// it's done processing an upload completion.
//
// Underlying method: SQLiteStore.UpdateArtifactStatus. Returns the row
// count for logging; callers don't typically branch on it.
func (ts *TransitionService) FinalizeArtifact(ctx context.Context, artifactID string) error {
	return ts.dbStore.UpdateArtifactStatus(ctx, artifactID, "READY")
}

// CompleteJobTx is the orchestrator-tier success path. It performs the
// atomic SUCCEEDED + attempt-close + outbox in BEGIN IMMEDIATE.
//
// Underlying method: SQLiteStore.CompleteJobTx. attemptID=0 closes the
// latest attempt (callers from the worker know the attempt number they
// just created via store.InsertJobAttemptTx).
func (ts *TransitionService) CompleteJobTx(ctx context.Context, jobID string, attemptID int64, outboxPayload string) error {
	return ts.dbStore.CompleteJobTx(ctx, jobID, attemptID, outboxPayload)
}

// maxRetries default helper retained for callers that want a single
// canonical fallback (the configured value lives in
// cfg.Workers.MaxJobAttempts and is read directly by FileQueue).
// New code should pass the configured retry ceiling explicitly through
// FailAttempt / ScheduleRetry so cluster policy is honored.
func (ts *TransitionService) maxRetries() int {
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
	if ts.jobRepo == nil {
		return fmt.Errorf("transition service: job repository not wired (call SetJobRepository at startup)")
	}
	return ts.jobRepo.StartJob(ctx, params)
}
