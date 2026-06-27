package jobs

import (
	"context"
	"time"

	"velox-server/internal/costmodel"
)

// Filter narrows list queries on the Reader surface.
// Zero-value means "no filter" — all jobs are returned.
type Filter struct {
	Statuses []Status // empty = all statuses
	Limit    int      // 0 = all
}

// Counts is the aggregate count by status returned by Reader.Counts.
type Counts map[Status]int64

// ── Domain parameter types for Writer methods ──────────────────────────────

// ClaimNextResult is the result of a successful ClaimNext call.
//
// Requirements is the per-job placement needs read from dedicated
// columns at claim time (PR #6). Populated symmetrically on
// ClaimNext + ClaimNextForProfile so the dispatcher
// (sendPushJobOffer) can read claimResult.Requirements directly
// without bouncing through jobs.Writer.Get.
type ClaimNextResult struct {
	JobID        string                    `json:"job_id"`
	Attempt      int                       `json:"attempt"`
	LeaseID      string                    `json:"lease_id,omitempty"`
	LeaseExpires time.Time                 `json:"lease_expires,omitempty"`
	Requirements costmodel.JobRequirements `json:"requirements,omitempty"`
}

// RequeueResult reports the outcome of a single expired-lease requeue.
type RequeueResult struct {
	JobID          string `json:"job_id"`
	PreviousStatus Status `json:"previous_status"`
	NewStatus      Status `json:"new_status"`
	Reason         string `json:"reason"`
	Attempt        int    `json:"attempt"`
}

// Reader is the read-only job query surface.
// Implemented by store.SQLiteJobRepository.
type Reader interface {
	// Get returns a single job by ID, or (nil, nil) on missing.
	Get(ctx context.Context, id string) (*Job, error)

	// List returns jobs matching the filter.
	List(ctx context.Context, filter Filter) ([]Job, error)

	// Counts returns the aggregate count of jobs grouped by status.
	Counts(ctx context.Context) (Counts, error)
}

// Writer is the canonical write-only job mutation surface.
// Implemented by store.SQLiteJobRepository.
//
// PR15.5: Start / RenewLease / FailWithRetry / Cancel / RequeueExpiredLeases /
// ClaimNext / ReleaseLease / RecordRenderFinished promoted from the legacy
// store.JobRepository. The legacy interface is dropped — Writer is now the
// single canonical write surface.
type Writer interface {
	// SetStatus performs a CAS status change from → to.
	// Returns ErrTransitionConflict if the precondition does not hold.
	SetStatus(ctx context.Context, id string, from, to Status) error

	// Lease atomically assigns a PENDING job to a worker.
	// Returns ErrTransitionConflict if the job is not in PENDING.
	Lease(ctx context.Context, id, workerID string) error

	// Fail marks a job FAILED and records the reason.
	// Simple fail — use FailWithRetry for retry-budget-aware failure.
	Fail(ctx context.Context, id string, reason string) error

	// Start atomically transitions LEASED → RUNNING, verifying the full
	// worker-identity CAS tuple (workerID, leaseID, attempt, revision).
	// Inserts history + JOB_STARTED event in the same tx.
	// Returns ErrTransitionConflict on any predicate mismatch.
	Start(ctx context.Context, id, workerID, leaseID string, attempt, revision int) error

	// RenewLease extends the lease on an active job (LEASED or RUNNING).
	// Verifies the worker-identity CAS tuple. When emitEvent is true,
	// a LEASE_RENEWED event is inserted in the same tx.
	// Returns ErrTransitionConflict if no rows matched.
	RenewLease(ctx context.Context, id, workerID, leaseID string, expiry time.Time, emitEvent bool, revision int) error

	// FailWithRetry marks a job FAILED or RETRY_WAIT depending on
	// retryable AND retry budget (retry_count < max_retries).
	// In one tx: UPDATE jobs, INSERT history, INSERT event, INSERT outbox.
	//
	// cleanup/remove-job-attempts-runtime: the legacy
	// `UPDATE job_attempts` is no longer part of this method.
	// Per-attempt close-out is now driven by
	// taskingestion.Ingest.TransitionTaskToTerminalAtomic on
	// task_attempts (canonical layer). See
	// internal/taskingestion and the doc above FailWithRetry's
	// implementation in internal/store/sqlite_jobs_writer.go.
	// Returns ErrTransitionConflict on revision mismatch.
	FailWithRetry(ctx context.Context, id, errorCode, errorMessage string, retryable bool, revision int) error

	// Cancel transitions a job to CANCELLED. Idempotent on terminal
	// states. Single-tx: UPDATE + history + event.
	Cancel(ctx context.Context, id, reason string, revision int) error

	// RequeueExpiredLeases processes up to `limit` LEASED/RUNNING jobs
	// whose lease has expired. Jobs with retry budget left go to PENDING;
	// jobs with exhausted budget go to FAILED. Returns per-job results.
	RequeueExpiredLeases(ctx context.Context, now time.Time, limit int) ([]RequeueResult, error)

	// ClaimNext atomically claims the next PENDING job for a worker.
	// Returns (nil, ErrNoClaimableJob) when nothing matches.
	ClaimNext(ctx context.Context, workerID string, allowedJobTypes []string) (*ClaimNextResult, error)

	// ClaimNextForProfile is the cost-rank path (PR-04.6). It loads
	// up to maxCandidates PENDING jobs whose job_type matches
	// allowedJobTypes, scores each against the supplied
	// costmodel.WorkerProfile, filters Eligible=true, and CAS-claims
	// the lowest-scored (best-fit) candidate.
	//
	// Race-safe: if the CAS fails for the top-scored candidate
	// (e.g., another worker raced the row), the runner-up is tried
	// in Score-sorted order. Returns ErrNoClaimableJob only when no
	// candidate was eligible OR every CAS attempt failed.
	//
	// maxCandidates > 100 is clamped to 100 for safety; <= 0
	// defaults to 20 (covers the typical pending backlog while
	// keeping the per-worker dispatch path bounded).
	ClaimNextForProfile(ctx context.Context, workerID string, allowedJobTypes []string, profile costmodel.WorkerProfile, maxCandidates int) (*ClaimNextResult, error)

	// ReleaseLease atomically resets a LEASED/RUNNING job back to
	// PENDING without incrementing retry count. Clears lease fields.
	ReleaseLease(ctx context.Context, id string) error

	// RecordRenderFinished verifies the worker-identity CAS tuple,
	// updates the attempt to RENDER_FINISHED, and inserts a
	// RENDER_FINISHED event in one tx. The job stays RUNNING.
	// Returns ErrTransitionConflict on mismatch.
	RecordRenderFinished(ctx context.Context, id, workerID, leaseID string, attempt, revision int) error

	// Delete hard-deletes a job and its supplementary rows from persistence.
	// Returns no error if the job is already gone (idempotent).
	Delete(ctx context.Context, id string) error
}

// Repository combines Reader and Writer into a single job persistence contract.
// There must be exactly ONE concrete implementation — store.SQLiteJobRepository.
//
// This is the canonical interface for job persistence (Ondata 3 complete,
// PR15.5 promoted PR3 write paths).
//
// Compile-time assertion:
//
//	var _ Repository = (*store.SQLiteJobRepository)(nil)
type Repository interface {
	Reader
	Writer
}
