// Package store / commands.go — PR 3 typed Command structs.
//
// Each Command carries the full identity tuple (JobID, WorkerID, LeaseID,
// Attempt, ExpectedRevision) needed for the new transactional JobRepository
// methods. Centralising them here keeps the repository interface contract
// narrow and forces every call site to declare its CAS assumptions up-front,
// rather than re-reading the job row in the service layer.
//
// Naming mirrors the spec: StartCommand, RenewLeaseCommand,
// RecordRenderFinishedCommand, FailCommand, CancelCommand.
package store

import "time"

// ── Command types used by PR3 transactional repository methods ─────────

// StartCommand captures the LEASED → RUNNING transition's full CAS tuple.
// Failure on any field raises ErrTransitionConflict so handlers can refuse
// stale JobAccepted messages.
type StartCommand struct {
	JobID            string
	WorkerID         string
	LeaseID          string
	Attempt          int
	ExpectedRevision int
	Now              time.Time // optional; defaults to clock.Now()
}

// RenewLeaseCommand extends the lease on an active job. The repository
// verifies (job_id, status IN {LEASED|RUNNING}, assigned_to, lease_id,
// revision) and bumps revision + lease_expiry atomically. The leased event
// (if any) is written in the same transaction.
//
// ExpectedRevision is REQUIRED: callers fetch the current jobs.revision
// (e.g. via JobRepository.GetJob) and supply it here. The repo's CAS
// rejects the renewal with ErrTransitionConflict on mismatch, which is
// how the spec's "renew con revision vecchia → conflict" test is
// driven deterministically. ExpectedRevision == 0 is treated as "skip
// the revision check" only when the caller explicitly opts in via
// SkipRevisionCAS (intended for one-off migrations; default false).
type RenewLeaseCommand struct {
	JobID            string
	WorkerID         string
	LeaseID          string
	ExpectedRevision int
	LeaseExpiry      time.Time
	Now              time.Time // optional; defaults to clock.Now()
	EmitEvent        bool      // if true, LEASE_RENEWED is logged in the same tx
	SkipRevisionCAS  bool      // false by default; tests + renewal helpers set true
}

// RecordRenderFinishedCommand is defined in jobs_writer_types.go.

// FailCommand terminates a job (FAILED) or schedules a retry (RETRY_WAIT).
// The repository decides which path based on Retryable + MaxRetries/RetryCount.
type FailCommand struct {
	JobID            string
	WorkerID         string
	LeaseID          string
	AttemptNumber    int
	ExpectedRevision int
	ErrorCode        string
	ErrorMessage     string
	Retryable        bool
	Now              time.Time
}

// MarkSucceededCommand has been REMOVED. SUCCEEDED is no longer
// writable through JobRepository. The single legal path is
// artifacts.FinalizationWriter.FinalizeVerified (internal/artifacts/
// sqlite_finalize_writer.go), requested via artifacts.Service.Finalize.
// Use artifacts.FinalizeVerifiedCommand + Service.Finalize.

// CancelCommand transitions a job to CANCELLED. Idempotent for terminal
// jobs (already CANCELLED / SUCCEEDED / FAILED).
type CancelCommand struct {
	JobID            string
	WorkerID         string // best-effort: empty = no worker identity check
	Reason           string
	ExpectedRevision int
	Now              time.Time
}

// Lease is the projected identity + expiry returned by ClaimNext so callers
// can present them transparently in HTTP/gRPC responses.
type Lease struct {
	LeaseID string
	Expires time.Time
}

// RequeueResult describes what happened to one zombie lease during
// RequeueExpiredLeases. The slice returned is bounded by `limit`.
type RequeueResult struct {
	JobID          string
	PreviousStatus JobStatus // LEASED or RUNNING
	NewStatus      JobStatus // PENDING, RETRY_WAIT, or FAILED
	Reason         string    // "expired_lease_no_retries_left" | "expired_lease_retry"
	Attempt        int       // attempt that just failed
}
