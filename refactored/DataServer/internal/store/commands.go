// Package store / commands.go — PR 3 typed Command structs.
//
// Each Command carries the full identity tuple (JobID, WorkerID, LeaseID,
// Attempt, ExpectedRevision) needed for the new transactional JobRepository
// methods. Centralising them here keeps the repository interface contract
// narrow and forces every call site to declare its CAS assumptions up-front,
// rather than re-reading the job row in the service layer.
//
// Naming mirrors the spec: CreateJobCommand, ClaimCommand, StartCommand,
// RenewLeaseCommand, RecordRenderFinishedCommand, FailCommand, RetryCommand,
// CancelCommand, MarkSucceededCommand (the artifact-private port).
package store

import "time"

// CreateJobCommand seeds a new job in PENDING state. JobID may be empty;
// the repository assigns one in that case (UUID for SQLite).
type CreateJobCommand struct {
	JobID      string                 `json:"job_id,omitempty"`
	Payload    map[string]interface{} `json:"payload"`
	VideoName  string                 `json:"video_name,omitempty"`
	ProjectID  string                 `json:"project_id,omitempty"`
	RunID      string                 `json:"run_id,omitempty"`
	MaxRetries int                    `json:"max_retries"`
}

// ClaimCommand captures the worker identity used to claim the next
// PENDING job. Now is mandatory so the repository can stamp the lease
// deterministically (callers in tests pass a MockClock value).
type ClaimCommand struct {
	WorkerID        string
	AllowedJobTypes []string
	Now             time.Time
}

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

// RetryCommand is the explicit ScheduleRetry path — equivalent to FailCommand
// with Retryable=true forced. Kept as a separate method so the repository
// can emit a JOB_RETRY_SCHEDULED outbox event instead of JOB_FAILED.
type RetryCommand struct {
	JobID            string
	WorkerID         string
	LeaseID          string
	AttemptNumber    int
	ExpectedRevision int
	ErrorCode        string
	ErrorMessage     string
	Now              time.Time
}

// MarkSucceededCommand has been REMOVED in PR 3.5-a. SUCCEEDED is no
// longer writable through JobRepository. The single legal path is
// artifacts.FinalizationRepository.FinalizeVerified (internal/artifacts/
// sqlite_finalization_repository.go), requested via artifacts.Service.Finalize.
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
