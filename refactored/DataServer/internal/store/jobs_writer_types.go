package store

import (
	"context"
	"errors"
	"time"
)

// JobStatus is the storage-string state of a job in the queue. Spec §5 lists
// the canonical set in conceptual form; here we map them to typed constants.
//
// The canonical set is Pending / Leased / Running / RetryWait / Succeeded /
// Failed / Cancelled. Two legacy strings — Processing ("PROCESSING") and
// Completed ("COMPLETED") — coexist on disk because older migrations and
// HTTP-handler paths still write them. They are NOT aliases of Running /
// Succeeded: they are distinct values. New code MUST use the canonical
// constants; filters that must catch both old and new have to list both
// values explicitly. ListByStatus does not collapse them — what is on disk
// is the truth.
type JobStatus string

const (
	JobStatusPending   JobStatus = "PENDING"
	JobStatusLeased    JobStatus = "LEASED"
	JobStatusRunning   JobStatus = "RUNNING"
	JobStatusRetryWait JobStatus = "RETRY_WAIT"
	JobStatusSucceeded JobStatus = "SUCCEEDED"
	JobStatusFailed    JobStatus = "FAILED"
	JobStatusCancelled JobStatus = "CANCELLED"

)

// IsTerminal reports whether a job in this state has finished its lifecycle.
func (s JobStatus) IsTerminal() bool {
	switch s {
	case JobStatusSucceeded, JobStatusFailed, JobStatusCancelled:
		return true
	}
	return false
}

// Job is a structural projection sufficient for read paths of the JobRepository.
// It deliberately omits the per-job payload blob, history, and tail of legacy
// mutable state — keep richer types in package queue for now, so consumer reads
// can stay narrow while migrators catch up.
type Job struct {
	JobID       string    `json:"job_id"`
	Status      JobStatus `json:"status"`
	VideoName   string    `json:"video_name,omitempty"`
	ProjectID   string    `json:"project_id,omitempty"`
	AssignedTo  string    `json:"assigned_to,omitempty"`
	LeaseID     string    `json:"lease_id,omitempty"`
	Revision    int       `json:"revision"`
	RetryCount  int       `json:"retry_count"`
	MaxRetries  int       `json:"max_retries"`
	CreatedAt   string    `json:"created_at,omitempty"`
	UpdatedAt   string    `json:"updated_at,omitempty"`
	StartedAt   string    `json:"started_at,omitempty"`
	CompletedAt string    `json:"completed_at,omitempty"`
}

// CreateJobParams is the input for JobRepository.CreateJob.
//
// JobID may be empty; the repository must generate a unique ID in that case.
// The rich payload map maps 1:1 onto the immutable request_json blob on disk.
type CreateJobParams struct {
	JobID      string                 `json:"job_id,omitempty"`
	Payload    map[string]interface{} `json:"payload"`
	VideoName  string                 `json:"video_name,omitempty"`
	ProjectID  string                 `json:"project_id,omitempty"`
	MaxRetries int                    `json:"max_retries"`
}

// ClaimParams carries worker identity and the timestamp at claim time.
// AllowedJobTypes is the worker capability filter — empty means "no filter".
type ClaimParams struct {
	WorkerID        string
	AllowedJobTypes []string
	Now             time.Time
}

// ClaimResult is what a successful ClaimNext returns.
//
// ResultJSON is the canonical opaque blob the worker should echo back on
// complete/fail; LeaseID and LeaseExpires are exposed separately so callers
// can present them in clear surface areas (e.g. the v2 HTTP contract).
type ClaimResult struct {
	JobID        string    `json:"job_id"`
	ResultJSON   []byte    `json:"-"`
	Attempt      int       `json:"attempt"`
	LeaseID      string    `json:"lease_id,omitempty"`
	LeaseExpires time.Time `json:"lease_expires,omitempty"`
}

// RenewLeaseParams carries the information needed to extend a job lease.
// The repository validates internally that the job is in a renewable status
// (LEASED, RUNNING, PROCESSING) and bumps the revision atomically.
// WorkerID is recorded for audit purposes.
type RenewLeaseParams struct {
	JobID       string
	WorkerID    string
	LeaseID     string
	LeaseExpiry time.Time
}

// TransitionParams encodes a CAS-style state change.
//
// ExpectedStatus is the status the caller last observed; NewStatus is the
// desired successor; Revision is the optimistic-locking counter. Both fields
// are evaluated atomically inside the repository — callers do not see
// BeginTx/Commit.
type TransitionParams struct {
	JobID          string
	ExpectedStatus JobStatus
	NewStatus      JobStatus
	Revision       int
}

// ErrTransitionConflict is returned when the CAS predicate does not match
// (ExpectedStatus wrong OR Revision stale). The repository layer raises this
// so callers can distinguish it from infrastructure errors. SQLiteStore.
// TransitionJobStatus and SQLiteJobRepository.Transition both wrap this
// sentinel with %w so errors.Is works.
var ErrTransitionConflict = errors.New("store: job transition conflict (status or revision mismatch)")

// ErrNoClaimableJob is returned by JobRepository.ClaimNext when no job in
// PENDING/QUEUED state matches the caller's filter. Distinct from a generic
// driver error so callers can fall back without re-trying blindly.
var ErrNoClaimableJob = errors.New("store: no claimable job available")

// JobRepository is the narrow write-aware contract for job persistence (spec §5).
//
// Atomicity rule (spec): every multi-row operation stays a single method.
// Callers never see BeginTx/Commit. Backends (SQLite, future Postgres) MUST
// guarantee per-method atomicity even on driver errors.
//
// Each backend also exposes a broader permissively-readable surface on the
// underlying concrete store (*SQLiteStore) for read paths that haven't migrated
// yet — see store_jobs.go (JobsRepository), used by HTTP handlers.
type JobRepository interface {
	// CreateJob inserts a new job in PENDING state. Atomic. If JobID is empty,
	// the repository assigns one and returns nil; otherwise the caller-supplied
	// ID is used verbatim.
	CreateJob(ctx context.Context, job CreateJobParams) error
	// GetJob returns a single job projection, or (nil, nil) on missing.
	GetJob(ctx context.Context, jobID string) (*Job, error)
	// ClaimNext atomically marks the next claimable job as leased-to-worker and
	// returns a ClaimResult. Returns (nil, ErrNoClaimableJob) when nothing
	// matches. Backends must NOT then read partial state — either fully commit
	// the claim or roll back to no-op.
	ClaimNext(ctx context.Context, claim ClaimParams) (*ClaimResult, error)
	// Transition performs a CAS status change, returning ErrTransitionConflict
	// if the precondition does not hold. Atomic.
	Transition(ctx context.Context, transition TransitionParams) error
	// ListByStatus returns up to limit jobs in any of the supplied statuses,
	// newest-updated first. limit <= 0 is treated as "all".
	ListByStatus(ctx context.Context, statuses []JobStatus, limit int) ([]Job, error)
	// RenewLease extends the lease on an active job atomically. Returns
	// ErrTransitionConflict if the job is not in a renewable state
	// (LEASED, RUNNING, PROCESSING).
	RenewLease(ctx context.Context, params RenewLeaseParams) error
}
