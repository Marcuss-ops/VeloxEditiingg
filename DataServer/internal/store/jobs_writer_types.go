package store

import (
	"errors"
	"time"

	"velox-server/internal/jobs"
)

// JobStatus is a type alias for the canonical jobs.Status. It exists so
// existing callers importing store.JobStatus continue to compile without
// changes while the type itself is unified at compile time with jobs.Status.
//
// All status constants are re-exported aliases from the jobs package.
// New code should import and use jobs.Status / jobs.StatusPending directly.
type JobStatus = jobs.Status

const (
	JobStatusPending   = jobs.StatusPending
	JobStatusLeased    = jobs.StatusLeased
	JobStatusRunning   = jobs.StatusRunning
	JobStatusRetryWait = jobs.StatusRetryWait
	JobStatusSucceeded = jobs.StatusSucceeded
	JobStatusFailed    = jobs.StatusFailed
	JobStatusCancelled = jobs.StatusCancelled
)

// IsTerminal delegates to the canonical jobs.Status.IsTerminal().
// Kept as a package-level function for backward compatibility with
// callers that use store.JobStatus.IsTerminal() — since JobStatus is
// a type alias, jobs.Status.IsTerminal() is automatically available.

// JobRecord is the DB-row representation of a job. It carries all SQL
// columns including raw JSON blobs (PayloadJSON). This is NOT the domain
// model — see internal/jobs.Job for the canonical business aggregate.
//
// PR15.5: the legacy `type Job = JobRecord` alias was removed. All callers
// now reference JobRecord directly.
type JobRecord struct {
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
	RunID       string    `json:"run_id,omitempty"`
	PayloadJSON string    `json:"-"`
}

// CreateJobParams is the input for CreateJob.
//
// JobID may be empty; the repository must generate a unique ID in that case.
// The rich payload map maps 1:1 onto the immutable request_json blob on disk.
type CreateJobParams struct {
	JobID      string                 `json:"job_id,omitempty"`
	Payload    map[string]interface{} `json:"payload"`
	VideoName  string                 `json:"video_name,omitempty"`
	ProjectID  string                 `json:"project_id,omitempty"`
	RunID      string                 `json:"run_id,omitempty"`
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

// StartJobParams encodes the LEASED → RUNNING transition.
//
// It carries the full identity of the worker that the master must verify
// atomically: JobID + WorkerID + LeaseID + Attempt + ExpectedRevision.
// Test callers use this directly on *SQLiteJobRepository.
type StartJobParams struct {
	JobID            string
	WorkerID         string
	LeaseID          string
	Attempt          int
	ExpectedRevision int
	Now              time.Time // optional; defaults to time.Now().UTC()
}

// CompleteJobParams encodes a RUNNING → terminal transition
// (SUCCEEDED, FAILED, or CANCELLED) with full worker-identity CAS tuple.
// Test callers use this directly on *SQLiteJobRepository.
type CompleteJobParams struct {
	JobID            string
	WorkerID         string
	LeaseID          string
	Attempt          int
	ExpectedRevision int
	FinalStatus      JobStatus
	ResultJSON       []byte
	Now              time.Time
}

// ── Shared error sentinels ────────────────────────────────────────────────

// ErrTransitionConflict is returned when the CAS predicate does not match
// (ExpectedStatus wrong OR Revision stale).
var ErrTransitionConflict = errors.New("store: job transition conflict (status or revision mismatch)")

// ErrNoClaimableJob is returned by ClaimNext when no job in PENDING state
// matches the caller's filter.
var ErrNoClaimableJob = errors.New("store: no claimable job available")

// RecordRenderFinishedCommand carries the identity tuple for recording render
// completion (used by PR3RecordRenderFinished).
type RecordRenderFinishedCommand struct {
	JobID            string
	WorkerID         string
	LeaseID          string
	AttemptNumber    int
	ExpectedRevision int
	FinishedAt       time.Time
}

// ErrRecordRenderFinishedNotFound is returned when the attempt to record
// render finished cannot find a matching attempt row.
var ErrRecordRenderFinishedNotFound = errors.New("store: render finished attempt not found")
