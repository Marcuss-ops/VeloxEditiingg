package store

import (
	"errors"
	"time"

	"velox-server/internal/costmodel"
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
	JobStatusPending          = jobs.StatusPending
	JobStatusLeased           = jobs.StatusLeased
	JobStatusRunning          = jobs.StatusRunning
	JobStatusAwaitingArtifact = jobs.StatusAwaitingArtifact
	JobStatusRetryWait        = jobs.StatusRetryWait
	JobStatusSucceeded        = jobs.StatusSucceeded
	JobStatusFailed           = jobs.StatusFailed
	JobStatusCancelled        = jobs.StatusCancelled
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
//
// PR #6: All 5 requirements fields live in dedicated columns only.
// They are read directly by claim paths (ClaimNextPendingJob,
// ClaimNextPendingJobForWorker) via reconstructRankRequirements.
// No JSON fallback exists — the _requirements sub-object was stripped
// from request_json/result_json by migration 047.
// PR #9: assigned_to, claimed_by, lease_id, lease_expiry, retry_count
// dropped from jobs table (migration 048). Runtime state lives on
// job_attempts + tasks now; lease identity flows through result_json.
type JobRecord struct {
	JobID       string    `json:"job_id"`
	Status      JobStatus `json:"status"`
	VideoName   string    `json:"video_name,omitempty"`
	ProjectID   string    `json:"project_id,omitempty"`
	Revision    int       `json:"revision"`
	MaxRetries  int       `json:"max_retries"`
	CreatedAt   string    `json:"created_at,omitempty"`
	UpdatedAt   string    `json:"updated_at,omitempty"`
	StartedAt   string    `json:"started_at,omitempty"`
	CompletedAt string    `json:"completed_at,omitempty"`
	RunID       string    `json:"run_id,omitempty"`
	PayloadJSON string    `json:"-"`

	// PR #6: per-job placement needs (canonical). All 5 fields live in
	// dedicated columns; no JSON fallback exists anymore.
	RequiredResourceClass    string  `json:"required_resource_class,omitempty"`
	RequiredTemporalMode     string  `json:"required_temporal_mode,omitempty"`
	RequiredDeterministic    bool    `json:"required_deterministic,omitempty"`
	RequiredCacheable        bool    `json:"required_cacheable,omitempty"`
	RequiredMinBandwidthMbps float64 `json:"required_min_bandwidth_mbps,omitempty"`
}

// PR #8: dead code after CreateJob was dropped from SQLiteJobRepository.
// The canonical creation path is now AtomicJobTaskCreator.CreateJobWithTask
// which reads job.Requirements from the *jobs.Job domain model directly.

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
	JobID        string                    `json:"job_id"`
	ResultJSON   []byte                    `json:"-"`
	Attempt      int                       `json:"attempt"`
	LeaseID      string                    `json:"lease_id,omitempty"`
	LeaseExpires time.Time                 `json:"lease_expires,omitempty"`
	Requirements costmodel.JobRequirements `json:"requirements,omitempty"`
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

// ClaimResultJSON is the typed representation of the result_json blob
// returned by ClaimNextPendingJob / ClaimNextPendingJobForWorker.  It
// replaces map[string]interface{} parsing in claimNext / ClaimNextForProfile.
type ClaimResultJSON struct {
	JobID       string  `json:"job_id"`
	Status      string  `json:"status"`
	LeaseID     string  `json:"lease_id"`
	LeaseExpiry string  `json:"lease_expiry"`
	Attempt     float64 `json:"attempt"` // JSON numbers unmarshal as float64
}
