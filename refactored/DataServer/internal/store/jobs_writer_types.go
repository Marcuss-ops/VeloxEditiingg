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
	JobStatusPending        JobStatus = "PENDING"
	JobStatusLeased         JobStatus = "LEASED"
	JobStatusRunning        JobStatus = "RUNNING"
	JobStatusRenderFinished JobStatus = "RENDER_FINISHED"
	JobStatusAwaitingArtifact JobStatus = "AWAITING_ARTIFACT"
	JobStatusRetryWait      JobStatus = "RETRY_WAIT"
	JobStatusSucceeded      JobStatus = "SUCCEEDED"
	JobStatusFailed         JobStatus = "FAILED"
	JobStatusCancelled      JobStatus = "CANCELLED"
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

// StartJobParams encodes the LEASED → RUNNING transition.
//
// It carries the full identity of the worker that the master must verify
// atomically: JobID + WorkerID + LeaseID + Attempt + ExpectedRevision. The
// repository performs a single CAS UPDATE against (job_id, worker_id,
// lease_id, attempt, status=LEASED, revision=ExpectedRevision) and bumps
// the revision + writes a started_at timestamp on success. Mismatch on any
// field raises ErrTransitionConflict (so handleJobAccepted can refuse the
// acceptance and the worker can be told its view is stale).
//
// This is the missing piece that Phase 5 push-mode forgot: ClaimNext
// already created the lease, but nothing transitioned LEASED → RUNNING,
// so a fast-completing job could try LEASED → SUCCEEDED and fail.
type StartJobParams struct {
	JobID            string
	WorkerID         string
	LeaseID          string
	Attempt          int
	ExpectedRevision int
	Now              time.Time // optional; defaults to time.Now().UTC()
}

// CompleteJobParams encodes a RUNNING → terminal transition
// (SUCCEEDED, FAILED, or CANCELLED). Carries the same worker-identity
// CAS tuple as StartJobParams (JobID, WorkerID, LeaseID, Attempt,
// ExpectedRevision) so the master can refuse out-of-order or duplicate
// completes. The repository runs a single UPDATE matching (job_id,
// UPPER(status)=RUNNING|LEASED|RETRY_WAIT to cover recovery flows,
// assigned_to=WorkerID, lease_id=LeaseID, COALESCE(attempt,0)=Attempt,
// revision=ExpectedRevision) and writes completed_at + result fields.
// Mismatch on any field raises ErrTransitionConflict so workers can be
// asked to resubmit or the duplicate is filtered at the dispatcher.
//
// Unlike StartJob which only allows LEASED, CompleteJob accepts the
// job in any post-lease state so lost-but-completed workers can
// reconcile without a fresh StartJob round-trip.
type CompleteJobParams struct {
	JobID            string
	WorkerID         string
	LeaseID          string
	Attempt          int
	ExpectedRevision int
	FinalStatus      JobStatus // JobStatusSucceeded | JobStatusFailed | JobStatusCancelled
	ResultJSON       []byte    // persisted to result_json
	Now              time.Time // optional; defaults to time.Now().UTC()
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

// LeaseJobParams carries the information needed to lease a PENDING job to a worker.
type LeaseJobParams struct {
	JobID    string
	WorkerID string
}

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
	// StartJob performs the LEASED → RUNNING transition atomically. Verifies
	// the full worker-identity tuple (worker_id, lease_id, attempt) plus
	// revision CAS in a single UPDATE. Returns ErrTransitionConflict on any
	// mismatch so handleJobAccepted can refuse stale acceptances. Atomic.
	StartJob(ctx context.Context, params StartJobParams) error
	// CompleteJob performs the RUNNING → terminal transition (SUCCEEDED /
	// FAILED / CANCELLED) atomically. Verifies the full worker-identity
	// tuple plus revision CAS. Writes completed_at + result_json on success.
	// Returns ErrTransitionConflict on mismatch. Atomic.
	CompleteJob(ctx context.Context, params CompleteJobParams) error
	// ListByStatus returns up to limit jobs in any of the supplied statuses,
	// newest-updated first. limit <= 0 is treated as "all".
	ListByStatus(ctx context.Context, statuses []JobStatus, limit int) ([]Job, error)
	// RenewLease extends the lease on an active job atomically. Returns
	// ErrTransitionConflict if the job is not in a renewable state
	// (LEASED, RUNNING, PROCESSING).
	RenewLease(ctx context.Context, params RenewLeaseParams) error
	// LeaseJob atomically leases a PENDING job to a worker.
	// Sets status=LEASED, lease_id, assigned_to, retry_count++, updated_at.
	LeaseJob(ctx context.Context, jobID, workerID string) error
	// ReleaseClaim atomically resets a LEASED/RUNNING job back to PENDING
	// without incrementing retry count. Clears lease info.
	ReleaseClaim(ctx context.Context, jobID string) error
	// RequeueZombieJobs finds jobs in LEASED/RUNNING state with expired leases
	// and atomically requeues them to PENDING. Returns count of requeued jobs.
	RequeueZombieJobs(ctx context.Context, timeout time.Duration) (int, error)
	// UpdateJobResult writes the result_json blob for a job.
	// Used by UpdateJobFields for persisting the full operational state.
	UpdateJobResult(ctx context.Context, jobID string, resultJSON []byte) error

	// ── PR 3 — fully-transactional lifecycle methods ────────────────────────
	//
	// Every method below wraps its UPDATE + history INSERT + event INSERT
	// (+ outbox INSERT, when applicable) in a single BEGIN/COMMIT. The
	// legacy methods above remain available for backward-compatible
	// callers — they delegate to or are sandwiched-in with the same
	// single-method atomicity contract.

	// PR3Start performs the LEASED → RUNNING transition with the full
	// CAS tuple, plus a job_history INSERT and a JOB_STARTED event INSERT
	// in one tx. Returns ErrTransitionConflict on any predicate mismatch.
	PR3Start(ctx context.Context, cmd StartCommand) error

	// PR3RenewLease extends the lease atomically with a single UPDATE
	// matching (job_id, status IN {LEASED,RUNNING}, assigned_to, lease_id,
	// revision) and bumps revision. If cmd.EmitEvent is true, the
	// LEASE_RENEWED event INSERT happens in the same tx so a process
	// crash between commit and LogJobEvent leaves no orphan event or
	// stale lease. Returns ErrTransitionConflict if no rows matched.
	PR3RenewLease(ctx context.Context, cmd RenewLeaseCommand) error

	// PR3RecordRenderFinished moves RUNNING → RENDER_FINISHED with the
	// full CAS tuple, plus history INSERT + render_finished event in one
	// tx. Idempotent: if the job is already in RENDER_FINISHED, returns
	// nil and does not duplicate the event.
	PR3RecordRenderFinished(ctx context.Context, cmd RecordRenderFinishedCommand) error

	// PR3Fail marks a job FAILED or RETRY_WAIT depending on Retryable +
	// retry-count/max-retries. In one tx: UPDATE jobs, UPDATE
	// job_attempts (status=FAILED|FAILED_RETRYABLE), INSERT history,
	// INSERT event, INSERT outbox (JOB_FAILED or JOB_RETRY_SCHEDULED).
	// Idempotent on already-terminal states.
	PR3Fail(ctx context.Context, cmd FailCommand) error

	// PR3ScheduleRetry forces the job to RETRY_WAIT regardless of
	// retryable flag. Equivalent to PR3Fail with Retryable=true but
	// emits JOB_RETRY_SCHEDULED specifically. Same single-tx shape.
	PR3ScheduleRetry(ctx context.Context, cmd RetryCommand) error

	// PR3Cancel transitions a job to CANCELLED. Idempotent on terminal
	// states. Single-tx: UPDATE + history + event.
	PR3Cancel(ctx context.Context, cmd CancelCommand) error

	// PR3RequeueExpiredLeases processes up to `limit` LEASED/RUNNING
	// jobs whose lease_expiry < now. Each job is decided atomically:
	//   - retry budget left → PENDING (or RETRY_WAIT)
	//   - retry budget exhausted → FAILED
	// Returns the per-job RequeueResult slice and the total processed.
	// No foreign callers should requeue zombies via the old
	// RequeueZombieJobs path.
	PR3RequeueExpiredLeases(ctx context.Context, now time.Time, limit int) ([]RequeueResult, error)

	// PR3MarkSucceeded is the artifact-success-gate port. Only the
	// ArtifactSuccessGate created in bootstrap may call this method;
	// the public LifecycleService never exposes SUCCEEDED. Whichever
	// caller invokes it MUST hold the secret JobRepository reference
	// passed in via the bootstrap composition root.
	// Single-tx: UPDATE jobs → SUCCEEDED + completed_at, INSERT history,
	// INSERT event, INSERT outbox (JOB_SUCCEEDED).
	PR3MarkSucceeded(ctx context.Context, cmd MarkSucceededCommand) error
}

// Compile-time guard: every JobRepository implementation MUST satisfy the
// PR 3 surface. If you add a new method to the interface, every backend
// must implement it or the build will fail at this line.
var _ PR3Repository = (JobRepository)(nil)

// PR3Repository is the contract every JobRepository implementation must
// satisfy. It is the interface method set the bootstrap composition root
// relies on (so a future Postgres driver cannot drop PR3 support). The
// alias is split out so the `var _` guard line stays readable.
type PR3Repository interface {
	PR3Start(ctx context.Context, cmd StartCommand) error
	PR3RenewLease(ctx context.Context, cmd RenewLeaseCommand) error
	PR3RecordRenderFinished(ctx context.Context, cmd RecordRenderFinishedCommand) error
	PR3Fail(ctx context.Context, cmd FailCommand) error
	PR3ScheduleRetry(ctx context.Context, cmd RetryCommand) error
	PR3Cancel(ctx context.Context, cmd CancelCommand) error
	PR3RequeueExpiredLeases(ctx context.Context, now time.Time, limit int) ([]RequeueResult, error)
	PR3MarkSucceeded(ctx context.Context, cmd MarkSucceededCommand) error
}
