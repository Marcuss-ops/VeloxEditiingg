// Package queue / service.go — consolidated LifecycleService.
//
// LifecycleService manages transactional job state transitions, requeueing,
// and queries. The struct, constructor, accessors, PR3 transactional
// mutators, and query helpers all live here so the canonical lifecycle
// contract reads in a single coherent linear scan.
//
// Dual-stack design (Ondata 3 PR3):
//   - repo   (store.JobRepository): legacy PR3 surface (PR3Start, PR3Fail,
//     PR3Cancel, PR3RequeueExpiredLeases). Still needed for the
//     multi-row transactional envelopes not yet migrated to the domain
//     interface.
//   - jobsRepo (jobs.Repository): canonical domain surface (Get, List,
//     Counts, Create, SetStatus, Lease, Fail). New code and simple
//     read/write paths use this; complex PR3 operations continue to use
//     repo until the migration completes (future PR). Both are satisfied
//     by the same concrete *store.SQLiteJobRepository.
//
// The legacy non-transactional methods (FailJob, SubmitJob,
// TransitionToRunning, LeaseJob, ReleaseClaim, RenewLease,
// RequeueZombieJobs, RecordRenderFinished, Validate) have been removed —
// zero external callers remained.
package queue

import (
	"context"
	"fmt"
	"time"

	"velox-server/internal/jobs"
	"velox-server/internal/platform/clock"
	"velox-server/internal/store"
)

// LifecycleService validates and executes job status transitions.
type LifecycleService struct {
	repo     store.JobRepository // legacy PR3 surface
	jobsRepo jobs.Repository     // canonical domain surface (Ondata 3 PR3)
	clock    Clock
}

// NewLifecycleService constructs the transactional LifecycleService.
// Both repo and jobsRepo are required; they are typically the same concrete
// *store.SQLiteJobRepository (which implements both interfaces).
// Returns an error (not a panic) so bootstrap can surface configuration
// mistakes via the standard error path.
func NewLifecycleService(repo store.JobRepository, jobsRepo jobs.Repository, c clock.Clock) (*LifecycleService, error) {
	if repo == nil {
		return nil, fmt.Errorf("job repository is required")
	}
	if jobsRepo == nil {
		return nil, fmt.Errorf("jobs.Repository is required")
	}
	if c == nil {
		return nil, fmt.Errorf("clock is required")
	}
	return &LifecycleService{repo: repo, jobsRepo: jobsRepo, clock: c}, nil
}

// ── Accessors ──────────────────────────────────────────────────────────────

// Repo exposes the underlying store.JobRepository for callers that need
// legacy PR3 operations (ClaimNext, StartJob, PR3RecordRenderFinished,
// PR3RenewLease, ReleaseClaim, etc.). These methods will be migrated to
// the canonical jobs.Repository in a future PR.
//
// NOTE: PR 3.5-a removed the previous ArtifactSuccessGate that wrapped
// this repo; the FinalizationRepository contract is now the single
// legal writer of jobs.status = 'SUCCEEDED' (see
// internal/artifacts/sqlite_finalization_repository.go).
func (l *LifecycleService) Repo() store.JobRepository { return l.repo }

// Jobs exposes the canonical jobs.Repository (Reader + Writer) for
// callers that need domain-level read/write operations. This is the
// recommended surface for new code and for simple Get/Create/Lease/Fail
// calls that don't require the PR3 transaction envelope.
//
// Concrete type: *store.SQLiteJobRepository (same instance as repo).
func (l *LifecycleService) Jobs() jobs.Repository { return l.jobsRepo }

// Clock returns the clock the service uses for time stamping.
func (l *LifecycleService) Clock() clock.Clock { return l.clock }

// ── PR 3 mutators ──────────────────────────────────────────────────────────

// Start delegates to JobRepository.PR3Start. The service validates that
// cmd is non-empty then forwards to the repo, which atomically performs
// the LEASED → RUNNING transition + history + event in one tx.
func (l *LifecycleService) Start(ctx context.Context, cmd store.StartCommand) error {
	if cmd.JobID == "" || cmd.WorkerID == "" || cmd.LeaseID == "" {
		return fmt.Errorf("lifecycle.Start: missing job/worker/lease identity")
	}
	if cmd.Now.IsZero() {
		cmd.Now = l.now(cmd.Now)
	}
	return l.repo.PR3Start(ctx, cmd)
}

// Fail delegates to JobRepository.PR3Fail. The repository decides
// FAILED vs RETRY_WAIT based on cmd.Retryable and the jobs row's
// retry_count/max_retries.
func (l *LifecycleService) Fail(ctx context.Context, cmd store.FailCommand) error {
	if cmd.JobID == "" {
		return fmt.Errorf("lifecycle.Fail: empty jobID")
	}
	if cmd.Now.IsZero() {
		cmd.Now = l.now(cmd.Now)
	}
	return l.repo.PR3Fail(ctx, cmd)
}

// Cancel delegates to JobRepository.PR3Cancel. Idempotent on already-
// terminal states.
func (l *LifecycleService) Cancel(ctx context.Context, cmd store.CancelCommand) error {
	if cmd.JobID == "" {
		return fmt.Errorf("lifecycle.Cancel: empty jobID")
	}
	if cmd.Now.IsZero() {
		cmd.Now = l.now(cmd.Now)
	}
	return l.repo.PR3Cancel(ctx, cmd)
}

// RequeueExpiredLeases delegates to JobRepository.PR3RequeueExpiredLeases.
// The reaper calls this on a timer (typically 60-300s) to recover jobs
// whose workers crashed without releasing the lease. limit <= 0 forces
// the safe default of 100.
func (l *LifecycleService) RequeueExpiredLeases(ctx context.Context, limit int) ([]store.RequeueResult, error) {
	if limit <= 0 {
		limit = 100
	}
	return l.repo.PR3RequeueExpiredLeases(ctx, l.now(time.Time{}), limit)
}

// ── Queries ────────────────────────────────────────────────────────────────

// GetJobsByStatus returns all jobs with a given status via JobRepository.
func (l *LifecycleService) GetJobsByStatus(ctx context.Context, status JobStatus) ([]*Job, error) {
	storeJobs, err := l.repo.ListByStatus(ctx, []store.JobStatus{toStoreJobStatus(status)}, 1000)
	if err != nil {
		return nil, fmt.Errorf("job repo list by status: %w", err)
	}
	result := make([]*Job, 0, len(storeJobs))
	for _, sj := range storeJobs {
		// Build a minimal queue.Job from the store.JobRecord projection.
		job := &Job{
			JobID:       sj.JobID,
			Status:      JobStatus(sj.Status),
			VideoName:   sj.VideoName,
			ProjectID:   sj.ProjectID,
			CreatedAt:   sj.CreatedAt,
			UpdatedAt:   sj.UpdatedAt,
			StartedAt:   sj.StartedAt,
			CompletedAt: sj.CompletedAt,
			RetryCount:  sj.RetryCount,
			MaxRetries:  sj.MaxRetries,
		}
		result = append(result, job)
	}
	return result, nil
}

// GetNextJobID returns the next pending job ID.
func (l *LifecycleService) GetNextJobID(ctx context.Context) (string, error) {
	pending, err := l.repo.ListByStatus(ctx, []store.JobStatus{store.JobStatusPending}, 1)
	if err != nil {
		return "", err
	}
	if len(pending) == 0 {
		return "", nil
	}
	return pending[0].JobID, nil
}

// ── Internal helpers ───────────────────────────────────────────────────────

// now resolves cmd.Now || clock.Now and normalizes to UTC.
func (l *LifecycleService) now(t time.Time) time.Time {
	if t.IsZero() && l.clock != nil {
		t = l.clock.Now()
	}
	return t.UTC()
}

// toStoreJobStatus maps a queue.JobStatus to the equivalent store.JobStatus.
func toStoreJobStatus(s JobStatus) store.JobStatus {
	return store.JobStatus(s)
}
