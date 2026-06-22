// Package jobs lifecycle_service.go — LifecycleService moved from internal/queue.
//
// LifecycleService manages transactional job state transitions, requeueing,
// and queries. The struct, constructor, accessors, PR3 transactional
// mutators, and query helpers all live here so the canonical lifecycle
// contract reads in a single coherent linear scan.
//
// PR15.5: LifecycleService uses jobs.Repository (canonical domain
// surface) exclusively. The legacy store.JobRepository field, Repo()
// accessor, and dual-stack design are removed. All PR3 operations
// (Start/Fail/Cancel/RequeueExpiredLeases) go through jobs.Writer methods
// (Start/FailWithRetry/Cancel/RequeueExpiredLeases) which are implemented
// by the store.jobsAdapter wrapper.
package jobs

import (
	"context"
	"fmt"
	"log"
	"time"

	"velox-server/internal/platform/clock"
)

// LifecycleService validates and executes job status transitions.
type LifecycleService struct {
	jobsRepo Repository // canonical domain surface (PR15.5: sole write surface)
	clock    clock.Clock
	// reaperDisabled gates the Job-side zombie reaper (PR-13). During
	// the cutover period between migration 048 (which dropped
	// jobs.lease_expiry) and PR-05 (which introduces the TaskLeaseReaper
	// over tasks.lease_expires_at) the Job-level reaper is either broken,
	// references dropped columns, or operates on stale Job-level semantics.
	// Operators can flip this on at boot (VELOX_DISABLE_JOB_REAPER=true)
	// or after a one-shot schema probe detects an unsafe condition.
	reaperDisabled bool
}

// NewLifecycleService constructs the transactional LifecycleService.
// jobsRepo is the canonical jobs.Repository (Reader + Writer + PR3 methods).
func NewLifecycleService(jobsRepo Repository, c clock.Clock) (*LifecycleService, error) {
	if jobsRepo == nil {
		return nil, fmt.Errorf("jobs.Repository is required")
	}
	if c == nil {
		return nil, fmt.Errorf("clock is required")
	}
	return &LifecycleService{jobsRepo: jobsRepo, clock: c}, nil
}

// ── Accessors ──────────────────────────────────────────────────────────────

// Jobs exposes the canonical jobs.Repository (Reader + Writer) for
// callers that need domain-level read/write operations.
func (l *LifecycleService) Jobs() Repository { return l.jobsRepo }

// Clock returns the clock the service uses for time stamping.
func (l *LifecycleService) Clock() clock.Clock { return l.clock }

// ── PR 3 mutators ──────────────────────────────────────────────────────────

// Start validates and performs the LEASED → RUNNING transition via
// jobs.Writer.Start (full CAS tuple + history + event in one tx).
func (l *LifecycleService) Start(ctx context.Context, id, workerID, leaseID string, attempt, revision int) error {
	if id == "" || workerID == "" || leaseID == "" {
		return fmt.Errorf("lifecycle.Start: missing job/worker/lease identity")
	}
	return l.jobsRepo.Start(ctx, id, workerID, leaseID, attempt, revision)
}

// Fail validates and performs a retry-budget-aware failure via
// jobs.Writer.FailWithRetry. The repository decides FAILED vs RETRY_WAIT
// based on retryable flag and the job's retry_count/max_retries.
func (l *LifecycleService) Fail(ctx context.Context, id, errorCode, errorMessage string, retryable bool, revision int) error {
	if id == "" {
		return fmt.Errorf("lifecycle.Fail: empty jobID")
	}
	return l.jobsRepo.FailWithRetry(ctx, id, errorCode, errorMessage, retryable, revision)
}

// Cancel validates and transitions a job to CANCELLED via jobs.Writer.Cancel.
// Idempotent on already-terminal states.
func (l *LifecycleService) Cancel(ctx context.Context, id, reason string, revision int) error {
	if id == "" {
		return fmt.Errorf("lifecycle.Cancel: empty jobID")
	}
	return l.jobsRepo.Cancel(ctx, id, reason, revision)
}

// RequeueExpiredLeases processes expired leases via jobs.Writer.RequeueExpiredLeases.
// The reaper calls this on a timer (typically 60-300s) to recover jobs
// whose workers crashed without releasing the lease. limit <= 0 forces
// the safe default of 100.
func (l *LifecycleService) RequeueExpiredLeases(ctx context.Context, limit int) ([]RequeueResult, error) {
	if limit <= 0 {
		limit = 100
	}
	return l.jobsRepo.RequeueExpiredLeases(ctx, l.now(time.Time{}), limit)
}

// ── Queries ────────────────────────────────────────────────────────────────

// GetJobsByStatus returns all jobs with a given status via jobs.Reader.List.
func (l *LifecycleService) GetJobsByStatus(ctx context.Context, status Status) ([]*QueueItem, error) {
	domainJobs, err := l.jobsRepo.List(ctx, Filter{
		Statuses: []Status{Status(status)},
		Limit:    1000,
	})
	if err != nil {
		return nil, fmt.Errorf("job repo list by status: %w", err)
	}
	result := make([]*QueueItem, 0, len(domainJobs))
	for _, j := range domainJobs {
		// Build a minimal QueueItem from the canonical jobs.Job.
		job := &QueueItem{
			JobID:       j.ID,
			Status:      Status(j.Status),
			VideoName:   j.VideoName,
			ProjectID:   j.ProjectID,
			CreatedAt:   j.CreatedAt,
			UpdatedAt:   j.UpdatedAt,
			StartedAt:   j.StartedAt,
			CompletedAt: j.CompletedAt,
			MaxRetries:  j.MaxRetries,
		}
		result = append(result, job)
	}
	return result, nil
}

// GetNextJobID returns the next pending job ID via jobs.Reader.List.
func (l *LifecycleService) GetNextJobID(ctx context.Context) (string, error) {
	pending, err := l.jobsRepo.List(ctx, Filter{
		Statuses: []Status{StatusPending},
		Limit:    1,
	})
	if err != nil {
		return "", err
	}
	if len(pending) == 0 {
		return "", nil
	}
	return pending[0].ID, nil
}

// ── Internal helpers ───────────────────────────────────────────────────────

// ── PR-13: Job-side reaper gate (DEPRECATED — superseded by PR-05 TaskLeaseReaper) ──
//
// History: PR-13 introduced VELOX_DISABLE_JOB_REAPER (default off) as a stop-gap
// while jobs.lease_expiry was dropped by migration 048 and the canonical lease
// TTL moved to tasks via migration 049 + PR-05. With TaskLeaseReaper (in the
// taskgraph package) registering as a separate supervisor runner (see
// cmd/server/bootstrap.go), the Job-side zombie reaper is now redundant.
//
// The methods below are KEPT for back-compat with operators still relying on
// the env flag during cutover, but their behaviour has narrowed: DisableReaper
// emits a one-time DEPRECATED warning on first call so operators know to
// migrate; ReaperDisabled() / RequeueExpiredLeasesSafe() preserve the original
// semantic so existing supervisor code keeps working without changes.

// DisableReaper disables the Job-side zombie reaper. Idempotent. Called
// once at boot when VELOX_DISABLE_JOB_REAPER=true OR after a one-shot
// probe detects a post-048 unsafe condition (e.g. PostgreSQL mirror
// still references jobs.lease_expiry). PR-05 has now superseded this
// gate by introducing TaskLeaseReaper over tasks.lease_expires_at.
//
// DEPRECATED: the gate is now a no-op on the Job side — the canonical
// master-side lease enforcer is TaskLeaseReaper (registered as a
// separate supervisor runner). Operators should migrate to
// VELOX_TASK_LEASE_REAPER_DISABLED (not yet wired; planned) if they
// need to disable the canonical reaper.
func (l *LifecycleService) DisableReaper() {
	if l.reaperDisabled {
		return
	}
	l.reaperDisabled = true
	// Emit a one-time DEPRECATED warning so operators notice during
	// cutover audits. After this first call, the method is silent.
	log.Printf("[DEPRECATED] LifecycleService.DisableReaper: PR-13 gate is now a no-op as PR-05 TaskLeaseReaper is canonical; env VELOX_DISABLE_JOB_REAPER has no effect on the Job-side reaper.")
}

// ReaperDisabled reports whether the Job-side zombie reaper is currently
// disabled. The supervisor uses this to decide whether to log the
// boot-time disabled_until_cutover line exactly once.
//
// DEPRECATED: retained for back-compat; new supervisor code should
// check TaskLeaseReaper state instead.
func (l *LifecycleService) ReaperDisabled() bool {
	return l.reaperDisabled
}

// RequeueExpiredLeasesSafe is the supervisor-facing variant of
// RequeueExpiredLeases. It honours the PR-13 disable gate: when the
// gate is active, the call no-ops returning (nil, nil) without
// touching the database.
//
// DEPRECATED: the canonical lease enforcement is now TaskLeaseReaper.
// This method is retained so the existing supervisor goroutine keeps
// working during cutover; new callers should use TaskLeaseReaper.Run
// directly.
func (l *LifecycleService) RequeueExpiredLeasesSafe(ctx context.Context, limit int) ([]RequeueResult, error) {
	if l.reaperDisabled {
		return nil, nil
	}
	return l.RequeueExpiredLeases(ctx, limit)
}

// ── Internal helpers ───────────────────────────────────────────────────────

// now resolves clock.Now and normalizes to UTC.
func (l *LifecycleService) now(t time.Time) time.Time {
	if t.IsZero() && l.clock != nil {
		t = l.clock.Now()
	}
	return t.UTC()
}
