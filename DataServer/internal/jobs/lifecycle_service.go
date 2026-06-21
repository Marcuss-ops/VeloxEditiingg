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
	"time"

	"velox-server/internal/platform/clock"
)

// LifecycleService validates and executes job status transitions.
type LifecycleService struct {
	jobsRepo Repository // canonical domain surface (PR15.5: sole write surface)
	clock    clock.Clock
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
			RetryCount:  j.Attempts,
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

// now resolves clock.Now and normalizes to UTC.
func (l *LifecycleService) now(t time.Time) time.Time {
	if t.IsZero() && l.clock != nil {
		t = l.clock.Now()
	}
	return t.UTC()
}
