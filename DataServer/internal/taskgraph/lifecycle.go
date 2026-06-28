// Package taskgraph defines the canonical Task domain model for distributed
// rendering. A Task is the unit of work assigned to a single worker execution.
//
// LifecycleService manages transactional task state transitions.
// Repository is the canonical persistence contract.
package taskgraph

import (
	"context"
	"fmt"
	"time"

	"velox-server/internal/jobs"
)

// LifecycleService manages transactional task state transitions.
type LifecycleService struct {
	repo Repository
	// jobsRepo is the canonical jobs-side surface ExpireTaskLease uses
	// to read retry budgets (jobs.max_retries) and post-commit-update
	// the Job aggregate when retries are exhausted. PR-04 wires this
	// post-commit so the reap atomic stays single-domain.
	jobsRepo JobsRetryQuerier
	// now is overridable for tests; nil falls back to time.Now().UTC().
	now func() time.Time
}

// JobsRetryQuerier is the narrow jobs-side surface ExpireTaskLease uses.
// Defined as an interface so LifecycleService has no hard dependency on
// the jobs.Repository surface and tests can substitute a stub.
type JobsRetryQuerier interface {
	// Get returns the canonical Job by ID, or (nil, nil) on missing.
	Get(ctx context.Context, id string) (*jobs.Job, error)
	// Fail marks the job FAILED with the given reason.
	Fail(ctx context.Context, id string, reason string) error
}

// NewLifecycleService constructs the transactional LifecycleService.
func NewLifecycleService(repo Repository) (*LifecycleService, error) {
	if repo == nil {
		return nil, fmt.Errorf("taskgraph.Repository is required")
	}
	return &LifecycleService{repo: repo, now: func() time.Time { return time.Now().UTC() }}, nil
}

// SetJobsRepo wires the jobs-side retry querier into the lifecycle
// service. Optional: when not set, ExpireTaskLease works without Job
// aggregate updates (the audit's "update Job aggregate when retries
// exhausted" step is skipped, all reaped tasks revert to READY).
func (l *LifecycleService) SetJobsRepo(jobsRepo JobsRetryQuerier) {
	l.jobsRepo = jobsRepo
}

// SetClock overrides the now() function used by ExpireTaskLease's
// post-commit jobsRepo path. Tests use this to drive deterministic
// timestamps. nil is a no-op (production keeps the default).
func (l *LifecycleService) SetClock(now func() time.Time) {
	if now != nil {
		l.now = now
	}
}

// Repo exposes the canonical taskgraph.Repository.
func (l *LifecycleService) Repo() Repository { return l.repo }

// CreateTask creates a new task in PENDING state.
func (l *LifecycleService) CreateTask(ctx context.Context, task *Task) error {
	if task == nil {
		return fmt.Errorf("taskgraph: nil task")
	}
	return l.repo.Create(ctx, task)
}

// Transition validates and executes a status transition.
func (l *LifecycleService) Transition(ctx context.Context, id string, from, to Status, revision int) error {
	if !CanTransition(from, to) {
		return fmt.Errorf("taskgraph: illegal transition %s → %s", from, to)
	}
	return l.repo.SetStatus(ctx, id, from, to, revision)
}

// Lease transitions READY → LEASED and assigns a worker.
func (l *LifecycleService) Lease(ctx context.Context, id, workerID, leaseID string) error {
	if id == "" || workerID == "" || leaseID == "" {
		return fmt.Errorf("taskgraph.Lease: missing identity")
	}
	return l.repo.Lease(ctx, id, workerID, leaseID)
}

// Start transitions LEASED → RUNNING.
func (l *LifecycleService) Start(ctx context.Context, id, workerID, leaseID string, attempt, revision int) error {
	if id == "" || workerID == "" || leaseID == "" {
		return fmt.Errorf("taskgraph.Start: missing identity")
	}
	return l.repo.Start(ctx, id, workerID, leaseID, attempt, revision)
}

// Fail marks a task FAILED.
func (l *LifecycleService) Fail(ctx context.Context, id, reason string, revision int) error {
	if id == "" {
		return fmt.Errorf("taskgraph.Fail: empty taskID")
	}
	return l.repo.Fail(ctx, id, reason, revision)
}

// RenewLease extends a currently-leased or running task's deadline
// (PR-03 / fix/task-lease-renewal-protocol). Validates non-empty
// identity and non-zero expiry, then delegates to the repository's
// CAS which accepts the task in either LEASED or RUNNING state and
// gates on the (task_id, worker_id, lease_id, revision) tuple — no
// attempt_id predicate (AcceptTaskAtomic is the SOLE writer of
// attempt_id; see Repository.RenewLease for the full CAS contract).
func (l *LifecycleService) RenewLease(ctx context.Context, id, workerID, leaseID string, expiry time.Time, revision int) error {
	if id == "" || workerID == "" || leaseID == "" {
		return fmt.Errorf("taskgraph.RenewLease: missing identity (task=%q worker=%q lease=%q)",
			id, workerID, leaseID)
	}
	if expiry.IsZero() {
		return fmt.Errorf("taskgraph.RenewLease: empty expiry")
	}
	return l.repo.RenewLease(ctx, id, workerID, leaseID, expiry.UTC(), revision)
}

// RequeueExpiredLeases scans tasks whose lease has expired as
// RequeueCandidate rows (PR-04: SELECT-only; the per-row atomic write
// is ExpireTaskLeaseAtomic). The caller-supplied nowRFC3339 lets the
// supervisor pin the sweep time so the tick is deterministic across
// goroutines.
func (l *LifecycleService) RequeueExpiredLeases(ctx context.Context, nowRFC3339 string, limit int) ([]RequeueCandidate, error) {
	if nowRFC3339 == "" {
		return nil, fmt.Errorf("taskgraph.RequeueExpiredLeases: empty nowRFC3339")
	}
	if limit <= 0 {
		limit = 100
	}
	return l.repo.RequeueExpiredLeases(ctx, nowRFC3339, limit)
}

// ExpireTaskLease wraps the audit-mandated reap for one task:
//
//  1. derive retry budget from the parent Job (jobs.max_retries, 0 default 3)
//  2. call Repository.ExpireTaskLeaseAtomic with candidate.LeaseID
//     and candidate.LeaseExpiresAt as the OBSERVED values the reaper
//     pulled from the SELECT phase
//  3. if AttemptsExhausted, post-commit Job update via jobsRepo.Fail
//
// The Job update runs OUTSIDE the atomic so the audit-strict transactional
// boundary (Task + Attempt CAS in one tx) is preserved. A failure to
// update the Job is surfaced but does NOT roll back the Task reap \u2014 the
// Task is already terminal; the next supervisor tick deduplicates.
func (l *LifecycleService) ExpireTaskLease(ctx context.Context, candidate RequeueCandidate) (ExpireResult, error) {
	if candidate.ID == "" {
		return ExpireResult{}, fmt.Errorf("taskgraph.ExpireTaskLease: candidate.ID is empty")
	}
	if candidate.LeaseID == "" {
		return ExpireResult{}, fmt.Errorf("taskgraph.ExpireTaskLease: candidate.LeaseID is empty for %s", candidate.ID)
	}

	// Retry budget derivation. Default to 3 so callers without
	// jobsRepo wired (or with a missing job) get a sane baseline.
	maxRetries := 3
	if l.jobsRepo != nil && candidate.ID != "" {
		t, terr := l.repo.Get(ctx, candidate.ID)
		if terr == nil && t != nil && t.JobID != "" {
			job, jerr := l.jobsRepo.Get(ctx, t.JobID)
			if jerr == nil && job != nil && job.MaxRetries > 0 {
				maxRetries = job.MaxRetries
			}
		}
	}

	res, err := l.repo.ExpireTaskLeaseAtomic(
		ctx,
		candidate.ID,
		candidate.LeaseID,
		candidate.LeaseExpiresAt,
		maxRetries,
	)
	if err != nil {
		return res, fmt.Errorf("taskgraph.ExpireTaskLease: atomic reap: %w", err)
	}

	// Post-commit Job aggregate update when retries are exhausted.
	// Deliberately NOT failing the reap if the Job update itself fails:
	// the Task reap committed already, lease is recovered regardless.
	if res.AttemptsExhausted && l.jobsRepo != nil && candidate.ID != "" {
		t, terr := l.repo.Get(ctx, candidate.ID)
		if terr == nil && t != nil && t.JobID != "" {
			job, jerr := l.jobsRepo.Get(ctx, t.JobID)
			if jerr == nil && job != nil && !job.Status.IsTerminal() {
				_ = l.jobsRepo.Fail(
					ctx, t.JobID,
					fmt.Sprintf("LEASE_EXPIRED_RETRIES_EXHAUSTED: task %s tripped retry budget via master-side reap", candidate.ID),
				)
			}
		}
	}

	res.TaskID = candidate.ID
	return res, nil
}

// now normalizes a time to UTC. If t is zero, returns current time.
func now(t time.Time) time.Time {
	if t.IsZero() {
		t = time.Now().UTC()
	}
	return t.UTC()
}

// TickReadiness evaluates PENDING tasks and transitions them to READY
// when their dependencies are resolved (PR #4: real dependency verification).
//
// For each PENDING task, the dispatcher checks whether ALL tasks in
// t.DependsOn are SUCCEEDED before flipping to READY. Tasks with no
// dependencies (DependsOn empty, the single-task model) transition
// unconditionally. CAS failures from concurrent goroutines are non-fatal.
//
// Returns the number of tasks transitioned. limit caps how many tasks are
// scanned per tick; 0 uses the safe default of 100.
func (l *LifecycleService) TickReadiness(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 100
	}
	tasks, err := l.repo.List(ctx, Filter{
		Statuses: []Status{StatusPending},
		Limit:    limit,
	})
	if err != nil {
		return 0, fmt.Errorf("taskgraph.TickReadiness: list PENDING: %w", err)
	}
	var transitioned int
	for _, t := range tasks {
		// PR #4: verify real dependencies before transitioning.
		// Single-task model (empty DependsOn) always passes.
		satisfied, depErr := l.repo.AreDependenciesSatisfied(ctx, t.DependsOn)
		if depErr != nil {
			continue
		}
		if !satisfied {
			continue
		}
		if err := l.repo.SetStatus(ctx, t.ID, StatusPending, StatusReady, t.Revision); err != nil {
			// CAS failure (another goroutine raced) is non-fatal — skip.
			continue
		}
		transitioned++
	}
	return transitioned, nil
}
