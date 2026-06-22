package taskgraph

import (
	"context"
	"time"

	"velox-server/internal/taskattempts"
)

// Filter narrows list queries on the Reader surface.
// Zero-value means "no filter" — all tasks are returned.
type Filter struct {
	JobIDs   []string // empty = all jobs
	Statuses []Status // empty = all statuses
	WorkerID string   // empty = no worker filter
	Limit    int      // 0 = all
}

// Reader is the read-only task query surface.
type Reader interface {
	// Get returns a single task by ID, or (nil, nil) on missing.
	Get(ctx context.Context, id string) (*Task, error)

	// List returns tasks matching the filter.
	List(ctx context.Context, filter Filter) ([]Task, error)

	// GetByJobID returns the task for a given job, or (nil, nil) on missing.
	// Invariant: each job has exactly one task.
	GetByJobID(ctx context.Context, jobID string) (*Task, error)
}

// Writer is the canonical write-only task mutation surface.
type Writer interface {
	// Create inserts a new task in PENDING state. If id is empty the
	// repository assigns one.
	Create(ctx context.Context, task *Task) error

	// SetStatus performs a CAS status change from → to, verifying revision.
	// Returns ErrTransitionConflict on mismatch.
	SetStatus(ctx context.Context, id string, from, to Status, revision int) error

	// Lease atomically assigns a READY task to a worker.
	// Returns ErrTransitionConflict if the task is not in READY.
	Lease(ctx context.Context, id, workerID, leaseID string) error

	// ClaimNextReadyTask atomically claims the next READY task for a worker.
	// Returns the claimed task with its spec payload, or (nil, nil) if none available.
	// PR #4: task-native dispatch path replaces job-based claim.
	ClaimNextReadyTask(ctx context.Context, workerID, leaseID string) (*TaskWithSpec, error)

	// ReleaseLease atomically resets a LEASED/RUNNING task back to READY.
	// Used on session teardown to release orphaned task claims (PR #4).
	ReleaseLease(ctx context.Context, taskID string) error

	// Start transitions LEASED → RUNNING with full CAS tuple.
	Start(ctx context.Context, id, workerID, leaseID string, attempt, revision int) error

	// RequeueExpiredLeases scans tasks whose `lease_expires_at` is in the
	// past and requeues them per audit §P0.4:
	//   - LEASED  expired → READY (re-claimable),
	//   - RUNNING expired → READY with attempt_count bumped (matches the
	//     existing retry semantics; Job FAILed-on-retries-exhausted path
	//     remains a follow-up that PR-08 will land with the reduced
	//     jobs.Repository surface).
	// Tasks with NULL `lease_expires_at` (pre-migration-049 rows) are
	// treated as "never expires" so a long-running pre-cutover task is
	// never wrongly reaped. limit caps how many tasks are scanned per
	// call (0 defaults to 100). Returns the reaped task IDs.
	//
	// PR-05: replaces the Job-side zombie reaper that PR-13 gated with
	// VELOX_DISABLE_JOB_REAPER; once the bootstrap registers this
	// reaper on top of taskgraph-dispatcher, the Job-level reaper can
	// be deprecated (a separate PR closes the Job-side path).
	RequeueExpiredLeases(ctx context.Context, nowRFC3339 string, limit int) (reaped []string, err error)

	// AcceptTaskAtomic transitions the task LEASED→RUNNING AND inserts
	// the matching TaskAttempt row in ONE transaction. The single legal
	// entry point for promoting a worker offer to a running execution;
	// preserves audit invariant §9.5 (Task RUNNING ⇒ Attempt RUNNING)
	// across crash windows. Returns ErrTransitionConflict on stale CAS.
	//
	// PR-04 / §9.5: supersedes the two-statement Start + Create sequence
	// previously used in handleTaskAccepted.
	AcceptTaskAtomic(ctx context.Context, attempt *taskattempts.TaskAttempt, revision int) error

	// AreDependenciesSatisfied returns true when all tasks in dependsOn
	// have status SUCCEEDED. Returns true when dependsOn is empty.
	// PR #4: used by TickReadiness for real dependency verification.
	AreDependenciesSatisfied(ctx context.Context, dependsOn []string) (bool, error)

	// Fail marks a task FAILED.
	Fail(ctx context.Context, id, reason string, revision int) error

	// RenewLease extends a currently-leased task's deadline. CAS-gated
	// on `status='LEASED' AND worker_id=? AND lease_id=? AND revision=?`.
	// On success the row's `lease_expires_at` is set to expiry (UTC,
	// RFC3339) and `updated_at` is set to now; revision is NOT bumped —
	// renewal is idempotent on its own lease and bumping would invalidate
	// a worker's in-flight message queue that references the old revision.
	// Returns taskgraph.ErrTransitionConflict on stale CAS (lease revoked,
	// task already terminal, or revision mismatch). The lease-id-based CAS
	// guarantees a worker cannot accidentally extend a lease that has
	// already been reaped and re-issued to another worker (different
	// leaseID ⇒ 0 rows affected).
	//
	// PR-05 follow-up: enables long-running tasks to extend their lease
	// past the 30-min defaultTaskLeaseTTL without losing the master-side
	// reaper's "abort after TTL" guarantee.
	RenewLease(ctx context.Context, id, workerID, leaseID string, expiry time.Time, revision int) error

	// IncrementAttempt bumps the attempt counter atomically.
	IncrementAttempt(ctx context.Context, id string) error

	// TransitionTaskToTerminalAtomic marks the task AND the matching
	// active TaskAttempt as terminal (SUCCEEDED/FAILED/CANCELLED) in
	// ONE transaction. The single legal entry point for closing out
	// a task; preserves audit invariant §9.5 (Task terminal ⇒ Attempt
	// terminal; no zombie RUNNING attempts after a crashed handler).
	// Returns ErrTransitionConflict on stale Task CAS; ErrStaleReport
	// (taskattempts) on a hardened §9.5 violation (Task RUNNING with
	// no matching attempt row).
	//
	// PR-04 / §9.5: supersedes the two-statement SetStatus|Fail +
	// CompleteFinal sequence previously used in handleTaskResult.
	TransitionTaskToTerminalAtomic(
		ctx context.Context,
		taskID, workerID, leaseID string,
		taskStatus Status,
		attemptStatus taskattempts.AttemptStatus,
		errorCode, errorMessage string,
	) error

	// Delete hard-deletes a task. Returns no error if already gone.
	Delete(ctx context.Context, id string) error
}

// Repository combines Reader and Writer into a single task persistence contract.
type Repository interface {
	Reader
	Writer
}
