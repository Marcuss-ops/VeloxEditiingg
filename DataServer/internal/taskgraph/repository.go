package taskgraph

import (
	"context"
	"time"

	"velox-server/internal/placement"
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
	//
	// Pre-PR-2 implementation: it populated only the lease tuple (worker_id,
	// lease_id, lease_expires_at) on the tasks row and a TaskAttempt was
	// inserted later at handleTaskAccepted time via AcceptTaskAtomic.
	// SUPERSEDED by ClaimNextWithAttemptAtomic which performs the canonical
	// atomic claim (Task + PENDING TaskAttempt + tasks.attempt_id + tasks.attempt_number).
	// Kept for backwards compatibility with non-task-native call sites;
	// new dispatch code MUST use ClaimNextWithAttemptAtomic.
	ClaimNextReadyTask(ctx context.Context, workerID, leaseID string) (*TaskWithSpec, error)

	// ClaimNextWithAttemptAtomic atomically claims the next READY task for a
	// worker AND inserts the matching PENDING TaskAttempt row AND stamps
	// (tasks.attempt_id, tasks.attempt_number) on the tasks row — all in
	// ONE transaction. PR-2 / fix/canonical-attempt-identity single-source
	// invariant: the canonical attempt identity is minted at Claim time
	// and is available on the wire in the subsequent TaskOffer envelope.
	//
	// Returns the claimed task with its spec payload AND the freshly-created
	// PENDING attempt, OR (nil, nil, nil) if no READY task is currently
	// available (replay-safe idempotent no-op for the next caller).
	//
	// Concurrency: SELECT…LIMIT 1 + CAS UPDATE READY→LEASED + INSERT attempt.
	// A concurrent claimer that races us returns (nil, nil, nil) with no
	// error — the caller treats this identical to "no READY task available"
	// and surfaces it identically in dispatch.
	//
	// On success:
	//   * tasks row reads: status=LEASED, worker_id=workerID, lease_id=leaseID,
	//     lease_expires_at=now+defaultTaskLeaseTTL, attempt_id=<uuid>,
	//     attempt_number=t.AttemptCount+1, revision=prev+1
	//   * task_attempts row inserts: id=<same uuid>, status=PENDING,
	//     report_version=0, task_id=t.ID, job_id=t.JobID,
	//     attempt_number=t.AttemptCount+1, worker_id=workerID, lease_id=leaseID
	ClaimNextWithAttemptAtomic(ctx context.Context, workerID, leaseID string) (*TaskWithSpec, *taskattempts.TaskAttempt, error)

	// ReleaseLease atomically resets a LEASED/RUNNING task back to READY.
	// CAS gates on (task_id, worker_id, lease_id) so a stale reject from
	// Worker A with lease L1 cannot release a task reassigned to Worker B
	// with lease L2 (TOCTOU closure for handleTaskRejected).
	//
	// Used on session teardown to release orphaned task claims (PR #4)
	// and by handleTaskRejected to return a rejected task to the pool.
	ReleaseLease(ctx context.Context, taskID, workerID, leaseID string) error

	// Start transitions LEASED → RUNNING with full CAS tuple.
	Start(ctx context.Context, id, workerID, leaseID string, attempt, revision int) error

	// RequeueExpiredLeases scans tasks whose `lease_expires_at` is in
	// the past and surfaces them as RequeueCandidate rows. SELECT-only:
	// no UPDATE happens here. Per-task ExpireTaskLeaseAtomic owns the
	// write so the audit-mandated CAS tuple + retry budget + Attempt
	// close always run in a single tx.
	//
	// Tasks with NULL `lease_expires_at` (pre-migration-049 rows) are
	// treated as "never expires" so a long-running pre-cutover task is
	// never wrongly reaped. limit caps how many tasks are scanned per
	// call (0 defaults to 100).
	//
	// PR-05: replaces the Job-side zombie reaper. PR-04 transforms
	// this method SELECT-only so the per-row atomic path owns the write.
	RequeueExpiredLeases(ctx context.Context, nowRFC3339 string, limit int) (candidates []RequeueCandidate, err error)

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

	// RenewLease extends a currently-leased or running task's deadline
	// (PR-03 / fix/task-lease-renewal-protocol). CAS tuple:
	//   task_id=? AND worker_id=? AND lease_id=?
	//   AND status IN ('LEASED','RUNNING') AND revision=?
	//
	// Acceptance of BOTH LEASED and RUNNING states is intentional: a
	// worker progressed to RUNNING after TaskLeaseGranted is acknowledged
	// and a task longer than the 30-min TTL must renew without first
	// being reaped. RenewLease does NOT gate on attempt_id because
	// AcceptTaskAtomic (PR-2 audit P0#5 fix) is the SOLE writer of
	// attempt_id on tasks; the (task_id, worker_id, lease_id) tuple
	// already binds the renewal to the canonical attempt implicitly.
	// A worker cannot hold two different attempt_ids for the same task
	// at the same time, so the TOCTOU race against reaper-reset is
	// closed by the worker_id + lease_id gate alone.
	//
	// expiry is the absolute UTC deadline the master stamps into
	// tasks.lease_expires_at (post-clamp to the master's TTL policy).
	// revision is NOT bumped — renewal is idempotent on its own
	// (task_id, worker_id, lease_id) tuple.
	//
	// Returns taskgraph.ErrTransitionConflict on stale CAS (lease
	// revoked, task already terminal, or revision mismatch).
	RenewLease(ctx context.Context, id, workerID, leaseID string, expiry time.Time, revision int) error

	// IncrementAttempt bumps the attempt counter atomically.
	IncrementAttempt(ctx context.Context, id string) error

	// ExpireTaskLeaseAtomic atomically reaps a single expired-lease
	// task in the audit-mandated atomic transitions:
	//
	//	1. verify task_id, lease_id, lease_expires_at observed (CAS gate)
	//	2. close the active attempt as TIMED_OUT via attemptRepo txn
	//	3. apply retry budget (compare t.AttemptCount + next_attempt_number
	//	   against maxRetries supplied by caller; 0 maxRetries defaults to 3)
	//	4. bring task to READY (re-claimable) or FAILED (retry budget
	//	   exhausted, terminal)
	//	5. clear worker_id, lease_id, lease_expires_at
	//	6. return ExpireResult so the caller can update the Job aggregate
	//	   when AttemptsExhausted is true
	//
	// The audit-mandated identity tuple is:
	//   task_id=? AND lease_id=? AND lease_expires_at=? AND worker_id=?
	//   AND status IN ('LEASED','RUNNING')
	//
	// attempt_count is INTENTIONALLY NOT bumped here (audit P0#4 fix:
	// the counter reflects STARTED attempts, which AcceptTaskAtomic
	// owns); a reaped task that has not yet reached the next Accept
	// remains at its previous attempt_count.
	//
	// Returns ErrTransitionConflict on stale CAS (worker renewed or
	// already terminal between SELECT and UPDATE). Returns
	// (ExpireResult{}, nil) when the row was already at the expected
	// state but no transition was required (replay-safe).
	ExpireTaskLeaseAtomic(
		ctx context.Context,
		taskID, leaseID, leaseExpiresAtObserved string,
		maxRetries int,
	) (ExpireResult, error)

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

	// IngestTaskResultAtomic is the single legal entry point for ingesting
	// a worker TaskResult. It atomically transitions Task + Attempt to
	// terminal AND persists typed metrics, cache stats, cost basis, AND
	// registers output artifact declarations in ONE database transaction.
	// No partial writes: if any step fails, everything rolls back.
	//
	// fix/atomic-ingestion: replaces the 4-step sequence
	// (TransitionTaskToTerminalAtomic + PersistMetrics + PersistCacheStats +
	// PersistCostBasis + per-artifact Register) with a single atomic call.
	// Returns ErrTransitionConflict on stale CAS; the caller must NOT
	// proceed with artifact registration or job roll-up on this error.
	IngestTaskResultAtomic(ctx context.Context, cmd IngestResultCommand) error

	// Delete hard-deletes a task. Returns no error if already gone.
	Delete(ctx context.Context, id string) error

	// ListReadyCandidates returns a lightweight list of READY tasks
	// suitable for the placement matcher. Only metadata fields
	// (task_id, job_id, revision, priority, created_at, executor_id,
	// executor_version) are loaded — full payloads are deferred to
	// the subsequent ClaimTaskForWorkerAtomic call.
	ListReadyCandidates(ctx context.Context, limit int) ([]placement.TaskCandidate, error)

	// ClaimTaskForWorkerAtomic atomically claims a specific READY task
	// chosen by the placement matcher. Unlike ClaimNextWithAttemptAtomic
	// (which picks the next available READY task), this method CAS-gates
	// on (task_id, revision, executor_id, executor_version) so the
	// repository verifies the task was not claimed by a concurrent
	// dispatcher between ListReadyCandidates and the claim.
	//
	// On success returns the claimed TaskWithSpec AND the freshly-created
	// PENDING TaskAttempt. On CAS failure (stale revision, executor
	// mismatch, or concurrent claim) returns (nil, nil,
	// taskgraph.ErrTransitionConflict) — identical to "no applicable
	// task" for the caller.
	ClaimTaskForWorkerAtomic(ctx context.Context, cmd ClaimTaskForWorkerCommand) (*TaskWithSpec, *taskattempts.TaskAttempt, error)
}

// Repository combines Reader and Writer into a single task persistence contract.
type Repository interface {
	Reader
	Writer
}

// RequeueCandidate is a single expired-lease candidate surfaced by
// RequeueExpiredLeases. The reaper iterates it and per-row runs
// ExpireTaskLeaseAtomic so the per-task CAS still holds.
//
// PR-04 / fix/task-expiry-atomic-transition: RequeueExpiredLeases is now
// SELECT-only. The legacy "bulk UPDATE tasks to READY, bump attempt_count"
// behavior is REMOVED — ExpireTaskLeaseAtomic owns the per-task write and
// applies the audit-mandated retry budget + attempt close.
type RequeueCandidate struct {
	ID             string
	LeaseID        string
	LeaseExpiresAt string
	WorkerID       string
	AttemptCount   int
}

// ExpireResult reports the outcome of ExpireTaskLeaseAtomic.
//
// PR-04 / fix/task-expiry-atomic-transition: the caller
// (LifecycleService.ExpireTaskLease / TaskLeaseReaper) uses
// AttemptsExhausted to decide whether to surface a Job-level failure
// via jobsRepo.Fail downstream of the atomic.
type ExpireResult struct {
	// TaskID is the task that was reaped (echoed for the caller's log path).
	TaskID string
	// TaskStatus is the post-reap status: StatusReady (re-claimable,
	// retries left) or StatusFailed (retries exhausted, terminal).
	TaskStatus Status
	// AttemptsExhausted is true when the task tripped retry budget and
	// was marked FAILED. The caller must signal jobsRepo so the Job
	// aggregate sees the FAILED-attempt count.
	AttemptsExhausted bool
	// AttemptID is the task_attempts.id that was closed as TIMED_OUT, if any.
	// Empty when no active attempt existed for the (task_id, worker_id, lease_id)
	// tuple (rare but possible — the reap path must still proceed Task-side).
	AttemptID string
	// AttemptClosed is true iff an active attempt was closed as TIMED_OUT by
	// the atomic. False when the task had no matching attempt (the reap is
	// still legal — the missing-attempt scenario is logged-only).
	AttemptClosed bool
}
