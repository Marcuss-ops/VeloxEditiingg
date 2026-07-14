package store

// sqlite_task_lease.go: lease management on the tasks table — claim
// (CAS-gated to LEASED), renew, release, reaper scan, and reap-close.
// All multi-row writes are committed in a single tx so the audit §9.5
// invariant ("Task RUNNING ⇒ Attempt RUNNING") cannot be violated by
// a process crash between statements. Single-row book-keeping CAS
// (Lease in CRUD; SetStatus/Start/Fail/IncrementAttempt/Delete in
// CRUD) stay in their respective files.
// Extracted from sqlite_task_repository.go (commit d7eff6f → next).

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"velox-server/internal/placement"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// defaultTaskLeaseTTL is the master-side lease TTL written by
// ClaimNextReadyTask into tasks.lease_expires_at. Workers may RenewLease
// via the gRPC TaskLeaseRenewal message (PR-05 follow-up). 30 minutes
// matches the Job-side renewal idiom in handleLeaseRenewal.
const defaultTaskLeaseTTL = 30 * time.Minute

// RenewLease extends a currently-leased or running task's deadline
// (PR-03 / fix/task-lease-renewal-protocol). CAS tuple:
//
//	task_id=? AND worker_id=? AND lease_id=?
//	AND status IN ('LEASED', 'RUNNING') AND revision=?
//
// Acceptance of BOTH states is intentional: a worker progressed to
// RUNNING after TaskLeaseGranted is acknowledged and a task longer
// than the 30-min TTL must renew without first being reaped.
//
// The CAS intentionally does NOT gate on attempt_id: AcceptTaskAtomic
// is the sole writer of attempt_id on tasks, and a worker cannot hold
// two different attempt_ids for the same task concurrently. The
// (worker_id, lease_id) tuple already binds the renewal to the
// canonical attempt implicitly. The TOCTOU race against reaper-reset
// is closed by (worker_id, lease_id) gates alone — a stale worker on
// (W1, L1) cannot match a freshly re-stamped row with (W2, L2).
//
// revision is intentionally NOT bumped (see the interface comment):
// renewal is idempotent on its own (task_id, worker_id, lease_id, revision)
// tuple.
func (r *SQLiteTaskRepository) RenewLease(ctx context.Context, id, workerID, leaseID string, expiry time.Time, revision int) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("task repository: store not initialized")
	}
	if id == "" || workerID == "" || leaseID == "" {
		return fmt.Errorf("task repository: RenewLease requires task_id, worker_id, lease_id")
	}
	if expiry.IsZero() {
		return fmt.Errorf("task repository: RenewLease requires a non-zero expiry")
	}

	now := time.Now().UTC().Format(time.RFC3339)
	res, err := r.store.db.ExecContext(ctx,
		`UPDATE tasks
		 SET lease_expires_at = ?, updated_at = ?
		 WHERE task_id = ?
		   AND worker_id = ? AND lease_id = ? AND revision = ?
		   AND status IN ('LEASED', 'RUNNING')`,
		expiry.UTC().Format(time.RFC3339), now,
		id, workerID, leaseID, revision,
	)
	if err != nil {
		return fmt.Errorf("task renew lease: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("task renew lease rows: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("task renew lease %s: %w", id, taskgraph.ErrTransitionConflict)
	}
	return nil
}

// ExpireTaskLeaseAtomic reaps a single task in one atomic transaction
// following the audit-mandated contract for the atomic transition:
//
//  1. CAS-gate on (task_id, lease_id, lease_expires_at, worker_id) where
//     lease_expires_at is the OBSERVED (pre-reap) value; a worker that
//     just renewed would have written a NEWER lease_expires_at and our
//     CAS sees 0 rows → ErrTransitionConflict (audit P0#6 fix).
//  2. Attempt close: TX-gated UPDATE on task_attempts for the
//     (task_id, worker_id, lease_id) tuple, status non-terminal →
//     TIMED_OUT. Inlined into the same tx so a process crash between
//     Task UPDATE and Attempt UPDATE cannot leave Task at READY/FAILED
//     with Attempt RUNNING (audit §9.5 invariant).
//  3. Retry budget: if attempt_count >= maxRetries + 1, set task →
//     FAILED (terminal). Otherwise task → READY (re-claimable).
//  4. Clear worker_id, lease_id, lease_expires_at; bump revision.
//  5. attempt_count is INTENTIONALLY NOT bumped here (audit P0#4:
//     counter reflects STARTED attempts, owned by AcceptTaskAtomic).
//
// maxRetries <= 0 falls back to a safe default of 3.
func (r *SQLiteTaskRepository) ExpireTaskLeaseAtomic(
	ctx context.Context,
	taskID, leaseID, leaseExpiresAtObserved string,
	maxRetries int,
) (taskgraph.ExpireResult, error) {
	if r.store == nil || r.store.db == nil {
		return taskgraph.ExpireResult{}, fmt.Errorf("task repository: store not initialized")
	}
	if taskID == "" || leaseID == "" {
		return taskgraph.ExpireResult{}, fmt.Errorf("task repository: ExpireTaskLeaseAtomic requires task_id and lease_id")
	}
	if maxRetries <= 0 {
		maxRetries = 3
	}

	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return taskgraph.ExpireResult{}, fmt.Errorf("task expire atomic begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	now := time.Now().UTC().Format(time.RFC3339)

	// 1. Read task to obtain attempt_count + status + worker_id + lease_id
	// + lease_expires_at for the CAS gate.
	var (
		attemptCount         int
		currentAttemptNumber int
		currentStatus        string
		currentWorker        string
		currentLeaseID       string
		currentLeaseExp      string
	)
	err = tx.QueryRowContext(ctx,
		`SELECT attempt_count, attempt_number, status,
		        COALESCE(worker_id, ''), COALESCE(lease_id, ''), COALESCE(lease_expires_at, '')
		 FROM tasks WHERE task_id = ?`,
		taskID,
	).Scan(&attemptCount, &currentAttemptNumber, &currentStatus, &currentWorker, &currentLeaseID, &currentLeaseExp)
	if err == sql.ErrNoRows {
		return taskgraph.ExpireResult{}, fmt.Errorf("task expire atomic %s: not found", taskID)
	}
	if err != nil {
		return taskgraph.ExpireResult{}, fmt.Errorf("task expire atomic read: %w", err)
	}

	if currentStatus != string(taskgraph.StatusLeased) && currentStatus != string(taskgraph.StatusRunning) {
		return taskgraph.ExpireResult{}, fmt.Errorf("task expire atomic %s: not in LEASED/RUNNING (status=%s): %w",
			taskID, currentStatus, taskgraph.ErrTransitionConflict)
	}
	if currentLeaseID != leaseID {
		return taskgraph.ExpireResult{}, fmt.Errorf("task expire atomic %s: lease_id mismatch (got=%s, db=%s): %w",
			taskID, leaseID, currentLeaseID, taskgraph.ErrTransitionConflict)
	}
	if currentLeaseExp != leaseExpiresAtObserved {
		return taskgraph.ExpireResult{}, fmt.Errorf("task expire atomic %s: lease_expires_at mismatch (got=%s, db=%s): %w",
			taskID, leaseExpiresAtObserved, currentLeaseExp, taskgraph.ErrTransitionConflict)
	}

	// 2. Attempt close: TX-gated UPDATE on task_attempts for the identity
	// tuple. Inlined into the same tx (no CompleteByIdentityTimedOut
	// indirection) so a process crash between Task UPDATE and Attempt
	// UPDATE cannot leave Task at READY/FAILED with Attempt still
	// RUNNING — both commit together or neither does (audit §9.5).
	attRes, err := tx.ExecContext(ctx,
		`UPDATE task_attempts
		 SET status = 'TIMED_OUT', completed_at = ?, error_code = ?, error_message = ?,
		     report_version = report_version + 1, updated_at = ?
		 WHERE task_id = ? AND worker_id = ? AND lease_id = ?
		   AND status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT')`,
		now, "LEASE_EXPIRED", "master-side lease TTL exceeded", now,
		taskID, currentWorker, leaseID,
	)
	if err != nil {
		return taskgraph.ExpireResult{}, fmt.Errorf("task expire atomic attempt cas: %w", err)
	}
	attemptRows, _ := attRes.RowsAffected()

	var attemptID string
	idProbeErr := tx.QueryRowContext(ctx,
		`SELECT id FROM task_attempts
		 WHERE task_id = ? AND worker_id = ? AND lease_id = ?
		 ORDER BY attempt_number DESC LIMIT 1`,
		taskID, currentWorker, leaseID,
	).Scan(&attemptID)
	if idProbeErr != nil {
		// Defensive §9.5 case: task in LEASED/RUNNING with no matching
		// attempt row. The Task CAS still proceeds (lease recovered),
		// but AttemptClosed=false so the reaper logs the breach.
		attemptID = ""
		attemptRows = 0
	}

	effectiveAttemptCount := maxAttemptOrdinal(attemptCount, currentAttemptNumber)

	// 3. Retry budget. attempt_count >= maxRetries + 1 means the next
	// AcceptTask would exceed the configured budget — reap terminates
	// the task as FAILED. Otherwise the task is requeueable as READY.
	exhausted := effectiveAttemptCount >= maxRetries+1
	newStatus := taskgraph.StatusReady
	if exhausted {
		newStatus = taskgraph.StatusFailed
	}

	// 4. Task CAS-gate update. Status flips to newStatus; worker/lease/
	// lease_expires_at cleared; revision bumped. CAS-tuple reinforces
	// the read above so a parallel AcceptTaskAtomic / Transition races
	// us out instead of us blindly overwriting.
	taskRes, err := tx.ExecContext(ctx,
		`UPDATE tasks
		 SET status = ?, completed_at = ?,
		     worker_id = '', lease_id = '', lease_expires_at = NULL,
		     attempt_count = ?, attempt_id = '', attempt_number = 0,
		     revision = revision + 1, updated_at = ?
		 WHERE task_id = ? AND status = ? AND worker_id = ? AND lease_id = ?`,
		string(newStatus), now, effectiveAttemptCount, now,
		taskID, currentStatus, currentWorker, leaseID,
	)
	if err != nil {
		return taskgraph.ExpireResult{}, fmt.Errorf("task expire atomic task cas: %w", err)
	}
	taskRows, _ := taskRes.RowsAffected()
	if taskRows == 0 {
		return taskgraph.ExpireResult{}, fmt.Errorf("task expire atomic %s: task CAS raced out: %w",
			taskID, taskgraph.ErrTransitionConflict)
	}

	if err := tx.Commit(); err != nil {
		return taskgraph.ExpireResult{}, fmt.Errorf("task expire atomic commit: %w", err)
	}
	committed = true

	return taskgraph.ExpireResult{
		TaskID:            taskID,
		TaskStatus:        newStatus,
		AttemptsExhausted: exhausted,
		AttemptID:         attemptID,
		AttemptClosed:     attemptRows > 0 && attemptID != "",
	}, nil
}

// ClaimNextReadyTask atomically claims the next READY task for a worker.
// CAS: READY → LEASED with workerID + leaseID. Returns the task with its
// spec payload from task_specs, or (nil, nil) if no READY task is available.
//
// PR-05: also persists `lease_expires_at = now + leaseTTL` so the master-
// side reaper (RequeueExpiredLeases) can sweep tasks whose workers have
// crashed without sending a final TaskResult. The TTL is configurable
// per-call via the leaseTTL parameter; 0 falls back to the safe default
// of 30 minutes.
//
// PR #4: task-native dispatch path replaces job-based claim.
func (r *SQLiteTaskRepository) ClaimNextReadyTask(ctx context.Context, workerID, leaseID string) (*taskgraph.TaskWithSpec, error) {
	if r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("task repository: store not initialized")
	}
	if workerID == "" || leaseID == "" {
		return nil, fmt.Errorf("task repository: claim requires workerID + leaseID")
	}

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339)
	leaseExpiresAt := now.Add(defaultTaskLeaseTTL).Format(time.RFC3339)

	// Find and CAS-claim the next READY task in a single tx.
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("task claim begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Select the next READY task (highest priority, then oldest).
	row := tx.QueryRowContext(ctx,
		`SELECT `+strings.Join(taskColumns, ",")+`
		 FROM tasks
		 WHERE status = 'READY'
		   AND (worker_id = '' OR worker_id IS NULL)
		 ORDER BY priority DESC, created_at ASC
		 LIMIT 1`,
	)
	t, err := scanTask(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("task claim select: %w", err)
	}

	// CAS: READY → LEASED with workerID + leaseID + lease_expires_at.
	res, err := tx.ExecContext(ctx,
		`UPDATE tasks
		 SET status = 'LEASED', worker_id = ?, lease_id = ?, lease_expires_at = ?,
		     revision = revision + 1, updated_at = ?
		 WHERE task_id = ? AND status = 'READY' AND revision = ?`,
		workerID, leaseID, leaseExpiresAt, nowStr, t.ID, t.Revision,
	)
	if err != nil {
		return nil, fmt.Errorf("task claim cas: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("task claim rows: %w", err)
	}
	if n == 0 {
		// Raced with another claimer — return nil gracefully.
		return nil, nil
	}

	// Read the task_spec payload.
	var specPayloadJSON sql.NullString
	err = tx.QueryRowContext(ctx,
		`SELECT payload_json FROM task_specs WHERE task_id = ?`,
		t.ID,
	).Scan(&specPayloadJSON)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("task claim spec read: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("task claim commit: %w", err)
	}

	// Update in-memory fields after successful commit.
	t.WorkerID = workerID
	t.LeaseID = leaseID
	t.Revision++

	tws := &taskgraph.TaskWithSpec{Task: *t}
	if specPayloadJSON.Valid && specPayloadJSON.String != "" && specPayloadJSON.String != "{}" {
		var payload map[string]interface{}
		if json.Unmarshal([]byte(specPayloadJSON.String), &payload) == nil {
			tws.SpecPayload = payload
		}
	}
	return tws, nil
}

// RequeueExpiredLeases scans tasks whose `lease_expires_at` is in the
// past and surfaces them as RequeueCandidate rows. SELECT-only: no
// UPDATE happens here. Per-task ExpireTaskLeaseAtomic owns the write
// so the audit-mandated CAS tuple + retry budget + Attempt close
// always run in a single tx.
//
// Tasks with NULL `lease_expires_at` (pre-migration-049 rows) are
// treated as "never expires" via the COALESCE-default so a long-running
// pre-cutover task is never wrongly reaped. limit caps how many tasks
// are scanned per call (0 defaults to 100). nowRFC3339 must be a
// RFC3339-encoded timestamp string (the format the column uses).
//
// PR-05 set up the master-side lease enforcement. The audit
// P0#4+P0#6 transforms this method into SELECT-only so per-task
// ExpireTaskLeaseAtomic closes the attempt + applies retry budget +
// CAS-gates on (task_id, lease_id, lease_expires_at, worker_id) in
// one tx.
func (r *SQLiteTaskRepository) RequeueExpiredLeases(ctx context.Context, nowRFC3339 string, limit int) ([]taskgraph.RequeueCandidate, error) {
	if r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("task repository: store not initialized")
	}
	if nowRFC3339 == "" {
		return nil, fmt.Errorf("task repository: RequeueExpiredLeases requires nowRFC3339")
	}
	if limit <= 0 {
		limit = 100
	}

	// Select expired tasks in LEASED or RUNNING with worker_id+lease_id
	// present. Full identity columns (worker_id, lease_id,
	// lease_expires_at) are pulled so the reaper can build the
	// candidate without a second roundtrip. A leased task without a
	// worker_id is a half-claim artefact and is skipped.
	rows, err := r.store.db.QueryContext(ctx,
		`SELECT task_id, COALESCE(worker_id, ''), COALESCE(lease_id, ''),
		        COALESCE(lease_expires_at, ''), attempt_count
		 FROM tasks
		 WHERE status IN ('LEASED', 'RUNNING')
		   AND COALESCE(lease_expires_at, '') <> ''
		   AND lease_expires_at < ?
		   AND COALESCE(worker_id, '') <> ''
		   AND COALESCE(lease_id, '') <> ''
		 ORDER BY lease_expires_at ASC
		 LIMIT ?`,
		nowRFC3339, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("task reaper select: %w", err)
	}
	defer rows.Close()

	var candidates []taskgraph.RequeueCandidate
	for rows.Next() {
		var c taskgraph.RequeueCandidate
		if scanErr := rows.Scan(&c.ID, &c.WorkerID, &c.LeaseID, &c.LeaseExpiresAt, &c.AttemptCount); scanErr != nil {
			continue
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("task reaper rows: %w", err)
	}
	return candidates, nil
}

// ReleaseLease atomically resets a LEASED/RUNNING task back to READY.
// CAS gates on (task_id, worker_id, lease_id) so a stale reject from
// Worker A with lease L1 cannot release a task reassigned to Worker B
// with lease L2 (TOCTOU closure for handleTaskRejected — the previously
// documented read-then-release gap is now closed at the SQL level).
//
// Used on session teardown to release orphaned task claims (PR #4)
// and by handleTaskRejected to return a rejected task to the pool.
func (r *SQLiteTaskRepository) ReleaseLease(ctx context.Context, taskID, workerID, leaseID string) error {
	if taskID == "" {
		return nil
	}
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("task release lease begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	now := time.Now().UTC().Format(time.RFC3339)
	res, err := tx.ExecContext(ctx,
		`UPDATE tasks
		 SET status = 'READY', worker_id = '', lease_id = '',
		     lease_expires_at = NULL, attempt_id = NULL, attempt_number = 0,
		     revision = revision + 1, updated_at = ?
		 WHERE task_id = ? AND worker_id = ? AND lease_id = ?
		   AND status IN ('LEASED', 'RUNNING')`,
		now, taskID, workerID, leaseID,
	)
	if err != nil {
		return fmt.Errorf("task release lease task update: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("task release lease rows: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("task release lease %s: %w", taskID, taskgraph.ErrTransitionConflict)
	}

	// A released offer was never accepted into RUNNING, so its canonical
	// PENDING attempt must be removed to let the next claim reuse the same
	// attempt number (attempt_count only advances on AcceptTaskAtomic).
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM task_attempts
		 WHERE task_id = ? AND worker_id = ? AND lease_id = ? AND status = 'PENDING'`,
		taskID, workerID, leaseID,
	); err != nil {
		return fmt.Errorf("task release lease delete pending attempt: %w", err)
	}

	// Recompute attempt_count from the immutable residual history after
	// deleting the released PENDING offer. This keeps
	// tasks.attempt_count >= MAX(task_attempts.attempt_number) without
	// permanently skipping an ordinal for offers that never started.
	if _, err := tx.ExecContext(ctx,
		`UPDATE tasks
		    SET attempt_count = COALESCE(
		    	(SELECT MAX(attempt_number) FROM task_attempts WHERE task_id = ?),
		    	0
		    )
		  WHERE task_id = ?`,
		taskID, taskID,
	); err != nil {
		return fmt.Errorf("task release lease reconcile attempt_count: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("task release lease commit: %w", err)
	}
	committed = true
	return nil
}

// ClaimTaskForWorkerAtomic atomically claims a specific READY task
// chosen by the placement matcher. CAS-gates on (task_id, revision,
// executor_id, executor_version) so a concurrent dispatcher that
// claimed the same task between ListReadyCandidates and this call
// will see the CAS fail and return ErrTransitionConflict.
//
// The transaction steps mirror ClaimNextWithAttemptAtomic:
//  1. SELECT task WHERE task_id=? AND status='READY' AND revision=?
//     AND executor_id=? AND executor_version=?
//  2. Self-heal attempt_count from immutable attempt history.
//  3. Generate canonical attempt ID before CAS.
//  4. CAS READY → LEASED + stamp attempt_id / attempt_number.
//  5. INSERT PENDING TaskAttempt.
//  6. Read task_spec payload.
//  7. Commit.
//
// SessionID and CapabilityRevision are carried through for fencing
// (the caller checks them before and after the claim); they are NOT
// persisted yet but must travel in the command so the eventual
// transactional fencing doesn't require a signature change.
func (r *SQLiteTaskRepository) ClaimTaskForWorkerAtomic(
	ctx context.Context,
	cmd taskgraph.ClaimTaskForWorkerCommand,
) (*taskgraph.TaskWithSpec, *taskattempts.TaskAttempt, error) {
	if r.store == nil || r.store.db == nil {
		return nil, nil, fmt.Errorf("task repository: store not initialized")
	}
	if cmd.TaskID == "" || cmd.WorkerID == "" || cmd.LeaseID == "" {
		return nil, nil, fmt.Errorf("task repository: ClaimTaskForWorkerAtomic requires task_id, worker_id, lease_id")
	}
	execKey := placement.NormalizeExecutorKey(cmd.ExecutorID, cmd.ExecutorVersion)
	if execKey.ID == "" || execKey.Version <= 0 {
		return nil, nil, fmt.Errorf("task repository: ClaimTaskForWorkerAtomic requires executor_id and executor_version > 0")
	}
	legacyExecutorID := placement.VersionedExecutorID(execKey.ID, execKey.Version)

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339)
	leaseExpiresAt := now.Add(defaultTaskLeaseTTL).Format(time.RFC3339)

	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("task claim-for-worker begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// 1. SELECT the specific task candidate with revision + executor gate.
	row := tx.QueryRowContext(ctx,
		`SELECT `+strings.Join(taskColumns, ", ")+`
		 FROM tasks
		 WHERE task_id = ?
		   AND status = 'READY'
		   AND revision = ?
		   AND executor_id IN (?, ?)
		   AND executor_version = ?
		   AND (worker_id = '' OR worker_id IS NULL)`,
		cmd.TaskID, cmd.ExpectedTaskRevision, execKey.ID, legacyExecutorID, execKey.Version,
	)
	t, err := scanTask(row)
	if err == sql.ErrNoRows {
		return nil, nil, fmt.Errorf("task claim-for-worker %s: task not READY or executor/revision mismatch: %w", cmd.TaskID, taskgraph.ErrTransitionConflict)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("task claim-for-worker select: %w", err)
	}

	// 2. Self-heal stale attempt_count from immutable attempt history.
	var maxSeenAttempt sql.NullInt64
	if err := tx.QueryRowContext(ctx,
		`SELECT MAX(attempt_number) FROM task_attempts WHERE task_id = ?`,
		t.ID,
	).Scan(&maxSeenAttempt); err != nil {
		return nil, nil, fmt.Errorf("task claim-for-worker max attempt read: %w", err)
	}
	effectiveAttemptCount := t.AttemptCount
	if maxSeenAttempt.Valid {
		effectiveAttemptCount = maxAttemptOrdinal(effectiveAttemptCount, int(maxSeenAttempt.Int64))
	}

	// 3. Generate canonical attempt identity BEFORE CAS.
	attemptID := uuid.NewString()
	attemptNumber := effectiveAttemptCount + 1

	// 4. CAS: READY → LEASED on tasks + stamp attempt_id + attempt_number.
	res, err := tx.ExecContext(ctx,
		`UPDATE tasks
		 SET status = 'LEASED', worker_id = ?, lease_id = ?, lease_expires_at = ?,
		     attempt_count = ?, attempt_id = ?, attempt_number = ?,
		     revision = revision + 1, updated_at = ?
		 WHERE task_id = ? AND status = 'READY' AND revision = ?
		   AND executor_id IN (?, ?) AND executor_version = ?`,
		cmd.WorkerID, cmd.LeaseID, leaseExpiresAt, attemptNumber, attemptID, attemptNumber,
		nowStr, t.ID, cmd.ExpectedTaskRevision,
		execKey.ID, legacyExecutorID, execKey.Version,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("task claim-for-worker cas: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return nil, nil, fmt.Errorf("task claim-for-worker rows: %w", err)
	}
	if n == 0 {
		return nil, nil, fmt.Errorf("task claim-for-worker %s: CAS raced out (revision/executor mismatch or concurrent claim): %w", cmd.TaskID, taskgraph.ErrTransitionConflict)
	}

	// 5. INSERT PENDING TaskAttempt in the same tx.
	_, err = tx.ExecContext(ctx,
		`INSERT INTO task_attempts (
			id, task_id, job_id, attempt_number, worker_id, lease_id,
			status, report_version, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, 'PENDING', 0, ?, ?)`,
		attemptID, t.ID, t.JobID, attemptNumber, cmd.WorkerID, cmd.LeaseID, nowStr, nowStr,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("task claim-for-worker insert: %w", err)
	}

	// 6. Read task_spec payload.
	var specPayloadJSON sql.NullString
	err = tx.QueryRowContext(ctx,
		`SELECT payload_json FROM task_specs WHERE task_id = ?`,
		t.ID,
	).Scan(&specPayloadJSON)
	if err != nil && err != sql.ErrNoRows {
		return nil, nil, fmt.Errorf("task claim-for-worker spec read: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, nil, fmt.Errorf("task claim-for-worker commit: %w", err)
	}

	// Update in-memory fields after successful commit.
	t.WorkerID = cmd.WorkerID
	t.LeaseID = cmd.LeaseID
	t.AttemptCount = attemptNumber
	t.AttemptID = attemptID
	t.AttemptNumber = attemptNumber
	t.Revision++

	tws := &taskgraph.TaskWithSpec{Task: *t}
	if specPayloadJSON.Valid && specPayloadJSON.String != "" && specPayloadJSON.String != "{}" {
		var payload map[string]interface{}
		if json.Unmarshal([]byte(specPayloadJSON.String), &payload) == nil {
			tws.SpecPayload = payload
		}
	}

	att := &taskattempts.TaskAttempt{
		ID:            attemptID,
		TaskID:        t.ID,
		JobID:         t.JobID,
		AttemptNumber: attemptNumber,
		WorkerID:      cmd.WorkerID,
		LeaseID:       cmd.LeaseID,
		Status:        taskattempts.AttemptStatusPending,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	return tws, att, nil
}
