// Package store / sqlite_task_lease_claim.go — lease claim and release.
// Extracted from sqlite_task_lease.go: the CAS-gated claim
// (ClaimNextReadyTask, ClaimTaskForWorkerAtomic) and the CAS-gated
// release (ReleaseLease) plus the shared defaultTaskLeaseTTL.
//
// Lease management on the tasks table spans claim (CAS-gated to
// LEASED), renew (sqlite_task_lease_renew.go), and expire/reap
// (sqlite_task_lease_expire.go). All multi-row writes are committed in
// a single tx so the audit §9.5 invariant ("Task RUNNING ⇒ Attempt
// RUNNING") cannot be violated by a process crash between statements.
// Single-row book-keeping CAS (Lease in CRUD; SetStatus/Start/Fail/
// IncrementAttempt/Delete in CRUD) stay in their respective files.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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

// ShortenSessionLeases fences active work from a disconnected gRPC session.
// The lease remains briefly recoverable so a transient reconnect can settle,
// then the canonical TaskLeaseReaper requeues it. This avoids leaving a
// RUNNING task alive for the full 30-minute TTL when its worker process died.
func (r *SQLiteTaskRepository) ShortenSessionLeases(ctx context.Context, workerID, sessionID string, deadline time.Time) (int, error) {
	if r == nil || r.store == nil || r.store.db == nil {
		return 0, fmt.Errorf("task repository: shorten session leases store not initialized")
	}
	if workerID == "" || sessionID == "" {
		return 0, fmt.Errorf("task repository: shorten session leases requires workerID + sessionID")
	}
	res, err := r.store.db.ExecContext(ctx, `
		UPDATE tasks
		SET lease_expires_at = ?, updated_at = ?
		WHERE worker_id = ?
		  AND status IN ('LEASED', 'RUNNING')
		  AND lease_id <> ''
		  AND EXISTS (
			SELECT 1 FROM task_attempts a
			WHERE a.task_id = tasks.task_id
			  AND a.worker_id = tasks.worker_id
			  AND a.lease_id = tasks.lease_id
			  AND a.worker_session_id = ?
			  AND a.status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT')
		  )`,
		deadline.UTC().Format(time.RFC3339), nowRFC3339(), workerID, sessionID)
	if err != nil {
		return 0, wrapDBInfrastructure("task shorten session leases", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, wrapDBInfrastructure("task shorten session leases rows", err)
	}
	return int(n), nil
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
		return nil, wrapDBInfrastructure("task claim begin", err)
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
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, wrapDBInfrastructure("task claim select", err)
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
		return nil, wrapDBInfrastructure("task claim cas", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return nil, wrapDBInfrastructure("task claim rows", err)
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
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, wrapDBInfrastructure("task claim spec read", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, wrapDBInfrastructure("task claim commit", err)
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
		return wrapDBInfrastructure("task release lease begin", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	now := nowRFC3339()
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
		return wrapDBInfrastructure("task release lease task update", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return wrapDBInfrastructure("task release lease rows", err)
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
		return wrapDBInfrastructure("task release lease delete pending attempt", err)
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
		return wrapDBInfrastructure("task release lease reconcile attempt_count", err)
	}

	if err := tx.Commit(); err != nil {
		return wrapDBInfrastructure("task release lease commit", err)
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
// SessionID and WorkerSnapshotID are persisted with the canonical attempt
// in the same transaction. CapabilityRevision remains an in-memory fencing
// value checked by the caller before and after the claim.
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
		return nil, nil, wrapDBInfrastructure("task claim-for-worker begin", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Runtime identity is canonical master-owned data. Validate the supplied
	// session/snapshot tuple before touching the task CAS or minting an attempt.
	if err := validateWorkerRuntimeIdentityTx(ctx, tx, cmd.WorkerID, cmd.SessionID, cmd.WorkerSnapshotID); err != nil {
		return nil, nil, fmt.Errorf("task claim-for-worker runtime identity: %w", err)
	}

	// 1. SELECT the specific task candidate with revision + executor gate.
	// Legacy/minimal fixtures may not have the reservation table; production
	// migrations always do. When present, only the owning worker can consume
	// a hard future reservation.
	reservationClause := ""
	var reservationTable int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='future_task_reservations'`).Scan(&reservationTable); err == nil && reservationTable > 0 {
		reservationClause = ` AND (NOT EXISTS (SELECT 1 FROM future_task_reservations fr WHERE fr.task_id = tasks.task_id AND fr.expires_at > ?) OR EXISTS (SELECT 1 FROM future_task_reservations fr WHERE fr.task_id = tasks.task_id AND fr.worker_id = ? AND fr.expires_at > ?))`
	}
	queryArgs := []interface{}{cmd.TaskID, cmd.ExpectedTaskRevision, execKey.ID, legacyExecutorID, execKey.Version}
	if reservationClause != "" {
		queryArgs = append(queryArgs, nowStr, cmd.WorkerID, nowStr)
	}
	row := tx.QueryRowContext(ctx,
		`SELECT `+strings.Join(taskColumns, ", ")+`
		 FROM tasks
		 WHERE task_id = ?
		   AND status = 'READY'
		   AND revision = ?
		   AND executor_id IN (?, ?)
		   AND executor_version = ?
		   AND (worker_id = '' OR worker_id IS NULL)`+reservationClause,
		queryArgs...,
	)
	t, err := scanTask(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, fmt.Errorf("task claim-for-worker %s: task not READY or executor/revision mismatch: %w", cmd.TaskID, taskgraph.ErrTransitionConflict)
	}
	if err != nil {
		return nil, nil, wrapDBInfrastructure("task claim-for-worker select", err)
	}

	// 2. Self-heal stale attempt_count from immutable attempt history.
	var maxSeenAttempt sql.NullInt64
	if err := tx.QueryRowContext(ctx,
		`SELECT MAX(attempt_number) FROM task_attempts WHERE task_id = ?`,
		t.ID,
	).Scan(&maxSeenAttempt); err != nil {
		return nil, nil, wrapDBInfrastructure("task claim-for-worker max attempt read", err)
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
		return nil, nil, wrapDBInfrastructure("task claim-for-worker cas", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return nil, nil, wrapDBInfrastructure("task claim-for-worker rows", err)
	}
	if n == 0 {
		return nil, nil, fmt.Errorf("task claim-for-worker %s: CAS raced out (revision/executor mismatch or concurrent claim): %w", cmd.TaskID, taskgraph.ErrTransitionConflict)
	}
	if reservationClause != "" {
		if _, err := tx.ExecContext(ctx, `DELETE FROM future_task_reservations WHERE task_id = ? AND worker_id = ?`, cmd.TaskID, cmd.WorkerID); err != nil {
			return nil, nil, wrapDBInfrastructure("task claim-for-worker reservation consume", err)
		}
	}

	// 5. INSERT PENDING TaskAttempt in the same tx.
	_, err = tx.ExecContext(ctx,
		`INSERT INTO task_attempts (
			id, task_id, job_id, attempt_number, worker_id,
			worker_session_id, worker_snapshot_id, lease_id,
			status, report_version, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, ?, ?)`,
		attemptID, t.ID, t.JobID, attemptNumber, cmd.WorkerID,
		cmd.SessionID, cmd.WorkerSnapshotID, cmd.LeaseID, nowStr, nowStr,
	)
	if err != nil {
		return nil, nil, wrapDBInfrastructure("task claim-for-worker insert", err)
	}

	// 6. Read task_spec payload.
	var specPayloadJSON sql.NullString
	err = tx.QueryRowContext(ctx,
		`SELECT payload_json FROM task_specs WHERE task_id = ?`,
		t.ID,
	).Scan(&specPayloadJSON)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, nil, wrapDBInfrastructure("task claim-for-worker spec read", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, nil, wrapDBInfrastructure("task claim-for-worker commit", err)
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
		ID:               attemptID,
		TaskID:           t.ID,
		JobID:            t.JobID,
		AttemptNumber:    attemptNumber,
		WorkerID:         cmd.WorkerID,
		WorkerSessionID:  cmd.SessionID,
		WorkerSnapshotID: cmd.WorkerSnapshotID,
		LeaseID:          cmd.LeaseID,
		Status:           taskattempts.AttemptStatusPending,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	return tws, att, nil
}
