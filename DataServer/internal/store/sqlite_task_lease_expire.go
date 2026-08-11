// Package store / sqlite_task_lease_expire.go — lease expiry and reaping.
// Extracted from sqlite_task_lease.go: the reaper scan
// (RequeueExpiredLeases, SELECT-only) and the per-task atomic reap
// (ExpireTaskLeaseAtomic, CAS + attempt close + retry budget in one tx).
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"velox-server/internal/audittrail"
	"velox-server/internal/taskgraph"
)

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
	return r.expireTaskLeaseAtomic(ctx, taskID, leaseID, leaseExpiresAtObserved, maxRetries, nil)
}

// ExpireTaskLeaseAtomicAudited applies the canonical lease reap and inserts
// the supplied append-only audit event before committing the same transaction.
// It is the operator-reconciler entry point; a failed audit insert rolls back
// the task and attempt transition.
func (r *SQLiteTaskRepository) ExpireTaskLeaseAtomicAudited(
	ctx context.Context,
	taskID, leaseID, leaseExpiresAtObserved string,
	maxRetries int,
	event audittrail.Event,
) (taskgraph.ExpireResult, error) {
	return r.expireTaskLeaseAtomic(ctx, taskID, leaseID, leaseExpiresAtObserved, maxRetries, &event)
}

func (r *SQLiteTaskRepository) expireTaskLeaseAtomic(
	ctx context.Context,
	taskID, leaseID, leaseExpiresAtObserved string,
	maxRetries int,
	auditEvent *audittrail.Event,
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
		return taskgraph.ExpireResult{}, wrapDBInfrastructure("task expire atomic begin", err)
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
	if errors.Is(err, sql.ErrNoRows) {
		return taskgraph.ExpireResult{}, fmt.Errorf("task expire atomic %s: %w", taskID, taskgraph.ErrTaskNotFound)
	}
	if err != nil {
		return taskgraph.ExpireResult{}, wrapDBInfrastructure("task expire atomic read", err)
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
		return taskgraph.ExpireResult{}, wrapDBInfrastructure("task expire atomic attempt cas", err)
	}
	attemptRows, rowsErr := readRowsAffected(attRes, "task expire atomic attempt cas")
	if rowsErr != nil {
		return taskgraph.ExpireResult{}, rowsErr
	}

	var attemptID string
	idProbeErr := tx.QueryRowContext(ctx,
		`SELECT id FROM task_attempts
		 WHERE task_id = ? AND worker_id = ? AND lease_id = ?
		 ORDER BY attempt_number DESC LIMIT 1`,
		taskID, currentWorker, leaseID,
	).Scan(&attemptID)
	if errors.Is(idProbeErr, sql.ErrNoRows) {
		// Defensive §9.5 case: task in LEASED/RUNNING with no matching
		// attempt row. The Task CAS still proceeds (lease recovered),
		// but AttemptClosed=false so the reaper logs the breach.
		attemptID = ""
		attemptRows = 0
	} else if idProbeErr != nil {
		return taskgraph.ExpireResult{}, wrapDBInfrastructure("task expire atomic attempt probe", idProbeErr)
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
		return taskgraph.ExpireResult{}, wrapDBInfrastructure("task expire atomic task cas", err)
	}
	taskRows, rowsErr := readRowsAffected(taskRes, "task expire atomic task cas")
	if rowsErr != nil {
		return taskgraph.ExpireResult{}, rowsErr
	}
	if taskRows == 0 {
		return taskgraph.ExpireResult{}, fmt.Errorf("task expire atomic %s: task CAS raced out: %w",
			taskID, taskgraph.ErrTransitionConflict)
	}

	if auditEvent != nil {
		if auditEvent.ID == "" {
			return taskgraph.ExpireResult{}, fmt.Errorf("task expire atomic audit event id is required")
		}
		if auditEvent.OccurredAt.IsZero() {
			auditEvent.OccurredAt = time.Now().UTC()
		}
		if auditEvent.MetadataJSON == "" {
			auditEvent.MetadataJSON = "{}"
		}
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO audit_events
			(id, occurred_at, actor_type, actor_id, action, resource_type, resource_id,
			 request_id, trace_id, before_hash, after_hash, metadata_json)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			auditEvent.ID, auditEvent.OccurredAt.UTC().Format(time.RFC3339Nano),
			auditEvent.ActorType, auditEvent.ActorID, auditEvent.Action,
			auditEvent.ResourceType, auditEvent.ResourceID, auditEvent.RequestID,
			auditEvent.TraceID, auditEvent.BeforeHash, auditEvent.AfterHash,
			audittrail.RedactMetadata(auditEvent.MetadataJSON)); err != nil {
			return taskgraph.ExpireResult{}, wrapDBInfrastructure("task expire atomic audit", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return taskgraph.ExpireResult{}, wrapDBInfrastructure("task expire atomic commit", err)
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
		return nil, wrapDBInfrastructure("task reaper select", err)
	}
	defer rows.Close()

	var candidates []taskgraph.RequeueCandidate
	for rows.Next() {
		var c taskgraph.RequeueCandidate
		if scanErr := rows.Scan(&c.ID, &c.WorkerID, &c.LeaseID, &c.LeaseExpiresAt, &c.AttemptCount); scanErr != nil {
			return nil, wrapDBInfrastructure("task reaper scan", scanErr)
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapDBInfrastructure("task reaper rows", err)
	}
	return candidates, nil
}
