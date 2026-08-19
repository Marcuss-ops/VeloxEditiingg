package stalereconcile

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"velox-server/internal/jobs"
	"velox-server/internal/taskgraph"
)

func (r *StaleExecutionReconciler) applyFinding(ctx context.Context, f StaleExecutionFinding, actor string, now time.Time) (bool, error) {
	switch f.Category {
	case StaleLeaseExpired:
		changed, err := r.applyExpiredLease(ctx, f, actor, now)
		if err != nil || !changed {
			return changed, err
		}
		return changed, nil
	case StaleOrphanTask:
		return r.applyOrphanTask(ctx, f, actor, now)
	case StaleCommittedArtifact:
		return r.applyCommittedArtifact(ctx, f, actor, now)
	case StaleUnconfirmedSpool:
		return r.applyUnconfirmedSpool(ctx, f, actor, now)
	case StaleWorkerOffline:
		return r.applyOfflineWorker(ctx, f, actor, now)
	case StaleOrphanAttempt:
		return r.applyOrphanAttempt(ctx, f, actor, now)
	default:
		return false, fmt.Errorf("unknown reconciliation category %q", f.Category)
	}
}

func (r *StaleExecutionReconciler) applyExpiredLease(ctx context.Context, f StaleExecutionFinding, actor string, now time.Time) (bool, error) {
	// Target the finding directly instead of rescanning the first N expired
	// rows. This keeps apply correct when the finding was beyond the reaper's
	// bounded scan window and preserves the canonical CAS in the repository.
	var leaseExpiresAt string
	err := r.db.QueryRowContext(ctx, `
		SELECT COALESCE(lease_expires_at,'') FROM tasks
		 WHERE task_id=? AND worker_id=? AND lease_id=?
		   AND status IN ('LEASED','RUNNING')
		   AND COALESCE(lease_expires_at,'')<>'' AND lease_expires_at < ?`,
		f.TaskID, f.WorkerID, f.LeaseID, now.UTC().Format(time.RFC3339Nano)).Scan(&leaseExpiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	maxRetries := 3
	if f.JobID != "" {
		job, jerr := r.jobs.Get(ctx, f.JobID)
		if jerr != nil {
			return false, jerr
		}
		if job != nil && job.MaxRetries > 0 {
			maxRetries = job.MaxRetries
		}
	}
	event := r.auditEventForFinding(f, actor, now)
	_, err = r.tasks.ExpireTaskLeaseAtomicAudited(ctx, f.TaskID, f.LeaseID, leaseExpiresAt, maxRetries, event)
	if errors.Is(err, taskgraph.ErrTransitionConflict) {
		return false, nil
	}
	return err == nil, err
}

func (r *StaleExecutionReconciler) applyOrphanTask(ctx context.Context, f StaleExecutionFinding, actor string, now time.Time) (bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	nowStr := now.UTC().Format(time.RFC3339Nano)
	res, err := tx.ExecContext(ctx, `UPDATE tasks SET status='CANCELLED', completed_at=?, worker_id='', lease_id='', lease_expires_at=NULL, revision=revision+1, updated_at=? WHERE task_id=? AND status IN ('READY','LEASED','RUNNING') AND (NOT EXISTS (SELECT 1 FROM jobs WHERE job_id=tasks.job_id) OR EXISTS (SELECT 1 FROM jobs WHERE job_id=tasks.job_id AND status IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')))`, nowStr, nowStr, f.TaskID)
	if err != nil {
		return false, err
	}
	n, err := readRowsAffected(res, "stale reconciler orphan task")
	if err != nil {
		return false, err
	}
	if n != 1 {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE task_attempts SET status='CANCELLED', completed_at=?, error_code='ORPHAN_TASK', error_message='parent job is missing or cancelled', report_version=report_version+1, updated_at=? WHERE task_id=? AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')`, nowStr, nowStr, f.TaskID); err != nil {
		return false, err
	}
	if err := appendReconcileAuditTx(ctx, tx, f, actor, now); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (r *StaleExecutionReconciler) applyCommittedArtifact(ctx context.Context, f StaleExecutionFinding, actor string, now time.Time) (bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	nowStr := now.UTC().Format(time.RFC3339Nano)

	// Reconstruct the terminal commit transition in one transaction. The
	// evidence is stronger than a mere job roll-up: the attempt commit is
	// already COMMITTED and its output artifact is READY. Mark the matching
	// attempt/task terminal, then let the job CAS require that every task is
	// SUCCEEDED before it becomes SUCCEEDED itself.
	attemptRes, err := tx.ExecContext(ctx, `
		UPDATE task_attempts
		   SET status='SUCCEEDED', completed_at=COALESCE(completed_at, ?),
		       report_version=report_version+1, updated_at=?
		 WHERE id=? AND task_id=? AND job_id=? AND worker_id=? AND lease_id=?
		   AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')`,
		nowStr, nowStr, f.AttemptID, f.TaskID, f.JobID, f.WorkerID, f.LeaseID)
	if err != nil {
		return false, err
	}
	attemptRows, err := readRowsAffected(attemptRes, "stale reconciler committed attempt")
	if err != nil {
		return false, err
	}
	if attemptRows != 1 {
		// A replay may have completed this exact attempt concurrently. Accept
		// only that terminal identity; never promote the task when its attempt
		// is missing, failed, or belongs to another lease/worker.
		var status, taskID, jobID, workerID, leaseID string
		err := tx.QueryRowContext(ctx, `
			SELECT status, task_id, job_id, worker_id, COALESCE(lease_id,'')
			  FROM task_attempts WHERE id=?`, f.AttemptID).Scan(
			&status, &taskID, &jobID, &workerID, &leaseID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return false, nil
			}
			return false, err
		}
		if status != string(taskgraph.StatusSucceeded) || taskID != f.TaskID ||
			jobID != f.JobID || workerID != f.WorkerID || leaseID != f.LeaseID {
			return false, nil
		}
	}
	taskRes, err := tx.ExecContext(ctx, `
		UPDATE tasks
		   SET status='SUCCEEDED', completed_at=COALESCE(completed_at, ?),
		       winning_attempt_id=?, winning_attempt_committed_at=?,
		       winning_attempt_terminal_pending=0, revision=revision+1, updated_at=?
		 WHERE task_id=? AND attempt_id=? AND worker_id=? AND lease_id=?
		   AND status IN ('RUNNING','LEASED')`,
		nowStr, f.AttemptID, nowStr, nowStr, f.TaskID, f.AttemptID, f.WorkerID, f.LeaseID)
	if err != nil {
		return false, err
	}
	taskRows, err := readRowsAffected(taskRes, "stale reconciler committed task")
	if err != nil {
		return false, err
	}
	if taskRows != 1 {
		// A replay may have completed the exact task concurrently. Accept
		// only that terminal identity; never roll up an unrelated task.
		var status, attemptID, workerID, leaseID, winningAttemptID string
		err := tx.QueryRowContext(ctx, `
			SELECT status, COALESCE(attempt_id,''), COALESCE(worker_id,''),
			       COALESCE(lease_id,''), COALESCE(winning_attempt_id,'')
			  FROM tasks WHERE task_id=?`, f.TaskID).Scan(&status, &attemptID, &workerID, &leaseID, &winningAttemptID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return false, nil
			}
			return false, err
		}
		if status != string(taskgraph.StatusSucceeded) ||
			(attemptID != f.AttemptID && winningAttemptID != f.AttemptID) ||
			(workerID != "" && workerID != f.WorkerID) ||
			(leaseID != "" && leaseID != f.LeaseID) {
			return false, nil
		}
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE jobs
		   SET status=?, completed_at=COALESCE(completed_at, ?), revision=revision+1, updated_at=?
		 WHERE job_id=? AND status IN ('RUNNING','LEASED','AWAITING_ARTIFACT')
		   AND EXISTS (SELECT 1 FROM tasks WHERE job_id=? AND status='SUCCEEDED')
		   AND NOT EXISTS (SELECT 1 FROM tasks WHERE job_id=? AND status<>'SUCCEEDED')`,
		string(jobs.StatusSucceeded), nowStr, nowStr, f.JobID, f.JobID, f.JobID)
	if err != nil {
		return false, err
	}
	n, err := readRowsAffected(res, "stale reconciler committed job")
	if err != nil {
		return false, err
	}
	if n != 1 {
		return false, nil
	}

	// Delivery reconstruction is deliberately plan-scoped. There is no
	// global-destination fallback here; a missing plan produces no delivery
	// rows and remains visible to the operator rather than routing output
	// implicitly.
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO job_deliveries
		    (delivery_id, artifact_id, publication_id, destination_id, status, max_attempts,
		     idempotency_key, created_at, updated_at)
		SELECT 'reconcile_' || a.id || '_' || p.publication_id || '_' || p.destination_id,
		       a.id, COALESCE(p.publication_id,''), p.destination_id, 'PENDING',
		       CASE WHEN p.retry_budget > 0 THEN p.retry_budget ELSE 5 END,
		       a.id || '_' || p.publication_id || '_' || p.destination_id, ?, ?
		  FROM artifacts a
		  JOIN job_delivery_plans p ON p.job_id=a.job_id AND p.enabled=1
		 WHERE a.id=? AND a.status='READY'`, nowStr, nowStr, f.ArtifactID); err != nil {
		return false, err
	}
	// Recreate the durable commit-protocol notification idempotently so
	// downstream consumers can converge exactly as they do on the normal
	// completion path. Marshal the payload rather than concatenating IDs.
	payload, err := json.Marshal(map[string]string{"commit_id": f.CommitID, "attempt_id": f.AttemptID, "job_id": f.JobID})
	if err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO outbox_events
		    (event_id, aggregate_type, aggregate_id, event_type, payload_json,
		     status, available_at, attempt_count, created_at)
		VALUES (?, 'task', ?, 'commit_protocol.committed', ?, 'PENDING', ?, 0, ?)`,
		"reconcile_commit_"+f.CommitID, f.TaskID, string(payload), nowStr, nowStr); err != nil {
		return false, err
	}
	if err := appendReconcileAuditTx(ctx, tx, f, actor, now); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (r *StaleExecutionReconciler) applyUnconfirmedSpool(ctx context.Context, f StaleExecutionFinding, actor string, now time.Time) (bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	nowStr := now.UTC().Format(time.RFC3339Nano)
	res, err := tx.ExecContext(ctx, `UPDATE task_output_declarations SET status='REJECTED', updated_at=? WHERE declaration_id=? AND worker_spool_key<>'' AND COALESCE(upload_id,'')='' AND COALESCE(artifact_id,'')=''`, nowStr, f.ResourceID)
	if err != nil {
		return false, err
	}
	n, err := readRowsAffected(res, "stale reconciler unconfirmed spool")
	if err != nil {
		return false, err
	}
	if n != 1 {
		return false, nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE attempt_commits SET status='EXPIRED', rejected_code='UNCONFIRMED_SPOOL', rejected_message='stale worker spool declaration has no upload', updated_at=? WHERE commit_id=? AND status IN ('DECLARED','UPLOADING','RECEIVED')`, nowStr, f.CommitID); err != nil {
		return false, err
	}
	if err := appendReconcileAuditTx(ctx, tx, f, actor, now); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (r *StaleExecutionReconciler) applyOrphanAttempt(ctx context.Context, f StaleExecutionFinding, actor string, now time.Time) (bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	nowStr := now.UTC().Format(time.RFC3339Nano)
	res, err := tx.ExecContext(ctx, `
		UPDATE task_attempts
		   SET status='CANCELLED', completed_at=COALESCE(completed_at, ?),
		       error_code='ORPHAN_ATTEMPT',
		       error_message='parent task is missing or terminal',
		       report_version=report_version+1, updated_at=?
		 WHERE id=? AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')
		   AND (NOT EXISTS (SELECT 1 FROM tasks WHERE task_id=task_attempts.task_id)
		        OR EXISTS (SELECT 1 FROM tasks WHERE task_id=task_attempts.task_id
		                   AND status IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT'))			        OR EXISTS (SELECT 1 FROM tasks t
		                   WHERE t.task_id=task_attempts.task_id
		                     AND (NOT EXISTS (SELECT 1 FROM jobs j WHERE j.job_id=t.job_id)
		                          OR EXISTS (SELECT 1 FROM jobs j WHERE j.job_id=t.job_id
		                                     AND j.status IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')))))`,
		nowStr, nowStr, f.AttemptID)
	if err != nil {
		return false, err
	}
	n, err := readRowsAffected(res, "stale reconciler orphan attempt")
	if err != nil {
		return false, err
	}
	if n != 1 {
		return false, nil
	}
	if err := appendReconcileAuditTx(ctx, tx, f, actor, now); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (r *StaleExecutionReconciler) applyOfflineWorker(ctx context.Context, f StaleExecutionFinding, actor string, now time.Time) (bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	nowStr := now.UTC().Format(time.RFC3339Nano)
	res, err := tx.ExecContext(ctx, `UPDATE workers SET connection_state='PARTITIONED', connection_reason='stale_execution_reconciler', status='PARTITIONED', last_state_change_at=? WHERE worker_id=? AND COALESCE(connection_state,'')<>'PARTITIONED'`, nowStr, f.WorkerID)
	if err != nil {
		return false, err
	}
	n, err := readRowsAffected(res, "stale reconciler offline worker")
	if err != nil {
		return false, err
	}
	if n != 1 {
		return false, nil
	}
	if err := appendReconcileAuditTx(ctx, tx, f, actor, now); err != nil {
		return false, err
	}

	// Keep partitioning and expired-lease recovery in the same transaction.
	// A valid lease remains fenced until its persisted TTL expires; expired
	// leases are recovered with the owning job's retry budget.
	rows, err := tx.QueryContext(ctx, `
		SELECT t.task_id, t.job_id, t.lease_id, t.lease_expires_at,
		       t.attempt_count, t.attempt_number
		  FROM tasks t
		 WHERE t.worker_id=? AND t.status IN ('LEASED','RUNNING')
		   AND COALESCE(t.lease_id,'')<>''
		   AND COALESCE(t.lease_expires_at,'')<>''
		   AND t.lease_expires_at < ?`, f.WorkerID, nowStr)
	if err != nil {
		return false, err
	}
	type expiredLease struct {
		taskID, jobID, leaseID, leaseExpiresAt string
		attemptCount, attemptNumber            int
	}
	var leases []expiredLease
	for rows.Next() {
		var lease expiredLease
		if err := rows.Scan(&lease.taskID, &lease.jobID, &lease.leaseID, &lease.leaseExpiresAt, &lease.attemptCount, &lease.attemptNumber); err != nil {
			rows.Close()
			return false, err
		}
		leases = append(leases, lease)
	}
	if err := rows.Close(); err != nil {
		return false, err
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	for _, lease := range leases {
		maxRetries := 3
		var configured int
		jobErr := tx.QueryRowContext(ctx, `SELECT max_retries FROM jobs WHERE job_id=?`, lease.jobID).Scan(&configured)
		if jobErr != nil && !errors.Is(jobErr, sql.ErrNoRows) {
			return false, jobErr
		}
		if jobErr == nil && configured > 0 {
			maxRetries = configured
		}
		effectiveAttempt := maxAttemptOrdinal(lease.attemptCount, lease.attemptNumber)
		newStatus := "READY"
		if effectiveAttempt >= maxRetries+1 {
			newStatus = "FAILED"
		}
		taskRes, err := tx.ExecContext(ctx, `
			UPDATE tasks SET status=?, completed_at=?, worker_id='', lease_id='',
			    lease_expires_at=NULL, attempt_count=?, attempt_id='', attempt_number=0,
			    revision=revision+1, updated_at=?
			 WHERE task_id=? AND worker_id=? AND lease_id=?
			   AND lease_expires_at=? AND status IN ('LEASED','RUNNING')`,
			newStatus, nowStr, effectiveAttempt, nowStr, lease.taskID, f.WorkerID, lease.leaseID, lease.leaseExpiresAt)
		if err != nil {
			return false, err
		}
		taskRows, err := readRowsAffected(taskRes, "stale reconciler expired offline lease")
		if err != nil {
			return false, err
		}
		if taskRows != 1 {
			// A renewal or another reaper won the CAS. Do not mutate the
			// attempt or emit a recovery audit for the stale observation.
			continue
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE task_attempts
			   SET status='TIMED_OUT', completed_at=?, error_code='LEASE_EXPIRED',
			       error_message='offline worker lease expired', report_version=report_version+1, updated_at=?
			 WHERE task_id=? AND worker_id=? AND lease_id=?
			   AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')`,
			nowStr, nowStr, lease.taskID, f.WorkerID, lease.leaseID); err != nil {
			return false, err
		}
		finding := StaleExecutionFinding{
			Category: StaleLeaseExpired, ResourceType: "task", ResourceID: lease.taskID,
			JobID: lease.jobID, TaskID: lease.taskID, WorkerID: f.WorkerID,
			LeaseID: lease.leaseID, OldStatus: "RUNNING", ProposedStatus: newStatus,
			Reason: "worker is offline and persisted lease expired", ObservedAt: now.UTC(),
		}
		if err := appendReconcileAuditTx(ctx, tx, finding, actor, now); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// readRowsAffected is the leaf boundary for driver-specific RowsAffected
// failures. A transition must never treat an unreadable row count as zero or
// as a successful CAS decision.
func readRowsAffected(result sql.Result, operation string) (int64, error) {
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("%s rows affected: %w", operation, err)
	}
	return affected, nil
}

// maxAttemptOrdinal returns the larger of the stored attempt_count and the
// current attempt_number (both are maintained independently on some paths).
func maxAttemptOrdinal(a, b int) int {
	if b > a {
		return b
	}
	return a
}
