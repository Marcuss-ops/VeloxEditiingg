package store

import (
	"context"
	"fmt"
	"time"
)

func (r *StaleExecutionReconciler) scanExpiredLeases(ctx context.Context, now time.Time, limit int, out []StaleExecutionFinding) ([]StaleExecutionFinding, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT t.task_id, COALESCE(t.job_id,''), COALESCE(t.worker_id,''), COALESCE(t.lease_id,''), t.status, COALESCE(t.attempt_id,'') FROM tasks t JOIN jobs j ON j.job_id=t.job_id WHERE t.status IN ('LEASED','RUNNING') AND t.worker_id<>'' AND t.lease_id<>'' AND COALESCE(t.lease_expires_at,'')<>'' AND t.lease_expires_at < ? AND j.status <> 'CANCELLED' ORDER BY t.lease_expires_at, t.task_id LIMIT ?`, now.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, fmt.Errorf("scan expired leases: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var taskID, jobID, workerID, leaseID, status, attemptID string
		if err := rows.Scan(&taskID, &jobID, &workerID, &leaseID, &status, &attemptID); err != nil {
			return nil, err
		}
		out = append(out, StaleExecutionFinding{Category: StaleLeaseExpired, ResourceType: "task", ResourceID: taskID, JobID: jobID, TaskID: taskID, AttemptID: attemptID, WorkerID: workerID, LeaseID: leaseID, OldStatus: status, ProposedStatus: "READY or FAILED", Reason: "lease expired; canonical task reaper will close the attempt atomically", ObservedAt: now.UTC()})
	}
	return out, rows.Err()
}

func (r *StaleExecutionReconciler) scanOrphanTasks(ctx context.Context, now time.Time, limit int, out []StaleExecutionFinding) ([]StaleExecutionFinding, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT t.task_id, COALESCE(t.job_id,''), t.status, COALESCE(t.worker_id,''), COALESCE(t.lease_id,''), COALESCE(t.attempt_id,''), CASE WHEN j.job_id IS NULL THEN 'missing_job' ELSE 'cancelled_job' END FROM tasks t LEFT JOIN jobs j ON j.job_id=t.job_id		 WHERE t.status IN ('READY','LEASED','RUNNING')			AND (j.job_id IS NULL OR j.status IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT'))

			AND NOT (t.status IN ('LEASED','RUNNING')
			            AND COALESCE(t.lease_expires_at,'')<>''
			            AND t.lease_expires_at < ?
			            AND j.job_id IS NOT NULL
			            AND j.status <> 'CANCELLED')
 ORDER BY t.task_id LIMIT ?`, now.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, fmt.Errorf("scan orphan tasks: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var taskID, jobID, status, workerID, leaseID, attemptID, reason string
		if err := rows.Scan(&taskID, &jobID, &status, &workerID, &leaseID, &attemptID, &reason); err != nil {
			return nil, err
		}
		out = append(out, StaleExecutionFinding{Category: StaleOrphanTask, ResourceType: "task", ResourceID: taskID, JobID: jobID, TaskID: taskID, AttemptID: attemptID, WorkerID: workerID, LeaseID: leaseID, OldStatus: status, ProposedStatus: "CANCELLED", Reason: reason, ObservedAt: now.UTC()})
	}
	return out, rows.Err()
}

func (r *StaleExecutionReconciler) scanCommittedArtifactDrift(ctx context.Context, now time.Time, limit int, out []StaleExecutionFinding) ([]StaleExecutionFinding, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT MIN(a.id), a.job_id, ac.commit_id, ac.attempt_id, ac.task_id, ac.worker_id, ac.lease_id, j.status FROM artifacts a JOIN task_output_declarations d ON d.artifact_id=a.id JOIN attempt_commits ac ON ac.commit_id=d.commit_id AND ac.job_id=a.job_id AND ac.task_id=d.task_id AND ac.attempt_id=d.attempt_id AND ac.status='COMMITTED' JOIN jobs j ON j.job_id=a.job_id WHERE a.status='READY' AND j.status IN ('RUNNING','LEASED') GROUP BY a.job_id, ac.commit_id, ac.attempt_id, ac.task_id, ac.worker_id, ac.lease_id, j.status ORDER BY MIN(a.id) LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("scan committed artifact drift: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var artifactID, jobID, commitID, attemptID, taskID, workerID, leaseID, jobStatus string
		if err := rows.Scan(&artifactID, &jobID, &commitID, &attemptID, &taskID, &workerID, &leaseID, &jobStatus); err != nil {
			return nil, err
		}
		out = append(out, StaleExecutionFinding{Category: StaleCommittedArtifact, ResourceType: "job", ResourceID: jobID, JobID: jobID, TaskID: taskID, AttemptID: attemptID, ArtifactID: artifactID, CommitID: commitID, WorkerID: workerID, LeaseID: leaseID, OldStatus: jobStatus, ProposedStatus: "SUCCEEDED", Reason: "READY artifact and COMMITTED attempt exist while job is non-terminal", ObservedAt: now.UTC()})
	}
	return out, rows.Err()
}

func (r *StaleExecutionReconciler) scanUnconfirmedSpool(ctx context.Context, now time.Time, limit int, out []StaleExecutionFinding) ([]StaleExecutionFinding, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT d.declaration_id, d.commit_id, d.task_id, d.attempt_id, ac.job_id, ac.worker_id, ac.lease_id, d.status FROM task_output_declarations d JOIN attempt_commits ac ON ac.commit_id=d.commit_id WHERE d.worker_spool_key<>'' AND COALESCE(d.upload_id,'')='' AND COALESCE(d.artifact_id,'')='' AND ac.status IN ('DECLARED','UPLOADING','RECEIVED') AND ac.commit_deadline_at < ? ORDER BY ac.commit_deadline_at, d.declaration_id LIMIT ?`, now.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, fmt.Errorf("scan unconfirmed spool: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var declarationID, commitID, taskID, attemptID, jobID, workerID, leaseID, status string
		if err := rows.Scan(&declarationID, &commitID, &taskID, &attemptID, &jobID, &workerID, &leaseID, &status); err != nil {
			return nil, err
		}
		out = append(out, StaleExecutionFinding{Category: StaleUnconfirmedSpool, ResourceType: "task_output_declaration", ResourceID: declarationID, JobID: jobID, TaskID: taskID, AttemptID: attemptID, CommitID: commitID, WorkerID: workerID, LeaseID: leaseID, OldStatus: status, ProposedStatus: "REJECTED/EXPIRED", Reason: "worker spool declaration has no upload or artifact after commit deadline", ObservedAt: now.UTC()})
	}
	return out, rows.Err()
}

func (r *StaleExecutionReconciler) scanOfflineWorkers(ctx context.Context, now time.Time, limit int, out []StaleExecutionFinding) ([]StaleExecutionFinding, error) {
	cutoff := now.UTC().Add(-r.workerOfflineAfter).Format(time.RFC3339Nano)
	rows, err := r.db.QueryContext(ctx, `SELECT worker_id, COALESCE(connection_state,''), COALESCE(last_heartbeat_at,'') FROM workers WHERE COALESCE(connection_state,'')<>'PARTITIONED' AND (last_heartbeat_at IS NULL OR last_heartbeat_at='' OR last_heartbeat_at < ?) ORDER BY COALESCE(last_heartbeat_at,''), worker_id LIMIT ?`, cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("scan offline workers: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var workerID, state, heartbeat string
		if err := rows.Scan(&workerID, &state, &heartbeat); err != nil {
			return nil, err
		}
		out = append(out, StaleExecutionFinding{Category: StaleWorkerOffline, ResourceType: "worker", ResourceID: workerID, WorkerID: workerID, OldStatus: state, ProposedStatus: "PARTITIONED", Reason: "worker heartbeat exceeded the offline threshold; active leases are separately CAS-recovered only when expired", ObservedAt: now.UTC()})
	}
	return out, rows.Err()
}

func (r *StaleExecutionReconciler) scanOrphanAttempts(ctx context.Context, now time.Time, limit int, out []StaleExecutionFinding) ([]StaleExecutionFinding, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT a.id, a.task_id, COALESCE(a.job_id,''), a.status,
		       COALESCE(a.worker_id,''), COALESCE(a.lease_id,''),
		       CASE
				WHEN t.task_id IS NULL THEN 'missing_task'
				WHEN t.status IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT') THEN 'terminal_task'
				WHEN NOT EXISTS (SELECT 1 FROM jobs j WHERE j.job_id=t.job_id) THEN 'missing_job'
				WHEN EXISTS (SELECT 1 FROM jobs j WHERE j.job_id=t.job_id
				             AND j.status IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')) THEN 'terminal_job'
				ELSE 'inconsistent_task'
		       END
		  FROM task_attempts a
		  LEFT JOIN tasks t ON t.task_id=a.task_id
		 WHERE a.status NOT IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')
		   AND (t.task_id IS NULL OR t.status IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')
		        OR NOT EXISTS (SELECT 1 FROM jobs j WHERE j.job_id=t.job_id)
		        OR EXISTS (SELECT 1 FROM jobs j WHERE j.job_id=t.job_id
		                   AND j.status IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')))
		 ORDER BY a.updated_at, a.id
		 LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("scan orphan attempts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var attemptID, taskID, jobID, status, workerID, leaseID, reason string
		if err := rows.Scan(&attemptID, &taskID, &jobID, &status, &workerID, &leaseID, &reason); err != nil {
			return nil, err
		}
		out = append(out, StaleExecutionFinding{
			Category: StaleOrphanAttempt, ResourceType: "task_attempt", ResourceID: attemptID,
			JobID: jobID, TaskID: taskID, AttemptID: attemptID, WorkerID: workerID,
			LeaseID: leaseID, OldStatus: status, ProposedStatus: "CANCELLED",
			Reason: "non-terminal attempt has no active parent task: " + reason, ObservedAt: now.UTC(),
		})
	}
	return out, rows.Err()
}
