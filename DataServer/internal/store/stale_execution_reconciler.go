package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/audittrail"
	"velox-server/internal/jobs"
	"velox-server/internal/taskgraph"
)

// StaleExecutionCategory is the stable operator-facing category for a finding.
type StaleExecutionCategory string

const (
	StaleLeaseExpired      StaleExecutionCategory = "expired_lease"
	StaleOrphanTask        StaleExecutionCategory = "orphan_task"
	StaleCommittedArtifact StaleExecutionCategory = "committed_artifact_job_drift"
	StaleUnconfirmedSpool  StaleExecutionCategory = "unconfirmed_spool"
	StaleWorkerOffline     StaleExecutionCategory = "worker_offline"
	StaleOrphanAttempt     StaleExecutionCategory = "orphan_attempt"
)

// StaleExecutionFinding is a read-only change proposal emitted by Scan.
type StaleExecutionFinding struct {
	Category       StaleExecutionCategory `json:"category"`
	ResourceType   string                 `json:"resource_type"`
	ResourceID     string                 `json:"resource_id"`
	JobID          string                 `json:"job_id,omitempty"`
	TaskID         string                 `json:"task_id,omitempty"`
	AttemptID      string                 `json:"attempt_id,omitempty"`
	ArtifactID     string                 `json:"artifact_id,omitempty"`
	CommitID       string                 `json:"commit_id,omitempty"`
	WorkerID       string                 `json:"worker_id,omitempty"`
	LeaseID        string                 `json:"lease_id,omitempty"`
	OldStatus      string                 `json:"old_status,omitempty"`
	ProposedStatus string                 `json:"proposed_status,omitempty"`
	Reason         string                 `json:"reason"`
	ObservedAt     time.Time              `json:"observed_at"`
}

// StaleExecutionReport is returned by both dry-run and apply modes.
type StaleExecutionReport struct {
	GeneratedAt string                  `json:"generated_at"`
	Mode        string                  `json:"mode"`
	Findings    []StaleExecutionFinding `json:"findings"`
	Applied     []StaleExecutionFinding `json:"applied,omitempty"`
	Skipped     int                     `json:"skipped,omitempty"`
}

// StaleExecutionReconciler is the typed application maintenance surface.
// SQL is intentionally confined to this store package; the admin command
// only handles flags, serialization, and process I/O.
type StaleExecutionReconciler struct {
	db                 *sql.DB
	tasks              *SQLiteTaskRepository
	jobs               *SQLiteJobRepository
	workerOfflineAfter time.Duration
}

func NewStaleExecutionReconciler(s *SQLiteStore) (*StaleExecutionReconciler, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("stale execution reconciler: store is not initialized")
	}
	return &StaleExecutionReconciler{
		db: s.db, tasks: NewSQLiteTaskRepository(s), jobs: NewSQLiteJobRepository(s),
		workerOfflineAfter: 10 * time.Minute,
	}, nil
}

func (r *StaleExecutionReconciler) SetWorkerOfflineAfter(d time.Duration) {
	if d > 0 {
		r.workerOfflineAfter = d
	}
}

// Scan is SELECT-only and deterministic. It never writes state or audit rows.
func (r *StaleExecutionReconciler) Scan(ctx context.Context, now time.Time, limit int) ([]StaleExecutionFinding, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if limit <= 0 {
		limit = 500
	}
	// Scan each category independently, then interleave the results. This
	// preserves the caller's global limit without allowing an early category
	// (for example, a large expired-lease backlog) to starve later categories.
	scanners := []func(context.Context, time.Time, int, []StaleExecutionFinding) ([]StaleExecutionFinding, error){
		r.scanExpiredLeases, r.scanOrphanTasks, r.scanCommittedArtifactDrift,
		r.scanUnconfirmedSpool, r.scanOfflineWorkers, r.scanOrphanAttempts,
	}
	perCategory := make([][]StaleExecutionFinding, len(scanners))
	for i, scan := range scanners {
		var scanErr error
		perCategory[i], scanErr = scan(ctx, now, limit, nil)
		if scanErr != nil {
			return nil, scanErr
		}
	}
	findings := make([]StaleExecutionFinding, 0, limit)
	for offset := 0; len(findings) < limit; offset++ {
		added := false
		for category := range perCategory {
			if offset >= len(perCategory[category]) {
				continue
			}
			findings = append(findings, perCategory[category][offset])
			added = true
			if len(findings) == limit {
				break
			}
		}
		if !added {
			break
		}
	}
	return findings, nil
}

func (r *StaleExecutionReconciler) Reconcile(ctx context.Context, now time.Time, limit int, apply bool, actor string) (StaleExecutionReport, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	actor = strings.TrimSpace(actor)
	if actor == "" {
		actor = "velox-admin"
	}
	findings, err := r.Scan(ctx, now, limit)
	if err != nil {
		return StaleExecutionReport{}, err
	}
	report := StaleExecutionReport{GeneratedAt: now.UTC().Format(time.RFC3339Nano), Mode: "dry-run", Findings: findings}
	if !apply {
		return report, nil
	}
	report.Mode = "apply"
	for _, finding := range findings {
		changed, err := r.applyFinding(ctx, finding, actor, now)
		if err != nil {
			return report, fmt.Errorf("apply %s/%s: %w", finding.Category, finding.ResourceID, err)
		}
		if changed {
			report.Applied = append(report.Applied, finding)
		} else {
			report.Skipped++
		}
	}
	return report, nil
}

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
		if job, jerr := r.jobs.Get(ctx, f.JobID); jerr == nil && job != nil && job.MaxRetries > 0 {
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
	n, _ := res.RowsAffected()
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
	if _, err := tx.ExecContext(ctx, `
		UPDATE task_attempts
		   SET status='SUCCEEDED', completed_at=COALESCE(completed_at, ?),
		       report_version=report_version+1, updated_at=?
		 WHERE id=? AND task_id=? AND job_id=? AND worker_id=? AND lease_id=?
		   AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')`,
		nowStr, nowStr, f.AttemptID, f.TaskID, f.JobID, f.WorkerID, f.LeaseID); err != nil {
		return false, err
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
	if taskRows, _ := taskRes.RowsAffected(); taskRows != 1 {
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
	n, _ := res.RowsAffected()
	if n != 1 {
		return false, nil
	}

	// Delivery reconstruction is deliberately plan-scoped. There is no
	// global-destination fallback here; a missing plan produces no delivery
	// rows and remains visible to the operator rather than routing output
	// implicitly.
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO job_deliveries
		    (delivery_id, artifact_id, destination_id, status, max_attempts,
		     idempotency_key, created_at, updated_at)
		SELECT 'reconcile_' || a.id || '_' || p.destination_id,
		       a.id, p.destination_id, 'PENDING',
		       CASE WHEN p.retry_budget > 0 THEN p.retry_budget ELSE 5 END,
		       a.id || '_' || p.destination_id, ?, ?
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
	n, _ := res.RowsAffected()
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
	n, _ := res.RowsAffected()
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
	n, _ := res.RowsAffected()
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
		if err := tx.QueryRowContext(ctx, `SELECT max_retries FROM jobs WHERE job_id=?`, lease.jobID).Scan(&configured); err == nil && configured > 0 {
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
		taskRows, _ := taskRes.RowsAffected()
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

func (r *StaleExecutionReconciler) auditEventForFinding(f StaleExecutionFinding, actor string, now time.Time) audittrail.Event {
	metadata, _ := json.Marshal(map[string]any{"category": f.Category, "reason": f.Reason, "old_status": f.OldStatus, "proposed_status": f.ProposedStatus, "observed_at": f.ObservedAt.UTC().Format(time.RFC3339Nano)})
	h := sha256.Sum256([]byte(string(f.Category) + ":" + f.ResourceType + ":" + f.ResourceID + ":" + f.OldStatus))
	return audittrail.Event{ID: "reconcile-" + hex.EncodeToString(h[:]), OccurredAt: now, ActorType: "operator", ActorID: actor, Action: "STALE_EXECUTION_RECONCILED", ResourceType: f.ResourceType, ResourceID: f.ResourceID, BeforeHash: hashText(f.OldStatus), AfterHash: hashText(f.ProposedStatus), MetadataJSON: string(metadata)}
}

func appendReconcileAuditTx(ctx context.Context, tx *sql.Tx, f StaleExecutionFinding, actor string, now time.Time) error {
	metadata, err := json.Marshal(map[string]any{"category": f.Category, "reason": f.Reason, "old_status": f.OldStatus, "proposed_status": f.ProposedStatus, "observed_at": f.ObservedAt.UTC().Format(time.RFC3339Nano)})
	if err != nil {
		return err
	}
	h := sha256.Sum256([]byte(string(f.Category) + ":" + f.ResourceType + ":" + f.ResourceID + ":" + f.OldStatus))
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO audit_events (id, occurred_at, actor_type, actor_id, action, resource_type, resource_id, request_id, trace_id, before_hash, after_hash, metadata_json) VALUES (?, ?, 'operator', ?, 'STALE_EXECUTION_RECONCILED', ?, ?, '', '', ?, ?, ?)`, "reconcile-"+hex.EncodeToString(h[:]), now.UTC().Format(time.RFC3339Nano), actor, f.ResourceType, f.ResourceID, hashText(f.OldStatus), hashText(f.ProposedStatus), audittrail.RedactMetadata(string(metadata)))
	return err
}

func hashText(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
