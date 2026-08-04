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
	findings := make([]StaleExecutionFinding, 0)
	var err error
	for _, scan := range []func(context.Context, time.Time, int, []StaleExecutionFinding) ([]StaleExecutionFinding, error){
		r.scanExpiredLeases, r.scanOrphanTasks, r.scanCommittedArtifactDrift,
		r.scanUnconfirmedSpool, r.scanOfflineWorkers,
	} {
		findings, err = scan(ctx, now, limit, findings)
		if err != nil {
			return nil, err
		}
	}
	if len(findings) > limit {
		findings = findings[:limit]
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
	rows, err := r.db.QueryContext(ctx, `SELECT t.task_id, COALESCE(t.job_id,''), t.status, COALESCE(t.worker_id,''), COALESCE(t.lease_id,''), COALESCE(t.attempt_id,''), CASE WHEN j.job_id IS NULL THEN 'missing_job' ELSE 'cancelled_job' END FROM tasks t LEFT JOIN jobs j ON j.job_id=t.job_id WHERE t.status IN ('READY','LEASED','RUNNING') AND (j.job_id IS NULL OR j.status='CANCELLED')			AND NOT (t.status IN ('LEASED','RUNNING')
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
	rows, err := r.db.QueryContext(ctx, `SELECT MIN(a.id), a.job_id, ac.commit_id, ac.attempt_id, ac.task_id, j.status FROM artifacts a JOIN task_output_declarations d ON d.artifact_id=a.id JOIN attempt_commits ac ON ac.commit_id=d.commit_id AND ac.status='COMMITTED' JOIN jobs j ON j.job_id=a.job_id WHERE a.status='READY' AND j.status IN ('RUNNING','LEASED','AWAITING_ARTIFACT') GROUP BY a.job_id, ac.commit_id, ac.attempt_id, ac.task_id, j.status ORDER BY MIN(a.id) LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("scan committed artifact drift: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var artifactID, jobID, commitID, attemptID, taskID, jobStatus string
		if err := rows.Scan(&artifactID, &jobID, &commitID, &attemptID, &taskID, &jobStatus); err != nil {
			return nil, err
		}
		out = append(out, StaleExecutionFinding{Category: StaleCommittedArtifact, ResourceType: "job", ResourceID: jobID, JobID: jobID, TaskID: taskID, AttemptID: attemptID, ArtifactID: artifactID, CommitID: commitID, OldStatus: jobStatus, ProposedStatus: "AWAITING_ARTIFACT", Reason: "READY artifact and COMMITTED attempt exist while job is non-terminal", ObservedAt: now.UTC()})
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

func (r *StaleExecutionReconciler) applyFinding(ctx context.Context, f StaleExecutionFinding, actor string, now time.Time) (bool, error) {
	switch f.Category {
	case StaleLeaseExpired:
		changed, err := r.applyExpiredLease(ctx, f, actor)
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
	default:
		return false, fmt.Errorf("unknown reconciliation category %q", f.Category)
	}
}

func (r *StaleExecutionReconciler) applyExpiredLease(ctx context.Context, f StaleExecutionFinding, actor string) (bool, error) {
	candidates, err := r.tasks.RequeueExpiredLeases(ctx, time.Now().UTC().Format(time.RFC3339Nano), 100)
	if err != nil {
		return false, err
	}
	for _, c := range candidates {
		if c.ID != f.TaskID || c.LeaseID != f.LeaseID {
			continue
		}
		maxRetries := 3
		if f.JobID != "" {
			if job, jerr := r.jobs.Get(ctx, f.JobID); jerr == nil && job != nil && job.MaxRetries > 0 {
				maxRetries = job.MaxRetries
			}
		}
		event := r.auditEventForFinding(f, actor, time.Now().UTC())
		_, err := r.tasks.ExpireTaskLeaseAtomicAudited(ctx, c.ID, c.LeaseID, c.LeaseExpiresAt, maxRetries, event)
		if errors.Is(err, taskgraph.ErrTransitionConflict) {
			return false, nil
		}
		return err == nil, err
	}
	return false, nil
}

func (r *StaleExecutionReconciler) applyOrphanTask(ctx context.Context, f StaleExecutionFinding, actor string, now time.Time) (bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	nowStr := now.UTC().Format(time.RFC3339Nano)
	res, err := tx.ExecContext(ctx, `UPDATE tasks SET status='CANCELLED', completed_at=?, worker_id='', lease_id='', lease_expires_at=NULL, revision=revision+1, updated_at=? WHERE task_id=? AND status IN ('READY','LEASED','RUNNING') AND (NOT EXISTS (SELECT 1 FROM jobs WHERE job_id=tasks.job_id) OR EXISTS (SELECT 1 FROM jobs WHERE job_id=tasks.job_id AND status='CANCELLED'))`, nowStr, nowStr, f.TaskID)
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
	res, err := tx.ExecContext(ctx, `UPDATE jobs SET status=?, revision=revision+1, updated_at=? WHERE job_id=? AND status IN ('RUNNING','LEASED')`, string(jobs.StatusAwaitingArtifact), nowStr, f.JobID)
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
	if err := tx.Commit(); err != nil {
		return false, err
	}

	// Partitioning is fail-safe: only leases already past their persisted
	// deadline are recovered. A valid lease remains fenced until its TTL
	// expires, even when the worker heartbeat is absent.
	_, err = r.recoverOfflineWorkerLeases(ctx, f.WorkerID, actor, now)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (r *StaleExecutionReconciler) recoverOfflineWorkerLeases(ctx context.Context, workerID, actor string, now time.Time) (int, error) {
	candidates, err := r.tasks.RequeueExpiredLeases(ctx, now.UTC().Format(time.RFC3339Nano), 500)
	if err != nil {
		return 0, err
	}
	recovered := 0
	for _, c := range candidates {
		if c.WorkerID != workerID {
			continue
		}
		finding := StaleExecutionFinding{
			Category: StaleLeaseExpired, ResourceType: "task", ResourceID: c.ID,
			TaskID: c.ID, WorkerID: c.WorkerID, LeaseID: c.LeaseID,
			ProposedStatus: "READY or FAILED", Reason: "worker is offline and persisted lease expired",
			ObservedAt: now.UTC(),
		}
		event := r.auditEventForFinding(finding, actor, now)
		if _, err := r.tasks.ExpireTaskLeaseAtomicAudited(ctx, c.ID, c.LeaseID, c.LeaseExpiresAt, 3, event); err != nil {
			if errors.Is(err, taskgraph.ErrTransitionConflict) {
				continue
			}
			return recovered, err
		}
		recovered++
	}
	return recovered, nil
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
