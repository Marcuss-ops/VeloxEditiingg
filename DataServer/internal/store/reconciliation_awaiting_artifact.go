package store

// reconciliation_awaiting_artifact.go — canonical AWAITING_ARTIFACT
// reconciler (Phase A3).
//
// A job reaches AWAITING_ARTIFACT through the commit-protocol roll-up
// (all sibling tasks SUCCEEDED and every attempt_commits row
// COMMITTED). From there the verified-finalization path is the only
// forward route: the worker uploads the artifact, the master verifies
// it, and the finalize writer flips the job to SUCCEEDED.
//
// The job is stuck forever when that finalization never happens and no
// other reconciler closes it: the artifact reconciler expires stale
// upload sessions, the completion supervisor expires commit rows, and
// the task-lease reaper closes attempts — but nothing moves the JOB
// itself. This reconciler is the last-resort terminalizer.
//
// Conditions (ALL must hold — see the Phase A3 spec):
//
//  1. worker attempt non più attivo — no non-terminal task_attempt
//     exists for any task of the job;
//  2. artifact non registrato — no READY artifact row for the job;
//  3. nessun transfer attivo — no artifact_uploads session in
//     CREATED/UPLOADING/RECEIVED/FINALIZING and no attempt_commits row
//     in DECLARED/UPLOADING/RECEIVED for the job;
//  4. timeout superato — jobs.updated_at is older than the stale
//     threshold.
//
// When all four hold the reconciler, inside one transaction:
//
//   - expires any lingering attempt_commits rows
//     (DECLARED/UPLOADING/RECEIVED → EXPIRED, rejected_code
//     STALE_AWAITING_ARTIFACT);
//   - expires any lingering artifact_uploads sessions
//     (CREATED/UPLOADING/RECEIVED/FINALIZING → EXPIRED);
//   - CAS-transitions the JOB AWAITING_ARTIFACT → FAILED with
//     error_message STALE_AWAITING_ARTIFACT and stamps the
//     reconciliation traceability columns (reconciled_at,
//     reconciliation_reason, reconciliation_version);
//   - appends the audit-trail row.
//
// The job CAS (WHERE status='AWAITING_ARTIFACT') is the idempotency
// anchor: a second pass, or a concurrent finalize that already flipped
// the job SUCCEEDED, sees zero rows and does nothing. Terminal jobs are
// untouched by construction (the CAS only ever matches the
// non-terminal AWAITING_ARTIFACT state).

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"velox-server/internal/stalereconcile"
)

// ReconciliationReasonStaleAwaitingArtifact is the stable machine reason
// stamped into jobs.reconciliation_reason and the attempt/upload
// expiration codes when this reconciler closes a stuck job.
const ReconciliationReasonStaleAwaitingArtifact = "STALE_AWAITING_ARTIFACT"

// defaultAwaitingArtifactStaleAfter is the default inactivity threshold.
// Upload sessions expire 24h after creation and the artifact reconciler
// runs every 15min, so a job whose updated_at is still untouched 24h
// after the AWAITING_ARTIFACT flip — with no active attempt, artifact,
// transfer, or commit — is unambiguously dead.
const defaultAwaitingArtifactStaleAfter = 24 * time.Hour

// AwaitingArtifactCandidate is one read-only change proposal.
type AwaitingArtifactCandidate struct {
	JobID     string `json:"job_id"`
	OldStatus string `json:"old_status"`
	UpdatedAt string `json:"updated_at"`
}

// AwaitingArtifactReconciler implements reconcile.Reconciler for the
// AWAITING_ARTIFACT terminalization pass.
type AwaitingArtifactReconciler struct {
	db         *sql.DB
	staleAfter time.Duration
	limit      int
}

// NewAwaitingArtifactReconciler wires the reconciler to a SQLiteStore.
func NewAwaitingArtifactReconciler(s *SQLiteStore) (*AwaitingArtifactReconciler, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("awaiting artifact reconciler: store is not initialized")
	}
	return &AwaitingArtifactReconciler{
		db:         s.db,
		staleAfter: defaultAwaitingArtifactStaleAfter,
		limit:      500,
	}, nil
}

// SetStaleAfter overrides the inactivity threshold (positive durations
// only; a non-positive value leaves the default in place).
func (r *AwaitingArtifactReconciler) SetStaleAfter(d time.Duration) {
	if d > 0 {
		r.staleAfter = d
	}
}

// SetLimit bounds how many candidates one pass may apply.
func (r *AwaitingArtifactReconciler) SetLimit(n int) {
	if n > 0 {
		r.limit = n
	}
}

// Scan is SELECT-only and deterministic: it returns the jobs that
// currently satisfy all four staleness conditions. It never writes
// state or audit rows.
func (r *AwaitingArtifactReconciler) Scan(ctx context.Context, now time.Time) ([]AwaitingArtifactCandidate, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	cutoff := now.Add(-r.staleAfter).Format(time.RFC3339Nano)
	rows, err := r.db.QueryContext(ctx, `
		SELECT j.job_id, j.status, j.updated_at
		  FROM jobs j
		 WHERE j.status = 'AWAITING_ARTIFACT'
		   AND COALESCE(j.updated_at, j.created_at) < ?
		   -- worker attempt non più attivo
		   AND NOT EXISTS (
		       SELECT 1 FROM task_attempts ta
		         JOIN tasks t ON t.task_id = ta.task_id
		        WHERE t.job_id = j.job_id
		          AND ta.status NOT IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')
		   )
		   -- artifact non registrato
		   AND NOT EXISTS (
		       SELECT 1 FROM artifacts a
		        WHERE a.job_id = j.job_id AND a.status = 'READY'
		   )
		   -- nessun transfer attivo (upload session)
		   AND NOT EXISTS (
		       SELECT 1 FROM artifact_uploads u
		        WHERE u.job_id = j.job_id
		          AND u.status IN ('CREATED','UPLOADING','RECEIVED','FINALIZING')
		   )
		   -- nessun transfer attivo (commit protocol)
		   AND NOT EXISTS (
		       SELECT 1 FROM attempt_commits ac
		        WHERE ac.job_id = j.job_id
		          AND ac.status IN ('DECLARED','UPLOADING','RECEIVED')
		   )
		 ORDER BY COALESCE(j.updated_at, j.created_at), j.job_id
		 LIMIT ?`, cutoff, r.limit)
	if err != nil {
		return nil, fmt.Errorf("scan awaiting artifact: %w", err)
	}
	defer rows.Close()
	var out []AwaitingArtifactCandidate
	for rows.Next() {
		var c AwaitingArtifactCandidate
		if err := rows.Scan(&c.JobID, &c.OldStatus, &c.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// Reconcile implements reconcile.Reconciler. It applies every current
// candidate atomically and idempotently. A candidate whose job was
// concurrently finalized (or already FAILED by a previous pass) is
// skipped silently — the CAS is the conflict detector.
func (r *AwaitingArtifactReconciler) Reconcile(ctx context.Context, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	candidates, err := r.Scan(ctx, now)
	if err != nil {
		return err
	}
	for _, c := range candidates {
		if err := r.applyCandidate(ctx, c, now); err != nil {
			return fmt.Errorf("apply awaiting artifact %s: %w", c.JobID, err)
		}
	}
	return nil
}

func (r *AwaitingArtifactReconciler) applyCandidate(ctx context.Context, c AwaitingArtifactCandidate, now time.Time) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	nowStr := now.UTC().Format(time.RFC3339Nano)

	// 1. Expire lingering commit rows. In the normal flow every
	// attempt_commits row is already COMMITTED when the job reaches
	// AWAITING_ARTIFACT; the scan guarantees none are active, so this
	// is a defensive close for rows a concurrent path may have opened
	// after the scan snapshot.
	if _, err := tx.ExecContext(ctx, `
		UPDATE attempt_commits
		   SET status = 'EXPIRED',
		       rejected_code = ?,
		       rejected_message = 'stale AWAITING_ARTIFACT job: no artifact arrived',
		       updated_at = ?
		 WHERE job_id = ?
		   AND status IN ('DECLARED','UPLOADING','RECEIVED')`,
		ReconciliationReasonStaleAwaitingArtifact, nowStr, c.JobID); err != nil {
		return err
	}

	// 2. Expire lingering upload sessions.
	if _, err := tx.ExecContext(ctx, `
		UPDATE artifact_uploads
		   SET status = 'EXPIRED',
		       expires_at = ?,
		       completed_at = ?
		 WHERE job_id = ?
		   AND status IN ('CREATED','UPLOADING','RECEIVED','FINALIZING')`,
		nowStr, nowStr, c.JobID); err != nil {
		return err
	}

	// 3. CAS-transition the job. The status predicate is the
	// idempotency and terminal-safety anchor: only a job still in the
	// non-terminal AWAITING_ARTIFACT state can be matched, so terminal
	// jobs (SUCCEEDED/FAILED/CANCELLED) are untouched by construction
	// and a re-run of this pass matches zero rows.
	reason := ReconciliationReasonStaleAwaitingArtifact
	message := reason + ": no active attempt, READY artifact, or transfer after the stale threshold"
	res, err := tx.ExecContext(ctx, `
		UPDATE jobs
		   SET status = 'FAILED',
		       error_message = ?,
		       failed_at = ?,
		       failed_by = 'reconciliation',
		       updated_at = ?,
		       revision = revision + 1,
		       reconciled_at = ?,
		       reconciliation_reason = ?,
		       reconciliation_version = COALESCE(reconciliation_version, 0) + 1
		 WHERE job_id = ?
		   AND status = 'AWAITING_ARTIFACT'`,
		message, nowStr, nowStr, nowStr, reason, c.JobID)
	if err != nil {
		return err
	}
	n, err := readRowsAffected(res, "awaiting artifact terminalization")
	if err != nil {
		return err
	}
	if n != 1 {
		// Concurrent finalize or a previous pass won the CAS. The
		// job is no longer AWAITING_ARTIFACT; nothing to do.
		return nil
	}

	// 4. Audit trail (same append-only ledger as the stale-execution
	// reconciler; the action id is deterministic per finding identity).
	finding := stalereconcile.StaleExecutionFinding{
		Category: stalereconcile.StaleAwaitingArtifact, ResourceType: "job", ResourceID: c.JobID,
		JobID: c.JobID, OldStatus: c.OldStatus, ProposedStatus: "FAILED",
		Reason: reason, ObservedAt: now.UTC(),
	}
	if err := appendReconcileAuditTx(ctx, tx, finding, "reconciliation", now); err != nil {
		return err
	}
	return tx.Commit()
}
