package store

// reconciliation_delivery_pending.go — canonical DELIVERY_PENDING
// reconciler (Phase A3).
//
// Two idempotent passes, both CAS-guarded and both stamping the
// reconciliation traceability columns when they touch a job:
//
//  1. Job roll-up: a job stuck in DELIVERING whose deliveries are ALL
//     terminal (SUCCEEDED/FAILED/BLOCKED_AUTH/CANCELLED) — but whose
//     parent never rolled up (runner down, crash between callbacks,
//     manual terminalization) — is finalized exactly like
//     finalizeParentJobIfDeliveriesDone: SUCCEEDED when every delivery
//     SUCCEEDED, FAILED otherwise. Jobs with NO delivery rows are
//     deliberately left for the operator (plan-scoped routing contract).
//
//  2. Budget-exhausted deliveries: a delivery stuck in a non-terminal
//     state (PENDING/RUNNING/RETRY_WAIT) that already exhausted its
//     retry budget (attempt_count >= max_attempts) and holds no valid
//     lease is FAILED with STALE_DELIVERY_PENDING, its latest
//     delivery_attempt closed, and the parent job re-evaluated.
//     The budget-exhausted precondition is what keeps this pass from
//     racing the delivery runner: a delivery that still has budget is
//     the runner's job, and a RUNNING delivery with a valid lease is
//     actively processing.
//
// Terminal jobs are untouched by construction: the job CAS only matches
// the non-terminal DELIVERING state, and the delivery CAS only matches
// non-terminal delivery states.

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// ReconciliationReasonStaleDeliveryPending is the stable machine reason
// for this reconciler's transitions.
const ReconciliationReasonStaleDeliveryPending = "STALE_DELIVERY_PENDING"

// defaultDeliveryPendingStaleAfter is the inactivity threshold. The
// delivery runner retries with backoffs up to 30m and a budget of 5
// attempts, so a delivery or job whose last write is older than 6h with
// no valid lease is unambiguously stalled.
const defaultDeliveryPendingStaleAfter = 6 * time.Hour

// DeliveryPendingCandidateKind discriminates the two pass types.
type DeliveryPendingCandidateKind string

const (
	DeliveryPendingJob      DeliveryPendingCandidateKind = "job"
	DeliveryPendingDelivery DeliveryPendingCandidateKind = "delivery"
)

// DeliveryPendingCandidate is one read-only change proposal.
type DeliveryPendingCandidate struct {
	Kind      DeliveryPendingCandidateKind `json:"kind"`
	ID        string                       `json:"id"`
	OldStatus string                       `json:"old_status"`
	JobID     string                       `json:"job_id,omitempty"`
}

// DeliveryPendingReconciler implements reconcile.Reconciler for the
// DELIVERY_PENDING pass.
type DeliveryPendingReconciler struct {
	db         *sql.DB
	staleAfter time.Duration
	limit      int
}

// NewDeliveryPendingReconciler wires the reconciler to a SQLiteStore.
func NewDeliveryPendingReconciler(s *SQLiteStore) (*DeliveryPendingReconciler, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("delivery pending reconciler: store is not initialized")
	}
	return &DeliveryPendingReconciler{
		db:         s.db,
		staleAfter: defaultDeliveryPendingStaleAfter,
		limit:      500,
	}, nil
}

// SetStaleAfter overrides the inactivity threshold.
func (r *DeliveryPendingReconciler) SetStaleAfter(d time.Duration) {
	if d > 0 {
		r.staleAfter = d
	}
}

// SetLimit bounds how many candidates each scan query returns per pass
// (the job and delivery passes are scanned independently, so at most
// 2*limit candidates are visited in one Reconcile).
func (r *DeliveryPendingReconciler) SetLimit(n int) {
	if n > 0 {
		r.limit = n
	}
}

// Scan is SELECT-only: it returns the current roll-up and
// budget-exhausted candidates interleaved by staleness.
func (r *DeliveryPendingReconciler) Scan(ctx context.Context, now time.Time) ([]DeliveryPendingCandidate, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	cutoff := now.Add(-r.staleAfter).Format(time.RFC3339Nano)
	nowStr := now.UTC().Format(time.RFC3339Nano)

	// Pass 1: jobs stuck in DELIVERING with all deliveries terminal.
	// EXISTS(job_deliveries) keeps the no-plan-scoped contract: a job
	// with zero delivery rows stays visible to the operator.
	jobRows, err := r.db.QueryContext(ctx, `
		SELECT j.job_id, j.status
		  FROM jobs j
		 WHERE j.status = 'DELIVERING'
		   AND COALESCE(j.updated_at, j.created_at) < ?
		   AND EXISTS (
		       SELECT 1 FROM artifacts a
		         JOIN job_deliveries d ON d.artifact_id = a.id
		        WHERE a.job_id = j.job_id
		   )
		   AND NOT EXISTS (
		       SELECT 1 FROM artifacts a
		         JOIN job_deliveries d ON d.artifact_id = a.id
		        WHERE a.job_id = j.job_id
		          AND d.status NOT IN ('SUCCEEDED','FAILED','BLOCKED_AUTH','CANCELLED')
		   )
		 ORDER BY COALESCE(j.updated_at, j.created_at), j.job_id
		 LIMIT ?`, cutoff, r.limit)
	if err != nil {
		return nil, fmt.Errorf("scan delivery pending jobs: %w", err)
	}
	defer jobRows.Close()
	var out []DeliveryPendingCandidate
	for jobRows.Next() {
		var c DeliveryPendingCandidate
		c.Kind = DeliveryPendingJob
		if err := jobRows.Scan(&c.ID, &c.OldStatus); err != nil {
			return nil, err
		}
		c.JobID = c.ID
		out = append(out, c)
	}
	if err := jobRows.Err(); err != nil {
		return nil, err
	}

	// Pass 2: budget-exhausted deliveries stuck non-terminal with no
	// valid lease. nowStr is used for the lease-expiry comparison so a
	// lease that expires in the future (active runner) is never touched.
	deliveryRows, err := r.db.QueryContext(ctx, `
		SELECT d.delivery_id, d.status, COALESCE(a.job_id, '')
		  FROM job_deliveries d
		  LEFT JOIN artifacts a ON a.id = d.artifact_id
		 WHERE d.status IN ('PENDING','RUNNING','RETRY_WAIT')
		   AND d.attempt_count >= d.max_attempts
		   AND (d.lease_expires_at IS NULL OR d.lease_expires_at = '' OR d.lease_expires_at < ?)
		   AND COALESCE(d.updated_at, d.created_at) < ?
		 ORDER BY COALESCE(d.updated_at, d.created_at), d.delivery_id
		 LIMIT ?`, nowStr, cutoff, r.limit)
	if err != nil {
		return nil, fmt.Errorf("scan delivery pending rows: %w", err)
	}
	defer deliveryRows.Close()
	for deliveryRows.Next() {
		var c DeliveryPendingCandidate
		c.Kind = DeliveryPendingDelivery
		if err := deliveryRows.Scan(&c.ID, &c.OldStatus, &c.JobID); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, deliveryRows.Err()
}

// Reconcile implements reconcile.Reconciler.
func (r *DeliveryPendingReconciler) Reconcile(ctx context.Context, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	candidates, err := r.Scan(ctx, now)
	if err != nil {
		return err
	}
	for _, c := range candidates {
		switch c.Kind {
		case DeliveryPendingJob:
			if err := r.applyJobRollup(ctx, c, now); err != nil {
				return fmt.Errorf("apply delivery rollup %s: %w", c.ID, err)
			}
		case DeliveryPendingDelivery:
			if err := r.applyDeliveryTerminal(ctx, c, now); err != nil {
				return fmt.Errorf("apply delivery terminal %s: %w", c.ID, err)
			}
		}
	}
	return nil
}

// applyJobRollup finalizes a job whose deliveries are all terminal. The
// CASE mirrors finalizeParentJobIfDeliveriesDone: FAILED when any
// delivery is not SUCCEEDED, SUCCEEDED otherwise. CAS on
// status='DELIVERING' keeps the pass idempotent and terminal-safe.
func (r *DeliveryPendingReconciler) applyJobRollup(ctx context.Context, c DeliveryPendingCandidate, now time.Time) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	nowStr := now.UTC().Format(time.RFC3339Nano)
	reason := ReconciliationReasonStaleDeliveryPending

	res, err := tx.ExecContext(ctx, `
		UPDATE jobs
		   SET status = CASE
		           WHEN EXISTS (
		               SELECT 1 FROM job_deliveries d2
		                 JOIN artifacts a2 ON a2.id = d2.artifact_id
		                WHERE a2.job_id = jobs.job_id
		                  AND d2.status <> 'SUCCEEDED'
		           ) THEN 'FAILED'
		           ELSE 'SUCCEEDED'
		       END,
		       completed_at = ?,
		       updated_at = ?,
		       revision = revision + 1,
		       reconciled_at = ?,
		       reconciliation_reason = ?,
		       reconciliation_version = COALESCE(reconciliation_version, 0) + 1
		 WHERE job_id = ?
		   AND status = 'DELIVERING'
		   AND NOT EXISTS (
		       SELECT 1 FROM job_deliveries d3
		         JOIN artifacts a3 ON a3.id = d3.artifact_id
		        WHERE a3.job_id = jobs.job_id
		          AND d3.status NOT IN ('SUCCEEDED','FAILED','BLOCKED_AUTH','CANCELLED')
		   )`,
		nowStr, nowStr, nowStr, reason, c.ID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		// Concurrent runner already rolled the job up, or it left
		// DELIVERING. Nothing to do.
		return nil
	}

	finding := StaleExecutionFinding{
		Category: StaleDeliveryPending, ResourceType: "job", ResourceID: c.ID,
		JobID: c.ID, OldStatus: c.OldStatus, ProposedStatus: "SUCCEEDED or FAILED",
		Reason: reason, ObservedAt: now.UTC(),
	}
	if err := appendReconcileAuditTx(ctx, tx, finding, "reconciliation", now); err != nil {
		return err
	}
	return tx.Commit()
}

// applyDeliveryTerminal fails a budget-exhausted delivery, closes its
// latest delivery_attempt, and re-evaluates the parent job (which may
// itself roll up in the same transaction when it was the last pending
// delivery).
func (r *DeliveryPendingReconciler) applyDeliveryTerminal(ctx context.Context, c DeliveryPendingCandidate, now time.Time) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	nowStr := now.UTC().Format(time.RFC3339Nano)
	reason := ReconciliationReasonStaleDeliveryPending

	res, err := tx.ExecContext(ctx, `
		UPDATE job_deliveries
		   SET status = 'FAILED',
		       locked_by = NULL,
		       lease_id = NULL,
		       lease_expires_at = NULL,
		       last_error_code = ?,
		       last_error_message = 'delivery exhausted its retry budget while stalled',
		       completed_at = ?,
		       updated_at = ?
		 WHERE delivery_id = ?
		   AND status IN ('PENDING','RUNNING','RETRY_WAIT')
		   AND attempt_count >= max_attempts
		   AND (lease_expires_at IS NULL OR lease_expires_at = '' OR lease_expires_at < ?)`,
		reason, nowStr, nowStr, c.ID, nowStr)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return nil
	}

	// Close the latest delivery_attempt row (same ledger discipline as
	// MarkDeliveryFailed).
	if _, err := tx.ExecContext(ctx, `
		UPDATE delivery_attempts
		   SET status = 'FAILED', completed_at = ?, error_message = ?
		 WHERE delivery_id = ?
		   AND id = (SELECT MAX(id) FROM delivery_attempts WHERE delivery_id = ?)`,
		nowStr, reason, c.ID, c.ID); err != nil {
		return err
	}

	// Re-evaluate the parent job: if this was the last non-terminal
	// delivery, the job rolls up inside the same transaction.
	if c.JobID != "" {
		if err := finalizeParentJobIfDeliveriesDone(ctx, tx, c.ID, nowStr); err != nil {
			return err
		}
	}

	finding := StaleExecutionFinding{
		Category: StaleDeliveryPending, ResourceType: "delivery", ResourceID: c.ID,
		JobID: c.JobID, OldStatus: c.OldStatus, ProposedStatus: "FAILED",
		Reason: reason, ObservedAt: now.UTC(),
	}
	if err := appendReconcileAuditTx(ctx, tx, finding, "reconciliation", now); err != nil {
		return err
	}
	return tx.Commit()
}
