package store

import (
	"context"
	"database/sql"
	"time"
)

// ── Typed terminal/retry marks (PR4e) — facade over the deliverystore leaf ──
//
// The lease claim + renewal live in store_deliveries_lease.go (also a facade).
// The terminal / retry transitions below delegate to the deliverystore leaf;
// only the cross-domain parent-job aggregate flip stays here because it
// targets the jobs table (a store-owned domain, injected into the leaf as the
// ParentJobFinalizer).

// MarkDeliverySucceeded moves a RUNNING delivery to SUCCEEDED with CAS guard.
func (s *SQLiteStore) MarkDeliverySucceeded(ctx context.Context, deliveryID, runnerID, leaseID, remoteID, remoteURL string) error {
	return s.deliveryStore().MarkDeliverySucceeded(ctx, deliveryID, runnerID, leaseID, remoteID, remoteURL)
}

// MarkDeliveryRetry moves a RUNNING delivery to RETRY_WAIT with the next
// attempt scheduled after a backoff delay.
func (s *SQLiteStore) MarkDeliveryRetry(ctx context.Context, deliveryID, runnerID, leaseID, errorCode, errorMsg string, nextAttemptAt time.Time) error {
	return s.deliveryStore().MarkDeliveryRetry(ctx, deliveryID, runnerID, leaseID, errorCode, errorMsg, nextAttemptAt)
}

// MarkDeliveryFailed moves a RUNNING delivery to FAILED (permanent failure).
func (s *SQLiteStore) MarkDeliveryFailed(ctx context.Context, deliveryID, runnerID, leaseID, errorCode, errorMsg string) error {
	return s.deliveryStore().MarkDeliveryFailed(ctx, deliveryID, runnerID, leaseID, errorCode, errorMsg)
}

// MarkDeliveryBlockedAuth moves a RUNNING delivery to BLOCKED_AUTH when the
// provider returns an authentication/authorization error that will not be
// resolved by retrying.
func (s *SQLiteStore) MarkDeliveryBlockedAuth(ctx context.Context, deliveryID, runnerID, leaseID, errorCode, errorMsg string) error {
	return s.deliveryStore().MarkDeliveryBlockedAuth(ctx, deliveryID, runnerID, leaseID, errorCode, errorMsg)
}

// FinalizeParentJobIfDeliveriesDone implements deliverystore.ParentJobFinalizer.
// It is the cross-domain jobs touch injected into the leaf so the terminal
// MarkDelivery* transitions can flip the parent job aggregate inside the same
// transaction as the job_deliveries CAS.
func (s *SQLiteStore) FinalizeParentJobIfDeliveriesDone(ctx context.Context, tx *sql.Tx, deliveryID, now string) error {
	return finalizeParentJobIfDeliveriesDone(ctx, tx, deliveryID, now)
}

// finalizeParentJobIfDeliveriesDone closes only a job currently in the
// explicit-delivery gate, and only after every per-target child has reached
// a terminal state. A failed/blocked/cancelled child therefore does not
// cancel siblings that are still running or waiting for retry; it only
// contributes to the parent's final aggregate once the whole set is done.
// Render-only jobs are completed by artifact finalization and already
// terminal parent jobs are never regressed by delivery callbacks.
func finalizeParentJobIfDeliveriesDone(ctx context.Context, tx *sql.Tx, deliveryID, now string) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE jobs
		SET status = CASE
				WHEN EXISTS (
					SELECT 1
					FROM job_deliveries d2
					JOIN artifacts a2 ON a2.id = d2.artifact_id
					WHERE a2.job_id = jobs.job_id
					  AND d2.status <> 'SUCCEEDED'
				) THEN 'FAILED'
				ELSE 'SUCCEEDED'
			END,
			completed_at = ?,
			updated_at = ?,
			revision = revision + 1
		WHERE job_id = (
			SELECT a.job_id
			FROM job_deliveries d
			JOIN artifacts a ON a.id = d.artifact_id
			WHERE d.delivery_id = ?
		)
		AND status = 'DELIVERING'
		AND NOT EXISTS (
			SELECT 1
			FROM job_deliveries d3
			JOIN artifacts a3 ON a3.id = d3.artifact_id
			WHERE a3.job_id = jobs.job_id
			  AND d3.status NOT IN ('SUCCEEDED', 'FAILED', 'BLOCKED_AUTH', 'CANCELLED')
		)`, now, now, deliveryID)
	return err
}
