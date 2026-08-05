package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"velox-server/internal/statemachine"
)

// ── Typed terminal/retry marks (PR4e) ───────────────────────────────────────
//
// The lease claim + renewal live in store_deliveries_lease.go; the four
// terminal / retry transitions below move a RUNNING delivery to its next
// state, always CAS-guarded on (delivery_id, status=RUNNING, locked_by,
// lease_id) and always closing the latest delivery_attempt row in the
// same transaction.

// MarkDeliverySucceeded moves a RUNNING delivery to SUCCEEDED with CAS guard.
// Stamps completed_at and optionally remote_id/remote_url.
//
// The job_deliveries UPDATE and the delivery_attempts UPDATE both happen
// inside RunInTx so a crash between updates cannot leave delivery and
// attempt in mismatched states. CAS-miss surfaces ErrTransitionConflict
// verbatim so the caller can errors.Is on it.
func (s *SQLiteStore) MarkDeliverySucceeded(ctx context.Context, deliveryID, runnerID, leaseID, remoteID, remoteURL string) error {
	if err := statemachine.DefaultRegistry().Validate(statemachine.DomainDelivery, "RUNNING", "SUCCEEDED", ""); err != nil {
		return fmt.Errorf("store: MarkDeliverySucceeded: %w", err)
	}
	if deliveryID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: MarkDeliverySucceeded: missing required fields")
	}

	return NewTxManager(s).RunInTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		now := time.Now().UTC().Format(time.RFC3339)
		result, err := tx.ExecContext(ctx,
			`UPDATE job_deliveries
			 SET status = 'SUCCEEDED',
			     remote_id = COALESCE(NULLIF(?, ''), remote_id),
			     remote_url = COALESCE(NULLIF(?, ''), remote_url),
			     completed_at = ?,
			     updated_at = ?
			 WHERE delivery_id = ?
			   AND status = 'RUNNING'
			   AND locked_by = ?
			   AND lease_id = ?`,
			remoteID, remoteURL, now, now, deliveryID, runnerID, leaseID,
		)
		if err != nil {
			return err
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			return ErrTransitionConflict
		}

		// Close the latest delivery_attempt — same RunInTx tx.
		if _, err := tx.ExecContext(ctx,
			`UPDATE delivery_attempts
			 SET status = 'SUCCESS', completed_at = ?
			 WHERE delivery_id = ?
			   AND id = (SELECT MAX(id) FROM delivery_attempts WHERE delivery_id = ?)`,
			now, deliveryID, deliveryID,
		); err != nil {
			return fmt.Errorf("MarkDeliverySucceeded attempt UPDATE: %w", err)
		}
		return completeParentJobIfDeliveriesDone(ctx, tx, deliveryID, now)
	})
}

// MarkDeliveryRetry moves a RUNNING delivery to RETRY_WAIT with the next
// attempt scheduled after a backoff delay. Sets last_error_code and
// last_error_message for diagnostics.
//
// Runs through TxManager.RunInTx so the job_deliveries flip and the
// delivery_attempts close are atomic — a crash cannot leave them
// mismatched.
func (s *SQLiteStore) MarkDeliveryRetry(ctx context.Context, deliveryID, runnerID, leaseID, errorCode, errorMsg string, nextAttemptAt time.Time) error {
	if err := statemachine.DefaultRegistry().Validate(statemachine.DomainDelivery, "RUNNING", "RETRY_WAIT", ""); err != nil {
		return fmt.Errorf("store: MarkDeliveryRetry: %w", err)
	}
	if deliveryID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: MarkDeliveryRetry: missing required fields")
	}

	return NewTxManager(s).RunInTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		now := time.Now().UTC().Format(time.RFC3339)
		nextISO := nextAttemptAt.UTC().Format(time.RFC3339)
		result, err := tx.ExecContext(ctx,
			`UPDATE job_deliveries
			 SET status = 'RETRY_WAIT',
			     locked_by = NULL,
			     lease_id = NULL,
			     lease_expires_at = NULL,
			     next_attempt_at = ?,
			     last_error_code = ?,
			     last_error_message = ?,
			     updated_at = ?
			 WHERE delivery_id = ?
			   AND status = 'RUNNING'
			   AND locked_by = ?
			   AND lease_id = ?`,
			nextISO, nullIfEmpty(errorCode), nullIfEmpty(errorMsg), now,
			deliveryID, runnerID, leaseID,
		)
		if err != nil {
			return err
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			return ErrTransitionConflict
		}

		// Close the latest delivery_attempt — same RunInTx tx.
		if _, err := tx.ExecContext(ctx,
			`UPDATE delivery_attempts
			 SET status = 'RETRY_WAIT', completed_at = ?, error_message = ?
			 WHERE delivery_id = ?
			   AND id = (SELECT MAX(id) FROM delivery_attempts WHERE delivery_id = ?)`,
			now, nullIfEmpty(errorMsg), deliveryID, deliveryID,
		); err != nil {
			return fmt.Errorf("MarkDeliveryRetry attempt UPDATE: %w", err)
		}
		return nil
	})
}

// MarkDeliveryFailed moves a RUNNING delivery to FAILED (permanent failure).
// No further retry attempts will be scheduled.
//
// Runs through TxManager.RunInTx — delivery + attempt are all-or-nothing.
func (s *SQLiteStore) MarkDeliveryFailed(ctx context.Context, deliveryID, runnerID, leaseID, errorCode, errorMsg string) error {
	if err := statemachine.DefaultRegistry().Validate(statemachine.DomainDelivery, "RUNNING", "FAILED", ""); err != nil {
		return fmt.Errorf("store: MarkDeliveryFailed: %w", err)
	}
	if deliveryID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: MarkDeliveryFailed: missing required fields")
	}

	return NewTxManager(s).RunInTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		now := time.Now().UTC().Format(time.RFC3339)
		result, err := tx.ExecContext(ctx,
			`UPDATE job_deliveries
			 SET status = 'FAILED',
			     locked_by = NULL,
			     lease_id = NULL,
			     lease_expires_at = NULL,
			     last_error_code = ?,
			     last_error_message = ?,
			     completed_at = ?,
			     updated_at = ?
			 WHERE delivery_id = ?
			   AND status = 'RUNNING'
			   AND locked_by = ?
			   AND lease_id = ?`,
			nullIfEmpty(errorCode), nullIfEmpty(errorMsg), now, now,
			deliveryID, runnerID, leaseID,
		)
		if err != nil {
			return err
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			return ErrTransitionConflict
		}

		// Close the latest delivery_attempt — same RunInTx tx.
		if _, err := tx.ExecContext(ctx,
			`UPDATE delivery_attempts
			 SET status = 'FAILED', completed_at = ?, error_message = ?
			 WHERE delivery_id = ?
			   AND id = (SELECT MAX(id) FROM delivery_attempts WHERE delivery_id = ?)`,
			now, nullIfEmpty(errorMsg), deliveryID, deliveryID,
		); err != nil {
			return fmt.Errorf("MarkDeliveryFailed attempt UPDATE: %w", err)
		}
		return failParentJobForDelivery(ctx, tx, deliveryID, now)
	})
}

// completeParentJobIfDeliveriesDone closes only a job currently in the
// explicit-delivery gate. Render-only jobs are completed by artifact
// finalization and already-SUCCEEDED jobs are never regressed by delivery
// callbacks.
func completeParentJobIfDeliveriesDone(ctx context.Context, tx *sql.Tx, deliveryID, now string) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE jobs
		SET status = 'SUCCEEDED', completed_at = ?, updated_at = ?, revision = revision + 1
		WHERE job_id = (
			SELECT a.job_id FROM job_deliveries d JOIN artifacts a ON a.id = d.artifact_id
			WHERE d.delivery_id = ?
		)
		AND status = 'DELIVERING'
		AND NOT EXISTS (
			SELECT 1 FROM job_deliveries d2
			JOIN artifacts a2 ON a2.id = d2.artifact_id
			WHERE a2.job_id = jobs.job_id AND d2.status <> 'SUCCEEDED'
		)`, now, now, deliveryID)
	return err
}

func failParentJobForDelivery(ctx context.Context, tx *sql.Tx, deliveryID, now string) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE jobs
		SET status = 'FAILED', completed_at = ?, updated_at = ?, revision = revision + 1
		WHERE job_id = (
			SELECT a.job_id FROM job_deliveries d JOIN artifacts a ON a.id = d.artifact_id
			WHERE d.delivery_id = ?
		)
		AND status = 'DELIVERING'`, now, now, deliveryID)
	return err
}

// MarkDeliveryBlockedAuth moves a RUNNING delivery to BLOCKED_AUTH when the
// provider returns an authentication/authorization error that will not be
// resolved by retrying. The delivery stays blocked until operator intervention
// re-enables the destination credentials.
//
// Runs through TxManager.RunInTx — delivery + attempt are all-or-nothing.
func (s *SQLiteStore) MarkDeliveryBlockedAuth(ctx context.Context, deliveryID, runnerID, leaseID, errorCode, errorMsg string) error {
	if err := statemachine.DefaultRegistry().Validate(statemachine.DomainDelivery, "RUNNING", "BLOCKED_AUTH", ""); err != nil {
		return fmt.Errorf("store: MarkDeliveryBlockedAuth: %w", err)
	}
	if deliveryID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: MarkDeliveryBlockedAuth: missing required fields")
	}

	return NewTxManager(s).RunInTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		now := time.Now().UTC().Format(time.RFC3339)
		result, err := tx.ExecContext(ctx,
			`UPDATE job_deliveries
			 SET status = 'BLOCKED_AUTH',
			     locked_by = NULL,
			     lease_id = NULL,
			     lease_expires_at = NULL,
			     last_error_code = ?,
			     last_error_message = ?,
			     completed_at = ?,
			     updated_at = ?
			 WHERE delivery_id = ?
			   AND status = 'RUNNING'
			   AND locked_by = ?
			   AND lease_id = ?`,
			nullIfEmpty(errorCode), nullIfEmpty(errorMsg), now, now,
			deliveryID, runnerID, leaseID,
		)
		if err != nil {
			return err
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			return ErrTransitionConflict
		}

		// Close the latest delivery_attempt — same RunInTx tx.
		if _, err := tx.ExecContext(ctx,
			`UPDATE delivery_attempts
			 SET status = 'BLOCKED_AUTH', completed_at = ?, error_message = ?
			 WHERE delivery_id = ?
			   AND id = (SELECT MAX(id) FROM delivery_attempts WHERE delivery_id = ?)`,
			now, nullIfEmpty(errorMsg), deliveryID, deliveryID,
		); err != nil {
			return fmt.Errorf("MarkDeliveryBlockedAuth attempt UPDATE: %w", err)
		}
		return failParentJobForDelivery(ctx, tx, deliveryID, now)
	})
}
