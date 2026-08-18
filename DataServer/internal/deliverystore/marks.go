package deliverystore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"velox-server/internal/statemachine"
	"velox-server/internal/storecore"
)

// ── Typed terminal/retry marks (PR4e) ───────────────────────────────────────
//
// The lease claim + renewal live in lease.go; the four terminal / retry
// transitions below move a RUNNING delivery to its next state, always
// CAS-guarded on (delivery_id, status=RUNNING, locked_by, lease_id) and
// always closing the latest delivery_attempt row in the same transaction.

// MarkDeliverySucceeded moves a RUNNING delivery to SUCCEEDED with CAS guard.
// Stamps completed_at and optionally remote_id/remote_url.
func (w *SQLiteDeliveryStore) MarkDeliverySucceeded(ctx context.Context, deliveryID, runnerID, leaseID, remoteID, remoteURL string) error {
	if err := statemachine.DefaultRegistry().Validate(statemachine.DomainDelivery, "RUNNING", "SUCCEEDED", ""); err != nil {
		return fmt.Errorf("store: MarkDeliverySucceeded: %w", err)
	}
	if deliveryID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: MarkDeliverySucceeded: missing required fields")
	}

	return storecore.WrapDBInfrastructure("MarkDeliverySucceeded transaction", w.runInTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		now := nowRFC3339()
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
		affected, err := storecore.ReadRowsAffected(result, "MarkDeliverySucceeded")
		if err != nil {
			return err
		}
		if affected == 0 {
			return storecore.ErrTransitionConflict
		}

		if err := closeLatestDeliveryAttempt(ctx, tx, deliveryID, "SUCCESS", now, ""); err != nil {
			return err
		}
		return w.finalizeParentJobIfDeliveriesDone(ctx, tx, deliveryID, now)
	}))
}

// MarkDeliveryRetry moves a RUNNING delivery to RETRY_WAIT with the next
// attempt scheduled after a backoff delay. Sets last_error_code and
// last_error_message for diagnostics.
func (w *SQLiteDeliveryStore) MarkDeliveryRetry(ctx context.Context, deliveryID, runnerID, leaseID, errorCode, errorMsg string, nextAttemptAt time.Time) error {
	if err := statemachine.DefaultRegistry().Validate(statemachine.DomainDelivery, "RUNNING", "RETRY_WAIT", ""); err != nil {
		return fmt.Errorf("store: MarkDeliveryRetry: %w", err)
	}
	if deliveryID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: MarkDeliveryRetry: missing required fields")
	}

	return storecore.WrapDBInfrastructure("MarkDeliveryRetry transaction", w.runInTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		now := nowRFC3339()
		nextISO := nextAttemptAt.UTC().Format(time.RFC3339)
		result, err := tx.ExecContext(ctx,
			`UPDATE job_deliveries
			 SET status = 'RETRY_WAIT',
			     locked_by = NULL,
			     lease_id = NULL,
			     lease_expires_at = NULL,
			     next_attempt_at = ?,
			     queued_at = ?,
			     last_error_code = ?,
			     last_error_message = ?,
			     updated_at = ?
			 WHERE delivery_id = ?
			   AND status = 'RUNNING'
			   AND locked_by = ?
			   AND lease_id = ?`,
			nextISO, nextISO, nullIfEmpty(errorCode), nullIfEmpty(errorMsg), now,
			deliveryID, runnerID, leaseID,
		)
		if err != nil {
			return err
		}
		affected, err := storecore.ReadRowsAffected(result, "MarkDeliveryRetry")
		if err != nil {
			return err
		}
		if affected == 0 {
			return storecore.ErrTransitionConflict
		}

		if err := closeLatestDeliveryAttempt(ctx, tx, deliveryID, "RETRY_WAIT", now, errorMsg); err != nil {
			return err
		}
		return nil
	}))
}

// MarkDeliveryFailed moves a RUNNING delivery to FAILED (permanent failure).
// No further retry attempts will be scheduled.
func (w *SQLiteDeliveryStore) MarkDeliveryFailed(ctx context.Context, deliveryID, runnerID, leaseID, errorCode, errorMsg string) error {
	if err := statemachine.DefaultRegistry().Validate(statemachine.DomainDelivery, "RUNNING", "FAILED", ""); err != nil {
		return fmt.Errorf("store: MarkDeliveryFailed: %w", err)
	}
	if deliveryID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: MarkDeliveryFailed: missing required fields")
	}

	return storecore.WrapDBInfrastructure("MarkDeliveryFailed transaction", w.runInTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		now := nowRFC3339()
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
		affected, err := storecore.ReadRowsAffected(result, "MarkDeliveryFailed")
		if err != nil {
			return err
		}
		if affected == 0 {
			return storecore.ErrTransitionConflict
		}

		if err := closeLatestDeliveryAttempt(ctx, tx, deliveryID, "FAILED", now, errorMsg); err != nil {
			return err
		}
		return w.finalizeParentJobIfDeliveriesDone(ctx, tx, deliveryID, now)
	}))
}

// MarkDeliveryBlockedAuth moves a RUNNING delivery to BLOCKED_AUTH when the
// provider returns an authentication/authorization error that will not be
// resolved by retrying.
func (w *SQLiteDeliveryStore) MarkDeliveryBlockedAuth(ctx context.Context, deliveryID, runnerID, leaseID, errorCode, errorMsg string) error {
	if err := statemachine.DefaultRegistry().Validate(statemachine.DomainDelivery, "RUNNING", "BLOCKED_AUTH", ""); err != nil {
		return fmt.Errorf("store: MarkDeliveryBlockedAuth: %w", err)
	}
	if deliveryID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: MarkDeliveryBlockedAuth: missing required fields")
	}

	return storecore.WrapDBInfrastructure("MarkDeliveryBlockedAuth transaction", w.runInTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		now := nowRFC3339()
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
		affected, err := storecore.ReadRowsAffected(result, "MarkDeliveryBlockedAuth")
		if err != nil {
			return err
		}
		if affected == 0 {
			return storecore.ErrTransitionConflict
		}

		if err := closeLatestDeliveryAttempt(ctx, tx, deliveryID, "BLOCKED_AUTH", now, errorMsg); err != nil {
			return err
		}
		return w.finalizeParentJobIfDeliveriesDone(ctx, tx, deliveryID, now)
	}))
}

// finalizeParentJobIfDeliveriesDone delegates the cross-domain parent-job
// aggregate flip to the injected finalizer. It fails closed when no finalizer
// is wired: the job_deliveries CAS and the parent-job transition must share
// one transaction, and silently skipping the job flip would leave a DELIVERING
// job stuck forever.
func (w *SQLiteDeliveryStore) finalizeParentJobIfDeliveriesDone(ctx context.Context, tx *sql.Tx, deliveryID, now string) error {
	if w == nil || w.parentJobFinalizer == nil {
		return fmt.Errorf("deliverystore: parent job finalizer not configured (delivery=%s)", deliveryID)
	}
	return w.parentJobFinalizer.FinalizeParentJobIfDeliveriesDone(ctx, tx, deliveryID, now)
}

// closeLatestDeliveryAttempt keeps the delivery row and its attempt ledger
// atomic. A terminal/retry delivery without a matching latest attempt is a
// persistence invariant violation, not a successful transition.
func closeLatestDeliveryAttempt(ctx context.Context, tx *sql.Tx, deliveryID, status, now, errorMessage string) error {
	res, err := tx.ExecContext(ctx,
		`UPDATE delivery_attempts
		 SET status = ?, completed_at = ?, error_message = ?
		 WHERE delivery_id = ?
		   AND id = (SELECT MAX(id) FROM delivery_attempts WHERE delivery_id = ?)`,
		status, now, nullIfEmpty(errorMessage), deliveryID, deliveryID,
	)
	if err != nil {
		return fmt.Errorf("close latest delivery attempt: %w", err)
	}
	if n, rowsErr := res.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("close latest delivery attempt rows: %w", rowsErr)
	} else if n != 1 {
		return fmt.Errorf("%w: delivery=%s attempt rows=%d", storecore.ErrTransitionConflict, deliveryID, n)
	}
	return nil
}
