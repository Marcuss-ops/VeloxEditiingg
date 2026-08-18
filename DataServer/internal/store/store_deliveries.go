// Package store / store_deliveries.go
//
// Delivery type re-exports plus the single cross-domain finalizer that must
// stay in store. The read/write persistence surface (claim/lease/marks/
// destinations/job_deliveries/reconciliation) lives in the deliverystore leaf
// and is reached via SQLiteStore.Delivery(); the store package no longer owns
// delivery read/write facades.
package store

import (
	"context"
	"database/sql"

	"velox-server/internal/deliverystore"
)

// DeliveryDestination and JobDelivery are re-exported as aliases from the
// deliverystore leaf so existing store.<Type> call sites keep compiling while
// the canonical declaration lives in the leaf. DeliveryLease is intentionally
// NOT re-exported: no production call-site uses store.DeliveryLease, and
// callers now depend on the canonical deliverystore.DeliveryLease directly.
type (
	DeliveryDestination = deliverystore.DeliveryDestination
	JobDelivery         = deliverystore.JobDelivery
)

// DeliveryDestinationStatus is the 3-state verdict returned by
// BatchDeliveryDestinationsStatus. Re-exported as an alias from the
// deliverystore leaf (the canonical declaration).
type DeliveryDestinationStatus = deliverystore.DeliveryDestinationStatus

// The three buckets are re-exported so existing store.<Const> call sites keep
// compiling while the canonical constants live in the leaf.
const (
	DeliveryDestinationNotFound = deliverystore.DeliveryDestinationNotFound
	DeliveryDestinationDisabled = deliverystore.DeliveryDestinationDisabled
	DeliveryDestinationEnabled  = deliverystore.DeliveryDestinationEnabled
)

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
