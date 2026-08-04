// Package artifacts / finalize_deliveries.go
//
// Steps 4–6 of the FinalizeVerified atomic tx: per-job delivery plan
// resolution, durable job_deliveries insert, and the closing
// artifact_uploads FINALIZING→COMPLETED CAS. All three receive the
// caller's *sql.Tx and never manage their own transaction lifecycle.
package artifacts

import (
	"context"
	"database/sql"
	"fmt"

	"velox-server/internal/deliverycontract"
	"velox-server/internal/identity"
)

// ── Step 4: resolveDeliveryDestinationsTx ───────────────────────────────

// resolveDeliveryDestinationsTx computes the per-job delivery
// destination set inside the same tx that INSERTs into job_deliveries
// (transactional safety for the per-job delivery plan).
//
// Resolution order:
//  1. cmd.DestinationID explicit override → single-destination plan
//     with max_attempts=5 (schema default).
//  2. w.resolver wired → delegated via DeliveryPlanResolver.
//  3. nil resolver → fail closed; no global destination selection.
//
// Step 5/8 of the canonical-purity plan: switch from the legacy
// resolver (which dropped retry_budget at the interface boundary) to
// a per-destination projection that carries MaxAttempts, then stamp
// it on the INSERT so the durable attempt cap survives worker
// restarts. Resolved inline because the resolution happens inside
// the same tx that INSERTs job_deliveries; splitting it out would
// force a destination slice across the writer boundary with no
// separation win.
//
// rows.Close is deferred inside the helper so cursor cleanup is
// automatic even on early-return Scan errors.
func (w *SQLiteFinalizeWriter) resolveDeliveryDestinationsTx(ctx context.Context, tx *sql.Tx, cmd FinalizeVerifiedCommand) ([]DeliveryDestination, error) {
	if cmd.DestinationID != "" {
		// Single-destination explicit path.
		return []DeliveryDestination{{
			DestinationID: cmd.DestinationID,
			MaxAttempts:   5,
		}}, nil
	}
	if w.resolver == nil {
		// No explicit destination and no resolver means this is a
		// render-only finalization. Do not create delivery rows and do
		// not select any global destination.
		return nil, nil
	}
	rd, rerr := w.resolver.ResolveDestinations(ctx, cmd.JobID, cmd.ArtifactID)
	if rerr != nil {
		return nil, fmt.Errorf("artifacts: FinalizeVerified plan resolver: %w", rerr)
	}
	if len(rd) == 0 {
		return nil, fmt.Errorf("%w: job_id=%s", deliverycontract.ErrNoExplicitPlan, cmd.JobID)
	}
	return rd, nil
}

// ── Step 5: insertPendingDeliveriesTx ───────────────────────────────────

// insertPendingDeliveriesTx materializes one job_deliveries row per
// resolved destination, idempotent on (artifact_id, destination_id)
// via the database uniqueness constraint and ON CONFLICT DO NOTHING,
// so a re-run of the same tx (e.g. after a transient commit error)
// cannot create duplicate delivery rows.
//
// Defense-in-depth: a resolver that returned MaxAttempts=0
// (e.g. pre-069 plan read returning the table default but also
// explicitly zeroed) must NOT translate to
// job_deliveries.max_attempts=0 — the runner's
// `lease.AttemptNumber >= maxAttempts` branch would mark FAILED on
// attempt 1. Re-enforce the schema default (5) here to keep the
// INSERT contract pinned.
//
// idempotency_key = "<artifact_id>_<destination_id>" and the
// database uniqueness constraint on (artifact_id, destination_id)
// make concurrent/replayed finalization a no-op for the same pair.
func (w *SQLiteFinalizeWriter) insertPendingDeliveriesTx(ctx context.Context, tx *sql.Tx, cmd FinalizeVerifiedCommand, nowStr string, resolved []DeliveryDestination) error {
	for _, dest := range resolved {
		deliveryID, err := identity.NewHex128()
		if err != nil {
			return fmt.Errorf("generate delivery ID: %w", err)
		}
		maxAttempts := dest.MaxAttempts
		if maxAttempts <= 0 {
			maxAttempts = 5
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO job_deliveries (delivery_id, artifact_id, destination_id, status, max_attempts, idempotency_key, created_at, updated_at)
			VALUES (?, ?, ?, 'PENDING', ?, ?, ?, ?)
			ON CONFLICT(artifact_id, destination_id) DO NOTHING`,
			deliveryID, cmd.ArtifactID, dest.DestinationID,
			maxAttempts, cmd.ArtifactID+"_"+dest.DestinationID, nowStr, nowStr,
		)
		if err != nil {
			return fmt.Errorf("artifacts: FinalizeVerified job_deliveries insert (dest=%s, max_attempts=%d): %w",
				dest.DestinationID, maxAttempts, err)
		}
	}
	return nil
}

// ── Step 6: completeUploadTx ────────────────────────────────────────────

// completeUploadTx is the closing write of the verified-finalization
// tx: artifact_uploads FINALIZING → COMPLETED. Joining the same
// *sql.Tx avoids a liveness bug where a process crash between
// tx-commit and a separate post-commit UPDATE would leave the upload
// row stuck in FINALIZING forever, blocking retries even though jobs
// and artifacts are already SUCCEEDED.
func (w *SQLiteFinalizeWriter) completeUploadTx(ctx context.Context, tx *sql.Tx, cmd FinalizeVerifiedCommand, nowStr string) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE artifact_uploads
		SET status = 'COMPLETED',
		    completed_at = ?
		WHERE upload_id = ?
		  AND status = 'FINALIZING'`,
		nowStr, cmd.UploadID)
	if err != nil {
		return fmt.Errorf("artifacts: FinalizeVerified artifact_uploads CAS: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: upload affected=%d upload=%s",
			ErrTransitionConflict, n, cmd.UploadID)
	}
	return nil
}
