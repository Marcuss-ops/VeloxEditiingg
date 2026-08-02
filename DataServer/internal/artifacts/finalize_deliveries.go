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

	"velox-server/internal/identity"
)

// ── Step 4: resolveDeliveryDestinationsTx ───────────────────────────────

// resolveDeliveryDestinationsTx computes the per-job delivery
// destination set inside the same tx that INSERTs into job_deliveries
// (transactional safety for the per-job delivery plan).
//
// Resolution order:
//  1. cmd.DestinationID explicit override → single-destination plan
//     with max_attempts=5 (schema default). The cmd-level pin always
//     wins over a per-job plan because it pins routing to one tail.
//  2. w.resolver wired → delegated via DeliveryPlanResolver.
//  3. nil resolver → legacy all-enabled-destinations SELECT inside
//     the tx. max_attempts defaults to 5 because there is no
//     per-plan budget to consult.
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
	if w.resolver != nil {
		rd, rerr := w.resolver.ResolveDestinations(ctx, cmd.JobID, cmd.ArtifactID)
		if rerr != nil {
			return nil, fmt.Errorf("artifacts: FinalizeVerified plan resolver: %w", rerr)
		}
		return rd, nil
	}
	// No resolver wired: legacy all-enabled-destinations SELECT
	// inside the tx. max_attempts defaults to 5.
	rows, qerr := tx.QueryContext(ctx,
		`SELECT destination_id FROM delivery_destinations WHERE enabled = 1`)
	if qerr != nil {
		return nil, fmt.Errorf("artifacts: FinalizeVerified destinations SELECT: %w", qerr)
	}
	defer rows.Close()
	var resolved []DeliveryDestination
	for rows.Next() {
		var did string
		if err := rows.Scan(&did); err != nil {
			return nil, fmt.Errorf("artifacts: FinalizeVerified destinations Scan: %w", err)
		}
		if did == "" {
			continue
		}
		resolved = append(resolved, DeliveryDestination{
			DestinationID: did,
			MaxAttempts:   5,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("artifacts: FinalizeVerified destinations iter: %w", err)
	}
	return resolved, nil
}

// ── Step 5: insertPendingDeliveriesTx ───────────────────────────────────

// insertPendingDeliveriesTx materializes one job_deliveries row per
// resolved destination, idempotent on (artifact_id, destination_id)
// via the WHERE NOT EXISTS guard so a re-run of the same tx (e.g.
// after a transient commit error) cannot create duplicate delivery
// rows.
//
// Defense-in-depth: a resolver that returned MaxAttempts=0
// (e.g. pre-069 plan read returning the table default but also
// explicitly zeroed) must NOT translate to
// job_deliveries.max_attempts=0 — the runner's
// `lease.AttemptNumber >= maxAttempts` branch would mark FAILED on
// attempt 1. Re-enforce the schema default (5) here to keep the
// INSERT contract pinned.
//
// idempotency_key = "<artifact_id>_<destination_id>" so the
// deterministic uniqueness constraint at the SQL layer (see
// migrations 0xx) is also a no-op when the same (artifact,
// destination) pair is presented twice.
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
			SELECT ?, ?, ?, 'PENDING', ?, ?, ?, ?
			WHERE NOT EXISTS (
				SELECT 1 FROM job_deliveries
				WHERE artifact_id = ? AND destination_id = ?
			)`,
			deliveryID, cmd.ArtifactID, dest.DestinationID,
			maxAttempts, cmd.ArtifactID+"_"+dest.DestinationID, nowStr, nowStr,
			cmd.ArtifactID, dest.DestinationID,
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
