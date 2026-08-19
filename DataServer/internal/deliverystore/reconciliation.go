package deliverystore

import (
	"context"

	"velox-server/internal/storecore"
)

// ListDeliveryReconciliationCandidates returns RUNNING / RETRY_WAIT deliveries
// that carry a remote_id and were updated in the last 15 minutes. These are
// the rows the delivery runner sweeps for provider-side reconciliation when a
// legacy (non-phase) provider owns the asynchronous lifecycle.
func (w *SQLiteDeliveryStore) ListDeliveryReconciliationCandidates(ctx context.Context, limit int) ([]JobDelivery, error) {
	w.observeDBOperation(false)
	if limit <= 0 {
		limit = 100
	}
	rows, err := w.db.QueryContext(ctx, `
		SELECT delivery_id, artifact_id, COALESCE(publication_id,''), destination_id, status,
		       COALESCE(remote_id,''), COALESCE(remote_url,''),
		       created_at, updated_at
		FROM job_deliveries
		WHERE COALESCE(remote_id,'') <> ''
		  AND status IN ('RUNNING','RETRY_WAIT')
		  AND updated_at >= datetime('now','-15 minutes')
		ORDER BY updated_at ASC LIMIT ?`, limit)
	if err != nil {
		return nil, storecore.WrapDBInfrastructure("ListDeliveryReconciliationCandidates query", err)
	}
	defer rows.Close()
	var out []JobDelivery
	for rows.Next() {
		var d JobDelivery
		if err := rows.Scan(&d.DeliveryID, &d.ArtifactID, &d.PublicationID, &d.DestinationID, &d.Status, &d.RemoteID, &d.RemoteURL, &d.CreatedAt, &d.UpdatedAt); err != nil {
			return nil, storecore.WrapDBInfrastructure("ListDeliveryReconciliationCandidates scan", err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, storecore.WrapDBInfrastructure("ListDeliveryReconciliationCandidates rows", err)
	}
	return out, nil
}

// ApplyReconciledDelivery projects a provider-side reconciliation verdict onto
// a job_deliveries row. Terminal rows are never regressed: SUCCEEDED / FAILED /
// BLOCKED_AUTH / CANCELLED keep their status. Empty evidence fields leave the
// existing value untouched.
func (w *SQLiteDeliveryStore) ApplyReconciledDelivery(ctx context.Context, deliveryID, status, remoteID, remoteURL, errorCode, errorMessage string) error {
	now := nowRFC3339Nano()
	result, err := w.db.ExecContext(ctx, `
		UPDATE job_deliveries
		SET status = CASE
		              WHEN status IN ('SUCCEEDED', 'FAILED', 'BLOCKED_AUTH', 'CANCELLED') THEN status
		              ELSE ?
		            END,
		    remote_id = CASE WHEN ? <> '' THEN ? ELSE remote_id END,
		    remote_url = CASE WHEN ? <> '' THEN ? ELSE remote_url END,
		    last_error_code = CASE WHEN ? <> '' THEN ? ELSE last_error_code END,
		    last_error_message = CASE WHEN ? <> '' THEN ? ELSE last_error_message END,
		    updated_at = ?
		WHERE delivery_id = ?`, status, remoteID, remoteID, remoteURL, remoteURL,
		errorCode, errorCode, errorMessage, errorMessage, now, deliveryID)
	if err != nil {
		return storecore.WrapDBInfrastructure("ApplyReconciledDelivery exec", err)
	}
	affected, err := storecore.ReadRowsAffected(result, "ApplyReconciledDelivery")
	if err != nil {
		return err
	}
	if affected != 1 {
		return storecore.ErrDeliveryNoRow
	}
	return nil
}
