package deliverystore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"velox-server/internal/deliverycontract"
	"velox-server/internal/storecore"
)

// InsertJobDelivery persists a new per-(artifact, destination) row. The
// INSERT OR IGNORE keeps retries idempotent on delivery_id.
func (w *SQLiteDeliveryStore) InsertJobDelivery(jobD *JobDelivery) error {
	w.observeDBOperation(true)
	if jobD.DeliveryID == "" || jobD.ArtifactID == "" || jobD.DestinationID == "" {
		return fmt.Errorf("store: InsertJobDelivery: missing required fields")
	}
	now := nowRFC3339()
	if jobD.CreatedAt == "" {
		jobD.CreatedAt = now
	}
	if jobD.UpdatedAt == "" {
		jobD.UpdatedAt = now
	}
	if jobD.Status == "" {
		jobD.Status = deliverycontract.DeliveryPending
	}
	if jobD.MaxAttempts == 0 {
		jobD.MaxAttempts = 5
	}
	_, err := w.db.Exec(
		`INSERT OR IGNORE INTO job_deliveries
		 (delivery_id, artifact_id, destination_id, status,
		  idempotency_key, remote_id, remote_url, created_at, updated_at,
		  locked_by, lease_id, lease_expires_at, next_attempt_at,
		  attempt_count, max_attempts, last_error_code, last_error_message, completed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		jobD.DeliveryID, jobD.ArtifactID, jobD.DestinationID,
		jobD.Status, nullIfEmpty(jobD.IdempotencyKey),
		nullIfEmpty(jobD.RemoteID), nullIfEmpty(jobD.RemoteURL),
		jobD.CreatedAt, jobD.UpdatedAt,
		nullIfEmpty(jobD.LockedBy), nullIfEmpty(jobD.LeaseID),
		nullIfEmpty(jobD.LeaseExpiresAt), nullIfEmpty(jobD.NextAttemptAt),
		jobD.AttemptCount, jobD.MaxAttempts,
		nullIfEmpty(jobD.LastError), nullIfEmpty(jobD.LastErrorMessage),
		nullIfEmpty(jobD.CompletedAt),
	)
	if err != nil {
		return storecore.WrapDBInfrastructure("InsertJobDelivery exec", err)
	}
	return nil
}

// ListJobDeliveriesByJob returns all deliveries for a job's READY artifacts.
func (w *SQLiteDeliveryStore) ListJobDeliveriesByJob(jobID string) ([]JobDelivery, error) {
	w.observeDBOperation(false)
	rows, err := w.db.Query(
		`SELECT jd.delivery_id, jd.artifact_id, jd.destination_id,
		        jd.status,
		        COALESCE(jd.idempotency_key,''), COALESCE(jd.remote_id,''),
		        COALESCE(jd.remote_url,''),
		        jd.created_at, jd.updated_at,
		        COALESCE(jd.locked_by,''), COALESCE(jd.lease_id,''),
		        COALESCE(jd.lease_expires_at,''), COALESCE(jd.next_attempt_at,''),
		        jd.attempt_count, jd.max_attempts,
		        COALESCE(jd.last_error_code,''), COALESCE(jd.last_error_message,''),
		        COALESCE(jd.completed_at,''),
		        COALESCE(jd.queued_at, jd.created_at),
		        COALESCE((SELECT da.started_at FROM delivery_attempts da
		                  WHERE da.delivery_id = jd.delivery_id
		                  ORDER BY da.id DESC LIMIT 1), '')
		 FROM job_deliveries jd
		 JOIN artifacts a ON a.id = jd.artifact_id
		 WHERE a.job_id = ?
		 ORDER BY jd.delivery_id ASC`, jobID)
	if err != nil {
		return nil, storecore.WrapDBInfrastructure("ListJobDeliveriesByJob query", err)
	}
	defer rows.Close()
	var out []JobDelivery
	for rows.Next() {
		var jd JobDelivery
		if err := rows.Scan(&jd.DeliveryID, &jd.ArtifactID, &jd.DestinationID,
			&jd.Status, &jd.IdempotencyKey, &jd.RemoteID,
			&jd.RemoteURL, &jd.CreatedAt, &jd.UpdatedAt,
			&jd.LockedBy, &jd.LeaseID, &jd.LeaseExpiresAt, &jd.NextAttemptAt,
			&jd.AttemptCount, &jd.MaxAttempts, &jd.LastError,
			&jd.LastErrorMessage, &jd.CompletedAt, &jd.QueuedAt, &jd.StartedAt); err != nil {
			return nil, storecore.WrapDBInfrastructure("ListJobDeliveriesByJob scan", err)
		}
		out = append(out, jd)
	}
	if err := rows.Err(); err != nil {
		return nil, storecore.WrapDBInfrastructure("ListJobDeliveriesByJob rows", err)
	}
	return out, nil
}

// GetJobDelivery retrieves a single job_delivery by ID, or ErrDeliveryNoRow
// when missing (sql.ErrNoRows is normalized).
func (w *SQLiteDeliveryStore) GetJobDelivery(ctx context.Context, deliveryID string) (*JobDelivery, error) {
	w.observeDBOperation(false)
	row := w.db.QueryRowContext(ctx,
		`SELECT delivery_id, artifact_id, destination_id,
		        status,
		        COALESCE(idempotency_key,''), COALESCE(remote_id,''),
		        COALESCE(remote_url,''),
		        created_at, updated_at, COALESCE(completed_at, ''),
		        COALESCE(queued_at, created_at),
		        COALESCE((SELECT da.started_at FROM delivery_attempts da
		                  WHERE da.delivery_id = job_deliveries.delivery_id
		                  ORDER BY da.id DESC LIMIT 1), ''),
		        COALESCE(locked_by, ''), COALESCE(lease_id, ''),
		        COALESCE(lease_expires_at, ''), COALESCE(next_attempt_at, ''),
		        attempt_count, max_attempts,
		        COALESCE(last_error_code, ''), COALESCE(last_error_message, '')
		 FROM job_deliveries WHERE delivery_id = ?`, deliveryID)
	var jd JobDelivery
	var idempotencyKey, remoteID, remoteURL string
	err := row.Scan(&jd.DeliveryID, &jd.ArtifactID, &jd.DestinationID,
		&jd.Status, &idempotencyKey, &remoteID,
		&remoteURL, &jd.CreatedAt, &jd.UpdatedAt, &jd.CompletedAt, &jd.QueuedAt, &jd.StartedAt,
		&jd.LockedBy, &jd.LeaseID, &jd.LeaseExpiresAt, &jd.NextAttemptAt,
		&jd.AttemptCount, &jd.MaxAttempts, &jd.LastError, &jd.LastErrorMessage)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, storecore.ErrDeliveryNoRow
		}
		return nil, storecore.WrapDBInfrastructure("GetJobDelivery scan", err)
	}
	jd.IdempotencyKey = idempotencyKey
	jd.RemoteID = remoteID
	jd.RemoteURL = remoteURL
	return &jd, nil
}
