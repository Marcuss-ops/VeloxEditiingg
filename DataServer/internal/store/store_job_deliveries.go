// Package store / store_job_deliveries.go
//
// CRUD for job_deliveries (per-artifact × per-destination join rows).
// Split out of store_deliveries.go.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ── Job Delivery CRUD ────────────────────────────────────────────────────────

// InsertJobDelivery persists a new per-(artifact, destination) row.
func (s *SQLiteStore) InsertJobDelivery(jobD *JobDelivery) error {
	s.observeDBOperation(true)
	if jobD.DeliveryID == "" || jobD.ArtifactID == "" || jobD.DestinationID == "" {
		return fmt.Errorf("store: InsertJobDelivery: missing required fields")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if jobD.CreatedAt == "" {
		jobD.CreatedAt = now
	}
	if jobD.UpdatedAt == "" {
		jobD.UpdatedAt = now
	}
	if jobD.Status == "" {
		jobD.Status = "PENDING"
	}
	if jobD.MaxAttempts == 0 {
		jobD.MaxAttempts = 5
	}
	_, err := s.db.Exec(
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
		return wrapDBInfrastructure("InsertJobDelivery exec", err)
	}
	return nil
}

// ListJobDeliveriesByJob returns all deliveries for a job's READY artifacts.
func (s *SQLiteStore) ListJobDeliveriesByJob(jobID string) ([]JobDelivery, error) {
	s.observeDBOperation(false)
	rows, err := s.db.Query(
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
		        COALESCE((SELECT da.started_at FROM delivery_attempts da
		                  WHERE da.delivery_id = jd.delivery_id
		                  ORDER BY da.id DESC LIMIT 1), '')
		 FROM job_deliveries jd
		 JOIN artifacts a ON a.id = jd.artifact_id
		 WHERE a.job_id = ?
		 ORDER BY jd.delivery_id ASC`, jobID)
	if err != nil {
		return nil, wrapDBInfrastructure("ListJobDeliveriesByJob query", err)
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
			&jd.LastErrorMessage, &jd.CompletedAt, &jd.StartedAt); err != nil {
			return nil, wrapDBInfrastructure("ListJobDeliveriesByJob scan", err)
		}
		out = append(out, jd)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapDBInfrastructure("ListJobDeliveriesByJob rows", err)
	}
	return out, nil
}

// GetJobDelivery retrieves a single job_delivery by ID.
func (s *SQLiteStore) GetJobDelivery(ctx context.Context, deliveryID string) (*JobDelivery, error) {
	s.observeDBOperation(false)
	row := s.db.QueryRowContext(ctx,
		`SELECT delivery_id, artifact_id, destination_id,
		        status,
		        COALESCE(idempotency_key,''), COALESCE(remote_id,''),
		        COALESCE(remote_url,''),
		        created_at, updated_at, COALESCE(completed_at, ''),
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
		&remoteURL, &jd.CreatedAt, &jd.UpdatedAt, &jd.CompletedAt, &jd.StartedAt,
		&jd.LockedBy, &jd.LeaseID, &jd.LeaseExpiresAt, &jd.NextAttemptAt,
		&jd.AttemptCount, &jd.MaxAttempts, &jd.LastError, &jd.LastErrorMessage)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrDeliveryNoRow
		}
		return nil, wrapDBInfrastructure("GetJobDelivery scan", err)
	}
	jd.IdempotencyKey = idempotencyKey
	jd.RemoteID = remoteID
	jd.RemoteURL = remoteURL
	return &jd, nil
}

func (s *SQLiteStore) ListDeliveryReconciliationCandidates(ctx context.Context, limit int) ([]JobDelivery, error) {
	s.observeDBOperation(false)
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT delivery_id, artifact_id, destination_id, status,
		       COALESCE(remote_id,''), COALESCE(remote_url,''),
		       created_at, updated_at
		FROM job_deliveries
		WHERE COALESCE(remote_id,'') <> ''
		  AND status IN ('RUNNING','RETRY_WAIT')
		  AND updated_at >= datetime('now','-15 minutes')
		ORDER BY updated_at ASC LIMIT ?`, limit)
	if err != nil {
		return nil, wrapDBInfrastructure("ListDeliveryReconciliationCandidates query", err)
	}
	defer rows.Close()
	var out []JobDelivery
	for rows.Next() {
		var d JobDelivery
		if err := rows.Scan(&d.DeliveryID, &d.ArtifactID, &d.DestinationID, &d.Status, &d.RemoteID, &d.RemoteURL, &d.CreatedAt, &d.UpdatedAt); err != nil {
			return nil, wrapDBInfrastructure("ListDeliveryReconciliationCandidates scan", err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapDBInfrastructure("ListDeliveryReconciliationCandidates rows", err)
	}
	return out, nil
}

func (s *SQLiteStore) ApplyReconciledDelivery(ctx context.Context, deliveryID, status, remoteID, remoteURL, errorCode, errorMessage string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `
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
		return wrapDBInfrastructure("ApplyReconciledDelivery exec", err)
	}
	return nil
}
