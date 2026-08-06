// Package store / artifact_uploads_sessions.go
//
// Per-session CRUD over artifact_uploads rows: lookup, status updates,
// CAS transitions, deletion, and the reconciler/worker-bridge queries.
// All methods share the upload_id primary key and are part of the
// UploadRepository contract (see artifact_uploads.go).
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"velox-server/internal/statemachine"
)

// GetUploadSession returns a session by ID, or (nil, nil) when missing.
func (r *SQLiteUploadRepository) GetUploadSession(ctx context.Context, uploadID string) (*UploadSession, error) {
	if uploadID == "" {
		return nil, fmt.Errorf("store: GetUploadSession: empty uploadID")
	}
	row := r.db.QueryRowContext(ctx, `
		SELECT upload_id, artifact_id, job_id, attempt_number, worker_id, lease_id,
		       status, temporary_storage_key,
		       COALESCE(expected_size_bytes, 0), COALESCE(expected_sha256, ''),
		       COALESCE(received_size_bytes, 0), COALESCE(received_sha256, ''),
		       COALESCE(expected_revision, 0),
		       created_at, expires_at, completed_at
		FROM artifact_uploads WHERE upload_id = ?`, uploadID)

	var s UploadSession
	var createdAt, expiresAt string
	var completedAt sql.NullString
	if err := row.Scan(
		&s.UploadID, &s.ArtifactID, &s.JobID, &s.AttemptNumber, &s.WorkerID, &s.LeaseID,
		&s.Status, &s.TemporaryStorageKey,
		&s.ExpectedSizeBytes, &s.ExpectedSHA256,
		&s.ReceivedSizeBytes, &s.ReceivedSHA256,
		&s.ExpectedRevision,
		&createdAt, &expiresAt, &completedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("store: GetUploadSession: %w", err)
	}
	if err := parseTimeRFC3339(&s.CreatedAt, createdAt); err != nil {
		return nil, fmt.Errorf("store: GetUploadSession: invalid created_at: %w", err)
	}
	if err := parseTimeRFC3339(&s.ExpiresAt, expiresAt); err != nil {
		return nil, fmt.Errorf("store: GetUploadSession: invalid expires_at: %w", err)
	}
	if completedAt.Valid {
		if err := parseTimeRFC3339(&s.CompletedAt, completedAt.String); err != nil {
			return nil, fmt.Errorf("store: GetUploadSession: invalid completed_at: %w", err)
		}
	}
	return &s, nil
}

// UpdateUploadStatus applies UploadFields atomically. Status is
// required. RowsAffected is checked: must be 1 for success, otherwise
// ErrUploadStateInvalid wraps the actual affected count.
func (r *SQLiteUploadRepository) UpdateUploadStatus(ctx context.Context, uploadID string, fields UploadFields) error {
	if uploadID == "" {
		return fmt.Errorf("store: UpdateUploadStatus: empty uploadID")
	}
	if fields.Status == nil {
		return fmt.Errorf("store: UpdateUploadStatus: status is required")
	}
	var currentStatus string
	if err := r.db.QueryRowContext(ctx, `SELECT status FROM artifact_uploads WHERE upload_id = ?`, uploadID).Scan(&currentStatus); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: upload=%s", ErrUploadStateInvalid, uploadID)
		}
		return fmt.Errorf("store: UpdateUploadStatus: read current status: %w", err)
	}
	if err := statemachine.DefaultRegistry().Validate(statemachine.DomainArtifactUpload, currentStatus, *fields.Status, ""); err != nil {
		return fmt.Errorf("store: UpdateUploadStatus: %w", err)
	}
	res, err := r.db.ExecContext(ctx, `
		UPDATE artifact_uploads
		SET status = ?,
		    received_size_bytes = COALESCE(?, received_size_bytes),
		    received_sha256    = COALESCE(?, received_sha256),
		    completed_at       = COALESCE(?, completed_at)
		WHERE upload_id = ?`,
		*fields.Status,
		fields.ReceivedSizeBytes,
		nilOrStringPtr(fields.ReceivedSHA256),
		formatTimePtr(fields.CompletedAt),
		uploadID,
	)
	if err != nil {
		return fmt.Errorf("store: UpdateUploadStatus: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("%w: upload=%s affected=%d", ErrUploadStateInvalid, uploadID, n)
	}
	return nil
}

// TransitionUploadStatus atomically CAS-flips the upload session
// status from `from` to `to`. Returns ErrUploadStateInvalid when 0
// rows are affected (row missing OR the source status does not
// match). Used by Service.Finalize to serialize concurrent
// finalize callers at the SQL layer.
func (r *SQLiteUploadRepository) TransitionUploadStatus(ctx context.Context, uploadID, from, to string) error {
	if err := statemachine.DefaultRegistry().Validate(statemachine.DomainArtifactUpload, from, to, ""); err != nil {
		return fmt.Errorf("store: TransitionUploadStatus: %w", err)
	}
	if uploadID == "" || from == "" || to == "" {
		return fmt.Errorf("store: TransitionUploadStatus: missing required arg")
	}
	res, err := r.db.ExecContext(ctx,
		`UPDATE artifact_uploads SET status = ? WHERE upload_id = ? AND status = ?`,
		to, uploadID, from)
	if err != nil {
		return fmt.Errorf("store: TransitionUploadStatus: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("%w: upload=%s from=%s to=%s affected=%d",
			ErrUploadStateInvalid, uploadID, from, to, n)
	}
	return nil
}

// DeleteUploadSession removes the session row. Reconciler calls this
// after EXPIRED cleanup or after COMPLETED retention window.
func (r *SQLiteUploadRepository) DeleteUploadSession(ctx context.Context, uploadID string) error {
	if uploadID == "" {
		return fmt.Errorf("store: DeleteUploadSession: empty uploadID")
	}
	if _, err := r.db.ExecContext(ctx,
		`DELETE FROM artifact_uploads WHERE upload_id = ?`, uploadID); err != nil {
		return fmt.Errorf("store: DeleteUploadSession: %w", err)
	}
	return nil
}

// FindStuckStaging returns CREATED/UPLOADING/FINALIZING sessions whose
// created_at is older than `olderThan`. The reconciler uses this list
// to mark them FAILED/EXPIRED. We keep the old sessions alive in DB
// (rather than delete) so audit trails survive until DeleteUploadSession
// is later called by a retention pass.
func (r *SQLiteUploadRepository) FindStuckStaging(ctx context.Context, olderThan time.Time, limit int) ([]UploadSession, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT upload_id, artifact_id, job_id, attempt_number, worker_id, lease_id,
		       status, temporary_storage_key,
		       COALESCE(expected_size_bytes, 0), COALESCE(expected_sha256, ''),
		       COALESCE(received_size_bytes, 0), COALESCE(received_sha256, ''),
		       COALESCE(expected_revision, 0),
		       created_at, expires_at, completed_at
		FROM artifact_uploads
		WHERE status IN ('CREATED', 'UPLOADING', 'FINALIZING')
		  AND created_at < ?
		ORDER BY created_at ASC
		LIMIT ?`, olderThan.UTC().Format(time.RFC3339), limit)
	if err != nil {
		return nil, fmt.Errorf("store: FindStuckStaging: %w", err)
	}
	defer rows.Close()

	var out []UploadSession
	for rows.Next() {
		var s UploadSession
		var createdAt, expiresAt string
		var completedAt sql.NullString
		if err := rows.Scan(
			&s.UploadID, &s.ArtifactID, &s.JobID, &s.AttemptNumber, &s.WorkerID, &s.LeaseID,
			&s.Status, &s.TemporaryStorageKey,
			&s.ExpectedSizeBytes, &s.ExpectedSHA256,
			&s.ReceivedSizeBytes, &s.ReceivedSHA256,
			&s.ExpectedRevision,
			&createdAt, &expiresAt, &completedAt,
		); err != nil {
			return nil, fmt.Errorf("store: FindStuckStaging scan: %w", err)
		}
		if err := parseTimeRFC3339(&s.CreatedAt, createdAt); err != nil {
			return nil, fmt.Errorf("store: FindStuckStaging: invalid created_at: %w", err)
		}
		if err := parseTimeRFC3339(&s.ExpiresAt, expiresAt); err != nil {
			return nil, fmt.Errorf("store: FindStuckStaging: invalid expires_at: %w", err)
		}
		if completedAt.Valid {
			if err := parseTimeRFC3339(&s.CompletedAt, completedAt.String); err != nil {
				return nil, fmt.Errorf("store: FindStuckStaging: invalid completed_at: %w", err)
			}
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: FindStuckStaging rows: %w", err)
	}
	return out, nil
}

// GetActiveUploadByJob returns the most recent CREATED or UPLOADING upload
// session for the given job_id. This is the bridge between the worker protocol
// (which identifies uploads by job_id) and the persistent artifact_uploads
// (which use upload_id as primary key).
func (r *SQLiteUploadRepository) GetActiveUploadByJob(ctx context.Context, jobID string) (*UploadSession, error) {
	if jobID == "" {
		return nil, fmt.Errorf("store: GetActiveUploadByJob: empty jobID")
	}
	row := r.db.QueryRowContext(ctx, `
		SELECT upload_id, artifact_id, job_id, attempt_number, worker_id, lease_id,
		       status, temporary_storage_key,
		       COALESCE(expected_size_bytes, 0), COALESCE(expected_sha256, ''),
		       COALESCE(received_size_bytes, 0), COALESCE(received_sha256, ''),
		       COALESCE(expected_revision, 0),
		       created_at, expires_at, completed_at
		FROM artifact_uploads
		WHERE job_id = ? AND status IN ('CREATED', 'UPLOADING')
		ORDER BY created_at DESC LIMIT 1`, jobID)

	var s UploadSession
	var createdAt, expiresAt string
	var completedAt sql.NullString
	if err := row.Scan(
		&s.UploadID, &s.ArtifactID, &s.JobID, &s.AttemptNumber, &s.WorkerID, &s.LeaseID,
		&s.Status, &s.TemporaryStorageKey,
		&s.ExpectedSizeBytes, &s.ExpectedSHA256,
		&s.ReceivedSizeBytes, &s.ReceivedSHA256,
		&s.ExpectedRevision,
		&createdAt, &expiresAt, &completedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("store: GetActiveUploadByJob: %w", err)
	}
	if err := parseTimeRFC3339(&s.CreatedAt, createdAt); err != nil {
		return nil, fmt.Errorf("store: GetActiveUploadByJob: invalid created_at: %w", err)
	}
	if err := parseTimeRFC3339(&s.ExpiresAt, expiresAt); err != nil {
		return nil, fmt.Errorf("store: GetActiveUploadByJob: invalid expires_at: %w", err)
	}
	if completedAt.Valid {
		if err := parseTimeRFC3339(&s.CompletedAt, completedAt.String); err != nil {
			return nil, fmt.Errorf("store: GetActiveUploadByJob: invalid completed_at: %w", err)
		}
	}
	return &s, nil
}
