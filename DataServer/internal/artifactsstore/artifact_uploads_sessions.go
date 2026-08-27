package artifactsstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
	"velox-server/internal/repository"
	"velox-server/internal/statemachine"
)

func (r *SQLiteUploadRepository) GetUploadSession(ctx context.Context, uploadID string) (*UploadSession, error) {
	if uploadID == "" {
		return nil, fmt.Errorf("artifactsstore: GetUploadSession: empty uploadID")
	}
	row := r.db.QueryRowContext(ctx, `SELECT upload_id,artifact_id,job_id,attempt_number,worker_id,lease_id,status,temporary_storage_key,COALESCE(expected_size_bytes,0),COALESCE(expected_sha256,''),COALESCE(received_size_bytes,0),COALESCE(received_sha256,''),COALESCE(expected_revision,0),created_at,expires_at,completed_at,first_byte_received_at,last_byte_received_at,verify_started_at,verify_completed_at,promote_started_at,promote_completed_at,commit_started_at,commit_completed_at FROM artifact_uploads WHERE upload_id=?`, uploadID)
	var s UploadSession
	var createdAt, expiresAt string
	var times [9]sql.NullString
	if err := row.Scan(&s.UploadID, &s.ArtifactID, &s.JobID, &s.AttemptNumber, &s.WorkerID, &s.LeaseID, &s.Status, &s.TemporaryStorageKey, &s.ExpectedSizeBytes, &s.ExpectedSHA256, &s.ReceivedSizeBytes, &s.ReceivedSHA256, &s.ExpectedRevision, &createdAt, &expiresAt, &times[0], &times[1], &times[2], &times[3], &times[4], &times[5], &times[6], &times[7], &times[8]); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("artifactsstore: GetUploadSession: %w", err)
	}
	if err := parseTimeRFC3339(&s.CreatedAt, createdAt); err != nil {
		return nil, err
	}
	if err := parseTimeRFC3339(&s.ExpiresAt, expiresAt); err != nil {
		return nil, err
	}
	fields := []*time.Time{&s.CompletedAt, &s.FirstByteReceivedAt, &s.LastByteReceivedAt, &s.VerifyStartedAt, &s.VerifyCompletedAt, &s.PromoteStartedAt, &s.PromoteCompletedAt, &s.CommitStartedAt, &s.CommitCompletedAt}
	for i := range times {
		if times[i].Valid {
			if err := parseTimeRFC3339(fields[i], times[i].String); err != nil {
				return nil, err
			}
		}
	}
	return &s, nil
}

func (r *SQLiteUploadRepository) UpdateUploadStatus(ctx context.Context, uploadID string, fields repository.UploadFields) error {
	if uploadID == "" || fields.Status == nil {
		return fmt.Errorf("artifactsstore: UpdateUploadStatus: required argument missing")
	}
	var current string
	if err := r.db.QueryRowContext(ctx, `SELECT status FROM artifact_uploads WHERE upload_id=?`, uploadID).Scan(&current); err != nil {
		return err
	}
	if err := statemachine.DefaultRegistry().Validate(statemachine.DomainArtifactUpload, current, *fields.Status, ""); err != nil {
		return fmt.Errorf("artifactsstore: UpdateUploadStatus: %w", err)
	}
	_, err := r.db.ExecContext(ctx, `UPDATE artifact_uploads SET status=?,received_size_bytes=COALESCE(?,received_size_bytes),received_sha256=COALESCE(?,received_sha256),completed_at=COALESCE(?,completed_at),first_byte_received_at=COALESCE(?,first_byte_received_at),last_byte_received_at=COALESCE(?,last_byte_received_at),verify_started_at=COALESCE(?,verify_started_at),verify_completed_at=COALESCE(?,verify_completed_at),promote_started_at=COALESCE(?,promote_started_at),promote_completed_at=COALESCE(?,promote_completed_at),commit_started_at=COALESCE(?,commit_started_at),commit_completed_at=COALESCE(?,commit_completed_at) WHERE upload_id=?`, *fields.Status, fields.ReceivedSizeBytes, nilOrStringPtr(fields.ReceivedSHA256), formatTimePtr(fields.CompletedAt), formatTimePtr(fields.FirstByteReceivedAt), formatTimePtr(fields.LastByteReceivedAt), formatTimePtr(fields.VerifyStartedAt), formatTimePtr(fields.VerifyCompletedAt), formatTimePtr(fields.PromoteStartedAt), formatTimePtr(fields.PromoteCompletedAt), formatTimePtr(fields.CommitStartedAt), formatTimePtr(fields.CommitCompletedAt), uploadID)
	return err
}

func (r *SQLiteUploadRepository) TransitionUploadStatus(ctx context.Context, uploadID, from, to string) error {
	if err := statemachine.DefaultRegistry().Validate(statemachine.DomainArtifactUpload, from, to, ""); err != nil {
		return err
	}
	res, err := r.db.ExecContext(ctx, `UPDATE artifact_uploads SET status=? WHERE upload_id=? AND status=?`, to, uploadID, from)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return fmt.Errorf("%w: upload=%s", ErrUploadStateInvalid, uploadID)
	}
	return nil
}
func (r *SQLiteUploadRepository) DeleteUploadSession(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM artifact_uploads WHERE upload_id=?`, id)
	return err
}
func (r *SQLiteUploadRepository) FindStuckStaging(ctx context.Context, olderThan time.Time, limit int) ([]UploadSession, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT upload_id
		FROM artifact_uploads
		WHERE status IN ('CREATED', 'UPLOADING', 'RECEIVED', 'FINALIZING')
		  AND created_at < ?
		ORDER BY created_at ASC
		LIMIT ?`, olderThan.UTC().Format(time.RFC3339), limit)
	if err != nil {
		return nil, fmt.Errorf("artifactsstore: FindStuckStaging: %w", err)
	}
	defer rows.Close()
	var sessions []UploadSession
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("artifactsstore: FindStuckStaging scan: %w", err)
		}
		session, err := r.GetUploadSession(ctx, id)
		if err != nil {
			return nil, err
		}
		if session != nil {
			sessions = append(sessions, *session)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("artifactsstore: FindStuckStaging rows: %w", err)
	}
	return sessions, nil
}
func (r *SQLiteUploadRepository) GetActiveUploadByJob(ctx context.Context, job string) (*UploadSession, error) {
	row := r.db.QueryRowContext(ctx, `SELECT upload_id FROM artifact_uploads WHERE job_id=? AND status IN ('CREATED','UPLOADING') ORDER BY created_at DESC LIMIT 1`, job)
	var id string
	if err := row.Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return r.GetUploadSession(ctx, id)
}
