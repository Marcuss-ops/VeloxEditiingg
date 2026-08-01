package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"velox-server/internal/credentials"
)

func (s *SQLiteStore) PutCredential(ctx context.Context, record credentials.StoredCredential) error {
	scopes, err := json.Marshal(record.Scopes)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO credential_vault
		(credential_ref, provider, owner, ciphertext, key_version, scopes_json, expires_at, rotation_due_at, revoked_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, record.Ref, record.Provider, record.Owner, record.Ciphertext, record.KeyVersion, string(scopes), credentialNullableTime(record.ExpiresAt), credentialNullableTime(record.RotationDueAt), credentialNullableTimePtr(record.RevokedAt), record.CreatedAt.UTC().Format(time.RFC3339), record.UpdatedAt.UTC().Format(time.RFC3339))
	return err
}

func (s *SQLiteStore) GetCredential(ctx context.Context, ref string) (*credentials.StoredCredential, error) {
	var record credentials.StoredCredential
	var scopes, expires, rotation, revoked, created, updated, used sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT credential_ref, provider, owner, ciphertext, key_version, scopes_json, expires_at, rotation_due_at, revoked_at, created_at, updated_at, last_used_at FROM credential_vault WHERE credential_ref = ?`, ref).Scan(&record.Ref, &record.Provider, &record.Owner, &record.Ciphertext, &record.KeyVersion, &scopes, &expires, &rotation, &revoked, &created, &updated, &used)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, credentials.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal([]byte(scopes.String), &record.Scopes); err != nil {
		return nil, err
	}
	record.ExpiresAt = parseTime(expires.String)
	record.RotationDueAt = parseTime(rotation.String)
	record.CreatedAt = parseTime(created.String)
	record.UpdatedAt = parseTime(updated.String)
	if revoked.Valid {
		parsed := parseTime(revoked.String)
		record.RevokedAt = &parsed
	}
	if used.Valid {
		parsed := parseTime(used.String)
		record.LastUsedAt = &parsed
	}
	return &record, nil
}

func (s *SQLiteStore) UpdateCredential(ctx context.Context, record credentials.StoredCredential) error {
	scopes, err := json.Marshal(record.Scopes)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `UPDATE credential_vault SET ciphertext=?, key_version=?, scopes_json=?, expires_at=?, rotation_due_at=?, updated_at=? WHERE credential_ref=?`, record.Ciphertext, record.KeyVersion, string(scopes), credentialNullableTime(record.ExpiresAt), credentialNullableTime(record.RotationDueAt), record.UpdatedAt.UTC().Format(time.RFC3339), record.Ref)
	return err
}

func (s *SQLiteStore) RevokeCredential(ctx context.Context, ref string, at time.Time) error {
	result, err := s.db.ExecContext(ctx, `UPDATE credential_vault SET revoked_at=?, updated_at=? WHERE credential_ref=?`, at.UTC().Format(time.RFC3339), at.UTC().Format(time.RFC3339), ref)
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	if count == 0 {
		return credentials.ErrNotFound
	}
	return nil
}

func (s *SQLiteStore) RecordCredentialUse(ctx context.Context, ref string, event credentials.UsageEvent) error {
	scopes, err := json.Marshal(event.Scope)
	if err != nil {
		return err
	}
	success := 0
	if event.Success {
		success = 1
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO credential_usage_audit (credential_ref, worker_id, publication_id, scopes_json, used_at, success, error_code) VALUES (?, ?, ?, ?, ?, ?, ?)`, ref, event.WorkerID, event.PublicationID, string(scopes), event.UsedAt.UTC().Format(time.RFC3339), success, event.ErrorCode)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `UPDATE credential_vault SET last_used_at=?, updated_at=? WHERE credential_ref=?`, event.UsedAt.UTC().Format(time.RFC3339), event.UsedAt.UTC().Format(time.RFC3339), ref)
	return err
}

func credentialNullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC().Format(time.RFC3339)
}
func credentialNullableTimePtr(value *time.Time) any {
	if value == nil || value.IsZero() {
		return nil
	}
	return value.UTC().Format(time.RFC3339)
}
func parseTime(value string) time.Time { parsed, _ := time.Parse(time.RFC3339, value); return parsed }
