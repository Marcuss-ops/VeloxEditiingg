package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// SQLiteArtifactReader is the SQLite-backed implementation of the
// artifacts.ArtifactReader consumer-owned port.
//
// Single SELECT projection of the artifacts table; no transaction
// required (read-only statement on a connection from the pool).
type SQLiteArtifactReader struct {
	db *sql.DB
}

// NewSQLiteArtifactReader wraps an existing *sql.DB.
func NewSQLiteArtifactReader(db *sql.DB) *SQLiteArtifactReader {
	if db == nil {
		panic("store: NewSQLiteArtifactReader requires a non-nil *sql.DB")
	}
	return &SQLiteArtifactReader{db: db}
}

// Compile-time check that the store adapter satisfies the consumer port.
// The interface lives in the consumer package (internal/artifacts)
// so the artifacts domain owns its contract.
var _ interface {
	GetByID(ctx context.Context, id string) (*Artifact, error)
} = (*SQLiteArtifactReader)(nil)

func (r *SQLiteArtifactReader) GetByID(ctx context.Context, id string) (*Artifact, error) {
	if id == "" {
		return nil, fmt.Errorf("store: SQLiteArtifactReader.GetByID: empty id")
	}
	row := r.db.QueryRowContext(ctx, `
		SELECT id, job_id, COALESCE(attempt_id, 0), type, storage_provider,
		       COALESCE(storage_key, ''), COALESCE(storage_url, ''),
		       COALESCE(local_path, ''), COALESCE(sha256, ''),
		       COALESCE(size_bytes, 0), COALESCE(duration_seconds, 0),
		       status, COALESCE(verified_at, ''), created_at
		FROM artifacts WHERE id = ?`, id)
	var a Artifact
	var verifiedAtStr string
	if err := row.Scan(&a.ID, &a.JobID, &a.AttemptID, &a.Type, &a.StorageProvider,
		&a.StorageKey, &a.StorageURL, &a.LocalPath, &a.SHA256,
		&a.SizeBytes, &a.DurationSeconds, &a.Status, &verifiedAtStr, &a.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("store: SQLiteArtifactReader.GetByID: %w", err)
	}
	return &a, nil
}
