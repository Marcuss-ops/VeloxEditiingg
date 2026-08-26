package artifactsstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"velox-server/internal/repository"
)

// SQLiteArtifactReader is the SQLite-backed implementation of the
// artifacts.ArtifactReader consumer-owned port.
type SQLiteArtifactReader struct {
	db *sql.DB
}

// NewSQLiteArtifactReader wraps an existing *sql.DB.
func NewSQLiteArtifactReader(db *sql.DB) *SQLiteArtifactReader {
	if db == nil {
		panic("artifactsstore: NewSQLiteArtifactReader requires a non-nil *sql.DB")
	}
	return &SQLiteArtifactReader{db: db}
}

var _ interface {
	GetByID(ctx context.Context, id string) (*repository.Artifact, error)
} = (*SQLiteArtifactReader)(nil)

func (r *SQLiteArtifactReader) GetByID(ctx context.Context, id string) (*repository.Artifact, error) {
	if id == "" {
		return nil, fmt.Errorf("artifactsstore: SQLiteArtifactReader.GetByID: empty id")
	}
	row := r.db.QueryRowContext(ctx, `
		SELECT id, job_id, COALESCE(attempt_id, 0), type, storage_provider,
		       COALESCE(storage_key, ''), COALESCE(storage_url, ''),
		       COALESCE(local_path, ''), COALESCE(sha256, ''),
		       COALESCE(size_bytes, 0), COALESCE(duration_seconds, 0),
		       status, COALESCE(verified_at, ''), created_at
		FROM artifacts WHERE id = ?`, id)
	var a repository.Artifact
	var verifiedAtStr string
	if err := row.Scan(&a.ID, &a.JobID, &a.AttemptID, &a.Type, &a.StorageProvider,
		&a.StorageKey, &a.StorageURL, &a.LocalPath, &a.SHA256,
		&a.SizeBytes, &a.DurationSeconds, &a.Status, &verifiedAtStr, &a.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("artifactsstore: SQLiteArtifactReader.GetByID: %w", err)
	}
	return &a, nil
}
