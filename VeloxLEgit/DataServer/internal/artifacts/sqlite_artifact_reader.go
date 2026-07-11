// Package artifacts / sqlite_artifact_reader.go
//
// Read-only artifact projection reader. Used by the finalize-writer
// post-tx SELECT and by the Service.idempotentCompletedPath. MUST NOT
// mutate; concurrent writers on the same artifact_id would race the
// finalize tx.
package artifacts

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"velox-server/internal/store"
)

// ArtifactReader is the read-only artifact contract: one method,
// one projection, no tx (statements run on the connection pool).
//
// Invariants:
//   - GetByID returns (nil, nil) when the row is absent so the caller
//     can decide whether absence is a bug or expected (the finalize
//     post-tx wraps nil as a hard error because a successful CAS on
//     the same id guarantees the row exists).
//   - SELECT column list is the canonical *store.Artifact projection;
//     adding verified_at_full / retention_class / etc. happens here
//     in exactly one place.
//
// Preconditions:
//   - id non-empty.
//
// Error behavior:
//   - Empty id                  → fmt.Errorf("artifacts: GetByID: empty id").
//   - Scan error ≠ ErrNoRows    → wrapped ("...: %w").
type ArtifactReader interface {
	GetByID(ctx context.Context, id string) (*store.Artifact, error)
}

// SQLiteArtifactReader is the SQLite-backed ArtifactReader.
//
// Single SELECT projection of the artifacts table; no transaction
// required (read-only statement on a connection from the pool).
type SQLiteArtifactReader struct {
	db *sql.DB
}

// NewSQLiteArtifactReader wraps an existing *sql.DB.
func NewSQLiteArtifactReader(db *sql.DB) *SQLiteArtifactReader {
	if db == nil {
		panic("artifacts: NewSQLiteArtifactReader requires a non-nil *sql.DB")
	}
	return &SQLiteArtifactReader{db: db}
}

var _ ArtifactReader = (*SQLiteArtifactReader)(nil)

func (r *SQLiteArtifactReader) GetByID(ctx context.Context, id string) (*store.Artifact, error) {
	if id == "" {
		return nil, fmt.Errorf("artifacts: GetByID: empty id")
	}
	row := r.db.QueryRowContext(ctx, `
		SELECT id, job_id, COALESCE(attempt_id, 0), type, storage_provider,
		       COALESCE(storage_key, ''), COALESCE(storage_url, ''),
		       COALESCE(local_path, ''), COALESCE(sha256, ''),
		       COALESCE(size_bytes, 0), COALESCE(duration_seconds, 0),
		       status, COALESCE(verified_at, ''), created_at
		FROM artifacts WHERE id = ?`, id)
	var a store.Artifact
	var verifiedAtStr string
	if err := row.Scan(&a.ID, &a.JobID, &a.AttemptID, &a.Type, &a.StorageProvider,
		&a.StorageKey, &a.StorageURL, &a.LocalPath, &a.SHA256,
		&a.SizeBytes, &a.DurationSeconds, &a.Status, &verifiedAtStr, &a.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("artifacts: GetByID: %w", err)
	}
	return &a, nil
}
