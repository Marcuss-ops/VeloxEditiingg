// Package store / postgres_artifact_repository.go
//
// Postgres-side implementation of ArtifactRepository (narrow contract
// declared in this package). Independent struct (no SQLiteStore
// god-object shape) so the narrow contract stays clean and
// port-specific behavior (placeholder syntax, error sentinels, type
// coercions) cannot leak across repositories in future Phase 2 slices.
//
// Connection ownership lives on platform/database.Handle — the repo
// only borrows the *sql.DB pointer. The previous *PostgresStore helper
// was dropped because platform/database now owns the connection
// lifecycle end-to-end (Open -> Handle -> handle.DB.Close), so every
// repo can depend on Handle uniformly across both Phase 2 backends.
//
// Methods mirror SQLiteArtifactRepository 1:1, only placeholder syntax
// differs (SQLite `?` -> Postgres `$N`). DDL columns match the SQLite
// artifacts table exactly: no `verified_at` here either, even though
// store_artifacts.go's FinalizeArtifactVerified stamps it on SQLite.
// Verified_at lives in the SQLite-only transactional outbox path
// (TransitionArtifactStatus / FinalizeArtifactVerified on the
// *SQLiteStore god-object), which is NOT part of the narrowed
// ArtifactRepository contract and therefore NOT ported to PG in
// Phase 2's narrow-format landing.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"velox-server/internal/platform/database"
)

// PostgresArtifactRepository implements ArtifactRepository against a
// *database.Handle (Postgres backend).
type PostgresArtifactRepository struct {
	handle *database.Handle
}

// NewPostgresArtifactRepository wraps a *database.Handle as an ArtifactRepository.
// The handle is held by reference — the caller retains ownership of
// Close() so repos can be cheaply swapped during test setup without
// tearing down the connection pool multiple times.
func NewPostgresArtifactRepository(handle *database.Handle) *PostgresArtifactRepository {
	return &PostgresArtifactRepository{handle: handle}
}

// Compile-time check that PostgresArtifactRepository satisfies ArtifactRepository.
// Future Phase 2 adapters should follow the same pattern.
var _ ArtifactRepository = (*PostgresArtifactRepository)(nil)

// Insert creates an artifact row with sane defaults. Mirrors the
// SQLiteArtifactRepository behaviour including the side-effect of
// mutating the caller's *Artifact when ID/Status/StorageProvider/CreatedAt
// were blank. This is a documented part of the Insert contract from
// spec section 5 (Insert may populate ID when caller leaves it blank).
func (r *PostgresArtifactRepository) Insert(ctx context.Context, artifact *Artifact) error {
	if artifact == nil {
		return fmt.Errorf("artifact is nil")
	}
	if artifact.ID == "" {
		artifact.ID = fmt.Sprintf("artifact_%d", time.Now().UnixNano())
	}
	if artifact.Status == "" {
		artifact.Status = "pending"
	}
	if artifact.StorageProvider == "" {
		artifact.StorageProvider = "local"
	}
	if artifact.CreatedAt == "" {
		artifact.CreatedAt = time.Now().UTC().Format(time.RFC3339)
	}
	_, err := r.handle.DB.ExecContext(ctx,
		`INSERT INTO artifacts (
			id, job_id, attempt_id, type, storage_provider, storage_key,
			storage_url, local_path, sha256, size_bytes, duration_seconds,
			status, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10, $11,
			$12, $13
		)`,
		artifact.ID, artifact.JobID, artifact.AttemptID, artifact.Type,
		artifact.StorageProvider, artifact.StorageKey, artifact.StorageURL,
		artifact.LocalPath, artifact.SHA256, artifact.SizeBytes,
		artifact.DurationSeconds, artifact.Status,
		artifact.CreatedAt,
	)
	return err
}

// GetByID returns one artifact, or (nil, nil) on missing.
func (r *PostgresArtifactRepository) GetByID(ctx context.Context, artifactID string) (*Artifact, error) {
	row := r.handle.DB.QueryRowContext(ctx,
		`SELECT id, job_id, COALESCE(attempt_id, 0), type, storage_provider,
		        COALESCE(storage_key, ''), COALESCE(storage_url, ''),
		        COALESCE(local_path, ''), COALESCE(sha256, ''),
		        COALESCE(size_bytes, 0), COALESCE(duration_seconds, 0),
		        status, created_at
		 FROM artifacts WHERE id = $1`,
		artifactID)

	var a Artifact
	err := row.Scan(&a.ID, &a.JobID, &a.AttemptID, &a.Type, &a.StorageProvider,
		&a.StorageKey, &a.StorageURL, &a.LocalPath, &a.SHA256,
		&a.SizeBytes, &a.DurationSeconds, &a.Status, &a.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

// ListByJob returns newest-first artifacts for a job, capped at limit.
func (r *PostgresArtifactRepository) ListByJob(ctx context.Context, jobID string, limit int) ([]Artifact, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := r.handle.DB.QueryContext(ctx,
		`SELECT id, job_id, COALESCE(attempt_id, 0), type, storage_provider,
		        COALESCE(storage_key, ''), COALESCE(storage_url, ''),
		        COALESCE(local_path, ''), COALESCE(sha256, ''),
		        COALESCE(size_bytes, 0), COALESCE(duration_seconds, 0),
		        status, created_at
		 FROM artifacts WHERE job_id = $1
		 ORDER BY created_at DESC
		 LIMIT $2`,
		jobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var artifacts []Artifact
	for rows.Next() {
		var a Artifact
		if err := rows.Scan(&a.ID, &a.JobID, &a.AttemptID, &a.Type, &a.StorageProvider,
			&a.StorageKey, &a.StorageURL, &a.LocalPath, &a.SHA256,
			&a.SizeBytes, &a.DurationSeconds, &a.Status, &a.CreatedAt); err != nil {
			continue
		}
		artifacts = append(artifacts, a)
	}
	return artifacts, rows.Err()
}
