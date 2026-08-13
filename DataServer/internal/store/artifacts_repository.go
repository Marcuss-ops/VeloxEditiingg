package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"velox-server/internal/repository"
)

// ArtifactRepository is re-exported from the repository leaf package.
type ArtifactRepository = repository.ArtifactRepository

// SQLiteArtifactRepository implements ArtifactRepository against a SQLiteStore.
//
// This is reference-typed so the connection (db/sql.DB) is reused across
// callers; we do not pool here, the underlying *sql.DB handles that.
type SQLiteArtifactRepository struct {
	store *SQLiteStore
}

// NewSQLiteArtifactRepository wraps a SQLiteStore as an ArtifactRepository.
func NewSQLiteArtifactRepository(store *SQLiteStore) *SQLiteArtifactRepository {
	return &SQLiteArtifactRepository{store: store}
}

// Insert creates an artifact row with sane defaults.
func (r *SQLiteArtifactRepository) Insert(ctx context.Context, artifact *Artifact) error {
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
	_, err := r.store.db.ExecContext(ctx,
		`INSERT INTO artifacts (id, job_id, attempt_id, type, storage_provider, storage_key,
		                        storage_url, local_path, sha256, size_bytes, duration_seconds, status, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		artifact.ID, artifact.JobID, nullInt(artifact.AttemptID), artifact.Type,
		artifact.StorageProvider, artifact.StorageKey, artifact.StorageURL,
		artifact.LocalPath, artifact.SHA256, artifact.SizeBytes,
		artifact.DurationSeconds, artifact.Status, artifact.CreatedAt,
	)
	return err
}

// GetByID returns one artifact, or (nil, nil) on missing.
func (r *SQLiteArtifactRepository) GetByID(ctx context.Context, artifactID string) (*Artifact, error) {
	row := r.store.db.QueryRowContext(ctx,
		`SELECT id, job_id, COALESCE(attempt_id,0), type, storage_provider,
		        COALESCE(storage_key,''), COALESCE(storage_url,''), COALESCE(local_path,''),
		        COALESCE(sha256,''), COALESCE(size_bytes,0), COALESCE(duration_seconds,0.0),
		        status, created_at
		 FROM artifacts WHERE id=?`, artifactID)
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
func (r *SQLiteArtifactRepository) ListByJob(ctx context.Context, jobID string, limit int) ([]Artifact, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := r.store.db.QueryContext(ctx,
		`SELECT id, job_id, COALESCE(attempt_id,0), type, storage_provider,
		        COALESCE(storage_key,''), COALESCE(storage_url,''), COALESCE(local_path,''),
		        COALESCE(sha256,''), COALESCE(size_bytes,0), COALESCE(duration_seconds,0.0),
		        status, created_at
		 FROM artifacts WHERE job_id=? ORDER BY created_at DESC LIMIT ?`,
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

// Compile-time check that SQLiteArtifactRepository satisfies ArtifactRepository.
var _ ArtifactRepository = (*SQLiteArtifactRepository)(nil)
