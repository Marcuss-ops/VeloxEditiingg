package artifactsstore

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"velox-server/internal/repository"
)

// SQLiteArtifactRepository implements repository.ArtifactRepository against
// an existing SQLite database.
type SQLiteArtifactRepository struct {
	db *sql.DB
}

// NewSQLiteArtifactRepository wraps an existing database connection.
func NewSQLiteArtifactRepository(db *sql.DB) *SQLiteArtifactRepository {
	if db == nil {
		panic("artifactsstore: NewSQLiteArtifactRepository requires a non-nil database")
	}
	return &SQLiteArtifactRepository{db: db}
}

// Insert creates an artifact row with sane defaults.
func (r *SQLiteArtifactRepository) Insert(ctx context.Context, artifact *repository.Artifact) error {
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
	_, err := r.db.ExecContext(ctx,
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

// GetByID returns one artifact, or (nil, nil) when missing.
func (r *SQLiteArtifactRepository) GetByID(ctx context.Context, artifactID string) (*repository.Artifact, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT id, job_id, COALESCE(attempt_id,0), type, storage_provider,
		        COALESCE(storage_key,''), COALESCE(storage_url,''), COALESCE(local_path,''),
		        COALESCE(sha256,''), COALESCE(size_bytes,0), COALESCE(duration_seconds,0.0),
		        status, created_at
		 FROM artifacts WHERE id=?`, artifactID)
	var artifact repository.Artifact
	err := row.Scan(&artifact.ID, &artifact.JobID, &artifact.AttemptID, &artifact.Type, &artifact.StorageProvider,
		&artifact.StorageKey, &artifact.StorageURL, &artifact.LocalPath, &artifact.SHA256,
		&artifact.SizeBytes, &artifact.DurationSeconds, &artifact.Status, &artifact.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &artifact, nil
}

// ListByJob returns newest-first artifacts for a job, capped at limit.
func (r *SQLiteArtifactRepository) ListByJob(ctx context.Context, jobID string, limit int) ([]repository.Artifact, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, job_id, COALESCE(attempt_id,0), type, storage_provider,
		        COALESCE(storage_key,''), COALESCE(storage_url,''), COALESCE(local_path,''),
		        COALESCE(sha256,''), COALESCE(size_bytes,0), COALESCE(duration_seconds,0.0),
		        status, created_at
		 FROM artifacts WHERE job_id=? ORDER BY created_at DESC LIMIT ?`, jobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var artifacts []repository.Artifact
	for rows.Next() {
		var artifact repository.Artifact
		if err := rows.Scan(&artifact.ID, &artifact.JobID, &artifact.AttemptID, &artifact.Type, &artifact.StorageProvider,
			&artifact.StorageKey, &artifact.StorageURL, &artifact.LocalPath, &artifact.SHA256,
			&artifact.SizeBytes, &artifact.DurationSeconds, &artifact.Status, &artifact.CreatedAt); err != nil {
			return nil, err
		}
		artifacts = append(artifacts, artifact)
	}
	return artifacts, rows.Err()
}

func nullInt(value int) interface{} {
	if value == 0 {
		return nil
	}
	return value
}

var _ repository.ArtifactRepository = (*SQLiteArtifactRepository)(nil)
