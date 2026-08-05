package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// CreateUploadSessionParams is the store-owned input for the atomic
// artifacts + artifact_uploads BeginUpload write. It deliberately contains
// only persistence data so internal/store does not import internal/artifacts.
type CreateUploadSessionParams struct {
	ArtifactID          string
	UploadID            string
	JobID               string
	AttemptID           int64
	Kind                string
	WorkerID            string
	LeaseID             string
	AttemptNumber       int
	ExpectedRevision    int
	StorageProvider     string
	ExpectedMIME        string
	ExpectedSizeBytes   int64
	ExpectedSHA256      string
	TemporaryStorageKey string
	CreatedAt           time.Time
	ExpiresAt           time.Time
}

// SQLiteUploadSessionWriter owns the paired BeginUpload insert. The
// artifacts package exposes only a narrow adapter interface and never sees
// this database handle.
type SQLiteUploadSessionWriter struct{ db *sql.DB }

func NewSQLiteUploadSessionWriter(db *sql.DB) *SQLiteUploadSessionWriter {
	if db == nil {
		panic("store: NewSQLiteUploadSessionWriter requires a non-nil database")
	}
	return &SQLiteUploadSessionWriter{db: db}
}

func (w *SQLiteUploadSessionWriter) CreateArtifactAndUploadSession(ctx context.Context, p CreateUploadSessionParams) error {
	if p.ArtifactID == "" || p.UploadID == "" || p.JobID == "" {
		return fmt.Errorf("store: CreateArtifactAndUploadSession: artifact_id, upload_id and job_id are required")
	}
	now := p.CreatedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	expiresAt := p.ExpiresAt
	if expiresAt.IsZero() {
		expiresAt = now.Add(24 * time.Hour)
	}
	provider := p.StorageProvider
	if provider == "" {
		provider = "local"
	}

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: CreateArtifactAndUploadSession begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO artifacts (id, job_id, attempt_id, type, storage_provider, status, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		p.ArtifactID, p.JobID, p.AttemptID, p.Kind, provider, "STAGING", now.UTC().Format(time.RFC3339)); err != nil {
		return fmt.Errorf("store: CreateArtifactAndUploadSession artifacts insert: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO artifact_uploads (
		    upload_id, artifact_id, job_id, attempt_number, worker_id, lease_id,
		    status, temporary_storage_key, expected_size_bytes, expected_sha256,
		    expected_revision, received_size_bytes, received_sha256,
		    created_at, expires_at, completed_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		p.UploadID, p.ArtifactID, p.JobID, p.AttemptNumber, p.WorkerID, p.LeaseID,
		"CREATED", p.TemporaryStorageKey, p.ExpectedSizeBytes, nilOrString(p.ExpectedSHA256),
		p.ExpectedRevision, 0, nil, now.UTC().Format(time.RFC3339),
		expiresAt.UTC().Format(time.RFC3339), nil); err != nil {
		return fmt.Errorf("store: CreateArtifactAndUploadSession artifact_uploads insert: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: CreateArtifactAndUploadSession commit: %w", err)
	}
	committed = true
	return nil
}

var _ interface {
	CreateArtifactAndUploadSession(context.Context, CreateUploadSessionParams) error
} = (*SQLiteUploadSessionWriter)(nil)
