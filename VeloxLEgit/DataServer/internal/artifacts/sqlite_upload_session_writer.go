// Package artifacts / sqlite_upload_session_writer.go
//
// Atomic writer of the BeginUpload paired-insert (artifacts: STAGING +
// artifact_uploads: CREATED). Single *sql.Tx wraps both writes; neither
// row can be observed without the other.
package artifacts

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// UploadSessionWriter is the BeginUpload-side persistence contract:
// one method, one tx, paired inserts.
//
// Invariants:
//   - Both rows commit or both roll back (single *sql.Tx).
//   - CreatedAt / ExpiresAt zero values are filled in by the writer
//     (CreatedAt=now, ExpiresAt=now+defaultUploadTTL=24h) so a caller
//     that forgets timestamps does not poison the schema with year
//     0001 RFC3339 strings.
//
// Preconditions:
//   - cmd.ArtifactID, cmd.UploadID, cmd.JobID must be non-empty.
//
// Error behavior:
//   - Empty identity field        → fmt.Errorf("artifacts: ... required").
//   - tx begin error              → wrapped ("... : begin: %w").
//   - per-table write fails (incl. context cancellation) → tx rolls
//     back via deferred Rollback; the inner error is returned so the
//     caller can fall through to the higher-level ErrDuplicateReadyArtifact
//     gate without losing the cause.
type UploadSessionWriter interface {
	CreateArtifactAndUploadSession(ctx context.Context, cmd CreateArtifactAndUploadSessionCommand) error
}

// SQLiteUploadSessionWriter is the SQLite-backed UploadSessionWriter.
//
// SQLite serializes concurrent writers at the SQL layer; the Service layer
// enforces the BEGIN-upload state-machine legality (job.RUNNING, attempt
// non-terminal, no preexisting READY artifact) before this code runs.
type SQLiteUploadSessionWriter struct {
	db *sql.DB
}

// NewSQLiteUploadSessionWriter wraps an existing *sql.DB. The caller owns
// the connection (typically the same one used by store.SQLiteStore).
func NewSQLiteUploadSessionWriter(db *sql.DB) *SQLiteUploadSessionWriter {
	if db == nil {
		panic("artifacts: NewSQLiteUploadSessionWriter requires a non-nil *sql.DB")
	}
	return &SQLiteUploadSessionWriter{db: db}
}

var _ UploadSessionWriter = (*SQLiteUploadSessionWriter)(nil)

func (w *SQLiteUploadSessionWriter) CreateArtifactAndUploadSession(ctx context.Context, cmd CreateArtifactAndUploadSessionCommand) error {
	if cmd.ArtifactID == "" || cmd.UploadID == "" || cmd.JobID == "" {
		return fmt.Errorf("artifacts: CreateArtifactAndUploadSession: artifact_id, upload_id and job_id are required")
	}
	now := cmd.CreatedAt
	if now.IsZero() {
		now = time.Now().UTC()
	}
	expiresAt := cmd.ExpiresAt
	if expiresAt.IsZero() {
		expiresAt = now.Add(defaultUploadTTL)
	}

	storageProvider := cmd.StorageProvider
	if storageProvider == "" {
		storageProvider = "local"
	}

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("artifacts: CreateArtifactAndUploadSession begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO artifacts (
		    id, job_id, attempt_id, type, storage_provider, status, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		cmd.ArtifactID, cmd.JobID, cmd.AttemptID, cmd.Kind,
		storageProvider, "STAGING", now.UTC().Format(time.RFC3339),
	); err != nil {
		return fmt.Errorf("artifacts: CreateArtifactAndUploadSession artifacts insert: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO artifact_uploads (
		    upload_id, artifact_id, job_id, attempt_number, worker_id, lease_id,
		    status, temporary_storage_key,
		    expected_size_bytes, expected_sha256,
		    expected_revision,
		    received_size_bytes, received_sha256,
		    created_at, expires_at, completed_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		cmd.UploadID, cmd.ArtifactID, cmd.JobID, cmd.AttemptNumber,
		cmd.WorkerID, cmd.LeaseID,
		"CREATED", cmd.TemporaryStorageKey,
		cmd.ExpectedSizeBytes, nilOrString(cmd.ExpectedSHA256),
		cmd.ExpectedRevision,
		0, nil,
		now.UTC().Format(time.RFC3339),
		expiresAt.UTC().Format(time.RFC3339),
		nil,
	); err != nil {
		return fmt.Errorf("artifacts: CreateArtifactAndUploadSession artifact_uploads insert: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("artifacts: CreateArtifactAndUploadSession commit: %w", err)
	}
	committed = true
	return nil
}

// nilOrString maps "" → nil so the column stores NULL rather than "",
// matching the migration's nullable TEXT columns for expected_sha256.
func nilOrString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
