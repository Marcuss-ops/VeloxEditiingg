// sql-allowlist: artifacts SQLiteRepository — persistent CRUD for artifact_uploads + artifact_upload_chunks; supplements SQLiteFinalizationRepository atomic-tx path. Future refactor candidate for internal/store typed repos.

package artifacts

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// BeginUploadCommand is the input to Service.BeginUpload (Fase 1).
//
// The worker presents this BEFORE the bytes are streamed. The master
// uses the auth fields (worker_id, lease_id, attempt_number,
// expected_revision) and the in-memory job/attempt state to authorize
// the upload. The hint fields (kind, mime, expected_size, expected_sha)
// are NEVER trusted as authoritative — they are stored for diagnostics
// and surfaced to the worker only when the master disagrees.
type BeginUploadCommand struct {
	JobID            string
	WorkerID         string
	LeaseID          string
	AttemptNumber    int
	ExpectedRevision int

	// Worker-declared hints (diagnostic only).
	Kind              string
	MimeType          string
	ExpectedSizeBytes int64
	ExpectedSHA256    string
}

// FinalizeArtifactCommand is the master-side adapter from the gRPC
// ArtifactUploaded message. It carries ONLY the IDs / auth fields —
// the legacy `artifact_path`, `artifact_size`, `sha256` fields are
// ignored because they cannot be trusted (see PR 2 spec, Fase 4).
type FinalizeArtifactCommand struct {
	UploadID         string
	JobID            string
	WorkerID         string
	LeaseID          string
	AttemptNumber    int
	ExpectedRevision int
}

// PR 3.5-a: FinalizeArtifactAndCompleteJobCommand has been REMOVED.
// The single legal writer of jobs.status = 'SUCCEEDED' is now the
// artifacts.FinalizationRepository.FinalizeVerified method (see
// internal/artifacts/sqlite_finalization_repository.go). Use
// artifacts.FinalizeVerifiedCommand + Service.Finalize.
//
// Historically this struct lived here so service.go's
// FinalizeArtifactAndCompleteJob method could use it directly; that method
// itself was deleted as part of the same migration.

// UploadSession is the persistent state of one upload.
//
// Receive and Finalize mutate it through Repository.UpdateUploadStatus.
// CreatedAt is server time; ExpiresAt is CreatedAt + Service.uploadTTL
// (default 24h, matching the spec's "blob finale senza riga DB dopo 24h"
// reconciler rule).
type UploadSession struct {
	UploadID         string
	ArtifactID       string
	JobID            string
	WorkerID         string
	LeaseID          string
	AttemptNumber    int
	ExpectedRevision int

	Kind         string
	ExpectedMIME string

	TemporaryStorageKey string

	ExpectedSizeBytes int64
	ExpectedSHA256    string

	ReceivedSizeBytes int64
	ReceivedSHA256    string

	// CREATED | UPLOADING | RECEIVED | FINALIZING | COMPLETED | FAILED | EXPIRED.
	Status    string
	CreatedAt time.Time
	ExpiresAt time.Time
	// Equals zero value when the session has not been completed.
	CompletedAt time.Time
}

// ReceiveResult is what Service.Receive returns after streaming bytes.
//
// The master-computed hash + size are stored on the row, and Status
// moves to RECEIVED so the next Finalize call can re-hash from disk
// (defence-in-depth in case Receive and the worker's Finalize report
// race or arrive out of order).
type ReceiveResult struct {
	UploadID          string
	ReceivedSizeBytes int64
	ReceivedSHA256    string
	Status            string
}

// UploadFields lets the caller update a subset of an UploadSession row.
// Each pointer is optional: nil leaves the column untouched. Status is
// required for any UpdateUploadStatus call (state machine must advance).
type UploadFields struct {
	Status            *string
	ReceivedSizeBytes *int64
	ReceivedSHA256    *string
	CompletedAt       *time.Time
}

// Repository is the narrow persistence contract for artifact_uploads rows.
// All methods treat upload_id as the canonical key. Application-level
// invariants (status state machine) live in Service — SQL CHECK constraints
// only block blatantly malformed rows.
//
// PR 3.5-a: CreateUploadSession has been REMOVED. Use
// FinalizationRepository.CreateArtifactAndUploadSession instead —
// the atomic-tx replacement that inserts the artifacts + artifact_uploads
// rows in one transaction.
type Repository interface {
	GetUploadSession(ctx context.Context, uploadID string) (*UploadSession, error)
	UpdateUploadStatus(ctx context.Context, uploadID string, fields UploadFields) error
	DeleteUploadSession(ctx context.Context, uploadID string) error
	FindStuckStaging(ctx context.Context, olderThan time.Time, limit int) ([]UploadSession, error)

	// TransitionUploadStatus atomically CAS-flips status from `from`
	// to `to`. Returns ErrUploadStateInvalid when 0 rows are affected
	// (row missing OR source status doesn't match). Used by Finalize
	// to serialize concurrent finalize callers at the SQL layer.
	TransitionUploadStatus(ctx context.Context, uploadID, from, to string) error

	// GetActiveUploadByJob returns the most recent CREATED/UPLOADING
	// upload session for a job_id. Returns (nil, nil) if none exists.
	GetActiveUploadByJob(ctx context.Context, jobID string) (*UploadSession, error)

	// Chunk methods (PR chunked upload persistence).
	InsertChunk(ctx context.Context, c ChunkRecord) error
	ListChunks(ctx context.Context, uploadID string) ([]ChunkRecord, error)
	DeleteChunks(ctx context.Context, uploadID string) error
}

// SQLITE IMPLEMENTATION
//
// SQLiteRepository implements Repository against a *sql.DB.
// SQLite serializes writers, so concurrent Create/Update on the same
// row are race-free; the application layer in Service enforces the
// state-machine legality.

type SQLiteRepository struct {
	db *sql.DB
}

// NewSQLiteRepository wraps an existing *sql.DB. The caller owns the
// connection (typically the same one used by store.SQLiteStore so
// FinalizeArtifactAndCompleteJob can join its tx via the same *sql.DB).
func NewSQLiteRepository(db *sql.DB) *SQLiteRepository {
	return &SQLiteRepository{db: db}
}

// Compile-time interface check.
var _ Repository = (*SQLiteRepository)(nil)

// PR 3.5-a: CreateUploadSession impl REMOVED. The artifacts + artifact_uploads
// INSERT now happens atomically inside
// FinalizationRepository.CreateArtifactAndUploadSession (see
// sqlite_finalization_repository.go). Service.BeginUpload calls that method.

// GetUploadSession returns a session by ID, or (nil, nil) when missing.
func (r *SQLiteRepository) GetUploadSession(ctx context.Context, uploadID string) (*UploadSession, error) {
	if uploadID == "" {
		return nil, fmt.Errorf("artifacts: GetUploadSession: empty uploadID")
	}
	row := r.db.QueryRowContext(ctx, `
		SELECT upload_id, artifact_id, job_id, attempt_number, worker_id, lease_id,
		       status, temporary_storage_key,
		       COALESCE(expected_size_bytes, 0), COALESCE(expected_sha256, ''),
		       COALESCE(received_size_bytes, 0), COALESCE(received_sha256, ''),
		       COALESCE(expected_revision, 0),
		       created_at, expires_at, completed_at
		FROM artifact_uploads WHERE upload_id = ?`, uploadID)

	var s UploadSession
	var createdAt, expiresAt string
	var completedAt sql.NullString
	if err := row.Scan(
		&s.UploadID, &s.ArtifactID, &s.JobID, &s.AttemptNumber, &s.WorkerID, &s.LeaseID,
		&s.Status, &s.TemporaryStorageKey,
		&s.ExpectedSizeBytes, &s.ExpectedSHA256,
		&s.ReceivedSizeBytes, &s.ReceivedSHA256,
		&s.ExpectedRevision,
		&createdAt, &expiresAt, &completedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("artifacts: GetUploadSession: %w", err)
	}
	if err := parseTimeRFC3339(&s.CreatedAt, createdAt); err != nil {
		return nil, fmt.Errorf("artifacts: GetUploadSession: invalid created_at: %w", err)
	}
	if err := parseTimeRFC3339(&s.ExpiresAt, expiresAt); err != nil {
		return nil, fmt.Errorf("artifacts: GetUploadSession: invalid expires_at: %w", err)
	}
	if completedAt.Valid {
		if err := parseTimeRFC3339(&s.CompletedAt, completedAt.String); err != nil {
			return nil, fmt.Errorf("artifacts: GetUploadSession: invalid completed_at: %w", err)
		}
	}
	return &s, nil
}

// UpdateUploadStatus applies UploadFields atomically. Status is
// required. RowsAffected is checked: must be 1 for success, otherwise
// ErrUploadStateInvalid wraps the actual affected count.
func (r *SQLiteRepository) UpdateUploadStatus(ctx context.Context, uploadID string, fields UploadFields) error {
	if uploadID == "" {
		return fmt.Errorf("artifacts: UpdateUploadStatus: empty uploadID")
	}
	if fields.Status == nil {
		return fmt.Errorf("artifacts: UpdateUploadStatus: status is required")
	}
	res, err := r.db.ExecContext(ctx, `
		UPDATE artifact_uploads
		SET status = ?,
		    received_size_bytes = COALESCE(?, received_size_bytes),
		    received_sha256    = COALESCE(?, received_sha256),
		    completed_at       = COALESCE(?, completed_at)
		WHERE upload_id = ?`,
		*fields.Status,
		fields.ReceivedSizeBytes,
		nilOrStringPtr(fields.ReceivedSHA256),
		formatTimePtr(fields.CompletedAt),
		uploadID,
	)
	if err != nil {
		return fmt.Errorf("artifacts: UpdateUploadStatus: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("%w: upload=%s affected=%d", ErrUploadStateInvalid, uploadID, n)
	}
	return nil
}

// TransitionUploadStatus atomically CAS-flips the upload session
// status from `from` to `to`. Returns ErrUploadStateInvalid when 0
// rows are affected (row missing OR the source status does not
// match). Used by Service.Finalize to serialize concurrent
// finalize callers at the SQL layer.
func (r *SQLiteRepository) TransitionUploadStatus(ctx context.Context, uploadID, from, to string) error {
	if uploadID == "" || from == "" || to == "" {
		return fmt.Errorf("artifacts: TransitionUploadStatus: missing required arg")
	}
	res, err := r.db.ExecContext(ctx,
		`UPDATE artifact_uploads SET status = ? WHERE upload_id = ? AND status = ?`,
		to, uploadID, from)
	if err != nil {
		return fmt.Errorf("artifacts: TransitionUploadStatus: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("%w: upload=%s from=%s to=%s affected=%d",
			ErrUploadStateInvalid, uploadID, from, to, n)
	}
	return nil
}

// DeleteUploadSession removes the session row. Reconciler calls this
// after EXPIRED cleanup or after COMPLETED retention window.
func (r *SQLiteRepository) DeleteUploadSession(ctx context.Context, uploadID string) error {
	if uploadID == "" {
		return fmt.Errorf("artifacts: DeleteUploadSession: empty uploadID")
	}
	if _, err := r.db.ExecContext(ctx,
		`DELETE FROM artifact_uploads WHERE upload_id = ?`, uploadID); err != nil {
		return fmt.Errorf("artifacts: DeleteUploadSession: %w", err)
	}
	return nil
}

// FindStuckStaging returns CREATED/UPLOADING/FINALIZING sessions whose
// created_at is older than `olderThan`. The reconciler uses this list
// to mark them FAILED/EXPIRED. We keep the old sessions alive in DB
// (rather than delete) so audit trails survive until DeleteUploadSession
// is later called by a retention pass.
func (r *SQLiteRepository) FindStuckStaging(ctx context.Context, olderThan time.Time, limit int) ([]UploadSession, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT upload_id, artifact_id, job_id, attempt_number, worker_id, lease_id,
		       status, temporary_storage_key,
		       COALESCE(expected_size_bytes, 0), COALESCE(expected_sha256, ''),
		       COALESCE(received_size_bytes, 0), COALESCE(received_sha256, ''),
		       COALESCE(expected_revision, 0),
		       created_at, expires_at, completed_at
		FROM artifact_uploads
		WHERE status IN ('CREATED', 'UPLOADING', 'FINALIZING')
		  AND created_at < ?
		ORDER BY created_at ASC
		LIMIT ?`, olderThan.UTC().Format(time.RFC3339), limit)
	if err != nil {
		return nil, fmt.Errorf("artifacts: FindStuckStaging: %w", err)
	}
	defer rows.Close()

	var out []UploadSession
	for rows.Next() {
		var s UploadSession
		var createdAt, expiresAt string
		var completedAt sql.NullString
		if err := rows.Scan(
			&s.UploadID, &s.ArtifactID, &s.JobID, &s.AttemptNumber, &s.WorkerID, &s.LeaseID,
			&s.Status, &s.TemporaryStorageKey,
			&s.ExpectedSizeBytes, &s.ExpectedSHA256,
			&s.ReceivedSizeBytes, &s.ReceivedSHA256,
			&s.ExpectedRevision,
			&createdAt, &expiresAt, &completedAt,
		); err != nil {
			return nil, fmt.Errorf("artifacts: FindStuckStaging scan: %w", err)
		}
		if err := parseTimeRFC3339(&s.CreatedAt, createdAt); err != nil {
			return nil, fmt.Errorf("artifacts: FindStuckStaging: invalid created_at: %w", err)
		}
		if err := parseTimeRFC3339(&s.ExpiresAt, expiresAt); err != nil {
			return nil, fmt.Errorf("artifacts: FindStuckStaging: invalid expires_at: %w", err)
		}
		if completedAt.Valid {
			if err := parseTimeRFC3339(&s.CompletedAt, completedAt.String); err != nil {
				return nil, fmt.Errorf("artifacts: FindStuckStaging: invalid completed_at: %w", err)
			}
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("artifacts: FindStuckStaging rows: %w", err)
	}
	return out, nil
} // ── GetActiveUploadByJob ─────────────────────────────────────────────────────

// GetActiveUploadByJob returns the most recent CREATED or UPLOADING upload
// session for the given job_id. This is the bridge between the worker protocol
// (which identifies uploads by job_id) and the persistent artifact_uploads
// (which use upload_id as primary key).
func (r *SQLiteRepository) GetActiveUploadByJob(ctx context.Context, jobID string) (*UploadSession, error) {
	if jobID == "" {
		return nil, fmt.Errorf("artifacts: GetActiveUploadByJob: empty jobID")
	}
	row := r.db.QueryRowContext(ctx, `
		SELECT upload_id, artifact_id, job_id, attempt_number, worker_id, lease_id,
		       status, temporary_storage_key,
		       COALESCE(expected_size_bytes, 0), COALESCE(expected_sha256, ''),
		       COALESCE(received_size_bytes, 0), COALESCE(received_sha256, ''),
		       COALESCE(expected_revision, 0),
		       created_at, expires_at, completed_at
		FROM artifact_uploads
		WHERE job_id = ? AND status IN ('CREATED', 'UPLOADING')
		ORDER BY created_at DESC LIMIT 1`, jobID)

	var s UploadSession
	var createdAt, expiresAt string
	var completedAt sql.NullString
	if err := row.Scan(
		&s.UploadID, &s.ArtifactID, &s.JobID, &s.AttemptNumber, &s.WorkerID, &s.LeaseID,
		&s.Status, &s.TemporaryStorageKey,
		&s.ExpectedSizeBytes, &s.ExpectedSHA256,
		&s.ReceivedSizeBytes, &s.ReceivedSHA256,
		&s.ExpectedRevision,
		&createdAt, &expiresAt, &completedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("artifacts: GetActiveUploadByJob: %w", err)
	}
	if err := parseTimeRFC3339(&s.CreatedAt, createdAt); err != nil {
		return nil, fmt.Errorf("artifacts: GetActiveUploadByJob: invalid created_at: %w", err)
	}
	if err := parseTimeRFC3339(&s.ExpiresAt, expiresAt); err != nil {
		return nil, fmt.Errorf("artifacts: GetActiveUploadByJob: invalid expires_at: %w", err)
	}
	if completedAt.Valid {
		if err := parseTimeRFC3339(&s.CompletedAt, completedAt.String); err != nil {
			return nil, fmt.Errorf("artifacts: GetActiveUploadByJob: invalid completed_at: %w", err)
		}
	}
	return &s, nil
}

// ── Chunk repository methods (PR chunked upload persistence) ─────────────────

// ChunkRecord represents one chunk in a chunked upload session.
type ChunkRecord struct {
	UploadID   string
	ChunkIndex int
	SizeBytes  int64
	SHA256     string
	StorageKey string
	ReceivedAt time.Time
}

// InsertChunk persists a single chunk record.
func (r *SQLiteRepository) InsertChunk(ctx context.Context, c ChunkRecord) error {
	if c.UploadID == "" {
		return fmt.Errorf("artifacts: InsertChunk: empty uploadID")
	}
	now := c.ReceivedAt.UTC().Format(time.RFC3339)
	if c.ReceivedAt.IsZero() {
		now = time.Now().UTC().Format(time.RFC3339)
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO artifact_upload_chunks
		 (upload_id, chunk_index, size_bytes, sha256, storage_key, received_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		c.UploadID, c.ChunkIndex, c.SizeBytes, nilOrString(c.SHA256),
		c.StorageKey, now,
	)
	if err != nil {
		return fmt.Errorf("artifacts: InsertChunk: %w", err)
	}
	return nil
}

// ListChunks returns all chunks for an upload, ordered by chunk_index.
func (r *SQLiteRepository) ListChunks(ctx context.Context, uploadID string) ([]ChunkRecord, error) {
	if uploadID == "" {
		return nil, fmt.Errorf("artifacts: ListChunks: empty uploadID")
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT upload_id, chunk_index, size_bytes,
		       COALESCE(sha256, ''), storage_key, received_at
		FROM artifact_upload_chunks
		WHERE upload_id = ?
		ORDER BY chunk_index ASC`, uploadID)
	if err != nil {
		return nil, fmt.Errorf("artifacts: ListChunks: %w", err)
	}
	defer rows.Close()

	var out []ChunkRecord
	for rows.Next() {
		var c ChunkRecord
		var receivedAt string
		if err := rows.Scan(&c.UploadID, &c.ChunkIndex, &c.SizeBytes,
			&c.SHA256, &c.StorageKey, &receivedAt); err != nil {
			return nil, fmt.Errorf("artifacts: ListChunks scan: %w", err)
		}
		if t, perr := time.Parse(time.RFC3339, receivedAt); perr == nil {
			c.ReceivedAt = t
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("artifacts: ListChunks rows: %w", err)
	}
	return out, nil
}

// DeleteChunks removes all chunk records for an upload (cleanup after finalize).
func (r *SQLiteRepository) DeleteChunks(ctx context.Context, uploadID string) error {
	if uploadID == "" {
		return fmt.Errorf("artifacts: DeleteChunks: empty uploadID")
	}
	if _, err := r.db.ExecContext(ctx,
		`DELETE FROM artifact_upload_chunks WHERE upload_id = ?`, uploadID); err != nil {
		return fmt.Errorf("artifacts: DeleteChunks: %w", err)
	}
	return nil
}

// ── package-level helpers ────────────────────────────────────────────────
//
// nilOrString maps "" -> nil so the column stores NULL rather than "",
// matching the migration's nullable TEXT columns for expected_sha256 /
// received_sha256.
//
// These helpers are used by both this file and the SQLiteFinalizationRepository
// implementation. Keep the package-level rather than file-scoped so the
// finalization repository can reuse them without redeclaration.

func nilOrString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func nilOrStringPtr(p *string) interface{} {
	if p == nil || *p == "" {
		return nil
	}
	return *p
}

func formatTimePtr(p *time.Time) interface{} {
	if p == nil || p.IsZero() {
		return nil
	}
	return p.UTC().Format(time.RFC3339)
}

func parseTimeRFC3339(t *time.Time, raw string) error {
	if raw == "" {
		*t = time.Time{}
		return nil
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return err
	}
	*t = parsed
	return nil
}
