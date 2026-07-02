// Package store / artifact_uploads.go
//
// Typed repository for the `artifact_uploads` + `artifact_upload_chunks`
// tables. Extracted from internal/artifacts/uploads.go as part of the
// migration that makes internal/store the canonical SQL gateway —
// mirror of the artifact_recovery.go pattern.
//
// Background: artifact_uploads is the persistent per-attempt upload
// session state (CREATED → UPLOADING → RECEIVED → FINALIZING →
// COMPLETED, plus stale-or-aborted EXPIRED/FAILED), and
// artifact_upload_chunks is the resumable-chunked-upload companion
// table. Both are owned by a single SQLite repository because they
// share the upload_id primary key.
//
// Migration contract:
//
//   - The "BeginUpload → uploades + artifact (atomic)" path stays on
//     artifacts.FinalizationRepository.CreateArtifactAndUploadSession
//     in artifacts/sqlite_finalization_repository.go (PR 3.5-a legacy
//     allowlist — its `SET status = 'SUCCEEDED'` writer is the sole
//     legal writer of the SUCCEEDED terminal state, enforced by
//     internal/artifacts/scan_test.go). Once file-3/4 of the
//     migration lands, that entire file follows the same path to
//     internal/store; for now it stays in artifacts with a duplicate
//     copy of the nilOrString/formatTimePtr helpers inline.
//
//   - Once BeginUpload produces a session, all per-session mutations
//     (UpdateUploadStatus, TransitionUploadStatus, DeleteUploadSession,
//     FindStuckStaging, GetActiveUploadByJob) and per-chunk operations
//     (InsertChunk, ListChunks, DeleteChunks) flow through this
//     typed repository — never through raw db.ExecContext from the
//     artifacts package.
//
//   - Sendinels returned by this repository (store.ErrUploadStateInvalid,
//     store.ErrTransitionConflict, store.ErrUploadNotFound,
//     store.ErrUploadExpired) are the canonical versions; the
//     artifacts package re-declares a same-named sentinel and the
//     Service boundary translates via fmt.Errorf("%w: ...", aX, err)
//     so call sites already using errors.Is(err, artifacts.ErrX) keep
//     working without churn. The store error is in the wrap chain
//     too, so the new-style test (store.UploadRepository unit tests)
//     can target it directly.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ── TYPES ────────────────────────────────────────────────────────────────

// UploadSession is the persistent state of one upload.
//
// Receive / Finalize mutate it through UploadRepository.UpdateUploadStatus.
// CreatedAt is server time; ExpiresAt is CreatedAt + uploadTTL
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

// UploadFields lets the caller update a subset of an UploadSession row.
// Each pointer is optional: nil leaves the column untouched. Status is
// required for any UpdateUploadStatus call (state machine must advance).
type UploadFields struct {
	Status            *string
	ReceivedSizeBytes *int64
	ReceivedSHA256    *string
	CompletedAt       *time.Time
}

// ChunkRecord represents one chunk in a chunked upload session.
type ChunkRecord struct {
	UploadID   string
	ChunkIndex int
	SizeBytes  int64
	SHA256     string
	StorageKey string
	ReceivedAt time.Time
}

// UploadStatus is the typed status for artifact_uploads rows.
// Mirrored from artifacts/status_types.go so artifacts callers can
// still reference string(UploadCreated) etc. without an extra import
// at every call site.
type UploadStatus string

const (
	UploadCreated    UploadStatus = "CREATED"
	UploadUploading  UploadStatus = "UPLOADING"
	UploadReceived   UploadStatus = "RECEIVED"
	UploadVerifying  UploadStatus = "VERIFYING"
	UploadFinalizing UploadStatus = "FINALIZING"
	UploadCompleted  UploadStatus = "COMPLETED"
	UploadFailed     UploadStatus = "FAILED"
	UploadExpired    UploadStatus = "EXPIRED"
)

// ── SENTINELS ────────────────────────────────────────────────────────────
//
// Most of these are the canonical versions for the artifact_uploads
// CAS chain. ErrTransitionConflict is shared with the canonical jobs
// CAS chain — it is declared once in this package at
// store/jobs_writer_types.go (declared earlier than this file) and
// is reused here. The artifacts package keeps same-named sentinels
// (artifacts.ErrUploadStateInvalid etc.) for caller compatibility,
// but the Service boundary translates via fmt.Errorf("%w: ...",
// artifacts.ErrX, err) so the store error stays in the errors.Is
// chain.

var (
	// ErrUploadNotFound is returned when an uploadID lookup matches 0
	// rows in artifact_uploads.
	ErrUploadNotFound = errors.New("store: upload session not found")
	// ErrUploadStateInvalid is returned when the upload session exists
	// but its status does not match an operation's precondition.
	ErrUploadStateInvalid = errors.New("store: upload session not in expected state")
	// ErrUploadExpired is returned when ExpiresAt has passed at lookup.
	ErrUploadExpired = errors.New("store: upload session expired")
	// ErrTransitionConflict is declared in jobs_writer_types.go for the
	// jobs CAS chain; this file reuses the same Go identifier so the
	// store.ErrTransitionConflict errors.Is target is identical across
	// the package. See the canonical declaration at
	// internal/store/jobs_writer_types.go.
)

// ── INTERFACE ────────────────────────────────────────────────────────────

// UploadRepository is the narrow persistence contract for
// artifact_uploads rows. All methods treat upload_id as the canonical
// key. Application-level invariants (status state machine) live in
// Service — SQL CHECK constraints only block blatantly malformed rows.
//
// PR 3.5-a: CreateUploadSession has been REMOVED. Use
// FinalizationRepository.CreateArtifactAndUploadSession instead —
// the atomic-tx replacement that inserts the artifacts + artifact_uploads
// rows in one transaction.
type UploadRepository interface {
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

// ── SQLITE IMPLEMENTATION ────────────────────────────────────────────────
//
// SQLiteUploadRepository implements UploadRepository against a *sql.DB.
// SQLite serializes writers, so concurrent Create/Update on the same
// row are race-free; the application layer in Service enforces the
// state-machine legality.

type SQLiteUploadRepository struct {
	db *sql.DB
}

// NewSQLiteUploadRepository wraps an existing *sql.DB. The caller
// owns the connection (typically the same one used by store.SQLiteStore
// so FinishFinalize's tx can join via the same *sql.DB).
func NewSQLiteUploadRepository(db *sql.DB) *SQLiteUploadRepository {
	return &SQLiteUploadRepository{db: db}
}

// Compile-time interface check.
var _ UploadRepository = (*SQLiteUploadRepository)(nil)

// ── METHODS ──────────────────────────────────────────────────────────────

// GetUploadSession returns a session by ID, or (nil, nil) when missing.
func (r *SQLiteUploadRepository) GetUploadSession(ctx context.Context, uploadID string) (*UploadSession, error) {
	if uploadID == "" {
		return nil, fmt.Errorf("store: GetUploadSession: empty uploadID")
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
		return nil, fmt.Errorf("store: GetUploadSession: %w", err)
	}
	if err := parseTimeRFC3339(&s.CreatedAt, createdAt); err != nil {
		return nil, fmt.Errorf("store: GetUploadSession: invalid created_at: %w", err)
	}
	if err := parseTimeRFC3339(&s.ExpiresAt, expiresAt); err != nil {
		return nil, fmt.Errorf("store: GetUploadSession: invalid expires_at: %w", err)
	}
	if completedAt.Valid {
		if err := parseTimeRFC3339(&s.CompletedAt, completedAt.String); err != nil {
			return nil, fmt.Errorf("store: GetUploadSession: invalid completed_at: %w", err)
		}
	}
	return &s, nil
}

// UpdateUploadStatus applies UploadFields atomically. Status is
// required. RowsAffected is checked: must be 1 for success, otherwise
// ErrUploadStateInvalid wraps the actual affected count.
func (r *SQLiteUploadRepository) UpdateUploadStatus(ctx context.Context, uploadID string, fields UploadFields) error {
	if uploadID == "" {
		return fmt.Errorf("store: UpdateUploadStatus: empty uploadID")
	}
	if fields.Status == nil {
		return fmt.Errorf("store: UpdateUploadStatus: status is required")
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
		return fmt.Errorf("store: UpdateUploadStatus: %w", err)
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
func (r *SQLiteUploadRepository) TransitionUploadStatus(ctx context.Context, uploadID, from, to string) error {
	if uploadID == "" || from == "" || to == "" {
		return fmt.Errorf("store: TransitionUploadStatus: missing required arg")
	}
	res, err := r.db.ExecContext(ctx,
		`UPDATE artifact_uploads SET status = ? WHERE upload_id = ? AND status = ?`,
		to, uploadID, from)
	if err != nil {
		return fmt.Errorf("store: TransitionUploadStatus: %w", err)
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
func (r *SQLiteUploadRepository) DeleteUploadSession(ctx context.Context, uploadID string) error {
	if uploadID == "" {
		return fmt.Errorf("store: DeleteUploadSession: empty uploadID")
	}
	if _, err := r.db.ExecContext(ctx,
		`DELETE FROM artifact_uploads WHERE upload_id = ?`, uploadID); err != nil {
		return fmt.Errorf("store: DeleteUploadSession: %w", err)
	}
	return nil
}

// FindStuckStaging returns CREATED/UPLOADING/FINALIZING sessions whose
// created_at is older than `olderThan`. The reconciler uses this list
// to mark them FAILED/EXPIRED. We keep the old sessions alive in DB
// (rather than delete) so audit trails survive until DeleteUploadSession
// is later called by a retention pass.
func (r *SQLiteUploadRepository) FindStuckStaging(ctx context.Context, olderThan time.Time, limit int) ([]UploadSession, error) {
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
		return nil, fmt.Errorf("store: FindStuckStaging: %w", err)
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
			return nil, fmt.Errorf("store: FindStuckStaging scan: %w", err)
		}
		if err := parseTimeRFC3339(&s.CreatedAt, createdAt); err != nil {
			return nil, fmt.Errorf("store: FindStuckStaging: invalid created_at: %w", err)
		}
		if err := parseTimeRFC3339(&s.ExpiresAt, expiresAt); err != nil {
			return nil, fmt.Errorf("store: FindStuckStaging: invalid expires_at: %w", err)
		}
		if completedAt.Valid {
			if err := parseTimeRFC3339(&s.CompletedAt, completedAt.String); err != nil {
				return nil, fmt.Errorf("store: FindStuckStaging: invalid completed_at: %w", err)
			}
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: FindStuckStaging rows: %w", err)
	}
	return out, nil
}

// GetActiveUploadByJob returns the most recent CREATED or UPLOADING upload
// session for the given job_id. This is the bridge between the worker protocol
// (which identifies uploads by job_id) and the persistent artifact_uploads
// (which use upload_id as primary key).
func (r *SQLiteUploadRepository) GetActiveUploadByJob(ctx context.Context, jobID string) (*UploadSession, error) {
	if jobID == "" {
		return nil, fmt.Errorf("store: GetActiveUploadByJob: empty jobID")
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
		return nil, fmt.Errorf("store: GetActiveUploadByJob: %w", err)
	}
	if err := parseTimeRFC3339(&s.CreatedAt, createdAt); err != nil {
		return nil, fmt.Errorf("store: GetActiveUploadByJob: invalid created_at: %w", err)
	}
	if err := parseTimeRFC3339(&s.ExpiresAt, expiresAt); err != nil {
		return nil, fmt.Errorf("store: GetActiveUploadByJob: invalid expires_at: %w", err)
	}
	if completedAt.Valid {
		if err := parseTimeRFC3339(&s.CompletedAt, completedAt.String); err != nil {
			return nil, fmt.Errorf("store: GetActiveUploadByJob: invalid completed_at: %w", err)
		}
	}
	return &s, nil
}

// InsertChunk persists a single chunk record.
func (r *SQLiteUploadRepository) InsertChunk(ctx context.Context, c ChunkRecord) error {
	if c.UploadID == "" {
		return fmt.Errorf("store: InsertChunk: empty uploadID")
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
		return fmt.Errorf("store: InsertChunk: %w", err)
	}
	return nil
}

// ListChunks returns all chunks for an upload, ordered by chunk_index.
func (r *SQLiteUploadRepository) ListChunks(ctx context.Context, uploadID string) ([]ChunkRecord, error) {
	if uploadID == "" {
		return nil, fmt.Errorf("store: ListChunks: empty uploadID")
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT upload_id, chunk_index, size_bytes,
		       COALESCE(sha256, ''), storage_key, received_at
		FROM artifact_upload_chunks
		WHERE upload_id = ?
		ORDER BY chunk_index ASC`, uploadID)
	if err != nil {
		return nil, fmt.Errorf("store: ListChunks: %w", err)
	}
	defer rows.Close()

	var out []ChunkRecord
	for rows.Next() {
		var c ChunkRecord
		var receivedAt string
		if err := rows.Scan(&c.UploadID, &c.ChunkIndex, &c.SizeBytes,
			&c.SHA256, &c.StorageKey, &receivedAt); err != nil {
			return nil, fmt.Errorf("store: ListChunks scan: %w", err)
		}
		if t, perr := time.Parse(time.RFC3339, receivedAt); perr == nil {
			c.ReceivedAt = t
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: ListChunks rows: %w", err)
	}
	return out, nil
}

// DeleteChunks removes all chunk records for an upload (cleanup after finalize).
func (r *SQLiteUploadRepository) DeleteChunks(ctx context.Context, uploadID string) error {
	if uploadID == "" {
		return fmt.Errorf("store: DeleteChunks: empty uploadID")
	}
	if _, err := r.db.ExecContext(ctx,
		`DELETE FROM artifact_upload_chunks WHERE upload_id = ?`, uploadID); err != nil {
		return fmt.Errorf("store: DeleteChunks: %w", err)
	}
	return nil
}

// ── package-level helpers ────────────────────────────────────────────────
//
// nilOrString maps "" -> nil so the column stores NULL rather than "",
// matching the migration's nullable TEXT columns for expected_sha256 /
// received_sha256. Private to the store package — the finalization
// repo (artifacts/sqlite_finalization_repository.go) maintains its
// own small inline duplicate of this helper until file-3/4 of the
// migration moves it to internal/store.

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
