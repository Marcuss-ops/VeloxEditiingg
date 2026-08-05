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
//   - The "BeginUpload → upload + artifact (atomic)" path stays on
//     artifacts.UploadSessionWriter.CreateArtifactAndUploadSession
//     in artifacts/sqlite_upload_session_writer.go. The verified-
//     finalization tx that flips jobs.status='SUCCEEDED' lives in
//     artifacts/sqlite_finalize_writer.go (the sole legal writer of
//     the SUCCEEDED terminal state, enforced by
//     internal/artifacts/scan_test.go).
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
//
// Layout of this package:
//   - artifact_uploads.go — contract surface: types, sentinels,
//     UploadRepository interface, SQLiteUploadRepository + wiring.
//   - artifact_uploads_sessions.go — per-session CRUD methods.
//   - artifact_uploads_chunks.go — per-chunk CRUD methods.
//   - artifact_uploads_helpers.go — package-level SQL helpers
//     (nilOrString, formatTimePtr, parseTimeRFC3339, ...).
package store

import (
	"context"
	"database/sql"
	"errors"
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
// // CreateUploadSession has been REMOVED. Use
// artifacts.UploadSessionWriter.CreateArtifactAndUploadSession instead —
//
//	the atomic-tx replacement that inserts the artifacts + artifact_uploads
//	rows in one transaction.
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

// NewSQLiteUploadRepositoryFromStore binds upload-session and chunk CRUD to
// the canonical SQLiteStore. The legacy *sql.DB constructor remains for
// isolated repository tests and compatibility callers.
func NewSQLiteUploadRepositoryFromStore(s *SQLiteStore) *SQLiteUploadRepository {
	if s == nil || s.db == nil {
		panic("store: NewSQLiteUploadRepositoryFromStore requires a non-nil SQLiteStore")
	}
	return &SQLiteUploadRepository{db: s.db}
}

// Compile-time interface check.
var _ UploadRepository = (*SQLiteUploadRepository)(nil)
