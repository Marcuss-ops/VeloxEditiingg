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
	"database/sql"
	"errors"

	"velox-server/internal/repository"
	"velox-server/internal/storecore"
)

// ── TYPES ────────────────────────────────────────────────────────────────

// UploadSession is re-exported from the repository leaf package.
type UploadSession = repository.UploadSession

// UploadFields is re-exported from the repository leaf package.
type UploadFields = repository.UploadFields

// ChunkRecord is re-exported from the repository leaf package.
type ChunkRecord = repository.ChunkRecord

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
	ErrUploadNotFound = storecore.ErrUploadNotFound
	// ErrUploadStateInvalid is returned when the upload session exists
	// but its status does not match an operation's precondition.
	ErrUploadStateInvalid = storecore.ErrUploadStateInvalid
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
type UploadRepository = repository.UploadRepository

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
