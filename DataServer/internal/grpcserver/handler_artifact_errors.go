// Package grpcserver / handler_artifact_errors.go
//
// Error classification for the artifact finalize path. Extracted from
// handler_artifacts.go (split per responsabilità) so the sentinel→class
// mapping lives apart from the upload handler and the commit gate.
package grpcserver

import (
	"errors"

	"velox-server/internal/artifacts"
)

// classifyFinalizeError maps a Service.Finalize error to a short class
// label that survives wrapping. It only inspects sentinel errors introduced
// by artifacts.Service — any other error is reported as "internal" so
// the worker's log line carries something searchable even when the
// failure is unrelated to the trust boundary (e.g. SQL driver errors).
//
// Sentinels covered MUST stay in sync with artifacts/errors.go. Coverage
// is verified by docs review today (chunk 4); a future PR that adds a
// new sentinel should also extend this switch or the new class silently
// degrades to "internal".
//
// Invariant: err is non-nil at the call site. Callers pass the error
// value ONLY after testing `if err != nil`, so we don't accept nil here
// and the caller's log line (which uses the class) is always meaningful.
func classifyFinalizeError(err error) string {
	switch {
	case errors.Is(err, artifacts.ErrTransitionConflict):
		return "transition_conflict"
	case errors.Is(err, artifacts.ErrArtifactTransferCorrupted):
		return "artifact_transfer_corrupted"
	case errors.Is(err, artifacts.ErrHashMismatch):
		return "hash_mismatch"
	case errors.Is(err, artifacts.ErrSizeMismatch):
		return "size_mismatch"
	case errors.Is(err, artifacts.ErrUploadStateInvalid):
		return "upload_state_invalid"
	case errors.Is(err, artifacts.ErrUploadNotFound):
		return "upload_not_found"
	case errors.Is(err, artifacts.ErrUploadExpired):
		return "upload_expired"
	case errors.Is(err, artifacts.ErrBlobWriteFailed):
		return "blob_write_failed"
	case errors.Is(err, artifacts.ErrBlobPromoteFailed):
		return "blob_promote_failed"
	case errors.Is(err, artifacts.ErrOrphanedBlob):
		return "orphaned_blob"
	case errors.Is(err, artifacts.ErrStorageKeyInvalid):
		return "storage_key_invalid"
	case errors.Is(err, artifacts.ErrAttemptMismatch):
		return "attempt_mismatch"
	case errors.Is(err, artifacts.ErrRevisionMismatch):
		return "revision_mismatch"
	case errors.Is(err, artifacts.ErrLeaseInvalid):
		return "lease_invalid"
	case errors.Is(err, artifacts.ErrWrongJobOwner):
		return "wrong_job_owner"
	case errors.Is(err, artifacts.ErrJobNotRunning):
		return "job_not_running"
	case errors.Is(err, artifacts.ErrAttemptNotRenderFinished):
		return "attempt_not_render_finished"
	case errors.Is(err, artifacts.ErrDuplicateReadyArtifact):
		return "duplicate_ready_artifact"
	default:
		return "internal"
	}
}
