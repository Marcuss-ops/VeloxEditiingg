package grpcserver

import (
	"context"
	"errors"
	"log"

	"velox-server/internal/artifacts"
	pb "velox-shared/controltransport/pb"
)

// handleArtifactUploaded processes typed ArtifactUploaded via gRPC stream.
//
// PR 2 (chunk 4) rewrite: the handler is no longer the trust boundary for
// artifact authenticity. It now ONLY maps the proto fields into a
// artifacts.FinalizeArtifactCommand and delegates the entire
// cryptographic + transactional pipeline to artifacts.Service.Finalize.
func (h *Handler) handleArtifactUploaded(workerID string, a *pb.ArtifactUploaded) {
	if h.artifactSvc == nil {
		log.Printf("[GRPC] ArtifactUploaded from worker %s but artifactSvc (artifacts.Service) is not wired — dropping", workerID)
		return
	}

	jobID := a.GetJobId()
	uploadID := a.GetUploadId()
	artifactID := a.GetArtifactId()

	if jobID == "" {
		log.Printf("[GRPC] ArtifactUploaded from worker %s missing job_id — skipping", workerID)
		return
	}
	if uploadID == "" {
		log.Printf("[GRPC] ArtifactUploaded from worker %s job=%s artifactID=%s has empty upload_id — skipping",
			workerID, jobID, artifactID)
		return
	}

	cmd := artifacts.FinalizeArtifactCommand{
		UploadID:         uploadID,
		JobID:            jobID,
		WorkerID:         workerID,
		LeaseID:          a.GetLeaseId(),
		AttemptNumber:    int(a.GetAttempt()),
		ExpectedRevision: int(a.GetExpectedRevision()),
	}

	log.Printf("[GRPC] Worker %s reporting artifact upload for job %s upload=%s artifactID=%s kind=%s",
		workerID, jobID, uploadID, artifactID, a.GetArtifactType())

	art, err := h.artifactSvc.Finalize(context.Background(), cmd)
	if err != nil {
		// Surface failure with enough context for the workers' log to find
		// the right line (job + upload + worker + error class). The
		// typed sentinel errors (ErrTransitionConflict / ErrHashMismatch /
		// ErrUploadExpired / ...) are matched with errors.Is so the class
		// shows up alongside the wrapped message — otherwise the worker
		// only sees the %v string and can't group failures by class.
		class := classifyFinalizeError(err)
		log.Printf("[GRPC] Artifact finalize FAILED class=%s job=%s upload=%s worker=%s: %v",
			class, jobID, uploadID, workerID, err)
		return
	}

	log.Printf("[GRPC] Artifact %s registered and job %s completed via upload %s (kind=%s sha256=%s)",
		art.ID, jobID, uploadID, art.Type, art.SHA256)
}

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
