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
//
// Specifically, this handler does NOT:
//   - trust a.GetArtifactPath() as a canonical storage_key (path is
//     ignored; Service.PromoteToCanonical derives it from the master-
//     computed SHA-256 + mime-derived extension)
//   - trust a.GetArtifactSize() / upload_status / error as authoritative
//     state (all ignored; size + sha + status are master-computed from
//     the streamed byte range covered by the matching upload_id)
//   - call dbStore.InsertArtifact (it would let the worker plant a
//     STAGING row that the master never hashed)
//   - call dbStore.FinalizeAndCompleteJob (the PR 1 hand-rolled
//     "STAGING->READY + job RUNNING->SUCCEEDED" 2-tx dance is replaced by
//     Service.FinalizeArtifactAndCompleteJob's single-tx CAS gates)
//   - run its own worker/lease/revision auth gate (Service.Finalize
//     already enforces session.WorkerID == cmd.WorkerID AND the
//     single-tx CAS gates close on `assigned_to = ? / lease_id = ? /
//     revision = ?`. A second gate here would be redundant and risks
//     drift).
//
// Lease / attempt / revision gap: the current .pb.go (chunk-3-pre-regen)
// still carries only job_id + artifact_id + the deprecated fields. LeaseID,
// AttemptNumber, and ExpectedRevision therefore come in as blanks/zero
// here, which Service skips via its `if X != 0` guard prefixes. The
// proper wire-up follows the .pb.go regen scheduled for chunk-4+ and
// will land in handler_artifacts.go once the proto regeneration lands.
// Until that point, the *CAS gates for lease/revision are no-ops at the
// gRPC boundary*, but the SHA / size trust boundary is already enforced
// via Service.PromoteToCanonical and the artifact_uploads status state
// machine.
func (h *Handler) handleArtifactUploaded(workerID string, a *pb.ArtifactUploaded) {
	if h.artifactSvc == nil {
		log.Printf("[GRPC] ArtifactUploaded from worker %s but artifactSvc (artifacts.Service) is not wired — dropping", workerID)
		return
	}

	jobID := a.GetJobId()
	uploadID := a.GetUploadId() // PR 2 — empty until .pb.go regen
	artifactID := a.GetArtifactId()

	// Hard-fail on empty job_id. Legacy workers (pre-.pb.go regen) carry
	// only job_id + artifact_id and never an upload_id, but we do NOT
	// fall back by coercing artifact_id -> upload_id: BeginUpload has
	// minted a separate upload_id (newID()), so the lookup against
	// artifact_id would-miss-surfaces-as ErrUploadNotFound and the
	// diagnostic on the worker side would point at the wrong cause.
	// Instead we surface a distinct "regen-pending" log so the fix is
	// obvious (regen the .pb.go).
	if jobID == "" {
		log.Printf("[GRPC] ArtifactUploaded from worker %s missing job_id — skipping", workerID)
		return
	}
	if uploadID == "" {
		log.Printf("[GRPC] ArtifactUploaded from worker %s job=%s artifactID=%s has empty upload_id — skipping (legacy pre-regen proto, regen-pending)",
			workerID, jobID, artifactID)
		return
	}

	// Build the finalize command. The artifact_type field is preserved
	// for log correlation only; it is not used as authoritative state.
	//
	// LeaseID / AttemptNumber / ExpectedRevision are intentionally blank
	// or zero on this legacy .pb.go path. Service.Finalize guards each
	// field with `if X != 0` so the worker can still drive successful
	// finalization through this code path even without the regenerated
	// proto fields.
	cmd := artifacts.FinalizeArtifactCommand{
		UploadID:         uploadID,
		JobID:            jobID,
		WorkerID:         workerID,
		LeaseID:          a.GetLeaseId(),   // empty until regen
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
