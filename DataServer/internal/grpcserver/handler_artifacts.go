package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"log"

	"velox-server/internal/artifacts"
	pb "velox-shared/controltransport/pb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// handleArtifactUploaded processes typed ArtifactUploaded via gRPC stream.
//
// PR 2 (chunk 4) rewrite: the handler is no longer the trust boundary for
// artifact authenticity. It now ONLY maps the proto fields into a
// artifacts.FinalizeArtifactCommand and delegates the entire
// cryptographic + transactional pipeline to artifacts.Service.Finalize.
//
// Blocco 1 final-wire (P0 #2, #3, #4): the Stream() dispatch invokes
// checkArtifactCommitGate() before this method. A not-ready
// capabilityRegistry surfaces codes.PermissionDenied as the gRPC
// error returned from Stream() so the worker treats the commit as
// non-retryable without retrying against an unhealthy master.
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

// checkArtifactCommitGate enforces the on-the-wire "artifact.commit.v1"
// readiness contract. It is invoked by Handler.Stream() inside the
// *pb.WorkerToMasterEnvelope_ArtifactUploaded switch case BEFORE
// handleArtifactUploaded is delegated to, so a master whose
// coordinator / spool / transport subsystem is unhealthy can never
// accept a commit.
//
// Fail-closed semantic (documented):
//
//   - codes.PermissionDenied when the registry reports at least one
//     capability not ready. The method exists and the auth layer
//     passed, but the master itself lacks the prerequisites to
//     safely run the commit. PermissionDenied is the canonical
//     "prerequisites not satisfied" gRPC code and matches the
//     pattern already established by the VELOX_ALLOWED_WORKERS
//     gate in handler.go (codes.PermissionDenied from the
//     WorkerAuthorizer path).
//   - codes.Unimplemented is REJECTED for this case: the method
//     exists, and returning Unimplemented would tell the worker to
//     drop the capability entry from its dispatch table — wrong
//     behaviour when the failure is transient/misconfiguration.
//   - codes.Unavailable is REJECTED: the spool+transport are
//     readable as "transient" which would let the worker auto-retry
//     in tight loop against an unhealthy master.
//   - codes.FailedPrecondition is REJECTED: it overlaps with the
//     protocol-version handshake's existing use of the same code,
//     so an operator reading gRPC logs could not distinguish
//     "wrong proto" from "spool broken".
//
// Backward-compat: if h.capabilityRegistry is nil (legacy test paths,
// partial-wiring variants), the gate returns nil and commit proceeds.
// This matches the nil-safe pattern of SetResourceSink /
// SetPlacementRejectionSink and the existing "NIL-safe" comments on
// those setters.
//
// Returning this custom error terminates the Stream() loop with the
// gRPC code mapped through GRPCStatus(). The worker treats
// PermissionDenied as non-retryable (its own policy) so the master
// stays off the worker's hot-path until an operator runs a /ready
// check + flips the gate back to healthy via a master restart.
//
// Why a custom wrapper type instead of status.Errorf:
//
//   google.golang.org/grpc/status.Errorf delegates to fmt.Sprintf,
//   NOT fmt.Errorf, so the %w directive is rendered as literal text
//   ("artifact commit refused: %!w(MISSING)") instead of wrapping.
//   To preserve the registry.ErrCapabilityNotReady sentinel in the
//   errors.Is chain AND surface the right gRPC code, we need BOTH a
//   proper Unwrap() AND a GRPCStatus() interface implementation.
//   status.Errorf alone gives us neither. artCommitGateError below
//   is the minimal correct shape.
func (h *Handler) checkArtifactCommitGate(workerID string) error {
	if h.capabilityRegistry == nil {
		return nil // Backward-compat — see godoc.
	}
	if err := h.capabilityRegistry.Readyz(); err != nil {
		log.Printf("[GRPC] artifact.commit.v1 from worker %s refused: %v", workerID, err)
		return &artCommitGateError{
			inner: err,
		}
	}
	return nil
}

// artCommitGateError carries the artifact-commit fail-closed error in
// a form that satisfies three callers at once:
//
//  1. The gRPC server framework: implements GRPCStatus() so the
//     framework serializes it as codes.PermissionDenied with the
//     wrapped message — NOT codes.Unknown (which is what a bare
//     fmt.Errorf wrap would yield).
//  2. Structured test assertions: Unwrap() exposes the inner
//     registry.Readyz() error, so errors.Is(returned,
//     registry.ErrCapabilityNotReady) returns true.
//  3. Operator dashboards: Error() renders "<preamble>: <inner>"
//     so logs and gRPC status messages both carry the failing probe
//     name + detail for ops greppability.
//
// The struct remains unexported on purpose — Handler.checkArtifactCommitGate
// is the single constructor; nothing else in the codebase should be
// returning this type.
type artCommitGateError struct {
	inner error
}

func (e *artCommitGateError) Error() string {
	return fmt.Sprintf("artifact commit refused: capability not ready: %v", e.inner)
}

func (e *artCommitGateError) Unwrap() error {
	return e.inner
}

// GRPCStatus implements the gRPC status-erreur contract so the
// framework serializes codes.PermissionDenied on the wire without an
// extra hop through status.Error.
func (e *artCommitGateError) GRPCStatus() *status.Status {
	return status.New(codes.PermissionDenied, e.Error())
}

// Sanity check at compile time: artCommitGateError must satisfy the
// grpc-Status-erreur contract that the gRPC framework inspects at
// runtime. If a future refactor drops GRPCStatus() (or renames it),
// the framework silently downgrades to codes.Unknown — and this
// compile-time guard prevents that regression from landing.
var _ interface {
	Error() string
	GRPCStatus() *status.Status
} = (*artCommitGateError)(nil)

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
