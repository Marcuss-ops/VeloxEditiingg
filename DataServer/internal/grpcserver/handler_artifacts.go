// Package grpcserver / handler_artifacts.go
//
// Artifact upload handling: maps the ArtifactUploaded proto into an
// artifacts.FinalizeArtifactCommand and delegates to artifacts.Service.
// Extracted from the original handler_artifacts.go (split per
// responsabilità, 2026-08):
//
//	handler_artifacts.go        — handleArtifactUploaded (this file)
//	handler_artifact_gate.go    — checkArtifactCommitGate + artCommitGateError
//	handler_artifact_errors.go  — classifyFinalizeError (sentinels → class)
package grpcserver

import (
	"context"
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

	attemptID := a.GetAttemptId()

	// Pre-v1 workers do not carry attempt_id in the wire message.
	// Resolve it from the canonical tasks row (stamped at claim time)
	// so FinalizeVerified can stamp the authoritative artifact_sha256
	// on the winning attempt row (migration 148).
	if attemptID == "" && h.taskRepo != nil {
		if task, taskErr := h.taskRepo.GetByJobID(context.Background(), jobID); taskErr == nil && task != nil {
			attemptID = task.AttemptID
		}
	}

	cmd := artifacts.FinalizeArtifactCommand{
		UploadID:         uploadID,
		JobID:            jobID,
		WorkerID:         workerID,
		LeaseID:          a.GetLeaseId(),
		AttemptNumber:    int(a.GetAttempt()),
		ExpectedRevision: int(a.GetExpectedRevision()),
		AttemptID:        attemptID,
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
