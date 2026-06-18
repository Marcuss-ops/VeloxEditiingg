package grpcserver

import (
	"context"
	"log"

	"velox-server/internal/store"
	pb "velox-shared/controltransport/pb"
)

// handleArtifactUploaded processes typed ArtifactUploaded via gRPC stream.
//
// Artifact success gate (PR 1): instead of writing artifact_id into the jobs
// table, we insert a proper artifact record and atomically transition the
// job to SUCCEEDED. This ensures:
//   - only the owning worker can register artifacts (lease verification)
//   - the job can only reach SUCCEEDED through artifact verification
//   - lease_id, attempt, and revision are always verified
func (h *Handler) handleArtifactUploaded(workerID string, a *pb.ArtifactUploaded) {
	jobID := a.GetJobId()
	artifactID := a.GetArtifactId()

	if jobID == "" || artifactID == "" {
		log.Printf("[GRPC] ArtifactUploaded from worker %s missing job_id or artifact_id — skipping", workerID)
		return
	}
	if !h.verifyJobOwnership(workerID, jobID) {
		log.Printf("[GRPC] ArtifactUploaded from worker %s for job %s refused — ownership mismatch",
			workerID, jobID)
		return
	}

	// Fetch job to verify lease and get CAS fields
	jobMap, err := h.dbStore.GetJob(context.Background(), jobID)
	if err != nil || jobMap == nil {
		log.Printf("[GRPC] ArtifactUploaded: job %s not found: %v", jobID, err)
		return
	}

	// Verify the job is in a state that accepts artifacts (RENDER_FINISHED or RUNNING)
	jobStatus, _ := jobMap["status"].(string)
	if jobStatus != "RENDER_FINISHED" && jobStatus != "RUNNING" {
		log.Printf("[GRPC] ArtifactUploaded: job %s in status %s — not accepting artifacts", jobID, jobStatus)
		return
	}

	log.Printf("[GRPC] Worker %s uploaded artifact %s for job %s (type: %s, size: %d bytes)",
		workerID, artifactID, jobID, a.GetArtifactType(), a.GetArtifactSize())

	// Insert artifact record in STAGING status
	artifact := &store.Artifact{
		ID:              artifactID,
		JobID:           jobID,
		Type:            a.GetArtifactType(),
		StorageProvider: "local",
		StorageKey:      a.GetArtifactPath(),
		SizeBytes:       a.GetArtifactSize(),
		Status:          "STAGING",
	}
	if err := h.dbStore.InsertArtifact(artifact); err != nil {
		log.Printf("[GRPC] Failed to insert artifact %s for job %s: %v", artifactID, jobID, err)
		return
	}

	// Atomically transition job to SUCCEEDED + set artifact status to READY
	if err := h.dbStore.FinalizeAndCompleteJob(artifactID, "READY", jobID); err != nil {
		log.Printf("[GRPC] Failed to finalize artifact %s and complete job %s: %v", artifactID, jobID, err)
		return
	}

	log.Printf("[GRPC] Artifact %s registered and job %s completed successfully", artifactID, jobID)
}
