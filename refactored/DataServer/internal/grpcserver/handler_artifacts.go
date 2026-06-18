package grpcserver

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"velox-server/internal/dbutil"
	"velox-server/internal/queue"
	"velox-server/internal/store"
	pb "velox-shared/controltransport/pb"
)

// isUniqueConstraintError returns true if err looks like an INSERT-side
// UNIQUE/PRIMARY KEY constraint violation. Falls back to substring
// matching because the SQLite drivers in this codebase
// (modernc.org/sqlite + mattn/go-sqlite3) format UNIQUE failures
// slightly differently; substring matching catches both
// ("UNIQUE constraint failed", "constraint failed: UNIQUE", and the
// "primary key constraint failed" variant).
func isUniqueConstraintError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unique constraint failed") ||
		strings.Contains(msg, "primary key constraint failed") ||
		strings.Contains(msg, "constraint failed: unique") ||
		strings.Contains(msg, "constraint failed: primary key")
}

// handleArtifactUploaded processes typed ArtifactUploaded via gRPC stream.
//
// Artifact success gate (PR 2 — full closure). The legacy version trusted
// the worker for path / size / type / ID and promoted the artifact straight
// from STAGING to READY without any verification, which meant a misbehaving
// worker could ship a SUCCEEDED job with an artifact that did not exist,
// was wrong-sized, or pointed outside the storage namespace.
//
// The new flow enforces:
//
//	1. STRICT CAS on jobs.assigned_to + jobs.lease_id + jobs.revision.
//	   Master is authoritative — read from DB, do NOT trust message body.
//	2. Attempt CAS via latest job_attempts row (lease_id MUST match
//	   jobs.lease_id; attempt MUST be in 'processing' or 'RENDER_FINISHED' state).
//	3. Status guard: only RUNNING accepted. The worker MUST first call
//	   RecordRenderFinished via JobResult{status=success} which marks the
//	   attempt as RENDER_FINISHED while the job stays RUNNING.
//	4. Artifact verification (no longer trust worker-reported metadata):
//	     a. Insert artifact in STAGING (bookkeeping only).
//	     b. ArtifactFinalizationService.FinalizeRender performs
//	        STAGING → VERIFYING (Tx1), recomputes SHA-256 + size, sniffs
//	        MIME, VERIFYING → READY (Tx2 with ARTIFACT_READY outbox).
//	        On any failure the artifact goes to QUARANTINED.
//	5. Delivery fan-out: InsertJobDeliveriesForArtifact creates PENDING
//	   job_deliveries for enabled delivery_destinations.
//	6. Atomic completion: CompleteJobTx flips jobs.status → COMPLETED,
//	   closes the latest job_attempts row, emits JOB_SUCCEEDED to the
//	   outbox, all in a single transaction. Idempotent on retry.
func (h *Handler) handleArtifactUploaded(workerID string, a *pb.ArtifactUploaded) {
	// verifiedSize is set by FinalizeRender with the master-measured
	// byte-count and surfaced into the JOB_SUCCEEDED outbox payload so
	// downstream consumers see the authoritative value, not the
	// worker-claimed size. Stays zero on retry paths that bypass
	// verification (READY re-attempt).
	verifiedSize := int64(0)


	jobID := a.GetJobId()
	artifactID := a.GetArtifactId()

	if jobID == "" || artifactID == "" {
		log.Printf("[GRPC] ArtifactUploaded from worker %s missing job_id or artifact_id — skipping", workerID)
		return
	}
	if !h.verifyJobOwnership(workerID, jobID) {
		log.Printf("[GRPC] ArtifactUploaded from worker %s for job %s refused — ownership mismatch", workerID, jobID)
		return
	}

	ctx := context.Background()

	// ----------------------------------------------------------------
	// 1+2+3. Strict CAS: authoritatively verify the (worker, lease,
	//         revision, attempt, status) tuple against the DB before
	//         doing any DB mutation.
	// ----------------------------------------------------------------
	jobMap, err := h.dbStore.GetJob(ctx, jobID)
	if err != nil || jobMap == nil {
		log.Printf("[GRPC] ArtifactUploaded: job %s not found: %v", jobID, err)
		return
	}
	assignedTo, _ := jobMap["assigned_to"].(string)
	if assignedTo != workerID {
		log.Printf("[GRPC] ArtifactUploaded: job %s assigned_to=%s, refused for worker %s",
			jobID, assignedTo, workerID)
		return
	}
	jobLeaseID, _ := jobMap["lease_id"].(string)
	if jobLeaseID == "" {
		log.Printf("[GRPC] ArtifactUploaded: job %s has no lease_id — refused (lease not bound)", jobID)
		return
	}
	jobLeaseExpiryStr, _ := jobMap["lease_expiry"].(string)
	if jobLeaseExpiryStr == "" {
		log.Printf("[GRPC] ArtifactUploaded: job %s has no lease_expiry — refused (lease not bound to a deadline)", jobID)
		return
	}
	jobLeaseExpiry, parseErr := time.Parse(time.RFC3339, jobLeaseExpiryStr)
	if parseErr != nil || !time.Now().UTC().Before(jobLeaseExpiry) {
		log.Printf("[GRPC] ArtifactUploaded: job %s lease %s expired at %s — refused",
			jobID, jobLeaseID, jobLeaseExpiryStr)
		return
	}
	jobStatus, _ := jobMap["status"].(string)
	if jobStatus != "RUNNING" {
		log.Printf("[GRPC] ArtifactUploaded: job %s in status %s — not accepting artifacts (must be RUNNING)",
			jobID, jobStatus)
		return
	}
	jobRevision := dbutil.IntFromMap(jobMap, "revision")

	attempt, attErr := h.dbStore.GetLatestJobAttempt(jobID)
	if attErr != nil || attempt == nil {
		log.Printf("[GRPC] ArtifactUploaded: no job_attempts for job %s: %v", jobID, attErr)
		return
	}
	if attempt.LeaseID != jobLeaseID {
		log.Printf("[GRPC] ArtifactUploaded: attempt %d lease_id=%s does not match jobs.lease_id=%s for job %s — refused",
			attempt.AttemptNumber, attempt.LeaseID, jobLeaseID, jobID)
		return
	}
	// Attempt must be in 'processing' or 'RENDER_FINISHED' state — if it's
	// already 'succeeded' another finalization path completed; refuse so we
	// don't double-promote.
	if attempt.Status != "processing" && attempt.Status != "RENDER_FINISHED" {
		log.Printf("[GRPC] ArtifactUploaded: attempt %d for job %s is in status %s — refused (must be 'processing' or 'RENDER_FINISHED')",
			attempt.AttemptNumber, jobID, attempt.Status)
		return
	}

	log.Printf("[GRPC] Worker %s uploaded artifact %s for job %s (type=%s, reported_size=%d bytes, attempt=%d, rev=%d, lease=%s)",
		workerID, artifactID, jobID, a.GetArtifactType(), a.GetArtifactSize(),
		attempt.AttemptNumber, jobRevision, jobLeaseID)

	// ----------------------------------------------------------------
	// 4a. Insert the artifact in STAGING. Bookkeeping only — the final
	//     READY transition happens inside FinalizeRender below.
	//
	//     Retry path: if a previous attempt inserted the same artifact
	//     but the gRPC stream dropped before CompleteJobTx ran, the
	//     worker retries. We detect this via UNIQUE-constraint failure
	//     on artifacts.id and re-load the existing row. If it is already
	//     READY we skip FinalizeRender entirely (otherwise the second
	//     call would fail with ErrArtifactTransitionConflict because
	//     STAGING → VERIFYING cannot run from READY); if it is in
	//     VERIFYING/STAGING we re-enter FinalizeRender; if QUARANTINED
	//     we refuse and let the operator intervene.
	//     Non-UNIQUE errors (driver / schema / lock) are logged loudly;
	//     we do not silently recover from those.
	// ----------------------------------------------------------------
	artifact := &store.Artifact{
		ID:              artifactID,
		JobID:           jobID,
		AttemptID:       attempt.ID,
		Type:            a.GetArtifactType(),
		StorageProvider: "local",
		// Worker-reported metadata is treated as a hint; the verifier
		// re-stats the file and re-computes sha256 + mime. If anything
		// does not match, the artifact is QUARANTINED.
		StorageKey: a.GetArtifactPath(),
		SizeBytes:  a.GetArtifactSize(),
		Status:     "STAGING",
	}
	alreadyReady := false
	if err := h.dbStore.InsertArtifact(artifact); err != nil {
		if !isUniqueConstraintError(err) {
			// Non-UNIQUE errors (driver / schema drift / busy DB / etc.)
			// are not safe to retry through. Log loudly and refuse.
			log.Printf("[GRPC] Failed to insert artifact %s for job %s (non-UNIQUE error): %v — refused",
				artifactID, jobID, err)
			return
		}
		existing, gerr := h.dbStore.GetArtifact(artifactID)
		switch {
		case gerr != nil || existing == nil:
			log.Printf("[GRPC] Failed to insert artifact %s (UNIQUE: row vanished on re-read): %v — refused",
				artifactID, gerr)
			return
		case existing.AttemptID != attempt.ID:
			// artifact_id collision across attempts is a worker bug:
			// the same id should never be reused for a new attempt.
			log.Printf("[GRPC] Artifact %s belongs to attempt %d, current attempt is %d — refusing (worker reused artifact_id across attempts)",
				artifactID, existing.AttemptID, attempt.ID)
			return
		case existing.Status == "READY":
			alreadyReady = true
		case existing.Status == "VERIFYING":
			// Mid-flight retry: Tx1 (STAGING → VERIFYING) has already run.
			// FinalizeRender always starts with Tx1, so re-running it
			// would hit ErrArtifactTransitionConflict on Tx1 and bail
			// before Tx2 ever runs. We refuse and require the worker to
			// send a fresh artifact_id; the existing row stays as-is
			// so an operator can intervene or the reconciler can clean
			// it up via the retention window.
			log.Printf("[GRPC] Artifact %s in VERIFYING (mid-flight retry) — refusing duplicate for job %s; worker must send a fresh artifact_id",
				artifactID, jobID)
			return
		case existing.Status == "STAGING":
			log.Printf("[GRPC] Artifact %s in STAGING (retry path, partial) — re-running FinalizeRender for job %s",
				artifactID, jobID)
			// alreadyReady = false; FinalizeRender will run STAGING → VERIFYING → Tx2.
		case existing.Status == "QUARANTINED":
			log.Printf("[GRPC] Artifact %s is QUARANTINED from a previous attempt — refusing retry for job %s (operator intervention required)",
				artifactID, jobID)
			return
		default:
			log.Printf("[GRPC] Artifact %s in unexpected status %q — refusing retry for job %s",
				artifactID, existing.Status, jobID)
			return
		}
	}

	// ----------------------------------------------------------------
	// 4b. Authoritative verification via ArtifactFinalizationService.
	//     If artifactSvc is nil the handler is misconfigured — refuse
	//     rather than ship a SUCCEEDED job without verification.
	//     Skipped entirely if the retry path already established the
	//     artifact is READY (alreadyReady=true): the worker is just
	//     retransmitting after a stream blip, not uploading fresh bytes.
	// ----------------------------------------------------------------
	if alreadyReady {
		log.Printf("[GRPC] Artifact %s already READY for job %s — FinalizeRender skipped (retry path)",
			artifactID, jobID)
	} else {
		if h.artifactSvc == nil {
			log.Printf("[GRPC] ArtifactUploaded: artifactSvc not wired — refusing %s for job %s (handler misconfigured)",
				artifactID, jobID)
			return
		}
		finInput := queue.FinalizeRenderInput{
			ArtifactID:    artifactID,
			JobID:         jobID,
			AttemptID:     int64(attempt.ID),
			WorkerID:      workerID,
			LeaseID:       jobLeaseID,
			TemporaryPath: a.GetArtifactPath(),
			ExpectedSize:  a.GetArtifactSize(),
			// WorkerSHA256 is intentionally left blank — the master is the
			// authority on the hash and stores its own sha256 inside
			// FinalizeArtifactVerified. The optional client-side compare
			// (worker-supplied SHA) is not part of the security gate; the
			// master re-hash IS.
		}
		result, err := h.artifactSvc.FinalizeRender(ctx, finInput)
		if err != nil {
			log.Printf("[GRPC] ArtifactUploaded: verification failed for %s (job %s): %v — artifact QUARANTINED, job NOT promoted",
				artifactID, jobID, err)
			return
		}
		// Stash the verified size/type/sha from the master so the
		// outbox payload ships authoritative values (not the worker-
		// claimed ones) — closes the discrepancy Item #3 in the
		// code review.
		verifiedSize = result.SizeBytes
	}

	// ----------------------------------------------------------------
	// 5. Delivery fan-out. PENDING job_deliveries are created against
	//    enabled delivery_destinations so the DeliveryRunner can pick
	//    them up. Best-effort — failure is logged but does not block
	//    the SUCCEEDED transition (deliveries can be re-created by a
	//    later reconciler pass).
	// ----------------------------------------------------------------
	if n, derr := h.dbStore.InsertJobDeliveriesForArtifact(ctx, artifactID, jobID); derr != nil {
		log.Printf("[GRPC] ArtifactUploaded: failed to create job_deliveries for %s (job %s): %v",
			artifactID, jobID, derr)
	} else if n > 0 {
		log.Printf("[GRPC] ArtifactUploaded: created %d PENDING job_deliveries for artifact %s", n, artifactID)
	}

	// ----------------------------------------------------------------
	// 6. Atomic completion. The WHERE status NOT IN (SUCCEEDED,
	//    COMPLETED, ...) clause is the second-line CAS that prevents
	//    double-completion. The job_attempts UPDATE closes the running
	//    attempt; the outbox emit lets downstream consumers react to
	//    JOB_SUCCEEDED.
	//
	//    size_bytes in the outbox payload is the MASTER-verified size
	//    (finalized.SizeBytes), not the worker-claimed a.GetArtifactSize().
	//    If verification ran via the resume-from-VERIFYING retry path,
	//    fall back to a.GetArtifactSize() since the resumed path cannot
	//    guarantee a re-measured size was captured in this scope.
	// ----------------------------------------------------------------
	if verifiedSize == 0 {
		verifiedSize = a.GetArtifactSize()
	}
	outboxPayload := fmt.Sprintf(`{"artifact_id":%q,"size_bytes":%d,"worker_id":%q,"attempt":%d}`,
		artifactID, verifiedSize, workerID, attempt.AttemptNumber)
	if err := h.dbStore.CompleteJobTx(ctx, jobID, int64(attempt.ID), outboxPayload, jobLeaseID, jobRevision); err != nil {
		// Surface a stale-CAS rejection explicitly so an operator can
		// see when a retry hit a lease reaper / revision bump instead
		// of silently leaving the job stuck in RUNNING with a closed attempt.
		log.Printf("[GRPC] ArtifactUploaded: CompleteJobTx failed for job %s (artifact %s): %v",
			jobID, artifactID, err)
		return
	}

	log.Printf("[GRPC] Artifact %s registered and job %s completed successfully (verified via master pipeline)",
		artifactID, jobID)
}
