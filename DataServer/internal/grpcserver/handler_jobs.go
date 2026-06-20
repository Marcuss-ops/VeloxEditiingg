package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"velox-server/internal/store"
	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// handleJobResult processes typed JobResult via gRPC stream.
//
// Artifact success gate (PR 1): a worker reporting status=success does NOT
// transition the job to SUCCEEDED. RecordRenderFinished logs the event while
// the job stays RUNNING. The actual SUCCEEDED transition is gated on the
// artifact service verifying and registering the artifact (see handleArtifactUploaded).
func (h *Handler) handleJobResult(workerID string, jr *pb.JobResult) {
	ctx := context.Background()
	jobID := jr.GetJobId()
	status := jr.GetStatus()
	errMsg := jr.GetError()

	if jobID == "" {
		log.Printf("[GRPC] JobResult from worker %s missing job_id — dropping", workerID)
		return
	}
	if !h.verifyJobOwnership(workerID, jobID) {
		log.Printf("[GRPC] JobResult from worker %s for job %s refused — ownership mismatch", workerID, jobID)
		return
	}

	if status == "success" {
		// Reject if lease_id or attempt is missing — these are required
		// for the identity CAS.
		leaseID := jr.GetLeaseId()
		attempt := int(jr.GetAttempt())
		if leaseID == "" {
			log.Printf("[GRPC] JobResult success from worker %s for job %s refused — missing lease_id", workerID, jobID)
			return
		}
		if attempt == 0 {
			log.Printf("[GRPC] JobResult success from worker %s for job %s refused — missing attempt", workerID, jobID)
			return
		}

		// Look up revision from the DB (protobuf does not carry it).
		currentRev, _, revErr := h.lookupJobCASFields(jobID)
		if revErr != nil {
			log.Printf("[GRPC] JobResult success from worker %s for job %s — cannot read revision: %v", workerID, jobID, revErr)
			return
		}

		cmd := store.RecordRenderFinishedCommand{
			JobID:            jobID,
			WorkerID:         workerID,
			LeaseID:          leaseID,
			AttemptNumber:    attempt,
			ExpectedRevision: currentRev,
			FinishedAt:       time.Now().UTC(),
		}
		if err := h.jobsRepo.RecordRenderFinished(ctx, cmd.JobID, cmd.WorkerID, cmd.LeaseID, cmd.AttemptNumber, cmd.ExpectedRevision); err != nil {
			log.Printf("[GRPC] RecordRenderFinished failed for %s: %v", jobID, err)
			return
		}
		log.Printf("[GRPC] Worker %s reported render finished for job %s — awaiting artifact", workerID, jobID)
	} else if status == "failed" {
		if err := h.jobsRepo.FailWithRetry(context.Background(), jobID, "", errMsg, true, 0); err != nil {
			log.Printf("[GRPC] Job failure transition failed for %s: %v", jobID, err)
		}
	}
}

// handleJobAccepted processes typed JobAccepted — Phase 5+ real push mode.
// The lease was already created by ClaimNextJob; we just verify and grant.
// Issue 4 fix: uses claimMu instead of h.mu for pendingOffer access.
func (h *Handler) handleJobAccepted(workerID string, ja *pb.JobAccepted) {
	if !h.config.PushMode {
		return
	}
	jobID := ja.GetJobId()

	sess := h.getSession(workerID)
	if sess == nil {
		return
	}

	// Issue 4 fix: lock claimMu to safely read and clear pendingOffer.
	sess.claimMu.Lock()
	offer := sess.pendingOffer
	var offerJobID, offerLeaseID string
	if offer != nil {
		offerJobID = offer.JobID
		offerLeaseID = offer.LeaseID
	}
	sess.claimMu.Unlock()

	if offer == nil || offerJobID != jobID {
		log.Printf("[GRPC] Worker %s accepted job %s but no matching pending offer", workerID, jobID)
		return
	}

	// Verify the lease_id the worker is accepting matches what we offered.
	declaredLeaseID := ja.GetLeaseId()
	if declaredLeaseID != "" && declaredLeaseID != offerLeaseID {
		log.Printf("[GRPC] Worker %s accepted job %s with mismatched lease_id: got %s, want %s",
			workerID, jobID, declaredLeaseID, offerLeaseID)
		return
	}

	// BUG FIX #1: LEASED → RUNNING transition MUST happen atomically BEFORE
	// sending JobLeaseGranted. Otherwise a fast-completing job that ends
	// before its first lease renewal would attempt LEASED → SUCCEEDED,
	// which the state machine forbids (only LEASED → RUNNING → SUCCEEDED).
	// The single CAS UPDATE inside StartJobWithLease verifies
	// (job_id, worker_id, lease_id, attempt, revision) atomically.
	//
	// Revision comes from a fresh GetJob (jobs.QueueItem is the rich projection
	// without revision; store.JobRecord carries it). The extra read is bounded by
	// SQLite single-writer semantics: ClaimNextJob already committed, so
	// the row is visible at our snapshot.
	currentRev, attemptNum, revErr := h.lookupJobCASFields(jobID)
	if revErr != nil {
		log.Printf("[GRPC] Worker %s JobAccepted for %s but CAS fields unavailable: %v",
			workerID, jobID, revErr)
		// Cannot promote without CAS identity — drop the offer, do NOT
		// send JobLeaseGranted (worker has stale view).
		sess.claimMu.Lock()
		if sess.pendingOffer != nil && sess.pendingOffer.JobID == jobID {
			sess.pendingOffer = nil
		}
		sess.claimMu.Unlock()
		return
	}

	if err := h.jobsRepo.Start(context.Background(), jobID, workerID, declaredLeaseID, attemptNum, currentRev); err != nil {
		if errors.Is(err, store.ErrTransitionConflict) {
			log.Printf("[GRPC] Worker %s accepted job %s but lease is stale (rev=%d attempt=%d) — rejecting",
				workerID, jobID, currentRev, attemptNum)
			// Stale lease: drop the offer, the lease reaper will reclaim
			// the job or another offer will be made.
			sess.claimMu.Lock()
			if sess.pendingOffer != nil && sess.pendingOffer.JobID == jobID {
				sess.pendingOffer = nil
			}
			sess.claimMu.Unlock()
			return
		}
		// Non-conflict error (driver / ctx / schema): keep the pending
		// offer so the next notifier tick can retry; do NOT send
		// JobLeaseGranted because we could not promote.
		log.Printf("[GRPC] StartJob (LEASED→RUNNING) failed for %s (worker %s): %v — keeping pending offer for retry",
			jobID, workerID, err)
		return
	}

	// Send typed JobLeaseGranted via sendCh — confirms the lease created by ClaimNextJob.
	// P0 #1: include the lease_id so the worker can use it for heartbeat/renewals.
	env := &pb.MasterToWorkerEnvelope{
		MessageId:       fmt.Sprintf("leasegrant-%s-%s", workerID, jobID),
		WorkerId:        workerID,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Msg: &pb.MasterToWorkerEnvelope_JobLeaseGranted{
			JobLeaseGranted: &pb.JobLeaseGranted{
				JobId:    jobID,
				WorkerId: workerID,
				Status:   "granted",
				LeaseId:  offer.LeaseID,
			},
		},
	}

	if !safeSend(sess.sendCh, &outboundMessage{Envelope: env}) {
		// Phase 4.2 hardening: when we cannot deliver JobLeaseGranted the
		// job must NOT stay LEASED with a stale pendingOffer. Release the
		// claim so the job returns to PENDING (or another worker can claim).
		log.Printf("[GRPC] sendCh full/closed for JobLeaseGranted to worker %s — releasing claim for job %s",
			workerID, jobID)
		if releaseErr := h.jobsRepo.ReleaseLease(context.Background(), jobID); releaseErr != nil {
			log.Printf("[GRPC] Failed to release claim for job %s after JobLeaseGranted send failure: %v",
				jobID, releaseErr)
		}
		sess.claimMu.Lock()
		if sess.pendingOffer != nil && sess.pendingOffer.JobID == jobID {
			sess.pendingOffer = nil
		}
		sess.claimMu.Unlock()
		return
	}

	// Issue 4 fix: clear pending offer under claimMu — job is now running on this worker.
	sess.claimMu.Lock()
	if sess.pendingOffer != nil && sess.pendingOffer.JobID == jobID {
		sess.pendingOffer = nil
	}
	sess.claimMu.Unlock()
}

// handleJobRejected processes typed JobRejected — releases the claimed job for requeue.
// Issue 4 fix: uses claimMu instead of h.mu for pendingOffer access.
//
// Phase 3.3: ownership gate — a worker can only reject a job it has been
// offered. Mismatched JobRejected messages are dropped silently (with a log).
func (h *Handler) handleJobRejected(workerID string, jr *pb.JobRejected) {
	jobID := jr.GetJobId()
	reason := jr.GetReason()
	if jobID == "" {
		return
	}
	if !h.verifyJobOwnership(workerID, jobID) {
		log.Printf("[GRPC] JobRejected from worker %s for job %s refused — ownership mismatch (reason=%q)",
			workerID, jobID, reason)
		return
	}
	log.Printf("[GRPC] Worker %s rejected job %s: %s", workerID, jobID, reason)

	if jobID != "" {
		if err := h.jobsRepo.FailWithRetry(context.Background(), jobID, "", reason, true, 0); err != nil {
			log.Printf("[GRPC] Failed to release rejected job %s: %v", jobID, err)
		}
	}

	// Issue 4 fix: lock claimMu to safely access and clear pendingOffer.
	sess := h.getSession(workerID)
	if sess == nil {
		return
	}
	sess.claimMu.Lock()
	if sess.pendingOffer != nil && sess.pendingOffer.JobID == jobID {
		sess.pendingOffer = nil
	}
	sess.claimMu.Unlock()
}

// handleJobProgress tracks per-job progress (typed, forwarded via heartbeat for now).
//
// Phase 3.3: relaxed ownership gate — progress is informational and is
// only dropped when the worker definitely does not own the job. Mismatch
// scenarios are logged but not rejected (a worker mid-reconnect might
// legitimately race a late JobProgress against a freshly-reassigned job).
func (h *Handler) handleJobProgress(workerID string, jp *pb.JobProgress) {
	jobID := jp.GetJobId()
	if jobID == "" {
		return
	}
	if !h.verifyJobOwnership(workerID, jobID) {
		log.Printf("[GRPC] JobProgress from worker %s for job %s ignored — ownership mismatch",
			workerID, jobID)
		return
	}
	log.Printf("[GRPC] Worker %s progress on job %s: stage=%s %d%% (scene %d/%d)",
		workerID, jobID, jp.GetStage(), jp.GetProgressPercent(), jp.GetScene(), jp.GetTotalScenes())
}

// handleLeaseRenewal processes a typed LeaseRenewal via gRPC stream.
//
// Phase 3.3: verify the worker owns the job before extending the lease.
// Without this gate a malicious worker could renew (and thus keep alive
// forever) a job assigned to a different worker.
func (h *Handler) handleLeaseRenewal(workerID string, lr *pb.LeaseRenewal) {
	jobID := lr.GetJobId()
	if jobID == "" {
		return
	}
	if !h.verifyJobOwnership(workerID, jobID) {
		log.Printf("[GRPC] LeaseRenewal from worker %s for job %s refused — ownership mismatch", workerID, jobID)
		return
	}
	leaseExpiry := time.Now().UTC().Add(30 * time.Minute)
	if lr.GetLeaseExpiresAt() != nil {
		leaseExpiry = lr.GetLeaseExpiresAt().AsTime()
	}
	if err := h.jobsRepo.RenewLease(context.Background(), lr.GetJobId(), workerID, lr.GetLeaseId(), leaseExpiry, true, 0); err != nil {
		log.Printf("[GRPC] Lease renewal failed for job %s worker %s: %v", lr.GetJobId(), workerID, err)
	}
}

// verifyJobOwnership checks that `jobID` currently belongs to `workerID`.
// Returns true when the job's WorkerID equals `workerID`. The function
// does NOT check the lease_id or the lease expiry here — callers that
// carry an explicit lease_id should pass it (see verifyJobOwnershipFull).
//
// Phase 3.3: this is the lightweight gate behind every mutating message
// (JobResult, LeaseRenewal, JobRejected, ArtifactUploaded, JobProgress).
// Without it, an authenticated worker A could complete or steal a job
// leased to worker B by sending JobResult{ job_id=<B's job> } — which
// the protobuf contract alone does not protect against.
//
// Ondata 3 PR3: migrated from dbStore.GetJob (map-based) to
// jobsRepo.Get (canonical jobs.Job domain model).
func (h *Handler) verifyJobOwnership(workerID, jobID string) bool {
	if workerID == "" || jobID == "" {
		return false
	}
	j, err := h.jobsRepo.Get(context.Background(), jobID)
	if err != nil || j == nil {
		return false
	}
	return j.WorkerID == workerID
}

// lookupJobCASFields fetches the (revision, attempt) tuple required for the
// StartJob CAS. Uses the canonical jobs.Job via Jobs().Get() — revision is
// on the domain model (Ondata 3 PR3 final), attempt maps to Attempts/RetryCount.
// No more map-based reads from dbStore.GetJob.
func (h *Handler) lookupJobCASFields(jobID string) (revision, attempt int, err error) {
	j, err := h.jobsRepo.Get(context.Background(), jobID)
	if err != nil {
		return 0, 0, err
	}
	if j == nil {
		return 0, 0, fmt.Errorf("job %s not found", jobID)
	}
	return j.Revision, j.Attempts, nil
}
