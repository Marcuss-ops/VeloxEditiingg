package grpcserver

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"velox-server/internal/queue"
	"velox-server/internal/store"
	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// handleHeartbeat processes a typed Heartbeat received via gRPC stream.
// Issue 7 fix: accepts sessionID and updates last_seen in worker_sessions table.
func (h *Handler) handleHeartbeat(workerID, sessionID string, hb *pb.Heartbeat) {
	extra := make(map[string]interface{})
	// Populate extra from typed fields for backward compat with registry
	extra["worker_name"] = hb.GetWorkerName()
	extra["worker_status"] = hb.GetWorkerStatus()
	extra["status"] = hb.GetStatus()
	extra["current_job"] = hb.GetCurrentJob()
	extra["code_version"] = hb.GetCodeVersion()
	extra["bundle_version"] = hb.GetBundleVersion()
	extra["bundle_hash"] = hb.GetBundleHash()
	extra["protocol_version"] = hb.GetProtocolVersion()
	extra["engine_version"] = hb.GetEngineVersion()
	extra["jobs_completed"] = hb.GetJobsCompleted()
	extra["jobs_failed"] = hb.GetJobsFailed()
	extra["active_jobs_count"] = hb.GetActiveJobsCount()

	if hb.GetExtra() != nil {
		for k, v := range hb.GetExtra().AsMap() {
			extra[k] = v
		}
	}

	// Update capacity tracking on the session (for max_parallel_jobs check).
	// Phase 4.1 fix: use atomic.Int32 so writes from this heartbeat goroutine
	// and reads from the notifier goroutine under claimMu are race-clean.
	sess := h.getSession(workerID)
	if sess != nil {
		sess.activeJobsCount.Store(int32(hb.GetActiveJobsCount()))
		if hb.GetExtra() != nil {
			extraMap := hb.GetExtra().AsMap()
			if mpj, ok := extraMap["max_parallel_jobs"]; ok {
				switch v := mpj.(type) {
				case float64:
					sess.maxParallelJobs.Store(int32(v))
				case int64:
					sess.maxParallelJobs.Store(int32(v))
				}
			}
			// Extract supported_job_types from capabilities for ClaimNext filtering.
			if caps, ok := extraMap["capabilities"].(map[string]interface{}); ok {
				if types := extractSupportedJobTypes(caps); len(types) > 0 {
					sess.supportedJobTypes.Store(types)
				}
			}
		}
	}

	if err := h.registry.Heartbeat(context.Background(), workerID, hb.GetWorkerName(), hb.GetStatus(), hb.GetCurrentJob(), extra); err != nil {
		log.Printf("[GRPC] Heartbeat failed for worker %s: %v", workerID, err)
	}

	// Issue 7 fix / Phase 4.2 hardening: if the persisted session is gone
	// or revoked or expired, we MUST tear the active session down — not just
	// log. The worker should reconnect with a fresh sessionID.
	if h.dbStore != nil && sessionID != "" {
		if dbSess, err := h.dbStore.ValidateSessionByID(sessionID); err != nil || dbSess == nil || dbSess.Revoked {
			log.Printf("[GRPC] Session %s for worker %s is invalid — tearing down (revoked=%v, err=%v)",
				sessionID, workerID, dbSess != nil && dbSess.Revoked, err)
			if activeSess := h.getSession(workerID); activeSess != nil && activeSess.sessionID == sessionID {
				// Also publish to writerErr so the main loop exits predictably.
				select {
				case activeSess.writerErr <- fmt.Errorf("session revoked or expired"):
				default:
				}
				activeSess.cancel()
			}
			return
		}
		_ = h.dbStore.UpdateSessionLastSeen(sessionID)
	}
}

// handleCommandAck processes typed CommandAck via gRPC stream.
// Only accepts ACK by command_id — the legacy type-based fallback is removed.
// Worker-scoped: the command_id must belong to the authenticated worker.
func (h *Handler) handleCommandAck(workerID string, ca *pb.CommandAck) {
	if ca.GetCommandId() != "" {
		if err := h.cmdMgr.AckCommandByID(workerID, ca.GetCommandId()); err != nil {
			log.Printf("[GRPC] Command ACK failed for %s (worker %s): %v", ca.GetCommandId(), workerID, err)
		}
	}
}

// notifyJobsAvailable checks for pending jobs and sends full JobOffers (push mode).
func (h *Handler) notifyJobsAvailable(ctx context.Context, workerID string, trigger <-chan struct{}, done <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-trigger:
		case <-ticker.C:
		}

		if h.config.PushMode {
			h.sendPushJobOffer(ctx, workerID)
		}
	}
}

// sendPushJobOffer does a real SQLite CAS claim and sends a typed JobOffer (Push mode).
// Issue 4 fix: claimMu serializes the entire check+claim+send+set flow to prevent
// TOCTOU races between concurrent notifyJobsAvailable calls.
func (h *Handler) sendPushJobOffer(ctx context.Context, workerID string) {
	sess := h.getSession(workerID)
	if sess == nil {
		return
	}

	// Issue 4 fix: atomically check pendingOffer, claim, send, and store.
	// claimMu prevents concurrent offers from racing ClaimNextJob.
	sess.claimMu.Lock()
	defer sess.claimMu.Unlock()

	// If a previous offer is still pending (not yet accepted/rejected), skip.
	if sess.pendingOffer != nil {
		return
	}

	// Respect max_parallel_jobs: don't offer if worker is at capacity.
	// pendingOffer is nil here (checked above), so the pending count is 1 (this offer).
	// Phase 4.1: atomic loads are lock-free reads.
	if sess.maxParallelJobs.Load() > 0 {
		capacity := sess.maxParallelJobs.Load() - sess.activeJobsCount.Load() - 1
		if capacity < 0 {
			return
		}
	}

	// Collect supported job types from worker capabilities for filtering.
	allowedJobTypes := h.collectAllowedJobTypes(sess)

	// Use repo directly since ClaimNextJob was removed from LifecycleService
	claimResult, err := h.lifecycleSvc.Repo().ClaimNext(ctx, store.ClaimParams{
		WorkerID:        workerID,
		AllowedJobTypes: allowedJobTypes,
		Now:             time.Now().UTC(),
	})
	if err != nil {
		log.Printf("[GRPC] ClaimNext failed for worker %s: %v", workerID, err)
		return
	}
	if claimResult == nil || claimResult.JobID == "" {
		return
	}
	sj, err := h.lifecycleSvc.Repo().GetJob(ctx, claimResult.JobID)
	if err != nil {
		log.Printf("[GRPC] GetJob after claim failed for worker %s: %v", workerID, err)
		// Release the claim so the job is not left orphaned.
		if releaseErr := h.lifecycleSvc.Repo().ReleaseClaim(ctx, claimResult.JobID); releaseErr != nil {
			log.Printf("[GRPC] Failed to release claim after GetJob error for %s: %v", claimResult.JobID, releaseErr)
		}
		return
	}
	if sj == nil {
		// Release the claim — job no longer exists.
		if releaseErr := h.lifecycleSvc.Repo().ReleaseClaim(ctx, claimResult.JobID); releaseErr != nil {
			log.Printf("[GRPC] Failed to release claim after nil GetJob for %s: %v", claimResult.JobID, releaseErr)
		}
		return
	}

	// Parse payload from the request_json column.
	var payload map[string]interface{}
	if sj.PayloadJSON != "" && sj.PayloadJSON != "{}" {
		_ = json.Unmarshal([]byte(sj.PayloadJSON), &payload)
	}

	job := &queue.Job{
		JobID:       sj.JobID,
		Status:      queue.JobStatus(sj.Status),
		VideoName:   sj.VideoName,
		ProjectID:   sj.ProjectID,
		AssignedTo:  sj.AssignedTo,
		LeaseID:     sj.LeaseID,
		RetryCount:  sj.RetryCount,
		MaxRetries:  sj.MaxRetries,
		CreatedAt:   sj.CreatedAt,
		UpdatedAt:   sj.UpdatedAt,
		StartedAt:   sj.StartedAt,
		CompletedAt: sj.CompletedAt,
		Attempt:     claimResult.Attempt,
		RunID:       sj.RunID,
		Payload:     payload,
		LeaseExpiry: claimResult.LeaseExpires,
	}
	// Build job payload struct from job.Payload map
	var jobPayload *structpb.Struct
	if job.Payload != nil {
		jobPayload, _ = structpb.NewStruct(job.Payload)
	}

	// Parse LeaseExpiry for the proto message.
	var leaseExpiryPB *timestamppb.Timestamp
	if !claimResult.LeaseExpires.IsZero() {
		leaseExpiryPB = timestamppb.New(claimResult.LeaseExpires)
	}

	// Parse CreatedAt timestamp if present
	var createdAt *timestamppb.Timestamp
	if job.CreatedAt != nil {
		switch t := job.CreatedAt.(type) {
		case time.Time:
			createdAt = timestamppb.New(t)
		case string:
			if parsed, err := time.Parse(time.RFC3339, t); err == nil {
				createdAt = timestamppb.New(parsed)
			}
		}
	}

	env := &pb.MasterToWorkerEnvelope{
		MessageId:       fmt.Sprintf("joboffer-%s-%s", workerID, job.JobID),
		WorkerId:        workerID,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Msg: &pb.MasterToWorkerEnvelope_JobOffer{
			JobOffer: &pb.JobOffer{
				JobId:        job.JobID,
				RunId:        job.RunID,
				VideoName:    job.VideoName,
				CreatedAt:    createdAt,
				LeaseId:      job.LeaseID,
				LeaseExpiry:  leaseExpiryPB,
				Attempt:      int32(job.Attempt),
				MaxRetries:   int32(job.MaxRetries),
				JobPayload:   jobPayload,
			},
		},
	}

	// Issue 5 fix: send via sendCh instead of direct stream.Send().
	if !safeSend(sess.sendCh, &outboundMessage{Envelope: env}) {
		log.Printf("[GRPC] sendCh full/closed for JobOffer to worker %s — releasing claim", workerID)
		if releaseErr := h.lifecycleSvc.Repo().ReleaseClaim(ctx, job.JobID); releaseErr != nil {
			log.Printf("[GRPC] Failed to release claim for job %s after send failure: %v", job.JobID, releaseErr)
		}
		return
	}

	// Issue 4 fix: store the offer under claimMu so we can match JobAccepted.
	sess.pendingOffer = job
}

// collectAllowedJobTypes extracts supported_job_types from the worker's
// capabilities stored in the session. Capabilities are received via the
// Hello message during connection and refreshed via heartbeat Extra.
// Returns nil (no filter) if no capabilities have been received yet — the
// ClaimNext query handles nil as "no filter" which ensures backward compat
// with workers that predate the supported_job_types capability.
func (h *Handler) collectAllowedJobTypes(sess *workerSession) []string {
	if sess == nil {
		return nil
	}
	v := sess.supportedJobTypes.Load()
	if v == nil {
		return nil
	}
	types, ok := v.([]string)
	if !ok || len(types) == 0 {
		return nil
	}
	return types
}

// extractSupportedJobTypes parses a supported_job_types value from a
// capabilities map extracted from protobuf Struct. structpb normalises
// Go slices to []interface{}, so both Worker→Master paths (Hello
// capabilities and heartbeat Extra) share this helper.
func extractSupportedJobTypes(capsMap map[string]interface{}) []string {
	sjt, ok := capsMap["supported_job_types"]
	if !ok {
		return nil
	}
	switch list := sjt.(type) {
	case []interface{}:
		types := make([]string, 0, len(list))
		for _, item := range list {
			if s, ok := item.(string); ok {
				types = append(types, s)
			}
		}
		return types
	case []string:
		// Safety net: proto-generated code may preserve []string in edge cases.
		return list
	}
	return nil
}
