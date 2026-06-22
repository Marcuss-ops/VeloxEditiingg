package grpcserver

import (
	"context"
	"fmt"
	"log"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"github.com/google/uuid"
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
		}
	}

	if err := h.registry.Heartbeat(context.Background(), workerID, hb.GetWorkerName(), hb.GetStatus(), hb.GetCurrentJob(), extra); err != nil {
		log.Printf("[GRPC] Heartbeat failed for worker %s: %v", workerID, err)
	}
	// RecoveryReport protocol: inspect hb.extra for recovery_report_v1 and
	// queue a ConfigurationUpdate carrying the master's directive via safeSend.
	h.handleRecoveryReport(workerID, sess, hb)

	// Issue 7 fix / Phase 4.2 hardening: if the persisted session is gone
	// or revoked or expired, we MUST tear the active session down.
	if h.dbStore != nil && sessionID != "" {
		if dbSess, err := h.dbStore.ValidateSessionByID(sessionID); err != nil || dbSess == nil || dbSess.Revoked {
			log.Printf("[GRPC] Session %s for worker %s is invalid — tearing down (revoked=%v, err=%v)",
				sessionID, workerID, dbSess != nil && dbSess.Revoked, err)
			if activeSess := h.getSession(workerID); activeSess != nil && activeSess.sessionID == sessionID {
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
func (h *Handler) handleCommandAck(workerID string, ca *pb.CommandAck) {
	if ca.GetCommandId() != "" {
		if err := h.cmdMgr.AckCommandByID(workerID, ca.GetCommandId()); err != nil {
			log.Printf("[GRPC] Command ACK failed for %s (worker %s): %v", ca.GetCommandId(), workerID, err)
		}
	}
}

// notifyTasksAvailable checks for READY tasks and sends TaskOffers (push mode, PR #4).
func (h *Handler) notifyTasksAvailable(ctx context.Context, workerID string, trigger <-chan struct{}, done <-chan struct{}) {
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
			h.sendPushTaskOffer(ctx, workerID)
		}
	}
}

// sendPushTaskOffer claims the next READY task for a worker, creates a
// TaskAttempt, and sends a typed TaskOffer via gRPC push (PR #4).
// Replaces the legacy sendPushJobOffer with task-native dispatch.
func (h *Handler) sendPushTaskOffer(ctx context.Context, workerID string) {
	sess := h.getSession(workerID)
	if sess == nil {
		return
	}

	// Serialize the check+claim+send+set flow to prevent TOCTOU races.
	sess.claimMu.Lock()
	defer sess.claimMu.Unlock()

	// If a previous offer is still pending, skip.
	if sess.pendingTaskOffer != nil {
		return
	}

	// Respect max_parallel_jobs: don't offer if worker is at capacity.
	if sess.maxParallelJobs.Load() > 0 {
		capacity := sess.maxParallelJobs.Load() - sess.activeJobsCount.Load() - 1
		if capacity < 0 {
			return
		}
	}

	// Generate a unique lease ID for this offer.
	leaseID := fmt.Sprintf("l-%s-%s", workerID, uuid.NewString()[:8])

	// CAS: READY → LEASED with workerID + leaseID + PENDING TaskAttempt
	// INSERT + (attempt_id, attempt_number) stamp on tasks row. All in
	// ONE tx via ClaimNextWithAttemptAtomic (PR-2 / canonical-attempt-identity).
	tws, attempt, err := h.taskRepo.ClaimNextWithAttemptAtomic(ctx, workerID, leaseID)
	if err != nil {
		log.Printf("[GRPC] ClaimNextWithAttemptAtomic failed for worker %s: %v", workerID, err)
		return
	}
	if tws == nil || tws.ID == "" || attempt == nil {
		// No READY task or no canonical attempt was minted — nothing to
		// do this tick. Caller invokes per-worker tick again later.
		return
	}

	// Build TaskSpec payload as structpb.Struct.
	var taskSpecPB *structpb.Struct
	if tws.SpecPayload != nil {
		taskSpecPB, _ = structpb.NewStruct(tws.SpecPayload)
	}

	// Calculate lease deadline (30 min from now).
	leaseDeadline := time.Now().UTC().Add(30 * time.Minute)

	// Build TaskOffer envelope with the canonical attempt_id (NOT the
	// lease_id echo that the pre-PR-2 implementation used) so handleTaskAccepted
	// can pass the canonical (attempt_id, attempt_number) tuple through to
	// AcceptTaskAtomic and task_attempts stays aligned 1:1 with tasks.attempt_id.
	env := &pb.MasterToWorkerEnvelope{
		MessageId:       fmt.Sprintf("taskoffer-%s-%s", workerID, tws.ID),
		WorkerId:        workerID,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Msg: &pb.MasterToWorkerEnvelope_TaskOffer{
			TaskOffer: &pb.TaskOffer{
				TaskId:          tws.ID,
				JobId:           tws.JobID,
				AttemptId:       attempt.ID,
				ExecutorId:      tws.ExecutorID,
				ExecutorVersion: int32(tws.ExecutorVersion),
				TaskSpec:        taskSpecPB,
				LeaseId:         leaseID,
				LeaseDeadline:   timestamppb.New(leaseDeadline),
				AttemptNumber:   int32(attempt.AttemptNumber),
			},
		},
	}

	// Send via sendCh (sessionWriter handles the actual stream.Send).
	if !safeSend(sess.sendCh, &outboundMessage{Envelope: env}) {
		log.Printf("[GRPC] sendCh full/closed for TaskOffer to worker %s — releasing claim for task %s", workerID, tws.ID)
		if releaseErr := h.taskRepo.ReleaseLease(ctx, tws.ID); releaseErr != nil {
			log.Printf("[GRPC] Failed to release claim for task %s after send failure: %v", tws.ID, releaseErr)
		}
		return
	}

	// Store the offer under claimMu so we can match TaskAccepted/TaskRejected.
	sess.pendingTaskOffer = tws
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
		return list
	}
	return nil
}
