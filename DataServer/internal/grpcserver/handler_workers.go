package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"velox-server/internal/placement"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// handleHeartbeat processes a typed Heartbeat received via gRPC stream.
// Issue 7 fix: accepts sessionID and updates last_seen in worker_sessions table.
//
// Scorecard v1 / F2: also decodes heartbeat.resources into a typed
// ResourceSnapshot and forwards it to the WorkerResourceSink (promoted
// to Prometheus gauge/counter families via metrics.Collector). The
// cumulative→delta conversion lives in handler_workers_metrics.go so
// the handler stays purely structural.
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

	// F2: merge the typed resource counters into `extra` so the
	// persistent worker_registry row surfaces the same Prometheus-side
	// fields via the legacy HTTP /admin/workers path (channelised
	// worker debugging tools depend on this JSON view).
	if resExtra := ResourcesToExtra(hb.GetResources()); resExtra != nil {
		for k, v := range resExtra {
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

	// F2: defensive nil-check on h.registry so handler-level unit tests
	// can wire a Handler without standing up the persistent worker_registry
	// (preserves the existing pre-F2 contract that handleHeartbeat is
	// safe with no registry; production code always supplies one).
	if h.registry != nil {
		if err := h.registry.Heartbeat(context.Background(), workerID, hb.GetWorkerName(), hb.GetStatus(), hb.GetCurrentJob(), extra); err != nil {
			log.Printf("[GRPC] Heartbeat failed for worker %s: %v", workerID, err)
		}
	}

	// F2: forward typed resource counters onto the Prometheus registry
	// via the sink interface. NIL-tolerant — handlers running WITHOUT a
	// metrics surface keep the registry.Heartbeat() side active and
	// silently skip the projection (legacy mode).
	if h.resourceSink != nil {
		if snap := decodeWorkerResources(workerID, hb.GetResources()); snap != nil {
			h.resourceSink.RecordWorker(workerID, snap)
		}
	}

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

// sendPushTaskOffer runs the placement pipeline: snapshot the worker,
// list READY candidates, select the best match via the placement
// matcher, atomically claim that specific task, and send a TaskOffer.
// Fencing is applied before and after the claim so a stale session or
// capability bump tears the offer down cleanly.
func (h *Handler) sendPushTaskOffer(ctx context.Context, workerID string) {
	sess := h.getSession(workerID)
	if sess == nil {
		return
	}

	// Serialize the check+select+claim+send flow to prevent TOCTOU races.
	sess.claimMu.Lock()
	defer sess.claimMu.Unlock()

	// If a previous offer is still pending, skip.
	if sess.pendingTaskOffer != nil {
		return
	}

	snapshot := sess.placementSnapshot(workerID)

	candidates, err := h.taskRepo.ListReadyCandidates(ctx, 64)
	if err != nil {
		log.Printf("[PLACEMENT] ListReadyCandidates failed worker=%s: %v", workerID, err)
		return
	}

	result := h.placementMatcher.Select(snapshot, candidates)

	if result.Candidate == nil {
		h.recordPlacementRejections(snapshot, result.Rejections)
		return
	}

	// ── Fencing pre-claim ────────────────────────────────────────────
	// After building the snapshot and selecting a candidate, verify
	// the session hasn't been replaced by a reconnect. If it has,
	// the chosen candidate belongs to a stale view of the worker.
	current := h.getSession(workerID)
	if current != sess || current.sessionID != snapshot.SessionID {
		return
	}

	candidate := result.Candidate
	leaseID := fmt.Sprintf("l-%s-%s", workerID, uuid.NewString()[:8])

	tws, attempt, err := h.taskRepo.ClaimTaskForWorkerAtomic(ctx, taskgraph.ClaimTaskForWorkerCommand{
		TaskID:               candidate.TaskID,
		ExpectedTaskRevision: candidate.Revision,
		WorkerID:             workerID,
		SessionID:            snapshot.SessionID,
		LeaseID:              leaseID,
		ExecutorID:           candidate.Executor.ID,
		ExecutorVersion:      candidate.Executor.Version,
		CapabilityRevision:   snapshot.CapabilityRevision,
	})

	if err != nil {
		if errors.Is(err, taskgraph.ErrTransitionConflict) {
			// The task was claimed by a concurrent dispatcher between
			// ListReadyCandidates and our Claim — harmless, retry on
			// the next tick.
			return
		}
		log.Printf("[PLACEMENT] ClaimTaskForWorkerAtomic failed worker=%s task=%s: %v", workerID, candidate.TaskID, err)
		return
	}
	if tws == nil || tws.ID == "" || attempt == nil {
		return
	}

	// ── Fencing post-claim ───────────────────────────────────────────
	// After the claim has been committed, verify the session is still
	// the current one AND the capability revision hasn't changed. If
	// the worker reconnected between the claim and this check, release
	// the lease immediately so it can be re-dispatched.
	current = h.getSession(workerID)
	if current != sess ||
		current.sessionID != snapshot.SessionID ||
		current.capabilityRevision.Load() != snapshot.CapabilityRevision {

		if releaseErr := h.taskRepo.ReleaseLease(ctx, tws.ID, workerID, leaseID); releaseErr != nil {
			log.Printf("[PLACEMENT] ReleaseLease after fencing failure worker=%s task=%s: %v", workerID, tws.ID, releaseErr)
		}
		return
	}

	h.sendClaimedTaskOffer(ctx, sess, tws, attempt, leaseID)
}

// sendClaimedTaskOffer builds the protobuf TaskOffer envelope from a
// successfully claimed task+attempt and sends it via the session's
// sendCh. Extracted from sendPushTaskOffer to keep the placement
// pipeline readable. On send failure the claim is released.
func (h *Handler) sendClaimedTaskOffer(
	ctx context.Context,
	sess *workerSession,
	tws *taskgraph.TaskWithSpec,
	attempt *taskattempts.TaskAttempt,
	leaseID string,
) {
	var taskSpecPB *structpb.Struct
	if tws.SpecPayload != nil {
		taskSpecPB, _ = structpb.NewStruct(tws.SpecPayload)
	}

	leaseDeadline := time.Now().UTC().Add(30 * time.Minute)

	env := &pb.MasterToWorkerEnvelope{
		MessageId:       fmt.Sprintf("taskoffer-%s-%s", sess.workerID, tws.ID),
		WorkerId:        sess.workerID,
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
				Revision:        int32(tws.Revision),
			},
		},
	}

	if !safeSend(sess.sendCh, &outboundMessage{Envelope: env}) {
		log.Printf("[PLACEMENT] sendCh full/closed for TaskOffer to worker %s — releasing claim for task %s", sess.workerID, tws.ID)
		if releaseErr := h.taskRepo.ReleaseLease(ctx, tws.ID, sess.workerID, leaseID); releaseErr != nil {
			log.Printf("[PLACEMENT] Failed to release claim for task %s after send failure: %v", tws.ID, releaseErr)
		}
		return
	}

	sess.pendingTaskOffer = tws
	log.Printf("[PLACEMENT] TaskOffer queued for worker %s: task=%s job=%s attempt=%s lease=%s executor=%s@%d rev=%d",
		sess.workerID, tws.ID, tws.JobID, attempt.ID, leaseID, tws.ExecutorID, tws.ExecutorVersion, tws.Revision)
}

// recordPlacementRejections logs the rejection reasons produced by the
// placement matcher and increments the per-reason Prometheus counter
// via the PlacementRejectionSink (when wired).
func (h *Handler) recordPlacementRejections(snapshot placement.WorkerSnapshot, rejections []placement.Rejection) {
	for _, r := range rejections {
		log.Printf("[PLACEMENT] Rejection worker=%s task=%s code=%s detail=%s",
			snapshot.WorkerID, r.TaskID, r.Code, r.Detail)
		if h.placementRejectionSink != nil {
			h.placementRejectionSink.RecordPlacementRejection(string(r.Code))
		}
	}
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
