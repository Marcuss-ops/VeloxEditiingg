// Package grpcserver / handler_placement.go
//
// Push-mode placement pipeline (PR #4): worker notification loop, the
// check→select→claim→send TaskOffer flow with pre/post-claim fencing,
// and placement rejection recording. Extracted from handler_workers.go
// (split per responsabilità) so heartbeat lifecycle and task placement
// stay in separate files.
package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/logging"
	"velox-server/internal/placement"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-shared/contract"
	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const pendingTaskOfferTimeout = 30 * time.Second

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

	// Reconcile an offer that can no longer receive an accept/reject. A job
	// cancellation, lease reaper, or a lost worker message can change the
	// durable task state without sending a worker response. Leaving the
	// in-memory offer set forever would make this connected worker look idle
	// while blocking every subsequent offer.
	if sess.pendingTaskOffer != nil {
		offer := sess.pendingTaskOffer
		stale := !sess.pendingTaskOfferAt.IsZero() && time.Since(sess.pendingTaskOfferAt) >= pendingTaskOfferTimeout
		if !stale {
			if current, err := h.taskRepo.Get(ctx, offer.ID); err == nil && (current == nil || current.Status != taskgraph.StatusLeased || current.WorkerID != workerID || current.LeaseID != offer.LeaseID) {
				stale = true
			}
		}
		if !stale {
			return
		}
		if err := h.taskRepo.ReleaseLease(ctx, offer.ID, workerID, offer.LeaseID); err != nil {
			// The lease may already have been released by cancellation/reaping.
			// Clear the local gate anyway; the durable state is authoritative and
			// the next tick will select a fresh READY task.
			logGRPCf(ctx, logging.LevelDebug, logging.CodeGRPCPlacementFailed, "[PLACEMENT] stale pending offer cleanup worker=%s task=%s: %v", workerID, offer.ID, err)
		}
		sess.pendingTaskOffer = nil
		sess.pendingTaskOfferAt = time.Time{}
	}

	snapshot := sess.placementSnapshot(workerID)
	if h.dbStore == nil {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCPlacementFailed, "[PLACEMENT] authoritative lease store unavailable worker=%s", workerID)
		return
	}
	if snapshot.MaxParallelJobs <= 0 {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCPlacementFailed, "[PLACEMENT] worker has no declared max slots worker=%s", workerID)
		return
	}
	capacity, err := h.dbStore.GetWorkerPhaseCapacity(ctx, workerID, time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		logGRPCf(ctx, logging.LevelError, logging.CodeGRPCPlacementFailed, "[PLACEMENT] authoritative lease capacity query failed worker=%s: %v", workerID, err)
		return
	}
	// Lease store is the sole occupancy source. The session only owns
	// the declared max slot limit; heartbeat active_jobs is telemetry.
	snapshot.ActiveJobs = capacity.ActiveSlots
	snapshot.ActiveRender = capacity.ActiveRender
	snapshot.ActivePrefetch = capacity.ActivePrefetch
	snapshot.ActivePublisher = capacity.ActivePublisher

	candidates, err := h.taskRepo.ListReadyCandidates(ctx, 64)
	if err != nil {
		logGRPCf(ctx, logging.LevelError, logging.CodeGRPCPlacementFailed, "[PLACEMENT] ListReadyCandidates failed worker=%s: %v", workerID, err)
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
	canClaim, err := h.ensureFutureReservationOwnership(ctx, workerID, candidate)
	if err != nil {
		logGRPCf(ctx, logging.LevelError, logging.CodeGRPCPlacementFailed, "[PLACEMENT] future reservation fallback check failed worker=%s task=%s: %v", workerID, candidate.TaskID, err)
		return
	}
	if !canClaim {
		// Another eligible worker still owns the preparation lease, or this
		// worker lost the fallback race. Leave the task READY and let the
		// owner/next placement tick claim it.
		return
	}
	if gate := h.getPrepGate(); h.config.StrictPrefetchClaim && gate != nil {
		decision, err := gate.EnsurePrepared(ctx, workerID, candidate)
		if err != nil {
			logGRPCf(ctx, logging.LevelError, logging.CodeGRPCPlacementFailed, "[PLACEMENT] preparation gate check failed worker=%s task=%s: %v", workerID, candidate.TaskID, err)
			return
		}
		switch decision {
		case PreparationReady:
			// All assets prepared — proceed to claim.
		case PreparationNotRequired:
			// No assets needed — proceed to claim.
		case PreparationWaiting:
			// Gate blocks. The EnsurePrepared call already sent the plan
			// if needed (first-job path). The next placement tick retries.
			return
		case PreparationExpired:
			// Stale reservation. Leave task READY for re-dispatch.
			return
		default:
			logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCPlacementFailed, "[PLACEMENT] preparation gate UNKNOWN decision=%s worker=%s task=%s", decision, workerID, candidate.TaskID)
			return
		}
	} else if h.config.StrictPrefetchClaim {
		// Fallback: gate not wired (test/legacy path).
		prepared, err := h.ensurePreparedBeforeClaim(ctx, workerID, candidate)
		if err != nil {
			logGRPCf(ctx, logging.LevelError, logging.CodeGRPCPlacementFailed, "[PLACEMENT] preparation gate check failed worker=%s task=%s: %v", workerID, candidate.TaskID, err)
			return
		}
		if !prepared {
			h.refreshFutureAssetPlan(ctx, workerID, "")
			return
		}
	}
	leaseID := fmt.Sprintf("l-%s-%s", workerID, uuid.NewString()[:8])

	tws, attempt, err := h.taskRepo.ClaimTaskForWorkerAtomic(ctx, taskgraph.ClaimTaskForWorkerCommand{
		TaskID:               candidate.TaskID,
		ExpectedTaskRevision: candidate.Revision,
		WorkerID:             workerID,
		SessionID:            snapshot.SessionID,
		WorkerSnapshotID:     sess.workerSnapshotID,
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
		logGRPCf(ctx, logging.LevelError, logging.CodeGRPCPlacementFailed, "[PLACEMENT] ClaimTaskForWorkerAtomic failed worker=%s task=%s: %v", workerID, candidate.TaskID, err)
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
			logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCPlacementFailed, "[PLACEMENT] ReleaseLease after fencing failure worker=%s task=%s: %v", workerID, tws.ID, releaseErr)
		}
		return
	}

	// Persist and publish the next worker-scoped hard reservation window while
	// the current task is still running. The planner is best-effort here: a
	// prefetch outage must never block correctness of the claimed task.
	h.refreshFutureAssetPlan(ctx, workerID, tws.JobID)
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
	workerPayload, projectionErr := projectPayloadForWorker(tws.SpecPayload, tws.ExecutorVersion)
	if projectionErr != nil {
		logGRPCf(ctx, logging.LevelError, logging.CodeGRPCPlacementFailed, "[PLACEMENT] Failed to project payload for worker %s task %s: %v", sess.workerID, tws.ID, projectionErr)
		if releaseErr := h.taskRepo.ReleaseLease(ctx, tws.ID, sess.workerID, leaseID); releaseErr != nil {
			logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCPlacementFailed, "[PLACEMENT] Failed to release claim for task %s after payload projection failure: %v", tws.ID, releaseErr)
		}
		return
	}

	// Fase D: validate/stamp the canonical compiled plan. Only a V2 plan that
	// was already present in the TaskSpec may be delivered through the V2 wire
	// keys. The compatibility compiler still persists its V1 plan on the
	// attempt for evidence, but must not place that V1 document in the V2
	// envelope: the worker's V2 resolver would (correctly) reject fields such
	// as job_id and attempt_id. Legacy tasks continue through their existing
	// payload path until they explicitly opt into CompiledRenderPlanV2.
	if _, _, v2Present := compiledV2Payload(tws.SpecPayload); v2Present {
		if planJSON, planSHA := h.compileAndStampAttemptRenderPlan(ctx, tws, attempt); planJSON != "" {
			workerPayload[contract.PayloadKeyCompiledRenderPlanJSON] = planJSON
			workerPayload[contract.PayloadKeyCompiledRenderPlanSHA] = planSHA
		}
	}
	// render_batch@1 has no safe legacy fallback: a missing or malformed V2
	// envelope must release the claim instead of offering a task that the
	// worker can only reject after lease acquisition.
	if isRenderBatchExecutor(tws.ExecutorID) {
		if err := contract.ValidateCompiledRenderPlanV2Payload(workerPayload); err != nil {
			logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCPlacementFailed, "[PLACEMENT] refusing render_batch task=%s: invalid CompiledRenderPlanV2: %v", tws.ID, err)
			if releaseErr := h.taskRepo.ReleaseLease(ctx, tws.ID, sess.workerID, leaseID); releaseErr != nil {
				logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCPlacementFailed, "[PLACEMENT] failed to release render_batch claim task=%s: %v", tws.ID, releaseErr)
			}
			return
		}
	}

	var taskSpecPB *structpb.Struct
	if workerPayload != nil {
		var err error
		taskSpecPB, err = structpb.NewStruct(workerPayload)
		if err != nil {
			logGRPCf(ctx, logging.LevelError, logging.CodeGRPCPlacementFailed, "[PLACEMENT] Failed to encode TaskOffer payload for worker %s task %s: %v", sess.workerID, tws.ID, err)
			if releaseErr := h.taskRepo.ReleaseLease(ctx, tws.ID, sess.workerID, leaseID); releaseErr != nil {
				logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCPlacementFailed, "[PLACEMENT] Failed to release claim for task %s after TaskOffer payload encoding failure: %v", tws.ID, releaseErr)
			}
			return
		}
	}

	leaseDeadline := time.Now().UTC().Add(30 * time.Minute)
	jobRevision := int32(0)
	if h.jobsRepo != nil {
		job, err := h.jobsRepo.Get(ctx, tws.JobID)
		if err != nil {
			logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCPlacementFailed, "[PLACEMENT] Failed to load job revision for task %s job %s: %v", tws.ID, tws.JobID, err)
		} else if job != nil {
			jobRevision = int32(job.Revision)
		}
	}

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
				JobRevision:     jobRevision,
			},
		},
	}

	if !safeSend(sess.sendCh, &outboundMessage{Envelope: env}) {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCPlacementFailed, "[PLACEMENT] sendCh full/closed for TaskOffer to worker %s — releasing claim for task %s", sess.workerID, tws.ID)
		if releaseErr := h.taskRepo.ReleaseLease(ctx, tws.ID, sess.workerID, leaseID); releaseErr != nil {
			logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCPlacementFailed, "[PLACEMENT] Failed to release claim for task %s after send failure: %v", tws.ID, releaseErr)
		}
		return
	}

	sess.pendingTaskOffer = tws
	sess.pendingTaskOfferAt = time.Now().UTC()
	logGRPCf(ctx, logging.LevelInfo, logging.CodeGRPCPlacement, "[PLACEMENT] TaskOffer queued for worker %s: task=%s job=%s attempt=%s lease=%s executor=%s@%d rev=%d", sess.workerID, tws.ID, tws.JobID, attempt.ID, leaseID, tws.ExecutorID, tws.ExecutorVersion, tws.Revision)
}

// compileAndStampAttemptRenderPlan stamps the already-compiled
// CompiledRenderPlanV2 (produced at enqueue time) onto the freshly-minted
// attempt and returns the canonical document + its SHA256 so the caller can
// DELIVER the plan in the TaskOffer payload. The legacy V1 RenderPlanCompiler
// is retired: only a pre-compiled V2 envelope in the TaskSpec is stamped, and
// a task without one skips the stamp entirely. Best-effort and NIL-safe — a
// missing repo, an invalid envelope, or a persist failure returns ("", ""),
// so the worker offer is never blocked by plan stamping.
func (h *Handler) compileAndStampAttemptRenderPlan(ctx context.Context, tws *taskgraph.TaskWithSpec, attempt *taskattempts.TaskAttempt) (string, string) {
	if h == nil || tws == nil || attempt == nil {
		return "", ""
	}
	if h.taskAttemptRepo == nil {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCRenderPlan, "[RENDERPLAN] skipped task=%s attempt=%s reason=persistence_unavailable", tws.ID, attempt.ID)
		return "", ""
	}

	// V2 is compiled before the task is persisted. Re-validate the exact
	// bytes delivered in the TaskSpec and stamp those bytes, rather than
	// reconstructing a second plan from legacy float-based fields.
	rawJSON, rawSHA, present := compiledV2Payload(tws.SpecPayload)
	if !present {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCRenderPlan, "[RENDERPLAN] skipped task=%s attempt=%s reason=no_compiled_v2", tws.ID, attempt.ID)
		return "", ""
	}
	if err := contract.ValidateCompiledRenderPlanV2Payload(tws.SpecPayload); err != nil {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCRenderPlan, "[RENDERPLAN] skipped task=%s attempt=%s reason=v2_validation_error error=%v", tws.ID, attempt.ID, err)
		return "", ""
	}
	if err := h.taskAttemptRepo.UpsertRenderPlan(ctx, attempt.ID, contract.CompiledPlanVersionV2, rawSHA, rawJSON); err != nil {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCRenderPlan, "[RENDERPLAN] skipped task=%s attempt=%s reason=v2_persist_error error=%v", tws.ID, attempt.ID, err)
		return "", ""
	}
	logGRPCf(ctx, logging.LevelInfo, logging.CodeGRPCRenderPlan, "[RENDERPLAN] stamped V2 attempt=%s task=%s plan_version=%d plan_sha256=%s", attempt.ID, tws.ID, contract.CompiledPlanVersionV2, rawSHA[:16])
	return rawJSON, rawSHA
}

func compiledV2Payload(payload map[string]interface{}) (string, string, bool) {
	if payload == nil {
		return "", "", false
	}
	rawJSON, hasJSON := payload[contract.PayloadKeyCompiledRenderPlanJSON].(string)
	rawSHA, hasSHA := payload[contract.PayloadKeyCompiledRenderPlanSHA].(string)
	if !hasJSON && !hasSHA {
		return "", "", false
	}
	return rawJSON, rawSHA, true
}

func isRenderBatchExecutor(executorID string) bool {
	return executorID == "render_batch" || strings.HasPrefix(executorID, "render_batch@")
}

// recordPlacementRejections logs the rejection reasons produced by the
// placement matcher and increments the per-reason Prometheus counter
// via the PlacementRejectionSink (when wired).
func (h *Handler) recordPlacementRejections(snapshot placement.WorkerSnapshot, rejections []placement.Rejection) {
	for _, r := range rejections {
		logGRPCf(context.Background(), logging.LevelInfo, logging.CodeGRPCPlacement, "[PLACEMENT] Rejection worker=%s task=%s code=%s detail=%s", snapshot.WorkerID, r.TaskID, r.Code, r.Detail)
		if h.placementRejectionSink != nil {
			h.placementRejectionSink.RecordPlacementRejection(string(r.Code))
		}
	}
}
