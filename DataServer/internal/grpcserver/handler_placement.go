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
	"log"
	"time"

	"velox-server/internal/placement"
	"velox-server/internal/renderplan"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-shared/contract"
	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
	if h.dbStore == nil {
		log.Printf("[PLACEMENT] authoritative lease store unavailable worker=%s", workerID)
		return
	}
	if snapshot.MaxParallelJobs <= 0 {
		log.Printf("[PLACEMENT] worker has no declared max slots worker=%s", workerID)
		return
	}
	capacity, err := h.dbStore.GetWorkerCapacity(ctx, workerID, time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		log.Printf("[PLACEMENT] authoritative lease capacity query failed worker=%s: %v", workerID, err)
		return
	}
	// Lease store is the sole occupancy source. The session only owns
	// the declared max slot limit; heartbeat active_jobs is telemetry.
	snapshot.ActiveJobs = capacity.ActiveSlots

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
		log.Printf("[PLACEMENT] Failed to project payload for worker %s task %s: %v", sess.workerID, tws.ID, projectionErr)
		if releaseErr := h.taskRepo.ReleaseLease(ctx, tws.ID, sess.workerID, leaseID); releaseErr != nil {
			log.Printf("[PLACEMENT] Failed to release claim for task %s after payload projection failure: %v", tws.ID, releaseErr)
		}
		return
	}

	// Fase D: compile the canonical render plan, stamp plan_version/
	// plan_sha256 on the attempt, and DELIVER the compiled document in the
	// offer so the worker's batch FFmpeg path consumes the master-compiled
	// segments instead of re-deriving a timeline from raw scenes. The keys
	// are additive: scene.composite tolerates unknown payload keys, and the
	// render-plan executor family admits them (batch path). Best-effort by
	// design: a compile or persist failure must never block the offer.
	if planJSON, planSHA := h.compileAndStampAttemptRenderPlan(ctx, tws, attempt); planJSON != "" {
		workerPayload[contract.PayloadKeyCompiledRenderPlanJSON] = planJSON
		workerPayload[contract.PayloadKeyCompiledRenderPlanSHA] = planSHA
	}

	var taskSpecPB *structpb.Struct
	if workerPayload != nil {
		var err error
		taskSpecPB, err = structpb.NewStruct(workerPayload)
		if err != nil {
			log.Printf("[PLACEMENT] Failed to encode TaskOffer payload for worker %s task %s: %v", sess.workerID, tws.ID, err)
			if releaseErr := h.taskRepo.ReleaseLease(ctx, tws.ID, sess.workerID, leaseID); releaseErr != nil {
				log.Printf("[PLACEMENT] Failed to release claim for task %s after TaskOffer payload encoding failure: %v", tws.ID, releaseErr)
			}
			return
		}
	}

	leaseDeadline := time.Now().UTC().Add(30 * time.Minute)
	jobRevision := int32(0)
	if h.jobsRepo != nil {
		job, err := h.jobsRepo.Get(ctx, tws.JobID)
		if err != nil {
			log.Printf("[PLACEMENT] Failed to load job revision for task %s job %s: %v", tws.ID, tws.JobID, err)
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

// compileAndStampAttemptRenderPlan compiles the canonical render plan for
// the claimed task payload (Fase D), persists plan_version/plan_sha256/
// render_plan_json on the freshly-minted attempt, and returns the canonical
// document + its SHA256 so the caller can DELIVER the compiled plan in the
// TaskOffer payload (contract.PayloadKeyCompiledRenderPlanJSON / *_SHA). It
// is deliberately best-effort and NIL-safe: a missing compiler, an
// uncompileable payload, or a persist failure returns ("", "") — the worker
// offer is never blocked by plan compilation.
func (h *Handler) compileAndStampAttemptRenderPlan(ctx context.Context, tws *taskgraph.TaskWithSpec, attempt *taskattempts.TaskAttempt) (string, string) {
	if h == nil || h.renderPlanCompiler == nil || h.taskAttemptRepo == nil || tws == nil || attempt == nil {
		return "", ""
	}
	plan, err := h.renderPlanCompiler.Compile(ctx, tws.SpecPayload, attempt.ID)
	if err != nil {
		log.Printf("[RENDERPLAN] compile skipped task=%s attempt=%s: %v", tws.ID, attempt.ID, err)
		return "", ""
	}
	if plan == nil {
		log.Printf("[RENDERPLAN] validation skipped task=%s attempt=%s: compiler returned nil plan", tws.ID, attempt.ID)
		return "", ""
	}
	if err := plan.Validate(); err != nil {
		log.Printf("[RENDERPLAN] validation skipped task=%s attempt=%s: %v", tws.ID, attempt.ID, err)
		return "", ""
	}
	if plan.AttemptID != attempt.ID {
		log.Printf("[RENDERPLAN] identity skipped task=%s attempt=%s: plan attempt_id=%s", tws.ID, attempt.ID, plan.AttemptID)
		return "", ""
	}
	if tws.JobID != "" && plan.JobID != tws.JobID {
		log.Printf("[RENDERPLAN] identity skipped task=%s attempt=%s: plan job_id=%s task job_id=%s", tws.ID, attempt.ID, plan.JobID, tws.JobID)
		return "", ""
	}
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		log.Printf("[RENDERPLAN] canonical encode skipped task=%s attempt=%s: %v", tws.ID, attempt.ID, err)
		return "", ""
	}
	// Hash from the already-canonical bytes (no second marshaling).
	planSHA := renderplan.HashCanonical(canonical)
	if err := h.taskAttemptRepo.UpsertRenderPlan(ctx, attempt.ID, plan.PlanVersion, planSHA, string(canonical)); err != nil {
		log.Printf("[RENDERPLAN] persist skipped task=%s attempt=%s: %v", tws.ID, attempt.ID, err)
		return "", ""
	}
	log.Printf("[RENDERPLAN] stamped attempt=%s task=%s plan_version=%d plan_sha256=%s duration_ms=%d segments=%d",
		attempt.ID, tws.ID, plan.PlanVersion, planSHA[:16], plan.DurationMS, len(plan.Segments))
	return string(canonical), planSHA
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
