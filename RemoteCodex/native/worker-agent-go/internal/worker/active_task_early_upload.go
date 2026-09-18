package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"velox-shared/contract"
	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/spool"
	"velox-worker-agent/pkg/video/pipeline"
)

var earlyUploadPlanWaitTimeout = 3 * time.Second

// earlyUploadState bridges the renderer's first safe byte range to the
// master's pre-render upload plan. It is deliberately task-scoped: an early
// upload is valid only for the same fenced task/attempt that later declares
// the final manifest.
type earlyUploadState struct {
	worker *Worker
	pte    *PendingTaskExecution
	ctx    context.Context
	cancel context.CancelFunc

	mu         sync.Mutex
	progress   pipeline.ArtifactWriteProgress
	plan       *pb.ArtifactEarlyUploadPlan
	intentSent bool
	disabled   bool
	result     *publisher.UploadResult
	err        error
	done       chan struct{}
	planReady  chan struct{}
	progressCh chan struct{}
	spoolID    string
	intentOnce sync.Once
	planOnce   sync.Once
	startOnce  sync.Once
	finishOnce sync.Once
}

func newEarlyUploadState(w *Worker, ctx context.Context, pte *PendingTaskExecution) *earlyUploadState {
	stateCtx, cancel := context.WithCancel(ctx)
	return &earlyUploadState{worker: w, pte: pte, ctx: stateCtx, cancel: cancel, done: make(chan struct{}), planReady: make(chan struct{}), progressCh: make(chan struct{}, 1)}
}

func (s *earlyUploadState) updateProgress(progress pipeline.ArtifactWriteProgress) {
	if s == nil || progress.Artifact != "final_video" {
		return
	}
	s.mu.Lock()
	if s.disabled {
		s.mu.Unlock()
		return
	}
	if progress.Path != "" && !s.progress.FirstProgressAt.IsZero() {
		progress.FirstProgressAt = s.progress.FirstProgressAt
	}
	if progress.Path != "" && s.progress.FirstProgressAt.IsZero() {
		progress.FirstProgressAt = time.Now()
	}
	s.progress = progress
	first := !s.intentSent && progress.Path != ""
	if first {
		s.intentSent = true
	}
	s.mu.Unlock()
	select {
	case s.progressCh <- struct{}{}:
	default:
	}
	if first {
		s.intentOnce.Do(func() { go s.sendIntent() })
	}
	s.tryStart()
}

func (s *earlyUploadState) sendIntent() {
	if s == nil || s.worker == nil || s.pte == nil {
		if s != nil && s.worker != nil {
			s.worker.logger.Warn("[ARTIFACT] early upload intent skipped: worker/task/transport unavailable")
		}
		s.disable(fmt.Errorf("early upload: control transport unavailable"))
		return
	}
	if s.worker.config == nil || s.worker.publisherRegistry == nil {
		s.worker.logger.Warn("[ARTIFACT] early upload intent skipped task=%s attempt=%s: publisher is not configured", s.pte.TaskID, s.pte.AttemptID)
		s.disable(fmt.Errorf("early upload: worker publisher is not configured"))
		return
	}
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()
	revision := s.pte.JobRevision
	if revision <= 0 {
		revision = s.pte.Revision
	}
	msg := controltransport.NewTypedMessage(
		controltransport.MsgArtifactUploadIntent,
		s.worker.config.WorkerID,
		s.worker.config.ProtocolVersion,
		&pb.ArtifactUploadIntent{
			TaskId:         s.pte.TaskID,
			JobId:          s.pte.JobID,
			AttemptId:      s.pte.AttemptID,
			LeaseId:        s.pte.LeaseID,
			WorkerSpoolKey: fmt.Sprintf("%s:output:0", s.pte.TaskID),
			OutputKind:     "final_video",
			LogicalName:    s.pte.JobID + ".mp4",
			MimeType:       "video/mp4",
			AttemptNumber:  int32(s.pte.AttemptNumber),
			Revision:       int32(revision),
		})
	if err := s.worker.transportSend(ctx, msg); err != nil {
		s.worker.logger.Warn("[ARTIFACT] early upload intent failed task=%s attempt=%s: %v", s.pte.TaskID, s.pte.AttemptID, err)
		s.disable(fmt.Errorf("early upload: send intent: %w", err))
		return
	}
	s.worker.logger.Info("[ARTIFACT] early upload intent sent task=%s attempt=%s", s.pte.TaskID, s.pte.AttemptID)
}

func (s *earlyUploadState) receivePlan(plan *pb.ArtifactEarlyUploadPlan) {
	if s == nil || plan == nil || plan.GetTaskId() != s.pte.TaskID || plan.GetAttemptId() != s.pte.AttemptID {
		return
	}
	s.mu.Lock()
	if !s.disabled {
		s.plan = plan
	}
	s.mu.Unlock()
	s.planOnce.Do(func() { close(s.planReady) })
	s.worker.logger.Info("[ARTIFACT] early upload plan received task=%s attempt=%s upload=%s", s.pte.TaskID, s.pte.AttemptID, plan.GetUploadId())
	s.tryStart()
}

func (s *earlyUploadState) tryStart() {
	if s == nil {
		return
	}
	s.mu.Lock()
	ready := !s.disabled && s.plan != nil && s.progress.Path != "" && s.progress.SafeOffsetBytes > 0
	plan := s.plan
	progress := s.progress
	s.mu.Unlock()
	if !ready {
		return
	}
	s.startOnce.Do(func() {
		entry, err := s.ensureOutputSpool(progress)
		if err != nil {
			s.worker.logger.Warn("[ARTIFACT] early upload spool registration failed task=%s attempt=%s: %v", s.pte.TaskID, s.pte.AttemptID, err)
			s.disable(fmt.Errorf("early upload: register output spool: %w", err))
			return
		}
		if entry != nil {
			targetJSON, marshalErr := json.Marshal(publisher.UploadTarget{
				ArtifactID:  plan.GetArtifactId(),
				UploadID:    plan.GetUploadId(),
				TransportID: plan.GetTransportId(),
				UploadURL:   plan.GetUploadUrl(),
				ChunkSize:   plan.GetChunkSize(),
			})
			if marshalErr != nil {
				s.disable(fmt.Errorf("early upload: marshal spool target: %w", marshalErr))
				return
			}
			if err := s.worker.outputSpool.StashEarlyUploadPlan(s.ctx, entry.SpoolID, plan.GetUploadId(), string(targetJSON), plan.GetCommitToken()); err != nil {
				s.disable(fmt.Errorf("early upload: stash spool target: %w", err))
				return
			}
			if err := s.worker.outputSpool.MarkUploading(s.ctx, entry.SpoolID, 0); err != nil {
				s.disable(fmt.Errorf("early upload: mark spool uploading: %w", err))
				return
			}
			s.spoolID = entry.SpoolID
		}
		s.worker.logger.Info("[ARTIFACT] early upload starting task=%s attempt=%s upload=%s safe_offset=%d", s.pte.TaskID, s.pte.AttemptID, plan.GetUploadId(), progress.SafeOffsetBytes)
		go s.run(plan, progress)
	})
}

// ensureOutputSpool reserves the same durable identity later used by the
// normal declaration path. The early plan has no commit_id yet, but the
// target/token are persisted immediately so a worker restart can resume the
// bytes and defer only the fenced completion message.
func (s *earlyUploadState) ensureOutputSpool(progress pipeline.ArtifactWriteProgress) (*spool.SpoolEntry, error) {
	if s == nil || s.worker == nil || s.worker.outputSpool == nil || s.pte == nil {
		return nil, nil
	}
	entry, _, err := s.worker.outputSpool.Ensure(s.ctx, spool.SpoolEntry{
		TaskID:         s.pte.TaskID,
		AttemptID:      s.pte.AttemptID,
		WorkerSpoolKey: fmt.Sprintf("%s:output:0", s.pte.TaskID),
		OutputKind:     "final_video",
		LocalPath:      progress.Path,
		Status:         spool.StatusRendering,
		StorageTier:    s.worker.outputStorageTier(progress.Path),
	})
	return entry, err
}

func (s *earlyUploadState) run(plan *pb.ArtifactEarlyUploadPlan, progress pipeline.ArtifactWriteProgress) {
	file := publisher.NewGrowingFile()
	go s.followProgress(file)
	target := publisher.UploadTarget{
		ArtifactID:  plan.GetArtifactId(),
		UploadID:    plan.GetUploadId(),
		TransportID: plan.GetTransportId(),
		UploadURL:   plan.GetUploadUrl(),
		ChunkSize:   plan.GetChunkSize(),
	}
	transport, err := s.worker.publisherRegistry.Resolve(target.TransportID)
	var result *publisher.UploadResult
	if err == nil {
		progressivePartConcurrency := 4
		if s.worker.config != nil && s.worker.config.ProgressivePartConcurrency > 0 {
			progressivePartConcurrency = s.worker.config.ProgressivePartConcurrency
		}
		result, err = uploadWithGrowingProgress(s.ctx, transport, publisher.UploadRequest{
			LocalPath: progress.Path, Target: target, CommitToken: plan.GetCommitToken(),
		}, progress, file, progressivePartConcurrency, progressiveJournalPath(publisher.UploadRequest{LocalPath: progress.Path, Target: target}), s.worker.outputSpool, s.spoolID)
	}
	if err != nil {
		s.worker.logger.Warn("[ARTIFACT] early upload failed task=%s attempt=%s upload=%s: %v", s.pte.TaskID, s.pte.AttemptID, plan.GetUploadId(), err)
		s.disable(err)
		return
	}
	s.worker.logger.Info("[ARTIFACT] early upload completed task=%s attempt=%s upload=%s bytes=%d overlap_ms=%d parts_before_render=%d", s.pte.TaskID, s.pte.AttemptID, result.UploadID, result.UploadedBytes, result.Breakdown.OverlapMS, result.Breakdown.PartsUploadedBeforeRenderEnd)
	s.complete(result)
}

func (s *earlyUploadState) followProgress(file *publisher.GrowingFile) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.done:
			return
		case <-s.progressCh:
		}
		s.mu.Lock()
		p := s.progress
		s.mu.Unlock()
		if p.SafeOffsetBytes > 0 {
			finalSize := int64(0)
			if p.Finalized {
				finalSize = p.SafeOffsetBytes
			}
			file.Update(p.SafeOffsetBytes, p.Finalized, finalSize)
			if p.Finalized && p.Path != "" {
				file.MarkDurable(p.SafeOffsetBytes)
			}
		}
	}
}

func (s *earlyUploadState) disable(err error) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.disabled = true
	s.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
	}
	s.finishOnce.Do(func() { close(s.done) })
}

func (s *earlyUploadState) complete(result *publisher.UploadResult) {
	s.mu.Lock()
	s.result = result
	s.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
	}
	s.finishOnce.Do(func() { close(s.done) })
}

func (s *earlyUploadState) wait(ctx context.Context) *publisher.UploadResult {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	intentSent := s.intentSent
	s.mu.Unlock()
	if !intentSent {
		return nil
	}
	s.mu.Lock()
	planReceived := s.plan != nil
	s.mu.Unlock()
	if !planReceived {
		timer := time.NewTimer(earlyUploadPlanWaitTimeout)
		defer timer.Stop()
		select {
		case <-s.planReady:
		case <-s.done:
		case <-timer.C:
			s.disable(fmt.Errorf("early upload: master plan not received before fallback timeout"))
		case <-ctx.Done():
			s.disable(ctx.Err())
		}
	} else {
		select {
		case <-s.done:
		case <-ctx.Done():
			s.disable(ctx.Err())
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.result
}

func (w *Worker) registerEarlyUpload(ctx context.Context, pte *PendingTaskExecution) *earlyUploadState {
	s := newEarlyUploadState(w, ctx, pte)
	w.earlyUploads.Store(pte.TaskID, s)
	// A fast append-only render can emit its first safe bytes and finalize
	// before an intent sent from the first write callback has made the round
	// trip through the Master, so the intent is sent eagerly for an eligible
	// plan. Do not negotiate an early session for a progressive MP4 profile:
	// that output is seekable and has no safe prefix until the trailer is
	// complete, so such a session would be an abandoned server-side upload
	// rather than useful overlap.
	if earlyUploadEligible(pte) {
		s.intentOnce.Do(s.sendIntent)
	}
	return s
}

// earlyUploadEligible is the single admission check for pre-render artifact
// publication. The artifact layout is the authoritative signal: the native
// sink exposes a positive safe offset only when it writes append-only (mode
// AppendOnly is selected from the plan's output profile), so a plan that
// declares a fragmented container can be published while it renders.
//
// The executor name used to be a hard requirement on top of that, which
// silently excluded every other renderer able to emit the append-only profile
// — most importantly the packet-copy / mixed_packet mux, which selects the
// same sink mode from the same plan field, so a copy job that asked for the
// streaming profile still could not overlap its upload. Admission now follows
// the plan identity exactly like the engine does, and any executor (including
// versioned IDs such as video.assemble.copy.v1@1) is eligible when its plan
// declares an append-only container.
func earlyUploadEligible(pte *PendingTaskExecution) bool {
	if pte == nil {
		return false
	}
	plan, err := contract.DecodeCompiledRenderPlanV2Payload(pte.Spec.Payload)
	if err != nil || plan == nil {
		return false
	}
	profile, err := contract.KnownCanonicalVideoProfileV1(plan.Output.ProfileID)
	if err != nil {
		return false
	}
	return profile.ContainerLayout == contract.ContainerLayoutFragmented
}

func (w *Worker) unregisterEarlyUpload(taskID string) {
	if w != nil {
		w.earlyUploads.Delete(taskID)
	}
}

func (w *Worker) updateEarlyUploadProgress(taskID string, progress pipeline.ArtifactWriteProgress) {
	if value, ok := w.earlyUploads.Load(taskID); ok {
		value.(*earlyUploadState).updateProgress(progress)
	}
}

func (w *Worker) dispatchEarlyUploadPlan(plan *pb.ArtifactEarlyUploadPlan) bool {
	if plan == nil {
		return false
	}
	value, ok := w.earlyUploads.Load(plan.GetTaskId())
	if !ok {
		return false
	}
	value.(*earlyUploadState).receivePlan(plan)
	return true
}

// earlyUploadID returns the server-created session as soon as its plan is
// available. It intentionally does not wait for upload completion: the ID is
// needed to bind the session during TaskOutputDeclared while the upload keeps
// running in parallel with the rest of publication.
func (w *Worker) earlyUploadID(taskID string) string {
	value, ok := w.earlyUploads.Load(taskID)
	if !ok {
		return ""
	}
	state := value.(*earlyUploadState)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.plan == nil {
		return ""
	}
	return state.plan.GetUploadId()
}

func (w *Worker) disableEarlyUpload(taskID string, err error) {
	if value, ok := w.earlyUploads.Load(taskID); ok {
		value.(*earlyUploadState).disable(err)
	}
}

func (w *Worker) waitEarlyUpload(ctx context.Context, taskID string) *publisher.UploadResult {
	value, ok := w.earlyUploads.Load(taskID)
	if !ok {
		w.logger.Info("[ARTIFACT] early upload unavailable at publish task=%s", taskID)
		return nil
	}
	state := value.(*earlyUploadState)
	// The early session is already bound to the declaration at this point.
	// Wait for its terminal result, but bound the pre-plan gap so a lost Master
	// message falls back to the normal upload path instead of consuming the
	// task deadline. Once a plan exists, the task/protocol context bounds the
	// upload wait and the state closes on failure or completion.
	result := state.wait(ctx)
	if result == nil {
		state.mu.Lock()
		err := state.err
		intentSent := state.intentSent
		planReceived := state.plan != nil
		state.mu.Unlock()
		w.logger.Warn("[ARTIFACT] early upload not reusable task=%s intent_sent=%t plan_received=%t err=%v", taskID, intentSent, planReceived, err)
	} else {
		w.logger.Info("[ARTIFACT] early upload reusable at publish task=%s upload=%s bytes=%d", taskID, result.UploadID, result.UploadedBytes)
	}
	return result
}
