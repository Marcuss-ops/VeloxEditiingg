package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/pkg/video/pipeline"
)

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
	intentOnce sync.Once
	startOnce  sync.Once
	finishOnce sync.Once
}

func newEarlyUploadState(w *Worker, ctx context.Context, pte *PendingTaskExecution) *earlyUploadState {
	stateCtx, cancel := context.WithCancel(ctx)
	return &earlyUploadState{worker: w, pte: pte, ctx: stateCtx, cancel: cancel, done: make(chan struct{})}
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
	if first {
		s.intentOnce.Do(func() { go s.sendIntent() })
	}
	s.tryStart()
}

func (s *earlyUploadState) sendIntent() {
	if s == nil || s.worker == nil || s.pte == nil || s.worker.transport == nil {
		s.disable(fmt.Errorf("early upload: control transport unavailable"))
		return
	}
	if s.worker.config == nil || s.worker.publisherRegistry == nil {
		s.disable(fmt.Errorf("early upload: worker publisher is not configured"))
		return
	}
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()
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
			Revision:       int32(s.pte.Revision),
		})
	if err := s.worker.transport.Send(ctx, msg); err != nil {
		s.worker.logger.Warn("[ARTIFACT] early upload intent failed task=%s attempt=%s: %v", s.pte.TaskID, s.pte.AttemptID, err)
		s.disable(fmt.Errorf("early upload: send intent: %w", err))
	}
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
		go s.run(plan, progress)
	})
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
		result, err = uploadWithGrowingProgress(s.ctx, transport, publisher.UploadRequest{
			LocalPath: progress.Path, Target: target, CommitToken: plan.GetCommitToken(),
		}, progress, file)
	}
	if err != nil {
		s.disable(err)
		return
	}
	s.complete(result)
}

func (s *earlyUploadState) followProgress(file *publisher.GrowingFile) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.done:
			return
		case <-time.After(25 * time.Millisecond):
		}
		s.mu.Lock()
		p := s.progress
		s.mu.Unlock()
		if p.SafeOffsetBytes > 0 {
			file.Update(p.SafeOffsetBytes, p.Finalized, 0)
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
	select {
	case <-s.done:
	case <-ctx.Done():
		s.disable(ctx.Err())
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.result
}

func (w *Worker) registerEarlyUpload(ctx context.Context, pte *PendingTaskExecution) *earlyUploadState {
	s := newEarlyUploadState(w, ctx, pte)
	w.earlyUploads.Store(pte.TaskID, s)
	// A fast packet-copy render can emit its first safe fMP4 bytes and
	// finalize before an intent sent from the first write callback can make
	// the round trip through the Master. Pre-negotiate only for the explicit
	// fMP4 producer profile; the upload itself still waits for a positive
	// safe offset from the native append-only sink.
	if pte.ExecutorID == "video.assemble.copy.v1" {
		s.intentOnce.Do(func() { go s.sendIntent() })
	}
	return s
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

func (w *Worker) waitEarlyUpload(ctx context.Context, taskID string) *publisher.UploadResult {
	value, ok := w.earlyUploads.Load(taskID)
	if !ok {
		return nil
	}
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return value.(*earlyUploadState).wait(waitCtx)
}
