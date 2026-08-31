package worker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-shared/futureasset"
	"velox-worker-agent/internal/prefetch"
)

// sendPrefetchLifecycleEvent sends a lightweight lifecycle event to the
// Master so it can persist it into the job_events journal. The Master
// then shows the full prefetch timeline in fleetctl job inspect.
//
// Fire-and-forget: errors are logged but never block the receive loop.
func (w *Worker) sendPrefetchLifecycleEvent(ctx context.Context, eventType, jobID, taskID string, plan futureasset.Plan, extra ...func(*pb.PrefetchLifecycleEvent)) {
	w.transportMu.RLock()
	t := w.transport
	w.transportMu.RUnlock()
	if t == nil {
		return
	}
	event := &pb.PrefetchLifecycleEvent{
		EventType:   eventType,
		JobId:       jobID,
		TaskId:      taskID,
		WorkerId:    w.config.WorkerID,
		PlanId:      plan.PlanID,
		PlanVersion: int64(plan.Version),
		OccurredAt:  timestamppb.New(time.Now().UTC()),
	}
	for _, fn := range extra {
		fn(event)
	}
	msg := controltransport.NewTypedMessage(
		controltransport.MsgPrefetchLifecycleEvent,
		w.config.WorkerID,
		w.config.ProtocolVersion,
		event,
	)
	if err := t.Send(ctx, msg); err != nil {
		w.logger.Warn("[PREFETCH] failed to send lifecycle event %s for job=%s: %v", eventType, jobID, err)
	}
}

// prefetchPreparedHook returns an OnPrepared callback that sends
// prefetch_prepared events to the Master. It captures the worker
// reference via closure; call it after the worker is fully initialized.
func (w *Worker) prefetchPreparedHook() func(prefetch.PreparedJob) {
	return func(job prefetch.PreparedJob) {
		prejob, err := w.prepareLocalPreJob(context.Background(), job)
		if err != nil {
			w.prefetchScheduler.InvalidatePreparedJob(job.JobID)
			w.logger.Error("[PREFETCH] pre-job finalization failed job=%s task=%s: %v", job.JobID, job.TaskID, err)
			w.sendPrefetchLifecycleEvent(context.Background(), "prejob_prepare_failed", job.JobID, job.TaskID, futureasset.Plan{PlanID: job.PlanID, Version: job.PlanVersion}, func(e *pb.PrefetchLifecycleEvent) {
				e.TaskRevision = int32(job.TaskRevision)
				e.ReservationId = job.ReservationID
				e.Distance = int32(job.Distance)
				e.ErrorReason = prejobErrorReason(err)
			})
			return
		}
		w.logger.Info("[PREFETCH] pre-job finalized job=%s task=%s evidence=%s output=%s publisher=%s warmed_bytes=%d disk_free_bytes=%d finalize_ms=%d output_ready_ms=%d warm_ms=%d evidence_ms=%d", job.JobID, job.TaskID, prejob.EvidencePath, prejob.OutputDir, prejob.PublisherDir, prejob.WarmAdvisedBytes, prejob.DiskFreeBytes, prejob.FinalizeMS, prejob.OutputReadyMS, prejob.WarmMS, prejob.EvidenceMS)
		w.rememberPreparedEvidence(job)
		w.logger.Info("[PREFETCH] state=PREPARED job=%s task=%s reservation=%s plan=%s@v%d distance=%d assets=%d prepared_at=%s", job.JobID, job.TaskID, job.ReservationID, job.PlanID, job.PlanVersion, job.Distance, len(job.Assets), job.PreparedAt.UTC().Format(time.RFC3339Nano))

		// Asset-scoped PREPARED messages are correctness evidence for the
		// Master's strict preparation gate. They intentionally stay lightweight:
		// the Master consumes them in-memory and does not journal every asset.
		for _, asset := range job.Assets {
			asset := asset
			w.sendPrefetchLifecycleEvent(context.Background(), "prefetch_prepared", job.JobID, job.TaskID, futureasset.Plan{PlanID: job.PlanID, Version: job.PlanVersion}, func(e *pb.PrefetchLifecycleEvent) {
				e.TaskRevision = int32(job.TaskRevision)
				e.AssetId = asset.AssetID
				e.AssetSha256 = asset.SHA256
				e.AssetSizeBytes = asset.SizeBytes
				e.LocalPath = asset.LocalPath
				e.ReservationId = job.ReservationID
				e.Distance = int32(job.Distance)
				e.OccurredAt = timestamppb.New(asset.PreparedAt)
			})
		}

		// Emit one aggregate PREPARED marker after every per-asset evidence
		// message. It deliberately carries no asset identity, allowing the
		// Master to persist one operator-facing journal row without putting N
		// synchronous SQLite writes on the placement critical path.
		w.sendPrefetchLifecycleEvent(context.Background(), "prefetch_prepared", job.JobID, job.TaskID, futureasset.Plan{PlanID: job.PlanID, Version: job.PlanVersion}, func(e *pb.PrefetchLifecycleEvent) {
			e.TaskRevision = int32(job.TaskRevision)
			e.ReservationId = job.ReservationID
			e.Distance = int32(job.Distance)
		})
	}
}

func (w *Worker) rememberPreparedEvidence(job prefetch.PreparedJob) {
	if w == nil || job.JobID == "" {
		return
	}
	w.preparedEvidenceMu.Lock()
	defer w.preparedEvidenceMu.Unlock()
	for i := range w.preparedEvidence {
		if w.preparedEvidence[i].JobID == job.JobID && w.preparedEvidence[i].TaskID == job.TaskID {
			w.preparedEvidence[i] = job
			return
		}
	}
	w.preparedEvidence = append(w.preparedEvidence, job)
}

func (w *Worker) preparedEvidenceSnapshot() []prefetch.PreparedJob {
	if w == nil {
		return nil
	}
	w.preparedEvidenceMu.Lock()
	defer w.preparedEvidenceMu.Unlock()
	out := make([]prefetch.PreparedJob, len(w.preparedEvidence))
	copy(out, w.preparedEvidence)
	return out
}

func prejobErrorReason(err error) string {
	if err == nil {
		return ""
	}
	reason := strings.TrimSpace(fmt.Sprint(err))
	if len(reason) > 512 {
		return reason[:512]
	}
	return reason
}
