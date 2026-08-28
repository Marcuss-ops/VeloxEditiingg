package grpcserver

import (
	"context"

	"velox-server/internal/logging"
	pb "velox-shared/controltransport/pb"
)

// handlePrefetchLifecycleEvent validates the worker identity and persists
// the prefetch lifecycle event into the job_events journal so fleetctl
// job inspect shows the full prefetch timeline.
func (h *Handler) handlePrefetchLifecycleEvent(workerID string, event *pb.PrefetchLifecycleEvent) {
	if event == nil || event.GetEventType() == "" {
		logGRPCf(context.Background(), logging.LevelWarn, logging.CodeGRPCPrefetchFailed, "[GRPC] prefetch lifecycle event from worker %s rejected: missing event_type", workerID)
		return
	}
	if declared := event.GetWorkerId(); declared != "" && declared != workerID {
		logGRPCf(context.Background(), logging.LevelWarn, logging.CodeGRPCPrefetchFailed, "[GRPC] prefetch lifecycle event from worker %s rejected: worker_id=%s mismatch", workerID, declared)
		return
	}
	if event.GetEventType() == "prefetch_prepared" && event.GetReservationId() != "" {
		h.markPreparedAsset(workerID, &preparedAssetEvidence{
			TaskID:       event.GetTaskId(),
			TaskRevision: int(event.GetTaskRevision()),
			AssetID:      event.GetAssetId(),
			SHA256:       event.GetAssetSha256(),
			SizeBytes:    event.GetAssetSizeBytes(),
		}, event.GetReservationId())
	}
	jobID := event.GetJobId()
	if jobID == "" {
		// Lifecycle events without a job ID (e.g. plan-scoped events that
		// were not yet per-job) cannot be attributed to a specific job.
		// Skip journal persistence for those.
		return
	}
	if h.dbStore == nil {
		return
	}
	extra := map[string]interface{}{
		"worker_id":    workerID,
		"task_id":      event.GetTaskId(),
		"plan_id":      event.GetPlanId(),
		"plan_version": event.GetPlanVersion(),
	}
	if event.GetReservationId() != "" {
		extra["reservation_id"] = event.GetReservationId()
	}
	if event.GetDistance() > 0 {
		extra["distance"] = event.GetDistance()
	}
	if event.GetAssetId() != "" {
		extra["asset_id"] = event.GetAssetId()
	}
	if event.GetAssetSha256() != "" {
		extra["asset_sha256"] = event.GetAssetSha256()
	}
	if event.GetAssetSizeBytes() > 0 {
		extra["asset_size_bytes"] = event.GetAssetSizeBytes()
	}
	if event.GetLocalPath() != "" {
		extra["local_path"] = event.GetLocalPath()
	}
	if err := h.dbStore.LogJobEvent(jobID, "prefetch."+event.GetEventType(), extra); err != nil {
		logGRPCf(context.Background(), logging.LevelWarn, logging.CodeGRPCPrefetchFailed, "[GRPC] failed to persist prefetch event %s for job=%s: %v", event.GetEventType(), jobID, err)
	}
}
