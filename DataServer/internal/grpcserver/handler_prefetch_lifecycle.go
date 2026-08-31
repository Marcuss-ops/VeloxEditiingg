package grpcserver

import (
	"context"

	"velox-server/internal/logging"
	pb "velox-shared/controltransport/pb"
)

// handlePrefetchLifecycleEvent validates the worker identity and persists
// operator-facing prefetch lifecycle events into the job_events journal.
// Asset-scoped prefetch_prepared messages are correctness evidence for the
// strict preparation gate and stay off the synchronous SQLite hot path; the
// worker follows them with one aggregate prefetch_prepared marker that is
// journaled normally.
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
		// Per-asset messages carry the gate evidence. The aggregate marker sent
		// after all assets deliberately has no asset identity and therefore does
		// not mutate the evidence map.
		if event.GetAssetId() != "" || event.GetAssetSha256() != "" {
			h.markPreparedAsset(workerID, &preparedAssetEvidence{
				TaskID:       event.GetTaskId(),
				TaskRevision: int(event.GetTaskRevision()),
				AssetID:      event.GetAssetId(),
				SHA256:       event.GetAssetSha256(),
				SizeBytes:    event.GetAssetSizeBytes(),
			}, event.GetReservationId())

			// Correctness evidence must become visible to placement immediately.
			// Do not serialize it behind one SQLite job_events write per asset;
			// the following aggregate marker preserves the operator timeline with
			// a single durable row per prepared job.
			return
		}
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
