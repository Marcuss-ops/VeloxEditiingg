package grpcserver

import (
	"context"
	"strings"

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
	h.recordPrefetchTelemetry(workerID, event)
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
	if event.GetAssetKey() != "" {
		extra["asset_key"] = event.GetAssetKey()
	}
	if event.GetOrigin() != "" {
		extra["origin"] = event.GetOrigin()
	}
	if event.GetErrorReason() != "" {
		extra["error_reason"] = event.GetErrorReason()
	}
	if event.GetCacheHit() {
		extra["cache_hit"] = true
	}
	if event.GetDownloadStartedAt() != nil {
		extra["download_started_at"] = event.GetDownloadStartedAt().AsTime().UTC()
	}
	if event.GetAssetReadyAt() != nil {
		extra["asset_ready_at"] = event.GetAssetReadyAt().AsTime().UTC()
	}
	if event.GetJobStartedAt() != nil {
		extra["job_started_at"] = event.GetJobStartedAt().AsTime().UTC()
	}
	if event.GetPrefetchReadyLeadMs() != 0 {
		extra["prefetch_ready_lead_ms"] = event.GetPrefetchReadyLeadMs()
	}
	if err := h.dbStore.LogJobEvent(jobID, "prefetch."+event.GetEventType(), extra); err != nil {
		logGRPCf(context.Background(), logging.LevelWarn, logging.CodeGRPCPrefetchFailed, "[GRPC] failed to persist prefetch event %s for job=%s: %v", event.GetEventType(), jobID, err)
	}
}

func (h *Handler) recordPrefetchTelemetry(workerID string, event *pb.PrefetchLifecycleEvent) {
	if h == nil || h.prefetchTelemetry == nil || event == nil {
		return
	}
	switch event.GetEventType() {
	case "future_plan_received":
		h.prefetchTelemetry.RecordPrefetchJob(workerID, "received")
	case "future_plan_applied":
		h.prefetchTelemetry.RecordPrefetchJob(workerID, "applied")
	case "prejob_prepare_failed", "prefetch_failed", "prefetch_error":
		h.prefetchTelemetry.RecordPrefetchFailure(workerID, classifyPrefetchFailure(event.GetErrorReason()))
	}
	if event.GetEventType() != "prefetch_prepared" || event.GetAssetId() == "" && event.GetAssetKey() == "" {
		return
	}
	result := "miss"
	if event.GetCacheHit() {
		result = "hit"
	}
	h.prefetchTelemetry.RecordPrefetchAsset(workerID, event.GetOrigin(), result, event.GetAssetSizeBytes())
	if start, ready := event.GetDownloadStartedAt(), event.GetAssetReadyAt(); start != nil && ready != nil {
		duration := ready.AsTime().Sub(start.AsTime())
		if duration > 0 {
			h.prefetchTelemetry.RecordPrefetchDuration(workerID, duration)
		}
	}
}

func classifyPrefetchFailure(reason string) string {
	s := strings.ToLower(strings.TrimSpace(reason))
	switch {
	case strings.Contains(s, "lease") || strings.Contains(s, "reservation"):
		return "lease"
	case strings.Contains(s, "cache") || strings.Contains(s, "hash"):
		return "cache"
	case strings.Contains(s, "download") || strings.Contains(s, "drive") || strings.Contains(s, "http"):
		return "download"
	case strings.Contains(s, "plan") || strings.Contains(s, "manifest"):
		return "plan"
	case strings.Contains(s, "protocol") || strings.Contains(s, "worker"):
		return "protocol"
	default:
		return "unknown"
	}
}
