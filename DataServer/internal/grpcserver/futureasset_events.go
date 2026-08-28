package grpcserver

// futureasset_events.go persists prefetch lifecycle events into the job
// journal so fleetctl job inspect shows the full prefetch timeline.

import (
	"context"

	"velox-server/internal/logging"
	"velox-shared/futureasset"
)

// persistReservationCreated logs a prefetch.reservation_created event.
func (h *Handler) persistReservationCreated(
	ctx context.Context,
	jobID string,
	workerID string,
	taskID string,
	reservationID string,
	distance int,
) {
	if h.dbStore == nil {
		return
	}
	if err := h.dbStore.LogJobEvent(jobID, "prefetch.reservation_created", map[string]interface{}{
		"worker_id":      workerID,
		"task_id":        taskID,
		"reservation_id": reservationID,
		"distance":       distance,
	}); err != nil {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCPrefetchFailed,
			"[PREFETCH] persist reservation_created failed job=%s event=prefetch.reservation_created err=%v",
			jobID, err)
	}
}

// persistPlanSent logs a prefetch.future_plan_sent event for each job in
// the plan.
func (h *Handler) persistPlanSent(
	ctx context.Context,
	workerID string,
	planID string,
	planVersion uint64,
	jobs []futureasset.Job,
) {
	if h.dbStore == nil {
		return
	}
	for _, job := range jobs {
		if job.JobID == "" {
			continue
		}
		assetKeys := make([]string, 0, len(job.Assets))
		for _, a := range job.Assets {
			assetKeys = append(assetKeys, a.AssetKey)
		}
		if err := h.dbStore.LogJobEvent(job.JobID, "prefetch.future_plan_sent", map[string]interface{}{
			"worker_id":      workerID,
			"task_id":        job.TaskID,
			"reservation_id": job.ReservationID,
			"plan_id":        planID,
			"plan_version":   planVersion,
			"distance":       job.Distance,
			"asset_count":    len(job.Assets),
			"asset_keys":     assetKeys,
		}); err != nil {
			logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCPrefetchFailed,
				"[PREFETCH] persist future_plan_sent failed job=%s event=prefetch.future_plan_sent err=%v",
				job.JobID, err)
		}
	}
}
