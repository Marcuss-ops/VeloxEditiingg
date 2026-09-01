package worker

import (
	"context"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-shared/futureasset"
)

// handleFutureAssetPlanMessage validates and applies the worker-scoped
// lookahead snapshot, then reconciles it through the canonical prefetch
// scheduler and emits lifecycle evidence back to the Master.
func (w *Worker) handleFutureAssetPlanMessage(ctx context.Context, msg controltransport.ControlMessage) {
	wirePlan, ok := msg.TypedPayload.(*pb.FutureAssetPlan)
	if !ok || wirePlan == nil {
		w.logger.Warn("[PREFETCH] FutureAssetPlan without typed payload")
		return
	}
	plan, err := futureasset.FromProto(wirePlan)
	if err != nil {
		w.logger.Warn("[PREFETCH] rejected invalid FutureAssetPlan: %v", err)
		return
	}
	w.futureAssetScheduler().RecordPlanEvent("future_plan_received", plan.Version, plan.PlanID)
	for _, job := range plan.PrefetchJobs {
		job := job
		w.sendPrefetchLifecycleEvent(ctx, "future_plan_received", job.JobID, job.TaskID, plan, func(e *pb.PrefetchLifecycleEvent) {
			e.ReservationId = job.ReservationID
			e.Distance = int32(job.Distance)
		})
	}

	result, err := w.futureAssetController().Apply(plan)
	if err != nil {
		w.logger.Warn("[PREFETCH] rejected FutureAssetPlan version=%d: %v", plan.Version, err)
		return
	}
	if result.Applied && !result.Stale {
		if err := w.futureAssetScheduler().Reconcile(plan); err != nil {
			w.logger.Warn("[PREFETCH] execution reconcile failed plan=%s version=%d: %v", plan.PlanID, plan.Version, err)
			return
		}
		w.futureAssetScheduler().RecordPlanEvent("future_plan_applied", plan.Version, plan.PlanID)
		for _, job := range plan.PrefetchJobs {
			job := job
			w.sendPrefetchLifecycleEvent(ctx, "future_plan_applied", job.JobID, job.TaskID, plan, func(e *pb.PrefetchLifecycleEvent) {
				e.ReservationId = job.ReservationID
				e.Distance = int32(job.Distance)
			})
		}
	}
	w.logger.Info("[PREFETCH] reconciled plan=%s version=%d added=%d removed=%d reprioritized=%d protected=%d expired=%t stale=%t", plan.PlanID, plan.Version, len(result.Added), len(result.Removed), len(result.Reprioritized), len(plan.Protect), result.Expired, result.Stale)
}

func (w *Worker) handleCancelPrefetchMessage(msg controltransport.ControlMessage) {
	cancel, ok := msg.TypedPayload.(*pb.CancelPrefetch)
	if !ok || cancel == nil {
		w.logger.Warn("[PREFETCH] CancelPrefetch without typed payload")
		return
	}
	if w.futureAssetController().Cancel(cancel.GetJobId(), cancel.GetReservationId(), cancel.GetPlanVersion()) {
		w.futureAssetScheduler().Cancel(cancel.GetJobId())
		w.logger.Info("[PREFETCH] cancelled job=%s reservation=%s plan_version=%d reason=%s", cancel.GetJobId(), cancel.GetReservationId(), cancel.GetPlanVersion(), cancel.GetReason())
	}
}
