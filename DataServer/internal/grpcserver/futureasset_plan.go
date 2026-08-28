package grpcserver

// futureasset_plan.go owns the plan-building call and the TTL helper
// used by the reservation expiry.

import (
	"fmt"
	"time"

	"velox-shared/futureasset"
)

// buildFuturePlan delegates to the planner and attaches limits.  Returns
// the plan and true on success, or zero and false on error.
func (h *Handler) buildFuturePlan(
	workerID string,
	currentJobID string,
	jobs []futureasset.Job,
) (futureasset.Plan, bool) {
	plan, err := h.futureAssetPlanner.Build(workerID, currentJobID, fmt.Sprintf("future:%s", workerID), jobs, h.futureAssetPlanTTL())
	if err != nil {
		return futureasset.Plan{}, false
	}
	plan.Limits = futureasset.Limits{
		PrefetchHorizon:    h.config.FutureAssetPrefetchHorizon,
		ProtectionLookahead: h.config.FutureAssetProtectionLookahead,
	}
	return plan, true
}

func (h *Handler) futureAssetPlanTTL() time.Duration {
	if h != nil && h.config != nil && h.config.FutureAssetPlanTTL > 0 {
		return h.config.FutureAssetPlanTTL
	}
	return 2 * time.Minute
}

func formatTimingTimestamp(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.Format(time.RFC3339Nano)
}

func durationBetween(start, end time.Time) int64 {
	if start.IsZero() || end.IsZero() || end.Before(start) {
		return 0
	}
	return end.Sub(start).Milliseconds()
}
