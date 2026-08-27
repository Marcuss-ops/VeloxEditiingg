package grpcserver

import (
	"fmt"
	"sort"
)

// PrefetchSkipReason classifies why a READY candidate was not reserved
// during refreshFutureAssetPlan. The enum is emitted in the structured
// prefetch.plan_decision log so operators can diagnose hard_reservations=0
// in under 5 seconds.
type PrefetchSkipReason string

const (
	// SkipReservedByOther — another worker already holds a reservation
	// for this task. The candidate will not be double-reserved.
	SkipReservedByOther PrefetchSkipReason = "reserved_by_other"

	// SkipPayloadUnavailable — FutureTaskPayload() returned an error,
	// meaning the task's payload is not yet available in the task_graph.
	SkipPayloadUnavailable PrefetchSkipReason = "payload_unavailable"

	// SkipDifferentWarmWorker — selectWarmPlacement() chose a different
	// worker (or returned an error), meaning this worker is not the
	// preferred warm-cache host for the candidate's assets.
	SkipDifferentWarmWorker PrefetchSkipReason = "different_warm_worker"

	// SkipPlacementRejected — the placement matcher rejected the
	// candidate (e.g. capacity, drain, or class mismatch).
	SkipPlacementRejected PrefetchSkipReason = "placement_rejected"

	// SkipReservationConflict — TryReserveFutureTask() failed to
	// acquire the reservation (CAS conflict or transient error).
	SkipReservationConflict PrefetchSkipReason = "reservation_conflict"
)

// prefetchSkipCounter accumulates skip-reason counts during a single
// refreshFutureAssetPlan pass and formats them as a compact JSON object
// for structured logging.
type prefetchSkipCounter struct {
	reasons map[PrefetchSkipReason]int
}

func newPrefetchSkipCounter() *prefetchSkipCounter {
	return &prefetchSkipCounter{reasons: make(map[PrefetchSkipReason]int)}
}

func (c *prefetchSkipCounter) add(reason PrefetchSkipReason) {
	c.reasons[reason]++
}

func (c *prefetchSkipCounter) total() int {
	n := 0
	for _, v := range c.reasons {
		n += v
	}
	return n
}

// summary returns a deterministic JSON string like:
// {"different_warm_worker":2,"manifest_incomplete":1}
// Keys are sorted for log-parsing stability.
func (c *prefetchSkipCounter) summary() string {
	if len(c.reasons) == 0 {
		return "{}"
	}
	// Collect and sort keys for deterministic output.
	keys := make([]string, 0, len(c.reasons))
	for k := range c.reasons {
		keys = append(keys, string(k))
	}
	sort.Strings(keys)

	var buf []byte
	buf = append(buf, '{')
	for i, k := range keys {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, '"')
		buf = append(buf, k...)
		buf = append(buf, '"', ':')
		// Format int without strconv import.
		buf = append(buf, []byte(fmt.Sprintf("%d", c.reasons[PrefetchSkipReason(k)]))...)
	}
	buf = append(buf, '}')
	return string(buf)
}
