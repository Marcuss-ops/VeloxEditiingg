package worker

import (
	"velox-worker-agent/internal/prefetch"
	"velox-worker-agent/internal/telemetry"
)

// recordPrefetchEvent keeps the timing waterfall in the worker's existing
// Prometheus surface. Identifiers remain available only to structured event
// consumers; metric labels are limited to distance and static totals.
func recordPrefetchEvent(event prefetch.Event) {
	metrics := telemetry.GetPrometheusMetrics()
	switch event.Name {
	case "prefetch_queued":
		metrics.SetPrefetchOperational(event.Active, event.QueueDepth)
	case "download_started":
		if !event.QueuedAt.IsZero() {
			metrics.RecordPrefetchQueueWait(event.Distance, event.StartedAt.Sub(event.QueuedAt))
		}
		metrics.SetPrefetchOperational(event.Active, event.QueueDepth)
	case "asset_ready":
		if !event.StartedAt.IsZero() && !event.ReadyAt.IsZero() {
			metrics.RecordPrefetchResolve(event.Distance, event.ReadyAt.Sub(event.StartedAt))
		}
		metrics.SetPrefetchOperational(event.Active, event.QueueDepth)
	case "prefetch_ready_lead":
		if !event.ReadyAt.IsZero() && !event.StartedAt.IsZero() {
			metrics.RecordPrefetchReadyLead(event.Distance, event.StartedAt.Sub(event.ReadyAt))
		}
	case "future_plan_received", "future_plan_applied":
		// The control-plane timestamps are intentionally carried by the event
		// stream first; their structured logs are the source for plan-stage
		// breakdowns until a master-side metric sink is wired.
	}
}
