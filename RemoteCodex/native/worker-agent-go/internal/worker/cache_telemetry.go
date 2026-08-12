package worker

import (
	"context"
	"time"

	"velox-worker-agent/internal/telemetry"
)

// recordCacheProjectionEvent is the only cache-to-observability bridge for
// per-attempt cache facts. Producers append raw facts to the attempt journal;
// PrometheusSink projects those events later. Worker-lifetime operational
// download-manager gauges remain outside this helper by design.
func recordCacheProjectionEvent(ctx context.Context, action string, duration time.Duration, status, reason string, bytes int64) {
	recorder := telemetry.RecorderFromContext(ctx)
	if recorder == nil {
		return
	}
	spec := telemetry.EventSpec{
		Origin:    telemetry.OriginWorker,
		Scope:     telemetry.ScopeTask,
		Component: "worker.cache",
		Action:    action,
	}
	if action == "hash_verify" {
		if duration < 0 {
			duration = 0
		}
		completedAt := time.Now().UTC()
		recorder.Record(spec, completedAt.Add(-duration), completedAt, duration.Milliseconds(), status, "", "")
		return
	}

	handle := recorder.Begin(spec)
	if handle == nil {
		return
	}
	if reason != "" {
		handle.SetMetadata("reason", reason)
	}
	handle.CompleteWith(bytes, 0, 0, status, "", "")
}
