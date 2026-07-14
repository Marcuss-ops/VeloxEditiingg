// Package forwarding provides the CreatorForwardingRunner.
package forwarding

import (
	"context"
	"log"
	"sync/atomic"
)

// ── Metrics ──────────────────────────────────────────────────────────────

// RunnerMetrics exposes lightweight counters for the CreatorForwardingRunner.
// Uses atomic integers so the metrics are safe to read from any goroutine
// (e.g. from a /metrics HTTP handler or a supervisor probe).
type RunnerMetrics struct {
	Claimed       atomic.Int64 // total forwardings claimed
	Forwarded     atomic.Int64 // successfully transitioned to READY_TO_FORWARD
	Failed        atomic.Int64 // terminal failures
	Retried       atomic.Int64 // retries scheduled
	QueueDepth    atomic.Int64 // approximate: PENDING + RETRY_WAIT count
	OldestPending atomic.Int64 // approximate: oldest PENDING age in seconds
}

// Snapshot returns a point-in-time copy of the metric values.
func (m *RunnerMetrics) Snapshot() map[string]int64 {
	return map[string]int64{
		"forwarding_claimed":        m.Claimed.Load(),
		"forwarding_forwarded":      m.Forwarded.Load(),
		"forwarding_failed":         m.Failed.Load(),
		"forwarding_retried":        m.Retried.Load(),
		"forwarding_queue_depth":    m.QueueDepth.Load(),
		"forwarding_oldest_pending": m.OldestPending.Load(),
	}
}

// refreshMetrics updates the lightweight queue depth and oldest pending
// age gauges. Called periodically by the Run loop (see refreshInterval).
// Errors are logged but not returned — metrics are best-effort.
//
// Delegates to the store's GetForwardingQueueMetrics so the runner
// never reaches through to r.dbStore.DB() directly — the repository
// owns the SQL, the runner owns the scheduling.
func (r *CreatorForwardingRunner) refreshMetrics(ctx context.Context) {
	if r.dbStore == nil {
		return
	}
	m, err := r.dbStore.GetForwardingQueueMetrics(ctx)
	if err != nil {
		log.Printf("[FORWARDING] metrics refresh: %v", err)
		return
	}
	r.metrics.QueueDepth.Store(m.QueueDepth)
	r.metrics.OldestPending.Store(int64(m.OldestPendingAge.Seconds()))
}
