package metrics

// ForwardingTelemetry carries the low-cardinality counters/gauges for the
// CreatorForwardingRunner, registered on the Prometheus Registry so the
// forwarding path is visible in /metrics (GAP 1 of the 2026-08-13
// observability audit — the runner's atomic counters were previously
// orphaned, consumed by no production sink).
type ForwardingTelemetry struct {
	claimed       *Family
	forwarded     *Family
	failed        *Family
	retried       *Family
	queueDepth    *Family
	oldestPending *Family
}

// NewForwardingTelemetry registers the forwarding families on reg. A nil reg
// yields a no-op sink (families left nil) so callers can always invoke the
// observer methods without a nil check.
func NewForwardingTelemetry(reg *Registry) *ForwardingTelemetry {
	t := &ForwardingTelemetry{}
	if reg == nil {
		return t
	}
	t.claimed = NewCounterFamily("velox_forwarding_claimed_total", "Creator forwardings claimed by the runner", []string{})
	t.forwarded = NewCounterFamily("velox_forwarding_forwarded_total", "Creator forwardings transitioned to READY_TO_FORWARD", []string{})
	t.failed = NewCounterFamily("velox_forwarding_failed_total", "Creator forwardings that reached a terminal failure", []string{})
	t.retried = NewCounterFamily("velox_forwarding_retried_total", "Creator forwardings that scheduled a retry", []string{})
	t.queueDepth = NewGaugeFamily("velox_forwarding_queue_depth", "Approximate PENDING + RETRY_WAIT forwarding count", []string{})
	t.oldestPending = NewGaugeFamily("velox_forwarding_oldest_pending_seconds", "Approximate age of the oldest pending forwarding in seconds", []string{})

	for _, f := range []*Family{t.claimed, t.forwarded, t.failed, t.retried, t.queueDepth, t.oldestPending} {
		reg.Register(f)
	}
	return t
}

// RecordClaimed increments the claimed counter by count (the number of
// forwardings claimed in one tick).
func (t *ForwardingTelemetry) RecordClaimed(count int64) {
	if t == nil || t.claimed == nil || count <= 0 {
		return
	}
	t.claimed.Inc([]string{}, uint64(count))
}

// RecordForwarded increments the successfully-forwarded counter.
func (t *ForwardingTelemetry) RecordForwarded() {
	if t != nil && t.forwarded != nil {
		t.forwarded.Inc([]string{}, 1)
	}
}

// RecordFailed increments the terminal-failure counter.
func (t *ForwardingTelemetry) RecordFailed() {
	if t != nil && t.failed != nil {
		t.failed.Inc([]string{}, 1)
	}
}

// RecordRetried increments the retry-scheduled counter.
func (t *ForwardingTelemetry) RecordRetried() {
	if t != nil && t.retried != nil {
		t.retried.Inc([]string{}, 1)
	}
}

// ObserveQueue projects the approximate queue depth and oldest-pending age
// gauges. Values are refreshed by the runner's periodic refreshMetrics.
func (t *ForwardingTelemetry) ObserveQueue(depth, oldestPendingAgeSeconds int64) {
	if t == nil {
		return
	}
	if t.queueDepth != nil {
		t.queueDepth.GaugeSet([]string{}, depth)
	}
	if t.oldestPending != nil {
		t.oldestPending.GaugeSet([]string{}, oldestPendingAgeSeconds)
	}
}
