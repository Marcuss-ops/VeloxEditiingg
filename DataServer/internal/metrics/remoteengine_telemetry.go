package metrics

// RemoteEngineTelemetry carries the low-cardinality counters for the remote
// engine client (internal/remoteengine), registered on the Prometheus
// Registry so the fragile retry loop at the remote-engine integration point
// is visible in /metrics. Label values are closed enums: `method` is one of
// start_pipeline|get_pipeline_status|cancel_pipeline and `class` is a
// RemoteErrorClass (RATE_LIMIT|TRANSIENT|MALFORMED_RESPONSE|...) or UNKNOWN —
// never a job/trace id, URL, or free-form error message.
type RemoteEngineTelemetry struct {
	requests *Family
	retries  *Family
	failures *Family
}

// NewRemoteEngineTelemetry registers the remote-engine families on reg. A nil
// reg yields a no-op sink (families left nil) so callers can always invoke
// the observer methods without a nil check.
func NewRemoteEngineTelemetry(reg *Registry) *RemoteEngineTelemetry {
	t := &RemoteEngineTelemetry{}
	if reg == nil {
		return t
	}
	t.requests = NewCounterFamily("velox_remote_engine_requests_total", "Remote engine HTTP attempts by method", []string{"method"})
	t.retries = NewCounterFamily("velox_remote_engine_retries_total", "Remote engine retries scheduled by error class", []string{"class"})
	t.failures = NewCounterFamily("velox_remote_engine_failures_total", "Remote engine terminal failures by error class", []string{"class"})

	for _, f := range []*Family{t.requests, t.retries, t.failures} {
		reg.Register(f)
	}
	return t
}

// RecordRequest increments the per-method HTTP attempt counter.
func (t *RemoteEngineTelemetry) RecordRequest(method string) {
	if t == nil || t.requests == nil || method == "" {
		return
	}
	t.requests.Inc([]string{method}, 1)
}

// RecordRetry increments the per-class retry-scheduled counter.
func (t *RemoteEngineTelemetry) RecordRetry(class string) {
	if t == nil || t.retries == nil || class == "" {
		return
	}
	t.retries.Inc([]string{class}, 1)
}

// RecordFailure increments the per-class terminal-failure counter.
func (t *RemoteEngineTelemetry) RecordFailure(class string) {
	if t == nil || t.failures == nil || class == "" {
		return
	}
	t.failures.Inc([]string{class}, 1)
}
