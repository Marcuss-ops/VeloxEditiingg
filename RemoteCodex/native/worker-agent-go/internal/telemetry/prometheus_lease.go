// prometheus_lease.go — cache lease lifecycle metric methods.
package telemetry

func normalizeLeaseAcquireResult(result string) string {
	if result == "success" {
		return result
	}
	return "failure"
}

func normalizeLeaseReleaseResult(result string) string {
	switch result {
	case "success", "failure", "not_found":
		return result
	default:
		return "failure"
	}
}

func normalizeLeaseRetrySource(source string) string {
	switch source {
	case "release_all", "reconciler":
		return source
	default:
		return "other"
	}
}

func normalizeLeaseCleanupStage(stage string) string {
	switch stage {
	case "release", "enqueue", "reconcile_list", "reconcile_release", "reconcile_retry_persist", "reconcile_delete", "reservation_release":
		return stage
	default:
		return "other"
	}
}

// RecordLeaseAcquire records one cache lease acquisition result. The result
// label is deliberately bounded and never contains an asset or job identity.
func (m *PrometheusMetrics) RecordLeaseAcquire(result string) {
	m.leaseAcquires.inc(normalizeLeaseAcquireResult(result))
}

// RecordLeaseRelease records one cache lease release attempt. not_found is
// separate from failure because an already-evicted asset is a successful
// cleanup outcome for the durable reconciler.
func (m *PrometheusMetrics) RecordLeaseRelease(result string) {
	m.leaseReleases.inc(normalizeLeaseReleaseResult(result))
}

// RecordLeaseRenew records one periodic lease renewal attempt.
func (m *PrometheusMetrics) RecordLeaseRenew(result string) {
	m.leaseRenewals.inc(normalizeLeaseReleaseResult(result))
}

// RecordLeaseRetry records a retry attempt from the in-memory ReleaseAll path
// or the durable reconciler.
func (m *PrometheusMetrics) RecordLeaseRetry(source string) {
	m.leaseRetries.inc(normalizeLeaseRetrySource(source))
}

// RecordLeaseCleanupFailure records a failure that could leave a lease
// protected beyond the current cleanup pass. Stage values are fixed to keep
// Prometheus cardinality bounded.
func (m *PrometheusMetrics) RecordLeaseCleanupFailure(stage string) {
	m.leaseCleanupFailures.inc(normalizeLeaseCleanupStage(stage))
}
