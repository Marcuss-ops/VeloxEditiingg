package metrics

import "time"

// PrefetchTelemetrySink is the bounded-label telemetry surface consumed by
// the gRPC worker event handler. Asset/job identifiers stay in the SQL event
// journal; Prometheus receives only worker identity and closed vocabularies.
type PrefetchTelemetrySink interface {
	RecordPrefetchJob(workerID, outcome string)
	RecordPrefetchAsset(workerID, origin, result string, bytes int64)
	RecordPrefetchFailure(workerID, reason string)
	RecordPrefetchDuration(workerID string, duration time.Duration)
}

var _ PrefetchTelemetrySink = (*Collector)(nil)

func (c *Collector) initPrefetchFamilies() {
	c.prefetchJobs = NewCounterFamily(
		"velox_prefetch_jobs_total",
		"Prefetch lifecycle milestones by worker and outcome (received, applied, failed)",
		[]string{"worker_id", "outcome"},
	)
	c.prefetchBytes = NewCounterFamily(
		"velox_prefetch_bytes_total",
		"Asset bytes observed by prefetch origin and worker",
		[]string{"worker_id", "origin"},
	)
	c.prefetchFailures = NewCounterFamily(
		"velox_prefetch_failures_total",
		"Prefetch failures by worker and closed failure reason",
		[]string{"worker_id", "reason"},
	)
	c.prefetchDuration = NewHistogramFamily(
		"velox_prefetch_duration_seconds",
		"Asset prefetch duration from download start to ready",
		[]string{"worker_id"},
		[]float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 300},
	)
	c.prefetchCache = NewCounterFamily(
		"velox_prefetch_cache_events_total",
		"Prefetch asset outcomes by worker, origin and result (hit or miss)",
		[]string{"worker_id", "origin", "result"},
	)
}

func (c *Collector) prefetchFamilies() []*Family {
	return []*Family{c.prefetchJobs, c.prefetchBytes, c.prefetchFailures, c.prefetchDuration, c.prefetchCache}
}

func (c *Collector) RecordPrefetchJob(workerID, outcome string) {
	workerID = boundedWorkerID(workerID)
	outcome = boundedPrefetchOutcome(outcome)
	if workerID == "" || outcome == "" {
		return
	}
	c.prefetchJobs.Inc([]string{workerID, outcome}, 1)
}

func (c *Collector) RecordPrefetchAsset(workerID, origin, result string, bytes int64) {
	workerID = boundedWorkerID(workerID)
	origin = boundedPrefetchOrigin(origin)
	result = boundedPrefetchResult(result)
	if workerID == "" || origin == "" || result == "" {
		return
	}
	c.prefetchCache.Inc([]string{workerID, origin, result}, 1)
	if bytes > 0 {
		c.prefetchBytes.Inc([]string{workerID, origin}, uint64(bytes))
	}
}

func (c *Collector) RecordPrefetchFailure(workerID, reason string) {
	workerID = boundedWorkerID(workerID)
	reason = boundedPrefetchFailureReason(reason)
	if workerID == "" || reason == "" {
		return
	}
	c.prefetchFailures.Inc([]string{workerID, reason}, 1)
	c.prefetchJobs.Inc([]string{workerID, "failed"}, 1)
}

func (c *Collector) RecordPrefetchDuration(workerID string, duration time.Duration) {
	workerID = boundedWorkerID(workerID)
	if workerID == "" || duration <= 0 {
		return
	}
	c.prefetchDuration.Observe([]string{workerID}, duration.Seconds())
}

func boundedWorkerID(value string) string {
	return value
}

func boundedPrefetchOutcome(value string) string {
	switch value {
	case "received", "applied", "failed":
		return value
	default:
		return ""
	}
}

func boundedPrefetchOrigin(value string) string {
	switch value {
	case "prefetch", "warm_cache", "runtime_download":
		return value
	default:
		return "unknown"
	}
}

func boundedPrefetchResult(value string) string {
	switch value {
	case "hit", "miss":
		return value
	default:
		return "miss"
	}
}

func boundedPrefetchFailureReason(value string) string {
	switch value {
	case "download", "cache", "lease", "plan", "protocol":
		return value
	default:
		return "unknown"
	}
}
