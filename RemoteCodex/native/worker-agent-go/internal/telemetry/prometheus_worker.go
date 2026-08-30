// prometheus_worker.go — worker, task, render, and fallback metric methods.
package telemetry

import "time"

func (m *PrometheusMetrics) WorkerErrorCount() float64 {
	return m.workerErrorsTotal.get("total")
}

// CacheEvictionCount returns the current low-cardinality eviction count.
func (m *PrometheusMetrics) RecordWorkerError() {
	m.workerErrorsTotal.inc("total")
}

// SetAssetDownloadOperational updates the low-cardinality aggregate asset
// download gauges and coalescing counter used by the worker dashboard.
func (m *PrometheusMetrics) SetWorkerActiveJobs(_ string, count float64) {
	m.workerActiveJobs.set("total", count)
}
func (m *PrometheusMetrics) SetWorkerStatus(_ string, status float64) {
	m.workerStatus.set("total", status)
}
func (m *PrometheusMetrics) RecordRender(duration time.Duration) {
	m.renderSeconds.observe("total", duration.Seconds())
}
func (m *PrometheusMetrics) RecordArtifactUpload(duration time.Duration) {
	m.artifactUploadSeconds.observe("total", duration.Seconds())
}
func (m *PrometheusMetrics) RecordArtifactLockWait(duration time.Duration) {
	m.artifactLockWaitSeconds.observe("total", duration.Seconds())
}
func (m *PrometheusMetrics) RecordRenderUploadOverlap(duration time.Duration) {
	if duration > 0 {
		m.renderUploadOverlapSeconds.observe("total", duration.Seconds())
	}
}

// RecordProgressiveUploadTiming records the progressive-upload telemetry:
// firstPartStarted (time to first part), parts/bytes uploaded while the
// render was still running, and the render/upload overlap window. All values
// are zero on the legacy non-progressive path and are skipped.
func (m *PrometheusMetrics) RecordProgressiveUploadTiming(firstPartStarted time.Duration, partsBeforeRenderEnd, bytesBeforeRenderEnd int64, overlap time.Duration) {
	if firstPartStarted > 0 {
		m.progressiveFirstPartStartedSeconds.observe("total", firstPartStarted.Seconds())
	}
	if partsBeforeRenderEnd > 0 {
		m.progressivePartsBeforeRenderEnd.observe("total", float64(partsBeforeRenderEnd))
	}
	if bytesBeforeRenderEnd > 0 {
		m.progressiveBytesBeforeRenderEnd.observe("total", float64(bytesBeforeRenderEnd))
	}
	m.RecordRenderUploadOverlap(overlap)
}

// RecordMuxToOpenUS records the Go-side latency from the first C++ progress
// event with a path to when the file was opened for progressive upload.
func (m *PrometheusMetrics) RecordMuxToOpenUS(microseconds int64) {
	if microseconds > 0 {
		m.muxToOpenMicroseconds.observe("total", float64(microseconds))
	}
}

func (m *PrometheusMetrics) RecordTaskResultSubmit(duration time.Duration) {
	m.taskResultSubmitSeconds.observe("total", duration.Seconds())
}
func (m *PrometheusMetrics) RecordTaskResultAck(duration time.Duration) {
	m.taskResultAckSeconds.observe("total", duration.Seconds())
}
func (m *PrometheusMetrics) RecordTaskResultAckReceived() {
	m.taskResultAcksTotal.inc("total")
}
func (m *PrometheusMetrics) RecordTelemetryInvalidEvent() {
	m.telemetryInvalidEvents.inc("catalog")
}

func (m *PrometheusMetrics) RecordTelemetryInvalidEvents(count int64) {
	for i := int64(0); i < count; i++ {
		m.RecordTelemetryInvalidEvent()
	}
}
func (m *PrometheusMetrics) RecordFallback(reason string) { m.fallbackCount.inc(reason) }
func (m *PrometheusMetrics) RecordPythonEmergencyPath(reason string) {
	m.pythonEmergencyPath.inc(reason)
}
func (m *PrometheusMetrics) GetFallbackCount() float64 { return m.fallbackCount.total() }
func (m *PrometheusMetrics) GetPythonEmergencyPathCount() float64 {
	return m.pythonEmergencyPath.total()
}

// Pending-offer dedup counters.
func (m *PrometheusMetrics) RecordOfferDuplicate()        { m.offerDuplicateTotal.inc("total") }
func (m *PrometheusMetrics) RecordOfferReplaced()         { m.offerReplacedTotal.inc("total") }
func (m *PrometheusMetrics) RecordOfferStale()            { m.offerStaleTotal.inc("total") }
func (m *PrometheusMetrics) RecordOfferIdentityConflict() { m.offerIdentityConflictTotal.inc("total") }
func (m *PrometheusMetrics) RecordOfferReconciled()       { m.offerReconciledTotal.inc("total") }

// Resource admission controller metrics.
func (m *PrometheusMetrics) RecordAdmissionRejection()           { m.admissionRejectionsTotal.inc("total") }
func (m *PrometheusMetrics) RecordBackpressureEvent(kind string) { m.backpressureEventsTotal.inc(kind) }

// SetAdmissionDiagnostics updates the live RSS pressure and throttle-state
// gauges from the admission controller. Called once per heartbeat cycle so
// Prometheus /graph shows real-time memory pressure without worker API queries.
func (m *PrometheusMetrics) SetAdmissionDiagnostics(rssPressurePct float64, rssBytes int64, throttledRender, throttledPrefetch, throttledPublish bool) {
	m.admissionRSSPressurePct.set("total", rssPressurePct)
	m.admissionRSSBytes.set("total", float64(rssBytes))
	renderVal := 0.0
	if throttledRender {
		renderVal = 1.0
	}
	prefetchVal := 0.0
	if throttledPrefetch {
		prefetchVal = 1.0
	}
	publishVal := 0.0
	if throttledPublish {
		publishVal = 1.0
	}
	m.admissionThrottledRender.set("total", renderVal)
	m.admissionThrottledPrefetch.set("total", prefetchVal)
	m.admissionThrottledPublish.set("total", publishVal)
}

// SetFileDescriptorMetrics updates the FD gauges from the latest resource sample.
func (m *PrometheusMetrics) SetFileDescriptorMetrics(open, max int64, ratio float64) {
	m.workerOpenFds.set("total", float64(open))
	m.workerMaxFds.set("total", float64(max))
	m.workerFdUtilization.set("total", ratio)
}

// RecordPrefetchCorrupted increments the prefetch corruption counter.
// reason should be "size_mismatch" or "hash_mismatch".
func (m *PrometheusMetrics) RecordPrefetchCorrupted(reason string) {
	m.prefetchCorruptedTotal.inc(reason)
}

// SetNetworkSaturation updates the NIC ingress/egress saturation gauges.
// Ratio is 0.0–1.0+ where >1.0 means the observed throughput exceeds budget.
func (m *PrometheusMetrics) SetNetworkSaturation(ingress, egress float64) {
	m.networkSaturationIngress.set("total", ingress)
	m.networkSaturationEgress.set("total", egress)
}

// SetNetworkConsumerBytes updates the per-consumer bytes-transferred gauge.
func (m *PrometheusMetrics) SetNetworkConsumerBytes(consumer string, bytes int64) {
	m.networkConsumerBytes.set(consumer, float64(bytes))
}

// SetNetworkThrottleMS updates the per-consumer cumulative throttle wait.
func (m *PrometheusMetrics) SetNetworkThrottleMS(consumer string, ms int64) {
	m.networkThrottleMS.set(consumer, float64(ms))
}

// SetNetworkSaturationAlertLevel sets the saturation alert gauge to the
// numeric level (0=normal, 1=warn, 2=throttle, 3=critical).
func (m *PrometheusMetrics) SetNetworkSaturationAlertLevel(level int) {
	m.networkSaturationAlert.set("total", float64(level))
}
