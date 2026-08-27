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
