// prometheus_job.go — job timing, retry, and resume metric methods.
package telemetry

func (m *PrometheusMetrics) RecordJobQueueWait(jobType string, durationMs float64) {
	m.jobQueueWaitMs.observe(jobType, durationMs)
}
func (m *PrometheusMetrics) RecordJobDispatch(jobType string, durationMs float64) {
	m.jobDispatchMs.observe(jobType, durationMs)
}
func (m *PrometheusMetrics) RecordJobRuntime(jobType string, durationMs float64) {
	m.jobRuntimeMs.observe(jobType, durationMs)
}
func (m *PrometheusMetrics) RecordJobCompleteAck(jobType string, durationMs float64) {
	m.jobCompleteAckMs.observe(jobType, durationMs)
}
func (m *PrometheusMetrics) RecordIdempotencyConflict(reason string) {
	m.jobIdempotencyConflicts.inc(reason)
}
func (m *PrometheusMetrics) RecordJobRetry(jobType string, count float64) {
	m.jobRetryCount.observe(jobType, count)
}
func (m *PrometheusMetrics) RecordJobResume(success bool) {
	m.jobResumeTotal.inc("total")
	if success {
		m.jobResumeSuccess.inc("total")
	}
}

// Query methods
func (m *PrometheusMetrics) GetJobQueueWaitP50() float64   { return m.jobQueueWaitMs.percentile(0.5) }
func (m *PrometheusMetrics) GetJobQueueWaitP95() float64   { return m.jobQueueWaitMs.percentile(0.95) }
func (m *PrometheusMetrics) GetJobDispatchP50() float64    { return m.jobDispatchMs.percentile(0.5) }
func (m *PrometheusMetrics) GetJobDispatchP95() float64    { return m.jobDispatchMs.percentile(0.95) }
func (m *PrometheusMetrics) GetJobRuntimeP50() float64     { return m.jobRuntimeMs.percentile(0.5) }
func (m *PrometheusMetrics) GetJobRuntimeP95() float64     { return m.jobRuntimeMs.percentile(0.95) }
func (m *PrometheusMetrics) GetJobCompleteAckP50() float64 { return m.jobCompleteAckMs.percentile(0.5) }
func (m *PrometheusMetrics) GetJobCompleteAckP95() float64 {
	return m.jobCompleteAckMs.percentile(0.95)
}
func (m *PrometheusMetrics) GetJobRetryAvg() float64 { return m.jobRetryCount.average() }
func (m *PrometheusMetrics) GetJobResumeSuccessRate() float64 {
	total := m.jobResumeTotal.get("total")
	if total == 0 {
		return 0
	}
	return (m.jobResumeSuccess.get("total") / total) * 100
}
