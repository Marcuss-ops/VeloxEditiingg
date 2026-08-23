package telemetry

func initPrometheusJobFamily(m *PrometheusMetrics) {
	m.jobQueueWaitMs = &HistogramVec{Name: "velox_job_queue_wait_ms", Help: "Time job spends in queue before dispatch (ms)", Buckets: []float64{100, 500, 1000, 5000, 10000, 30000, 60000}, values: make(map[string]*histogramData)}
	m.jobDispatchMs = &HistogramVec{Name: "velox_job_dispatch_ms", Help: "Time to dispatch job to worker (ms)", Buckets: []float64{10, 50, 100, 500, 1000, 5000}, values: make(map[string]*histogramData)}
	m.jobRuntimeMs = &HistogramVec{Name: "velox_job_runtime_ms", Help: "Job execution time (ms)", Buckets: []float64{1000, 5000, 10000, 30000, 60000, 300000, 600000, 1800000}, values: make(map[string]*histogramData)}
	m.jobCompleteAckMs = &HistogramVec{Name: "velox_job_complete_ack_ms", Help: "Time to acknowledge job completion (ms)", Buckets: []float64{10, 50, 100, 500, 1000, 5000}, values: make(map[string]*histogramData)}
	m.jobIdempotencyConflicts = &CounterVec{Name: "velox_job_idempotency_conflicts_total", Help: "Total number of idempotency key conflicts", values: make(map[string]float64)}
	m.jobRetryCount = &HistogramVec{Name: "velox_job_retry_count", Help: "Number of retries per job", Buckets: []float64{0, 1, 2, 3, 5, 10}, values: make(map[string]*histogramData)}
	m.jobResumeSuccess = &CounterVec{Name: "velox_job_resume_success_total", Help: "Total successful job resumes", values: make(map[string]float64)}
	m.jobResumeTotal = &CounterVec{Name: "velox_job_resume_total", Help: "Total job resume attempts", values: make(map[string]float64)}
}

func exportPrometheusJobFamily(m *PrometheusMetrics) string {
	return m.jobQueueWaitMs.export() + m.jobDispatchMs.export() + m.jobRuntimeMs.export() + m.jobCompleteAckMs.export() + m.jobRetryCount.export() + m.jobIdempotencyConflicts.export() + m.jobResumeSuccess.export() + m.jobResumeTotal.export()
}
