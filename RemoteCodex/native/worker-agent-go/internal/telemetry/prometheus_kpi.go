// prometheus_kpi.go — KPI report projection.
package telemetry

import "time"

type KPIReport struct {
	JobQueueWaitP50         float64 `json:"job_queue_wait_ms_p50"`
	JobQueueWaitP95         float64 `json:"job_queue_wait_ms_p95"`
	JobDispatchP50          float64 `json:"job_dispatch_ms_p50"`
	JobDispatchP95          float64 `json:"job_dispatch_ms_p95"`
	JobRuntimeP50           float64 `json:"job_runtime_ms_p50"`
	JobRuntimeP95           float64 `json:"job_runtime_ms_p95"`
	JobCompleteAckP50       float64 `json:"job_complete_ack_ms_p50"`
	JobCompleteAckP95       float64 `json:"job_complete_ack_ms_p95"`
	JobIdempotencyConflicts int64   `json:"job_idempotency_conflicts_total"`
	JobRetryAvg             float64 `json:"job_retry_count_avg"`
	JobResumeSuccessRate    float64 `json:"job_resume_success_rate"`
	AssetCacheHitRate       float64 `json:"asset_cache_hit_rate"`
	FallbackCount           float64 `json:"fallback_count_total"`
	Timestamp               string  `json:"timestamp"`
}

func (m *PrometheusMetrics) GetKPIReport() *KPIReport {
	return &KPIReport{
		JobQueueWaitP50:         m.GetJobQueueWaitP50(),
		JobQueueWaitP95:         m.GetJobQueueWaitP95(),
		JobDispatchP50:          m.GetJobDispatchP50(),
		JobDispatchP95:          m.GetJobDispatchP95(),
		JobRuntimeP50:           m.GetJobRuntimeP50(),
		JobRuntimeP95:           m.GetJobRuntimeP95(),
		JobCompleteAckP50:       m.GetJobCompleteAckP50(),
		JobCompleteAckP95:       m.GetJobCompleteAckP95(),
		JobIdempotencyConflicts: int64(m.jobIdempotencyConflicts.total()),
		JobRetryAvg:             m.GetJobRetryAvg(),
		JobResumeSuccessRate:    m.GetJobResumeSuccessRate(),
		AssetCacheHitRate:       m.GetAssetCacheHitRate(),
		FallbackCount:           m.GetFallbackCount(),
		Timestamp:               time.Now().UTC().Format(time.RFC3339),
	}
}
