// Package telemetry provides Prometheus metrics collection for the worker agent.
package telemetry

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

// PrometheusMetrics tracks KPI metrics for Prometheus export.
type PrometheusMetrics struct {
	mu sync.RWMutex

	jobQueueWaitMs          *HistogramVec
	jobDispatchMs           *HistogramVec
	jobRuntimeMs            *HistogramVec
	jobCompleteAckMs        *HistogramVec
	jobIdempotencyConflicts *CounterVec
	jobRetryCount           *HistogramVec
	jobResumeSuccess        *CounterVec
	jobResumeTotal          *CounterVec
	assetCacheHit           *CounterVec
	assetCacheMiss          *CounterVec
	workerActiveJobs        *GaugeVec
	workerStatus            *GaugeVec
	fallbackCount           *CounterVec
	pythonEmergencyPath     *CounterVec
}

// NewPrometheusMetrics creates a new Prometheus metrics collector.
func NewPrometheusMetrics() *PrometheusMetrics {
	return &PrometheusMetrics{
		jobQueueWaitMs: &HistogramVec{
			Name: "velox_job_queue_wait_ms", Help: "Time job spends in queue before dispatch (ms)",
			Buckets: []float64{100, 500, 1000, 5000, 10000, 30000, 60000},
			values:  make(map[string]*histogramData),
		},
		jobDispatchMs: &HistogramVec{
			Name: "velox_job_dispatch_ms", Help: "Time to dispatch job to worker (ms)",
			Buckets: []float64{10, 50, 100, 500, 1000, 5000},
			values:  make(map[string]*histogramData),
		},
		jobRuntimeMs: &HistogramVec{
			Name: "velox_job_runtime_ms", Help: "Job execution time (ms)",
			Buckets: []float64{1000, 5000, 10000, 30000, 60000, 300000, 600000, 1800000},
			values:  make(map[string]*histogramData),
		},
		jobCompleteAckMs: &HistogramVec{
			Name: "velox_job_complete_ack_ms", Help: "Time to acknowledge job completion (ms)",
			Buckets: []float64{10, 50, 100, 500, 1000, 5000},
			values:  make(map[string]*histogramData),
		},
		jobIdempotencyConflicts: &CounterVec{
			Name: "velox_job_idempotency_conflicts_total", Help: "Total number of idempotency key conflicts",
			values: make(map[string]float64),
		},
		jobRetryCount: &HistogramVec{
			Name: "velox_job_retry_count", Help: "Number of retries per job",
			Buckets: []float64{0, 1, 2, 3, 5, 10},
			values:  make(map[string]*histogramData),
		},
		jobResumeSuccess: &CounterVec{
			Name: "velox_job_resume_success_total", Help: "Total successful job resumes",
			values: make(map[string]float64),
		},
		jobResumeTotal: &CounterVec{
			Name: "velox_job_resume_total", Help: "Total job resume attempts",
			values: make(map[string]float64),
		},
		assetCacheHit: &CounterVec{
			Name: "velox_asset_cache_hit_total", Help: "Total asset cache hits",
			values: make(map[string]float64),
		},
		assetCacheMiss: &CounterVec{
			Name: "velox_asset_cache_miss_total", Help: "Total asset cache misses",
			values: make(map[string]float64),
		},
		workerActiveJobs: &GaugeVec{
			Name: "velox_worker_active_jobs", Help: "Number of active jobs per worker",
			values: make(map[string]float64),
		},
		workerStatus: &GaugeVec{
			Name: "velox_worker_status", Help: "Worker status (0=offline, 1=idle, 2=busy, 3=error)",
			values: make(map[string]float64),
		},
		fallbackCount: &CounterVec{
			Name: "velox_fallback_count_total", Help: "Total number of fallback usages (should be 0 in production)",
			values: make(map[string]float64),
		},
		pythonEmergencyPath: &CounterVec{
			Name: "velox_python_emergency_path_total", Help: "Total Python emergency path usages (should be 0 in production)",
			values: make(map[string]float64),
		},
	}
}

// Recording methods
func (m *PrometheusMetrics) RecordJobQueueWait(jobType string, durationMs float64)          { m.jobQueueWaitMs.observe(jobType, durationMs) }
func (m *PrometheusMetrics) RecordJobDispatch(jobType string, durationMs float64)            { m.jobDispatchMs.observe(jobType, durationMs) }
func (m *PrometheusMetrics) RecordJobRuntime(jobType string, durationMs float64)             { m.jobRuntimeMs.observe(jobType, durationMs) }
func (m *PrometheusMetrics) RecordJobCompleteAck(jobType string, durationMs float64)         { m.jobCompleteAckMs.observe(jobType, durationMs) }
func (m *PrometheusMetrics) RecordIdempotencyConflict(reason string)                         { m.jobIdempotencyConflicts.inc(reason) }
func (m *PrometheusMetrics) RecordJobRetry(jobType string, count float64)                    { m.jobRetryCount.observe(jobType, count) }
func (m *PrometheusMetrics) RecordAssetCacheHit(assetType string)                            { m.assetCacheHit.inc(assetType) }
func (m *PrometheusMetrics) RecordAssetCacheMiss(assetType string)                           { m.assetCacheMiss.inc(assetType) }
func (m *PrometheusMetrics) SetWorkerActiveJobs(workerID string, count float64)              { m.workerActiveJobs.set(workerID, count) }
func (m *PrometheusMetrics) SetWorkerStatus(workerID string, status float64)                 { m.workerStatus.set(workerID, status) }
func (m *PrometheusMetrics) RecordFallback(reason string)                                    { m.fallbackCount.inc(reason) }
func (m *PrometheusMetrics) RecordPythonEmergencyPath(reason string)                         { m.pythonEmergencyPath.inc(reason) }

func (m *PrometheusMetrics) RecordJobResume(success bool) {
	m.jobResumeTotal.inc("total")
	if success {
		m.jobResumeSuccess.inc("total")
	}
}

// Query methods
func (m *PrometheusMetrics) GetFallbackCount() float64                { return m.fallbackCount.total() }
func (m *PrometheusMetrics) GetPythonEmergencyPathCount() float64     { return m.pythonEmergencyPath.total() }
func (m *PrometheusMetrics) GetJobQueueWaitP50() float64              { return m.jobQueueWaitMs.percentile(0.5) }
func (m *PrometheusMetrics) GetJobQueueWaitP95() float64              { return m.jobQueueWaitMs.percentile(0.95) }
func (m *PrometheusMetrics) GetJobDispatchP50() float64               { return m.jobDispatchMs.percentile(0.5) }
func (m *PrometheusMetrics) GetJobDispatchP95() float64               { return m.jobDispatchMs.percentile(0.95) }
func (m *PrometheusMetrics) GetJobRuntimeP50() float64                { return m.jobRuntimeMs.percentile(0.5) }
func (m *PrometheusMetrics) GetJobRuntimeP95() float64                { return m.jobRuntimeMs.percentile(0.95) }
func (m *PrometheusMetrics) GetJobCompleteAckP50() float64            { return m.jobCompleteAckMs.percentile(0.5) }
func (m *PrometheusMetrics) GetJobCompleteAckP95() float64            { return m.jobCompleteAckMs.percentile(0.95) }
func (m *PrometheusMetrics) GetJobRetryAvg() float64                  { return m.jobRetryCount.average() }

func (m *PrometheusMetrics) GetJobResumeSuccessRate() float64 {
	total := m.jobResumeTotal.get("total")
	if total == 0 {
		return 0
	}
	return (m.jobResumeSuccess.get("total") / total) * 100
}

func (m *PrometheusMetrics) GetAssetCacheHitRate() float64 {
	hits := m.assetCacheHit.total()
	misses := m.assetCacheMiss.total()
	total := hits + misses
	if total == 0 {
		return 0
	}
	return (hits / total) * 100
}

// ExportPrometheus returns metrics in Prometheus text format.
func (m *PrometheusMetrics) ExportPrometheus() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var output string
	output += m.jobQueueWaitMs.export()
	output += m.jobDispatchMs.export()
	output += m.jobRuntimeMs.export()
	output += m.jobCompleteAckMs.export()
	output += m.jobRetryCount.export()
	output += m.jobIdempotencyConflicts.export()
	output += m.jobResumeSuccess.export()
	output += m.jobResumeTotal.export()
	output += m.assetCacheHit.export()
	output += m.assetCacheMiss.export()
	output += m.fallbackCount.export()
	output += m.pythonEmergencyPath.export()
	output += m.workerActiveJobs.export()
	output += m.workerStatus.export()
	return output
}

// Global Prometheus metrics instance
var globalPrometheus = NewPrometheusMetrics()

// GetPrometheusMetrics returns the global Prometheus metrics instance.
func GetPrometheusMetrics() *PrometheusMetrics {
	return globalPrometheus
}

// StartPrometheusServer starts an HTTP server for Prometheus metrics scraping.
func StartPrometheusServer(port int) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, globalPrometheus.ExportPrometheus())
	})
	server := &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: mux}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Prometheus server error: %v\n", err)
		}
	}()
	return nil
}

// KPIReport contains the KPI metrics for reporting.
type KPIReport struct {
	JobQueueWaitP50             float64 `json:"job_queue_wait_ms_p50"`
	JobQueueWaitP95             float64 `json:"job_queue_wait_ms_p95"`
	JobDispatchP50              float64 `json:"job_dispatch_ms_p50"`
	JobDispatchP95              float64 `json:"job_dispatch_ms_p95"`
	JobRuntimeP50               float64 `json:"job_runtime_ms_p50"`
	JobRuntimeP95               float64 `json:"job_runtime_ms_p95"`
	JobCompleteAckP50           float64 `json:"job_complete_ack_ms_p50"`
	JobCompleteAckP95           float64 `json:"job_complete_ack_ms_p95"`
	JobIdempotencyConflicts     int64   `json:"job_idempotency_conflicts_total"`
	JobRetryAvg                 float64 `json:"job_retry_count_avg"`
	JobResumeSuccessRate        float64 `json:"job_resume_success_rate"`
	AssetCacheHitRate           float64 `json:"asset_cache_hit_rate"`
	FallbackCount               float64 `json:"fallback_count_total"`
	Timestamp                   string  `json:"timestamp"`
}

// GetKPIReport returns a KPI report.
func (m *PrometheusMetrics) GetKPIReport() *KPIReport {
	return &KPIReport{
		JobQueueWaitP50:             m.GetJobQueueWaitP50(),
		JobQueueWaitP95:             m.GetJobQueueWaitP95(),
		JobDispatchP50:              m.GetJobDispatchP50(),
		JobDispatchP95:              m.GetJobDispatchP95(),
		JobRuntimeP50:               m.GetJobRuntimeP50(),
		JobRuntimeP95:               m.GetJobRuntimeP95(),
		JobCompleteAckP50:           m.GetJobCompleteAckP50(),
		JobCompleteAckP95:           m.GetJobCompleteAckP95(),
		JobIdempotencyConflicts:     int64(m.jobIdempotencyConflicts.total()),
		JobRetryAvg:                 m.GetJobRetryAvg(),
		JobResumeSuccessRate:        m.GetJobResumeSuccessRate(),
		AssetCacheHitRate:           m.GetAssetCacheHitRate(),
		FallbackCount:               m.GetFallbackCount(),
		Timestamp:                   time.Now().UTC().Format(time.RFC3339),
	}
}
