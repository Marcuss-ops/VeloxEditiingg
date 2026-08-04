// Package telemetry provides Prometheus metrics collection for the worker agent.
package telemetry

import (
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

// PrometheusMetrics tracks KPI metrics for Prometheus export.
type PrometheusMetrics struct {
	mu sync.RWMutex

	jobQueueWaitMs                   *HistogramVec
	jobDispatchMs                    *HistogramVec
	jobRuntimeMs                     *HistogramVec
	jobCompleteAckMs                 *HistogramVec
	jobIdempotencyConflicts          *CounterVec
	jobRetryCount                    *HistogramVec
	jobResumeSuccess                 *CounterVec
	jobResumeTotal                   *CounterVec
	assetCacheHit                    *CounterVec
	assetCacheMiss                   *CounterVec
	assetCacheHitsCanonical          *CounterVec
	assetCacheMissesCanonical        *CounterVec
	assetCacheRequests               *CounterVec
	assetCacheEvictions              *CounterVec
	assetCacheDownloads              *CounterVec
	assetCacheDownloadBytes          *CounterVec
	assetCacheDownloadMS             *HistogramVec
	assetDownloadSecondsCanonical    *HistogramVec
	assetCacheVerifyMS               *HistogramVec
	assetCacheCleanupMS              *HistogramVec
	assetCacheCleanupSkip            *CounterVec
	assetCacheProtectedSkipCanonical *CounterVec
	assetCacheSizeBytes              *GaugeVec
	assetCacheEntries                *GaugeVec
	workerActiveJobs                 *GaugeVec
	workerStatus                     *GaugeVec
	fallbackCount                    *CounterVec
	pythonEmergencyPath              *CounterVec
	renderSeconds                    *HistogramVec
	artifactUploadSeconds            *HistogramVec
	taskResultSubmitSeconds          *HistogramVec
	taskResultAckSeconds             *HistogramVec
	taskResultAcksTotal              *CounterVec
	telemetryInvalidEvents           *CounterVec
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
		assetCacheHitsCanonical: &CounterVec{
			Name: "velox_asset_cache_hits_total", Help: "Total asset cache hits (low-cardinality)",
			values: make(map[string]float64),
		},
		assetCacheMissesCanonical: &CounterVec{
			Name: "velox_asset_cache_misses_total", Help: "Total asset cache misses (low-cardinality)",
			values: make(map[string]float64),
		},
		assetCacheRequests:               &CounterVec{Name: "velox_cache_requests_total", Help: "Asset cache requests by result", values: make(map[string]float64)},
		assetCacheEvictions:              &CounterVec{Name: "velox_cache_evictions_total", Help: "Asset cache evictions by reason", values: make(map[string]float64)},
		assetCacheDownloads:              &CounterVec{Name: "velox_cache_downloads_total", Help: "Completed local asset downloads", values: make(map[string]float64)},
		assetCacheDownloadBytes:          &CounterVec{Name: "velox_cache_download_bytes_total", Help: "Bytes downloaded into the local asset cache", values: make(map[string]float64)},
		assetCacheDownloadMS:             &HistogramVec{Name: "velox_cache_download_duration_seconds", Help: "Asset cache download duration", Buckets: []float64{.01, .1, 1, 5, 30, 120, 600}, values: make(map[string]*histogramData)},
		assetDownloadSecondsCanonical:    &HistogramVec{Name: "velox_asset_download_seconds", Help: "Asset download duration", Buckets: []float64{.01, .1, 1, 5, 30, 120, 600}, values: make(map[string]*histogramData)},
		assetCacheVerifyMS:               &HistogramVec{Name: "velox_cache_sha_verify_duration_seconds", Help: "Asset cache verification duration", Buckets: []float64{.001, .01, .1, 1, 5, 30}, values: make(map[string]*histogramData)},
		assetCacheCleanupMS:              &HistogramVec{Name: "velox_cache_cleanup_duration_seconds", Help: "Asset cache cleanup duration", Buckets: []float64{.001, .01, .1, 1, 5, 30}, values: make(map[string]*histogramData)},
		assetCacheCleanupSkip:            &CounterVec{Name: "velox_cache_cleanup_skipped_total", Help: "Asset cache cleanup skips by reason", values: make(map[string]float64)},
		assetCacheProtectedSkipCanonical: &CounterVec{Name: "velox_cache_protected_skips_total", Help: "Asset cache entries skipped because they are protected", values: make(map[string]float64)},
		assetCacheSizeBytes:              &GaugeVec{Name: "velox_cache_size_bytes", Help: "Current local asset cache size", values: make(map[string]float64)},
		assetCacheEntries:                &GaugeVec{Name: "velox_cache_entries", Help: "Current local asset cache entries", values: make(map[string]float64)},
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
		renderSeconds:           &HistogramVec{Name: "velox_render_seconds", Help: "Render duration", Buckets: []float64{.1, 1, 5, 10, 30, 60, 300, 900, 1800}, values: make(map[string]*histogramData)},
		artifactUploadSeconds:   &HistogramVec{Name: "velox_artifact_upload_seconds", Help: "Artifact upload duration", Buckets: []float64{.01, .1, 1, 5, 30, 120, 600}, values: make(map[string]*histogramData)},
		taskResultSubmitSeconds: &HistogramVec{Name: "velox_task_result_submit_seconds", Help: "TaskResult persistence and send duration", Buckets: []float64{.001, .01, .1, 1, 5, 30, 120}, values: make(map[string]*histogramData)},
		taskResultAckSeconds:    &HistogramVec{Name: "velox_task_result_ack_seconds", Help: "TaskResult acknowledgement wait duration", Buckets: []float64{.001, .01, .1, 1, 5, 30, 120}, values: make(map[string]*histogramData)},
		taskResultAcksTotal:     &CounterVec{Name: "velox_task_result_acks_total", Help: "TaskResult acknowledgements received", values: make(map[string]float64)},
		telemetryInvalidEvents:  &CounterVec{Name: "velox_telemetry_invalid_events_total", Help: "Telemetry events rejected by the worker catalog and forwarded for master quarantine", values: make(map[string]float64)},
	}
}

// Recording methods
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
func (m *PrometheusMetrics) RecordAssetCacheHit(_ string) {
	m.assetCacheHit.inc("asset")
	m.assetCacheHitsCanonical.inc("total")
}
func (m *PrometheusMetrics) RecordAssetCacheMiss(_ string) {
	m.assetCacheMiss.inc("asset")
	m.assetCacheMissesCanonical.inc("total")
}
func (m *PrometheusMetrics) RecordCacheRequest(result string)  { m.assetCacheRequests.inc(result) }
func (m *PrometheusMetrics) RecordCacheEviction(reason string) { m.assetCacheEvictions.inc(reason) }
func (m *PrometheusMetrics) RecordCacheEvictions(reason string, count int) {
	if count > 0 {
		m.assetCacheEvictions.add(reason, float64(count))
	}
}
func (m *PrometheusMetrics) RecordCacheDownload(bytes int64, duration time.Duration) {
	m.assetCacheDownloads.inc("asset")
	m.assetCacheDownloadBytes.add("asset", float64(bytes))
	m.assetCacheDownloadMS.observe("asset", duration.Seconds())
	m.assetDownloadSecondsCanonical.observe("total", duration.Seconds())
}
func (m *PrometheusMetrics) RecordCacheVerify(duration time.Duration) {
	m.assetCacheVerifyMS.observe("asset", duration.Seconds())
}
func (m *PrometheusMetrics) RecordCacheCleanup(duration time.Duration) {
	m.assetCacheCleanupMS.observe("pass", duration.Seconds())
}
func (m *PrometheusMetrics) RecordCacheCleanupSkip(reason string) {
	m.assetCacheCleanupSkip.inc(reason)
}
func (m *PrometheusMetrics) RecordCacheCleanupSkips(reason string, count int) {
	if count > 0 {
		m.assetCacheCleanupSkip.add(reason, float64(count))
		if reason == "protected" {
			m.assetCacheProtectedSkipCanonical.add("total", float64(count))
		}
	}
}
func (m *PrometheusMetrics) SetCacheSize(entries int, bytes int64) {
	m.assetCacheEntries.set("total", float64(entries))
	m.assetCacheSizeBytes.set("total", float64(bytes))
}
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
func (m *PrometheusMetrics) RecordFallback(reason string) { m.fallbackCount.inc(reason) }
func (m *PrometheusMetrics) RecordPythonEmergencyPath(reason string) {
	m.pythonEmergencyPath.inc(reason)
}

func (m *PrometheusMetrics) RecordJobResume(success bool) {
	m.jobResumeTotal.inc("total")
	if success {
		m.jobResumeSuccess.inc("total")
	}
}

// Query methods
func (m *PrometheusMetrics) GetFallbackCount() float64 { return m.fallbackCount.total() }
func (m *PrometheusMetrics) GetPythonEmergencyPathCount() float64 {
	return m.pythonEmergencyPath.total()
}
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
	output += m.assetCacheHitsCanonical.export()
	output += m.assetCacheMissesCanonical.export()
	output += m.assetCacheRequests.export()
	output += m.assetCacheEvictions.export()
	output += m.assetCacheDownloads.export()
	output += m.assetCacheDownloadBytes.export()
	output += m.assetCacheDownloadMS.export()
	output += m.assetDownloadSecondsCanonical.export()
	output += m.assetCacheVerifyMS.export()
	output += m.assetCacheCleanupMS.export()
	output += m.assetCacheCleanupSkip.export()
	output += m.assetCacheProtectedSkipCanonical.export()
	output += m.assetCacheSizeBytes.export()
	output += m.assetCacheEntries.export()
	output += m.fallbackCount.export()
	output += m.pythonEmergencyPath.export()
	output += m.workerActiveJobs.export()
	output += m.workerStatus.export()
	output += m.renderSeconds.export()
	output += m.artifactUploadSeconds.export()
	output += m.taskResultSubmitSeconds.export()
	output += m.taskResultAckSeconds.export()
	output += m.taskResultAcksTotal.export()
	output += m.telemetryInvalidEvents.export()
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
	if port <= 0 || port > 65535 {
		return fmt.Errorf("invalid Prometheus port %d", port)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, globalPrometheus.ExportPrometheus())
	})
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("listen Prometheus port %d: %w", port, err)
	}
	server := &http.Server{Handler: mux}
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Prometheus server error: %v\n", err)
		}
	}()
	return nil
}

// KPIReport contains the KPI metrics for reporting.
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

// GetKPIReport returns a KPI report.
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
