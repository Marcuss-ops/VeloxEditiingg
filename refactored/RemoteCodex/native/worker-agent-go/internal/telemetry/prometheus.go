// Package telemetry provides Prometheus metrics collection for the worker agent.
//
// This implements Phase 1 deliverable: baseline KPI metrics with Prometheus export.
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

	// Job queue metrics
	jobQueueWaitMs *HistogramVec

	// Dispatch metrics
	jobDispatchMs *HistogramVec

	// Runtime metrics
	jobRuntimeMs *HistogramVec

	// Complete/ack metrics
	jobCompleteAckMs *HistogramVec

	// Idempotency metrics
	jobIdempotencyConflicts *CounterVec

	// Retry metrics
	jobRetryCount *HistogramVec

	// Resume metrics
	jobResumeSuccess *CounterVec
	jobResumeTotal   *CounterVec

	// Cache metrics
	assetCacheHit  *CounterVec
	assetCacheMiss *CounterVec

	// Worker metrics
	workerActiveJobs *GaugeVec
	workerStatus     *GaugeVec

	// Fallback metrics (Phase 2: Python decommission)
	fallbackCount *CounterVec
	pythonEmergencyPath *CounterVec // Track actual Python emergency path usage
}

// HistogramVec represents a Prometheus histogram metric.
type HistogramVec struct {
	Name    string
	Help    string
	Buckets []float64
	mu      sync.RWMutex
	values  map[string]*histogramData
}

type histogramData struct {
	count  int64
	sum    float64
	buckets map[float64]int64
}

// CounterVec represents a Prometheus counter metric.
type CounterVec struct {
	Name   string
	Help   string
	mu     sync.RWMutex
	values map[string]float64
}

// GaugeVec represents a Prometheus gauge metric.
type GaugeVec struct {
	Name   string
	Help   string
	mu     sync.RWMutex
	values map[string]float64
}

// NewPrometheusMetrics creates a new Prometheus metrics collector.
func NewPrometheusMetrics() *PrometheusMetrics {
	return &PrometheusMetrics{
		jobQueueWaitMs: &HistogramVec{
			Name:    "velox_job_queue_wait_ms",
			Help:    "Time job spends in queue before dispatch (ms)",
			Buckets: []float64{100, 500, 1000, 5000, 10000, 30000, 60000},
			values:  make(map[string]*histogramData),
		},
		jobDispatchMs: &HistogramVec{
			Name:    "velox_job_dispatch_ms",
			Help:    "Time to dispatch job to worker (ms)",
			Buckets: []float64{10, 50, 100, 500, 1000, 5000},
			values:  make(map[string]*histogramData),
		},
		jobRuntimeMs: &HistogramVec{
			Name:    "velox_job_runtime_ms",
			Help:    "Job execution time (ms)",
			Buckets: []float64{1000, 5000, 10000, 30000, 60000, 300000, 600000, 1800000},
			values:  make(map[string]*histogramData),
		},
		jobCompleteAckMs: &HistogramVec{
			Name:    "velox_job_complete_ack_ms",
			Help:    "Time to acknowledge job completion (ms)",
			Buckets: []float64{10, 50, 100, 500, 1000, 5000},
			values:  make(map[string]*histogramData),
		},
		jobIdempotencyConflicts: &CounterVec{
			Name:   "velox_job_idempotency_conflicts_total",
			Help:   "Total number of idempotency key conflicts",
			values: make(map[string]float64),
		},
		jobRetryCount: &HistogramVec{
			Name:    "velox_job_retry_count",
			Help:    "Number of retries per job",
			Buckets: []float64{0, 1, 2, 3, 5, 10},
			values:  make(map[string]*histogramData),
		},
		jobResumeSuccess: &CounterVec{
			Name:   "velox_job_resume_success_total",
			Help:   "Total successful job resumes",
			values: make(map[string]float64),
		},
		jobResumeTotal: &CounterVec{
			Name:   "velox_job_resume_total",
			Help:   "Total job resume attempts",
			values: make(map[string]float64),
		},
		assetCacheHit: &CounterVec{
			Name:   "velox_asset_cache_hit_total",
			Help:   "Total asset cache hits",
			values: make(map[string]float64),
		},
		assetCacheMiss: &CounterVec{
			Name:   "velox_asset_cache_miss_total",
			Help:   "Total asset cache misses",
			values: make(map[string]float64),
		},
		workerActiveJobs: &GaugeVec{
			Name:   "velox_worker_active_jobs",
			Help:   "Number of active jobs per worker",
			values: make(map[string]float64),
		},
		workerStatus: &GaugeVec{
			Name:   "velox_worker_status",
			Help:   "Worker status (0=offline, 1=idle, 2=busy, 3=error)",
			values: make(map[string]float64),
		},
		fallbackCount: &CounterVec{
			Name:   "velox_fallback_count_total",
			Help:   "Total number of fallback usages (deprecated - should be 0 in production)",
			values: make(map[string]float64),
		},
		pythonEmergencyPath: &CounterVec{
			Name:   "velox_python_emergency_path_total",
			Help:   "Total number of Python emergency path usages (should be 0 in production)",
			values: make(map[string]float64),
		},
	}
}

// RecordJobQueueWait records time job spent in queue.
func (m *PrometheusMetrics) RecordJobQueueWait(jobType string, durationMs float64) {
	m.jobQueueWaitMs.observe(jobType, durationMs)
}

// RecordJobDispatch records job dispatch time.
func (m *PrometheusMetrics) RecordJobDispatch(jobType string, durationMs float64) {
	m.jobDispatchMs.observe(jobType, durationMs)
}

// RecordJobRuntime records job execution time.
func (m *PrometheusMetrics) RecordJobRuntime(jobType string, durationMs float64) {
	m.jobRuntimeMs.observe(jobType, durationMs)
}

// RecordJobCompleteAck records job completion acknowledgment time.
func (m *PrometheusMetrics) RecordJobCompleteAck(jobType string, durationMs float64) {
	m.jobCompleteAckMs.observe(jobType, durationMs)
}

// RecordIdempotencyConflict records an idempotency conflict.
func (m *PrometheusMetrics) RecordIdempotencyConflict(reason string) {
	m.jobIdempotencyConflicts.inc(reason)
}

// RecordJobRetry records job retry count.
func (m *PrometheusMetrics) RecordJobRetry(jobType string, count float64) {
	m.jobRetryCount.observe(jobType, count)
}

// RecordJobResume records job resume attempt.
func (m *PrometheusMetrics) RecordJobResume(success bool) {
	m.jobResumeTotal.inc("total")
	if success {
		m.jobResumeSuccess.inc("total")
	}
}

// RecordAssetCacheHit records asset cache hit.
func (m *PrometheusMetrics) RecordAssetCacheHit(assetType string) {
	m.assetCacheHit.inc(assetType)
}

// RecordAssetCacheMiss records asset cache miss.
func (m *PrometheusMetrics) RecordAssetCacheMiss(assetType string) {
	m.assetCacheMiss.inc(assetType)
}

// SetWorkerActiveJobs sets the number of active jobs for a worker.
func (m *PrometheusMetrics) SetWorkerActiveJobs(workerID string, count float64) {
	m.workerActiveJobs.set(workerID, count)
}

// SetWorkerStatus sets the worker status.
func (m *PrometheusMetrics) SetWorkerStatus(workerID string, status float64) {
	m.workerStatus.set(workerID, status)
}

// RecordFallback records a fallback usage.
// This should be 0 in production.
func (m *PrometheusMetrics) RecordFallback(reason string) {
	m.fallbackCount.inc(reason)
}

// GetFallbackCount returns the total fallback count.
func (m *PrometheusMetrics) GetFallbackCount() float64 {
	return m.fallbackCount.total()
}

// RecordPythonEmergencyPath records a Python emergency path usage.
// This should be 0 in production after Python decommission.
func (m *PrometheusMetrics) RecordPythonEmergencyPath(reason string) {
	m.pythonEmergencyPath.inc(reason)
}

// GetPythonEmergencyPathCount returns the total Python emergency path count.
func (m *PrometheusMetrics) GetPythonEmergencyPathCount() float64 {
	return m.pythonEmergencyPath.total()
}

// GetJobQueueWaitP50 returns the p50 job queue wait time.
func (m *PrometheusMetrics) GetJobQueueWaitP50() float64 {
	return m.jobQueueWaitMs.percentile(0.5)
}

// GetJobQueueWaitP95 returns the p95 job queue wait time.
func (m *PrometheusMetrics) GetJobQueueWaitP95() float64 {
	return m.jobQueueWaitMs.percentile(0.95)
}

// GetJobDispatchP50 returns the p50 job dispatch time.
func (m *PrometheusMetrics) GetJobDispatchP50() float64 {
	return m.jobDispatchMs.percentile(0.5)
}

// GetJobDispatchP95 returns the p95 job dispatch time.
func (m *PrometheusMetrics) GetJobDispatchP95() float64 {
	return m.jobDispatchMs.percentile(0.95)
}

// GetJobRuntimeP50 returns the p50 job runtime.
func (m *PrometheusMetrics) GetJobRuntimeP50() float64 {
	return m.jobRuntimeMs.percentile(0.5)
}

// GetJobRuntimeP95 returns the p95 job runtime.
func (m *PrometheusMetrics) GetJobRuntimeP95() float64 {
	return m.jobRuntimeMs.percentile(0.95)
}

// GetJobCompleteAckP50 returns the p50 job complete ack time.
func (m *PrometheusMetrics) GetJobCompleteAckP50() float64 {
	return m.jobCompleteAckMs.percentile(0.5)
}

// GetJobCompleteAckP95 returns the p95 job complete ack time.
func (m *PrometheusMetrics) GetJobCompleteAckP95() float64 {
	return m.jobCompleteAckMs.percentile(0.95)
}

// GetJobRetryAvg returns the average job retry count.
func (m *PrometheusMetrics) GetJobRetryAvg() float64 {
	return m.jobRetryCount.average()
}

// GetJobResumeSuccessRate returns the job resume success rate.
func (m *PrometheusMetrics) GetJobResumeSuccessRate() float64 {
	total := m.jobResumeTotal.get("total")
	if total == 0 {
		return 0
	}
	success := m.jobResumeSuccess.get("total")
	return (success / total) * 100
}

// GetAssetCacheHitRate returns the asset cache hit rate.
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

	// Export histograms
	output += m.jobQueueWaitMs.export()
	output += m.jobDispatchMs.export()
	output += m.jobRuntimeMs.export()
	output += m.jobCompleteAckMs.export()
	output += m.jobRetryCount.export()

	// Export counters
	output += m.jobIdempotencyConflicts.export()
	output += m.jobResumeSuccess.export()
	output += m.jobResumeTotal.export()
	output += m.assetCacheHit.export()
	output += m.assetCacheMiss.export()
	output += m.fallbackCount.export()
	output += m.pythonEmergencyPath.export()

	// Export gauges
	output += m.workerActiveJobs.export()
	output += m.workerStatus.export()

	return output
}

// HistogramVec methods

func (h *HistogramVec) observe(label string, value float64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.values[label] == nil {
		h.values[label] = &histogramData{
			buckets: make(map[float64]int64),
		}
	}

	data := h.values[label]
	data.count++
	data.sum += value

	for _, bucket := range h.Buckets {
		if value <= bucket {
			data.buckets[bucket]++
		}
	}
}

func (h *HistogramVec) percentile(p float64) float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var totalCount int64
	var totalSum float64
	for _, data := range h.values {
		totalCount += data.count
		totalSum += data.sum
	}

	if totalCount == 0 {
		return 0
	}

	targetCount := int64(float64(totalCount) * p)
	var cumulative int64

	for _, bucket := range h.Buckets {
		for _, data := range h.values {
			cumulative += data.buckets[bucket]
		}
		if cumulative >= targetCount {
			return bucket
		}
	}

	return totalSum / float64(totalCount)
}

func (h *HistogramVec) average() float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var totalCount int64
	var totalSum float64
	for _, data := range h.values {
		totalCount += data.count
		totalSum += data.sum
	}

	if totalCount == 0 {
		return 0
	}
	return totalSum / float64(totalCount)
}

func (h *HistogramVec) export() string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var output string
	output += fmt.Sprintf("# HELP %s %s\n", h.Name, h.Help)
	output += fmt.Sprintf("# TYPE %s histogram\n", h.Name)

	for label, data := range h.values {
		for _, bucket := range h.Buckets {
			output += fmt.Sprintf("%s_bucket{le=\"%g\",label=\"%s\"} %d\n", h.Name, bucket, label, data.buckets[bucket])
		}
		output += fmt.Sprintf("%s_bucket{le=\"+Inf\",label=\"%s\"} %d\n", h.Name, label, data.count)
		output += fmt.Sprintf("%s_sum{label=\"%s\"} %g\n", h.Name, label, data.sum)
		output += fmt.Sprintf("%s_count{label=\"%s\"} %d\n", h.Name, label, data.count)
	}

	return output
}

// CounterVec methods

func (c *CounterVec) inc(label string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.values[label]++
}

func (c *CounterVec) get(label string) float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.values[label]
}

func (c *CounterVec) total() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var total float64
	for _, v := range c.values {
		total += v
	}
	return total
}

func (c *CounterVec) export() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var output string
	output += fmt.Sprintf("# HELP %s %s\n", c.Name, c.Help)
	output += fmt.Sprintf("# TYPE %s counter\n", c.Name)

	for label, value := range c.values {
		// Use reason label for fallback_count_total, generic label for others
		if c.Name == "velox_fallback_count_total" {
			output += fmt.Sprintf("%s{reason=\"%s\"} %g\n", c.Name, label, value)
		} else {
			output += fmt.Sprintf("%s{label=\"%s\"} %g\n", c.Name, label, value)
		}
	}

	return output
}

// GaugeVec methods

func (g *GaugeVec) set(label string, value float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.values[label] = value
}

func (g *GaugeVec) export() string {
	g.mu.RLock()
	defer g.mu.RUnlock()

	var output string
	output += fmt.Sprintf("# HELP %s %s\n", g.Name, g.Help)
	output += fmt.Sprintf("# TYPE %s gauge\n", g.Name)

	for label, value := range g.values {
		output += fmt.Sprintf("%s{label=\"%s\"} %g\n", g.Name, label, value)
	}

	return output
}

// Global Prometheus metrics instance.
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

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Prometheus server error: %v\n", err)
		}
	}()

	return nil
}

// KPIReport contains the KPI metrics for reporting.
type KPIReport struct {
	JobQueueWaitP50      float64 `json:"job_queue_wait_ms_p50"`
	JobQueueWaitP95      float64 `json:"job_queue_wait_ms_p95"`
	JobDispatchP50       float64 `json:"job_dispatch_ms_p50"`
	JobDispatchP95       float64 `json:"job_dispatch_ms_p95"`
	JobRuntimeP50        float64 `json:"job_runtime_ms_p50"`
	JobRuntimeP95        float64 `json:"job_runtime_ms_p95"`
	JobCompleteAckP50    float64 `json:"job_complete_ack_ms_p50"`
	JobCompleteAckP95    float64 `json:"job_complete_ack_ms_p95"`
	JobIdempotencyConflicts int64 `json:"job_idempotency_conflicts_total"`
	JobRetryAvg          float64 `json:"job_retry_count_avg"`
	JobResumeSuccessRate float64 `json:"job_resume_success_rate"`
	AssetCacheHitRate    float64 `json:"asset_cache_hit_rate"`
	FallbackCount        float64 `json:"fallback_count_total"` // Phase 2: Python decommission - should be 0
	Timestamp            string  `json:"timestamp"`
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
		FallbackCount:           m.GetFallbackCount(), // Phase 2: Python decommission
		Timestamp:               time.Now().UTC().Format(time.RFC3339),
	}
}
