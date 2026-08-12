// Package telemetry provides Prometheus metrics collection for the worker agent.
package telemetry

import (
	"fmt"
	"net"
	"net/http"
	"sync"
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
	assetCacheDuplicateDownloads     *CounterVec
	assetCacheDuplicateDownloadBytes *CounterVec
	leaseAcquires                    *CounterVec
	leaseReleases                    *CounterVec
	leaseRenewals                    *CounterVec
	leaseRetries                     *CounterVec
	leaseCleanupFailures             *CounterVec
	prefetchRequested                *CounterVec
	prefetchDownloaded               *CounterVec
	prefetchDownloadedBytes          *CounterVec
	prefetchUseful                   *CounterVec
	prefetchWastedBytes              *CounterVec
	prefetchQueueWaitSeconds         *HistogramVec
	prefetchResolveSeconds           *HistogramVec
	prefetchReadyLeadSeconds         *HistogramVec
	prefetchActive                   *GaugeVec
	prefetchQueueDepth               *GaugeVec
	workerErrorsTotal                *CounterVec
	assetDownloadActive              *GaugeVec
	assetDownloadQueued              *GaugeVec
	assetDownloadReady               *GaugeVec
	assetDownloadFailed              *GaugeVec
	assetDownloadCacheHits           *GaugeVec
	assetDownloadBytes               *GaugeVec
	assetDownloadTotalBytes          *GaugeVec
	assetDownloadThroughput          *GaugeVec
	assetDownloadETA                 *GaugeVec
	assetDownloadCoalesced           *CounterVec
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
		assetCacheRequests:               &CounterVec{Name: "velox_cache_requests_total", Help: "Asset cache requests by result", Label: "result", values: map[string]float64{"hit": 0, "miss": 0}},
		assetCacheEvictions:              &CounterVec{Name: "velox_cache_evictions_total", Help: "Asset cache evictions by reason", Label: "reason", values: make(map[string]float64)},
		assetCacheDownloads:              &CounterVec{Name: "velox_cache_downloads_total", Help: "Completed local asset downloads", values: map[string]float64{"asset": 0}},
		assetCacheDownloadBytes:          &CounterVec{Name: "velox_cache_download_bytes_total", Help: "Bytes downloaded into the local asset cache", values: make(map[string]float64)},
		assetCacheDownloadMS:             &HistogramVec{Name: "velox_cache_download_duration_seconds", Help: "Asset cache download duration", Buckets: []float64{.01, .1, 1, 5, 30, 120, 600}, values: make(map[string]*histogramData)},
		assetDownloadSecondsCanonical:    &HistogramVec{Name: "velox_asset_download_seconds", Help: "Asset download duration", Buckets: []float64{.01, .1, 1, 5, 30, 120, 600}, values: make(map[string]*histogramData)},
		assetCacheVerifyMS:               &HistogramVec{Name: "velox_cache_sha_verify_duration_seconds", Help: "Asset cache verification duration", Buckets: []float64{.001, .01, .1, 1, 5, 30}, values: make(map[string]*histogramData)},
		assetCacheCleanupMS:              &HistogramVec{Name: "velox_cache_cleanup_duration_seconds", Help: "Asset cache cleanup duration", Buckets: []float64{.001, .01, .1, 1, 5, 30}, values: make(map[string]*histogramData)},
		assetCacheCleanupSkip:            &CounterVec{Name: "velox_cache_cleanup_skipped_total", Help: "Asset cache cleanup skips by reason", Label: "reason", values: make(map[string]float64)},
		assetCacheProtectedSkipCanonical: &CounterVec{Name: "velox_cache_protected_skips_total", Help: "Asset cache entries skipped because they are protected", values: make(map[string]float64)},
		assetCacheSizeBytes:              &GaugeVec{Name: "velox_cache_size_bytes", Help: "Current local asset cache size", values: make(map[string]float64)},
		assetCacheEntries:                &GaugeVec{Name: "velox_cache_entries", Help: "Current local asset cache entries", values: make(map[string]float64)},
		assetCacheDuplicateDownloads: &CounterVec{
			Name: "velox_cache_duplicate_downloads_total", Help: "Concurrent duplicate asset downloads coalesced by AssetDownloadManager",
			values: map[string]float64{"asset": 0},
		},
		assetCacheDuplicateDownloadBytes: &CounterVec{
			Name: "velox_cache_duplicate_download_bytes_total", Help: "Expected bytes a duplicate request would have consumed when coalesced by AssetDownloadManager",
			values: map[string]float64{"asset": 0},
		},
		leaseAcquires: &CounterVec{
			Name: "velox_cache_lease_acquires_total", Help: "Cache lease acquisition attempts by result", Label: "result",
			values: map[string]float64{"success": 0, "failure": 0},
		},
		leaseReleases: &CounterVec{
			Name: "velox_cache_lease_releases_total", Help: "Cache lease release attempts by result", Label: "result",
			values: map[string]float64{"success": 0, "failure": 0, "not_found": 0},
		},
		leaseRenewals: &CounterVec{
			Name: "velox_cache_lease_renewals_total", Help: "Cache lease renewal attempts by result", Label: "result",
			values: map[string]float64{"success": 0, "failure": 0, "not_found": 0},
		},
		leaseRetries: &CounterVec{
			Name: "velox_cache_lease_retries_total", Help: "Lease retry attempts by lifecycle source", Label: "source",
			values: map[string]float64{"release_all": 0, "reconciler": 0, "other": 0},
		},
		leaseCleanupFailures: &CounterVec{
			Name: "velox_cache_lease_cleanup_failures_total", Help: "Lease cleanup failures by lifecycle stage", Label: "stage",
			values: map[string]float64{"release": 0, "enqueue": 0, "reconcile_list": 0, "reconcile_release": 0, "reconcile_retry_persist": 0, "reconcile_delete": 0, "other": 0},
		},
		prefetchRequested:        &CounterVec{Name: "velox_prefetch_assets_requested_total", Help: "Future assets requested by prefetch", values: map[string]float64{"asset": 0}},
		prefetchDownloaded:       &CounterVec{Name: "velox_prefetch_assets_downloaded_total", Help: "Future assets downloaded by prefetch", values: map[string]float64{"asset": 0}},
		prefetchDownloadedBytes:  &CounterVec{Name: "velox_prefetch_bytes_total", Help: "Bytes downloaded by prefetch", values: map[string]float64{"asset": 0}},
		prefetchUseful:           &CounterVec{Name: "velox_prefetch_useful_assets_total", Help: "Prefetched assets later used by foreground", values: map[string]float64{"asset": 0}},
		prefetchWastedBytes:      &CounterVec{Name: "velox_prefetch_wasted_bytes_total", Help: "Prefetched bytes abandoned before use", values: map[string]float64{"asset": 0}},
		prefetchQueueWaitSeconds: &HistogramVec{Name: "velox_prefetch_queue_wait_seconds", Help: "Time a prefetched asset waits in the scheduler queue", Buckets: []float64{.001, .01, .1, 1, 5, 30, 120}, values: make(map[string]*histogramData)},
		prefetchResolveSeconds:   &HistogramVec{Name: "velox_prefetch_resolve_seconds", Help: "Time spent resolving a prefetched asset through the canonical resolver", Buckets: []float64{.001, .01, .1, 1, 5, 30, 120, 600}, values: make(map[string]*histogramData)},
		prefetchReadyLeadSeconds: &HistogramVec{Name: "velox_prefetch_ready_lead_seconds", Help: "Time between asset READY and job start; negative means foreground catch-up", Buckets: []float64{-30, -10, -1, 0, .001, .01, .1, 1, 5, 30, 120}, values: make(map[string]*histogramData)},
		prefetchActive:           &GaugeVec{Name: "velox_prefetch_active", Help: "Active prefetch resolver calls", values: map[string]float64{"total": 0}},
		prefetchQueueDepth:       &GaugeVec{Name: "velox_prefetch_queue_depth", Help: "Queued prefetch asset work items", values: map[string]float64{"total": 0}},
		workerErrorsTotal: &CounterVec{
			Name: "velox_worker_errors_total", Help: "Worker task failures",
			values: map[string]float64{"total": 0},
		},
		assetDownloadActive:     &GaugeVec{Name: "velox_asset_download_transfers_active", Help: "Active asset transfers", values: map[string]float64{"total": 0}},
		assetDownloadQueued:     &GaugeVec{Name: "velox_asset_download_transfers_queued", Help: "Queued asset transfers", values: map[string]float64{"total": 0}},
		assetDownloadReady:      &GaugeVec{Name: "velox_asset_download_transfers_ready", Help: "Ready asset transfers retained by the manager", values: map[string]float64{"total": 0}},
		assetDownloadFailed:     &GaugeVec{Name: "velox_asset_download_transfers_failed", Help: "Failed asset transfers retained by the manager", values: map[string]float64{"total": 0}},
		assetDownloadCacheHits:  &GaugeVec{Name: "velox_asset_download_cache_hits", Help: "Ready asset transfers completed from cache", values: map[string]float64{"total": 0}},
		assetDownloadBytes:      &GaugeVec{Name: "velox_asset_download_bytes_downloaded", Help: "Bytes downloaded across registered asset transfers", values: map[string]float64{"total": 0}},
		assetDownloadTotalBytes: &GaugeVec{Name: "velox_asset_download_bytes_total", Help: "Expected bytes across registered asset transfers", values: map[string]float64{"total": 0}},
		assetDownloadThroughput: &GaugeVec{Name: "velox_asset_download_throughput_bytes_per_second", Help: "Current aggregate asset download throughput", values: map[string]float64{"total": 0}},
		assetDownloadETA:        &GaugeVec{Name: "velox_asset_download_eta_seconds", Help: "Longest remaining asset transfer ETA", values: map[string]float64{"total": 0}},
		assetDownloadCoalesced:  &CounterVec{Name: "velox_asset_download_coalesced_requests_total", Help: "Asset requests coalesced onto an existing transfer", values: map[string]float64{"total": 0}},
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
		telemetryInvalidEvents:  &CounterVec{Name: "velox_telemetry_invalid_events_total", Help: "Telemetry events rejected by the worker catalog and forwarded for master quarantine", Label: "reason", values: make(map[string]float64)},
	}
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
	output += m.assetCacheDuplicateDownloads.export()
	output += m.assetCacheDuplicateDownloadBytes.export()
	output += m.leaseAcquires.export()
	output += m.leaseReleases.export()
	output += m.leaseRenewals.export()
	output += m.leaseRetries.export()
	output += m.leaseCleanupFailures.export()
	output += m.prefetchRequested.export()
	output += m.prefetchDownloaded.export()
	output += m.prefetchDownloadedBytes.export()
	output += m.prefetchUseful.export()
	output += m.prefetchWastedBytes.export()
	output += m.prefetchQueueWaitSeconds.export()
	output += m.prefetchResolveSeconds.export()
	output += m.prefetchReadyLeadSeconds.export()
	output += m.prefetchActive.export()
	output += m.prefetchQueueDepth.export()
	output += m.workerErrorsTotal.export()
	output += m.assetDownloadActive.export()
	output += m.assetDownloadQueued.export()
	output += m.assetDownloadReady.export()
	output += m.assetDownloadFailed.export()
	output += m.assetDownloadCacheHits.export()
	output += m.assetDownloadBytes.export()
	output += m.assetDownloadTotalBytes.export()
	output += m.assetDownloadThroughput.export()
	output += m.assetDownloadETA.export()
	output += m.assetDownloadCoalesced.export()
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
