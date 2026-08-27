package telemetry

func initPrometheusWorkerFamily(m *PrometheusMetrics) {
	m.workerErrorsTotal = &CounterVec{Name: "velox_worker_errors_total", Help: "Worker task failures", values: map[string]float64{"total": 0}}
	m.assetDownloadActive = &GaugeVec{Name: "velox_asset_download_transfers_active", Help: "Active asset transfers", values: map[string]float64{"total": 0}}
	m.assetDownloadQueued = &GaugeVec{Name: "velox_asset_download_transfers_queued", Help: "Queued asset transfers", values: map[string]float64{"total": 0}}
	m.assetDownloadReady = &GaugeVec{Name: "velox_asset_download_transfers_ready", Help: "Ready asset transfers retained by the manager", values: map[string]float64{"total": 0}}
	m.assetDownloadFailed = &GaugeVec{Name: "velox_asset_download_transfers_failed", Help: "Failed asset transfers retained by the manager", values: map[string]float64{"total": 0}}
	m.assetDownloadCacheHits = &GaugeVec{Name: "velox_asset_download_cache_hits", Help: "Ready asset transfers completed from cache", values: map[string]float64{"total": 0}}
	m.assetDownloadBytes = &GaugeVec{Name: "velox_asset_download_bytes_downloaded", Help: "Bytes downloaded across registered asset transfers", values: map[string]float64{"total": 0}}
	m.assetDownloadTotalBytes = &GaugeVec{Name: "velox_asset_download_bytes_total", Help: "Expected bytes across registered asset transfers", values: map[string]float64{"total": 0}}
	m.assetDownloadThroughput = &GaugeVec{Name: "velox_asset_download_throughput_bytes_per_second", Help: "Current aggregate asset download throughput", values: map[string]float64{"total": 0}}
	m.assetDownloadChunksActive = &GaugeVec{Name: "velox_asset_download_chunks_active", Help: "Active parallel chunk connections across chunked asset transfers", values: map[string]float64{"total": 0}}
	m.assetDownloadChunkThroughput = &GaugeVec{Name: "velox_asset_download_chunk_throughput_bytes_per_second", Help: "Current chunked asset transfer throughput (bytes/s)", values: map[string]float64{"total": 0}}
	m.assetDownloadETA = &GaugeVec{Name: "velox_asset_download_eta_seconds", Help: "Longest remaining asset transfer ETA", values: map[string]float64{"total": 0}}
	m.assetDownloadCoalesced = &CounterVec{Name: "velox_asset_download_coalesced_requests_total", Help: "Asset requests coalesced onto an existing transfer", values: map[string]float64{"total": 0}}
	m.workerActiveJobs = &GaugeVec{Name: "velox_worker_active_jobs", Help: "Number of active jobs per worker", values: make(map[string]float64)}
	m.workerStatus = &GaugeVec{Name: "velox_worker_status", Help: "Worker status (0=offline, 1=idle, 2=busy, 3=error)", values: make(map[string]float64)}
	m.fallbackCount = &CounterVec{Name: "velox_fallback_count_total", Help: "Total number of fallback usages (should be 0 in production)", values: make(map[string]float64)}
	m.pythonEmergencyPath = &CounterVec{Name: "velox_python_emergency_path_total", Help: "Total Python emergency path usages (should be 0 in production)", values: make(map[string]float64)}
	m.renderSeconds = &HistogramVec{Name: "velox_render_seconds", Help: "Render duration", Buckets: []float64{.1, 1, 5, 10, 30, 60, 300, 900, 1800}, values: make(map[string]*histogramData)}
	m.artifactUploadSeconds = &HistogramVec{Name: "velox_artifact_upload_seconds", Help: "Artifact upload duration", Buckets: []float64{.01, .1, 1, 5, 30, 120, 600}, values: make(map[string]*histogramData)}
	m.artifactLockWaitSeconds = &HistogramVec{Name: "velox_artifact_lock_wait_seconds", Help: "Publisher and artifact lock wait duration", Buckets: []float64{.0001, .001, .01, .1, 1, 5, 30, 120}, values: make(map[string]*histogramData)}
	m.renderUploadOverlapSeconds = &HistogramVec{Name: "velox_render_upload_overlap_seconds", Help: "Time during which rendering and artifact upload overlapped", Buckets: []float64{.001, .01, .1, 1, 5, 30, 120, 600}, values: make(map[string]*histogramData)}
	m.progressiveFirstPartStartedSeconds = &HistogramVec{Name: "velox_progressive_upload_first_part_started_seconds", Help: "Time between the progressive upload run start and the first part sent", Buckets: []float64{.001, .01, .1, 1, 5, 30, 120, 600}, values: make(map[string]*histogramData)}
	m.progressivePartsBeforeRenderEnd = &HistogramVec{Name: "velox_progressive_upload_parts_before_render_end", Help: "Parts uploaded while the render was still running", Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256}, values: make(map[string]*histogramData)}
	m.progressiveBytesBeforeRenderEnd = &HistogramVec{Name: "velox_progressive_upload_bytes_before_render_end", Help: "Bytes uploaded while the render was still running", Buckets: []float64{1 << 20, 8 << 20, 32 << 20, 128 << 20, 512 << 20, 2 << 30, 4 << 30}, values: make(map[string]*histogramData)}
	m.muxToOpenMicroseconds = &HistogramVec{Name: "velox_mux_to_open_microseconds", Help: "Go-side latency from first C++ progress event with path to file open for upload", Buckets: []float64{100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000, 5000000}, values: make(map[string]*histogramData)}
	m.taskResultSubmitSeconds = &HistogramVec{Name: "velox_task_result_submit_seconds", Help: "TaskResult persistence and send duration", Buckets: []float64{.001, .01, .1, 1, 5, 30, 120}, values: make(map[string]*histogramData)}
	m.taskResultAckSeconds = &HistogramVec{Name: "velox_task_result_ack_seconds", Help: "TaskResult acknowledgement wait duration", Buckets: []float64{.001, .01, .1, 1, 5, 30, 120}, values: make(map[string]*histogramData)}
	m.taskResultAcksTotal = &CounterVec{Name: "velox_task_result_acks_total", Help: "TaskResult acknowledgements received", values: make(map[string]float64)}
	m.telemetryInvalidEvents = &CounterVec{Name: "velox_telemetry_invalid_events_total", Help: "Telemetry events rejected by the worker catalog and forwarded for master quarantine", Label: "reason", values: make(map[string]float64)}

	// Pending-offer dedup counters. Static "total" label — no worker/job
	// identifiers leak into Prometheus series.
	m.offerDuplicateTotal = &CounterVec{Name: "velox_offer_duplicate_total", Help: "TaskOffer deduplicated against identical pending entry", values: map[string]float64{"total": 0}}
	m.offerReplacedTotal = &CounterVec{Name: "velox_offer_replaced_total", Help: "TaskOffer replaced a stale pending entry with a newer attempt", values: map[string]float64{"total": 0}}
	m.offerStaleTotal = &CounterVec{Name: "velox_offer_stale_total", Help: "TaskOffer rejected as older than the existing pending entry", values: map[string]float64{"total": 0}}
	m.offerIdentityConflictTotal = &CounterVec{Name: "velox_offer_identity_conflict_total", Help: "TaskOffer conflicted on lease/revision for the same attempt_number", values: map[string]float64{"total": 0}}
	m.offerReconciledTotal = &CounterVec{Name: "velox_offer_reconciled_total", Help: "TaskOffer successfully reconciled from pending map into execution", values: map[string]float64{"total": 0}}

	// Resource admission controller metrics.
	m.admissionRejectionsTotal = &CounterVec{Name: "velox_admission_rejections_total", Help: "Resource admission requests rejected (memory pressure)", values: map[string]float64{"total": 0}}
	m.backpressureEventsTotal = &CounterVec{Name: "velox_backpressure_events_total", Help: "Hysteresis state transitions (throttle activations)", values: map[string]float64{"total": 0}}

	// Resource admission live diagnostics — updated every heartbeat.
	m.admissionRSSPressurePct = &GaugeVec{Name: "velox_admission_rss_pressure_percent", Help: "Current process RSS as percentage of total RAM", values: map[string]float64{"total": 0}}
	m.admissionRSSBytes = &GaugeVec{Name: "velox_admission_rss_bytes", Help: "Current process RSS in bytes", values: map[string]float64{"total": 0}}
	m.admissionThrottledRender = &GaugeVec{Name: "velox_admission_throttled_render", Help: "1 when render admission is throttled by RSS hysteresis, 0 otherwise", values: map[string]float64{"total": 0}}
	m.admissionThrottledPrefetch = &GaugeVec{Name: "velox_admission_throttled_prefetch", Help: "1 when prefetch admission is throttled by RSS hysteresis, 0 otherwise", values: map[string]float64{"total": 0}}
	m.admissionThrottledPublish = &GaugeVec{Name: "velox_admission_throttled_publish", Help: "1 when publish admission is throttled by RSS hysteresis, 0 otherwise", values: map[string]float64{"total": 0}}

	// File descriptor gauges — Linux /proc/self/fd + getrlimit.
	m.workerOpenFds = &GaugeVec{Name: "velox_worker_open_fds", Help: "Current open file descriptors for this worker process", values: map[string]float64{"total": 0}}
	m.workerMaxFds = &GaugeVec{Name: "velox_worker_max_fds", Help: "Soft limit of open file descriptors (RLIMIT_NOFILE)", values: map[string]float64{"total": 0}}
	m.workerFdUtilization = &GaugeVec{Name: "velox_worker_fd_utilization_ratio", Help: "File descriptor utilization ratio (0.0-1.0)", values: map[string]float64{"total": 0}}

	// Prefetch corruption counters.
	m.prefetchCorruptedTotal = &CounterVec{Name: "velox_prefetch_corrupted_total", Help: "Prefetched assets invalidated at runtime due to integrity failure", Label: "reason", values: map[string]float64{"size_mismatch": 0, "hash_mismatch": 0}}

	// Network admission controller metrics.
	m.networkSaturationIngress = &GaugeVec{Name: "velox_network_ingress_saturation_ratio", Help: "NIC ingress utilization ratio (0.0-1.0+, >1.0 = oversaturated)", values: map[string]float64{"total": 0}}
	m.networkSaturationEgress = &GaugeVec{Name: "velox_network_egress_saturation_ratio", Help: "NIC egress utilization ratio (0.0-1.0+)", values: map[string]float64{"total": 0}}
	m.networkConsumerBytes = &GaugeVec{Name: "velox_network_consumer_bytes_total", Help: "Bytes transferred per consumer (publish/runtime/prefetch)", Label: "consumer", values: map[string]float64{"publish": 0, "runtime": 0, "prefetch": 0}}
	m.networkThrottleMS = &GaugeVec{Name: "velox_network_throttle_ms_total", Help: "Cumulative time (ms) each consumer waited for bandwidth admission", Label: "consumer", values: map[string]float64{"publish": 0, "runtime": 0, "prefetch": 0}}
}

func exportPrometheusWorkerFamily(m *PrometheusMetrics) string {
	return m.workerErrorsTotal.export() +
		m.assetDownloadActive.export() +
		m.assetDownloadQueued.export() +
		m.assetDownloadReady.export() +
		m.assetDownloadFailed.export() +
		m.assetDownloadCacheHits.export() +
		m.assetDownloadBytes.export() +
		m.assetDownloadTotalBytes.export() +
		m.assetDownloadThroughput.export() +
		m.assetDownloadChunksActive.export() +
		m.assetDownloadChunkThroughput.export() +
		m.assetDownloadETA.export() +
		m.assetDownloadCoalesced.export() +
		m.fallbackCount.export() +
		m.pythonEmergencyPath.export() +
		m.workerActiveJobs.export() +
		m.workerStatus.export() +
		m.renderSeconds.export() +
		m.artifactUploadSeconds.export() +
		m.artifactLockWaitSeconds.export() +
		m.renderUploadOverlapSeconds.export() +
		m.progressiveFirstPartStartedSeconds.export() +
		m.progressivePartsBeforeRenderEnd.export() +
		m.progressiveBytesBeforeRenderEnd.export() +
		m.muxToOpenMicroseconds.export() +
		m.taskResultSubmitSeconds.export() +
		m.taskResultAckSeconds.export() +
		m.taskResultAcksTotal.export() +
		m.telemetryInvalidEvents.export() +
		m.offerDuplicateTotal.export() +
		m.offerReplacedTotal.export() +
		m.offerStaleTotal.export() +
		m.offerIdentityConflictTotal.export() +
		m.offerReconciledTotal.export() +
		m.admissionRejectionsTotal.export() +
		m.backpressureEventsTotal.export() +
		m.admissionRSSPressurePct.export() +
		m.admissionRSSBytes.export() +
		m.admissionThrottledRender.export() +
		m.admissionThrottledPrefetch.export() +
		m.admissionThrottledPublish.export() +
		m.workerOpenFds.export() +
		m.workerMaxFds.export() +
		m.workerFdUtilization.export() +
		m.prefetchCorruptedTotal.export() +
		m.networkSaturationIngress.export() +
		m.networkSaturationEgress.export() +
		m.networkConsumerBytes.export() +
		m.networkThrottleMS.export()
}
