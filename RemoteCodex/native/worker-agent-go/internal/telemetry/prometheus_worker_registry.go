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
	m.taskResultSubmitSeconds = &HistogramVec{Name: "velox_task_result_submit_seconds", Help: "TaskResult persistence and send duration", Buckets: []float64{.001, .01, .1, 1, 5, 30, 120}, values: make(map[string]*histogramData)}
	m.taskResultAckSeconds = &HistogramVec{Name: "velox_task_result_ack_seconds", Help: "TaskResult acknowledgement wait duration", Buckets: []float64{.001, .01, .1, 1, 5, 30, 120}, values: make(map[string]*histogramData)}
	m.taskResultAcksTotal = &CounterVec{Name: "velox_task_result_acks_total", Help: "TaskResult acknowledgements received", values: make(map[string]float64)}
	m.telemetryInvalidEvents = &CounterVec{Name: "velox_telemetry_invalid_events_total", Help: "Telemetry events rejected by the worker catalog and forwarded for master quarantine", Label: "reason", values: make(map[string]float64)}
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
		m.taskResultSubmitSeconds.export() +
		m.taskResultAckSeconds.export() +
		m.taskResultAcksTotal.export() +
		m.telemetryInvalidEvents.export()
}
