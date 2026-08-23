package telemetry

func initPrometheusCacheFamily(m *PrometheusMetrics) {
	m.assetCacheHit = &CounterVec{Name: "velox_asset_cache_hit_total", Help: "Total asset cache hits", values: make(map[string]float64)}
	m.assetCacheMiss = &CounterVec{Name: "velox_asset_cache_miss_total", Help: "Total asset cache misses", values: make(map[string]float64)}
	m.assetCacheHitsCanonical = &CounterVec{Name: "velox_asset_cache_hits_total", Help: "Total asset cache hits (low-cardinality)", values: make(map[string]float64)}
	m.assetCacheMissesCanonical = &CounterVec{Name: "velox_asset_cache_misses_total", Help: "Total asset cache misses (low-cardinality)", values: make(map[string]float64)}
	m.assetCacheRequests = &CounterVec{Name: "velox_cache_requests_total", Help: "Asset cache requests by result", Label: "result", values: map[string]float64{"hit": 0, "miss": 0}}
	m.assetCacheEvictions = &CounterVec{Name: "velox_cache_evictions_total", Help: "Asset cache evictions by reason", Label: "reason", values: make(map[string]float64)}
	m.assetCacheEvictedBytes = &CounterVec{Name: "velox_cache_evicted_bytes_total", Help: "Bytes physically evicted from the asset cache", values: map[string]float64{"total": 0}}
	m.assetCacheDiskUsagePercent = &GaugeVec{Name: "velox_cache_disk_usage_percent", Help: "Current asset cache disk usage percent observed by the pressure controller", values: map[string]float64{"total": 0}}
	m.assetCacheDownloads = &CounterVec{Name: "velox_cache_downloads_total", Help: "Completed local asset downloads", values: map[string]float64{"asset": 0}}
	m.assetCacheDownloadBytes = &CounterVec{Name: "velox_cache_download_bytes_total", Help: "Bytes downloaded into the local asset cache", values: make(map[string]float64)}
	m.assetCacheDownloadMS = &HistogramVec{Name: "velox_cache_download_duration_seconds", Help: "Asset cache download duration", Buckets: []float64{.01, .1, 1, 5, 30, 120, 600}, values: make(map[string]*histogramData)}
	m.assetDownloadSecondsCanonical = &HistogramVec{Name: "velox_asset_download_seconds", Help: "Asset download duration", Buckets: []float64{.01, .1, 1, 5, 30, 120, 600}, values: make(map[string]*histogramData)}
	m.assetCacheVerifyMS = &HistogramVec{Name: "velox_cache_sha_verify_duration_seconds", Help: "Asset cache verification duration", Buckets: []float64{.001, .01, .1, 1, 5, 30}, values: make(map[string]*histogramData)}
	m.assetCacheCleanupMS = &HistogramVec{Name: "velox_cache_cleanup_duration_seconds", Help: "Asset cache cleanup duration", Buckets: []float64{.001, .01, .1, 1, 5, 30}, values: make(map[string]*histogramData)}
	m.assetCacheCleanupSkip = &CounterVec{Name: "velox_cache_cleanup_skipped_total", Help: "Asset cache cleanup skips by reason", Label: "reason", values: make(map[string]float64)}
	m.assetCacheProtectedSkipCanonical = &CounterVec{Name: "velox_cache_protected_skips_total", Help: "Asset cache entries skipped because they are protected", values: make(map[string]float64)}
	m.assetCacheSizeBytes = &GaugeVec{Name: "velox_cache_size_bytes", Help: "Current local asset cache size", values: make(map[string]float64)}
	m.assetCacheEntries = &GaugeVec{Name: "velox_cache_entries", Help: "Current local asset cache entries", values: make(map[string]float64)}
	m.assetCacheDuplicateDownloads = &CounterVec{Name: "velox_cache_duplicate_downloads_total", Help: "Concurrent duplicate asset downloads coalesced by AssetDownloadManager", values: map[string]float64{"asset": 0}}
	m.assetCacheDuplicateDownloadBytes = &CounterVec{Name: "velox_cache_duplicate_download_bytes_total", Help: "Expected bytes a duplicate request would have consumed when coalesced by AssetDownloadManager", values: map[string]float64{"asset": 0}}

	m.leaseAcquires = &CounterVec{Name: "velox_cache_lease_acquires_total", Help: "Cache lease acquisition attempts by result", Label: "result", values: map[string]float64{"success": 0, "failure": 0}}
	m.leaseReleases = &CounterVec{Name: "velox_cache_lease_releases_total", Help: "Cache lease release attempts by result", Label: "result", values: map[string]float64{"success": 0, "failure": 0, "not_found": 0}}
	m.leaseRenewals = &CounterVec{Name: "velox_cache_lease_renewals_total", Help: "Cache lease renewal attempts by result", Label: "result", values: map[string]float64{"success": 0, "failure": 0, "not_found": 0}}
	m.leaseRetries = &CounterVec{Name: "velox_cache_lease_retries_total", Help: "Lease retry attempts by lifecycle source", Label: "source", values: map[string]float64{"release_all": 0, "reconciler": 0, "other": 0}}
	m.leaseCleanupFailures = &CounterVec{Name: "velox_cache_lease_cleanup_failures_total", Help: "Lease cleanup failures by lifecycle stage", Label: "stage", values: map[string]float64{"release": 0, "enqueue": 0, "reconcile_list": 0, "reconcile_release": 0, "reconcile_retry_persist": 0, "reconcile_delete": 0, "reservation_release": 0, "other": 0}}

	m.prefetchRequested = &CounterVec{Name: "velox_prefetch_assets_requested_total", Help: "Future assets requested by prefetch", values: map[string]float64{"asset": 0}}
	m.prefetchDownloaded = &CounterVec{Name: "velox_prefetch_assets_downloaded_total", Help: "Future assets downloaded by prefetch", values: map[string]float64{"asset": 0}}
	m.prefetchDownloadedBytes = &CounterVec{Name: "velox_prefetch_bytes_total", Help: "Bytes downloaded by prefetch", values: map[string]float64{"asset": 0}}
	m.prefetchUseful = &CounterVec{Name: "velox_prefetch_useful_assets_total", Help: "Prefetched assets later used by foreground", values: map[string]float64{"asset": 0}}
	m.prefetchWastedBytes = &CounterVec{Name: "velox_prefetch_wasted_bytes_total", Help: "Prefetched bytes abandoned before use", values: map[string]float64{"asset": 0}}
	m.prefetchQueueWaitSeconds = &HistogramVec{Name: "velox_prefetch_queue_wait_seconds", Help: "Time a prefetched asset waits in the scheduler queue", Buckets: []float64{.001, .01, .1, 1, 5, 30, 120}, values: make(map[string]*histogramData)}
	m.prefetchResolveSeconds = &HistogramVec{Name: "velox_prefetch_resolve_seconds", Help: "Time spent resolving a prefetched asset through the canonical resolver", Buckets: []float64{.001, .01, .1, 1, 5, 30, 120, 600}, values: make(map[string]*histogramData)}
	m.prefetchReadyLeadSeconds = &HistogramVec{Name: "velox_prefetch_ready_lead_seconds", Help: "Time between asset READY and job start; negative means foreground catch-up", Buckets: []float64{-30, -10, -1, 0, .001, .01, .1, 1, 5, 30, 120}, values: make(map[string]*histogramData)}
	m.prefetchActive = &GaugeVec{Name: "velox_prefetch_active", Help: "Active prefetch resolver calls", values: map[string]float64{"total": 0}}
	m.prefetchQueueDepth = &GaugeVec{Name: "velox_prefetch_queue_depth", Help: "Queued prefetch asset work items", values: map[string]float64{"total": 0}}
}

func exportPrometheusCacheFamily(m *PrometheusMetrics) string {
	return m.assetCacheHit.export() +
		m.assetCacheMiss.export() +
		m.assetCacheHitsCanonical.export() +
		m.assetCacheMissesCanonical.export() +
		m.assetCacheRequests.export() +
		m.assetCacheEvictions.export() +
		m.assetCacheEvictedBytes.export() +
		m.assetCacheDiskUsagePercent.export() +
		m.assetCacheDownloads.export() +
		m.assetCacheDownloadBytes.export() +
		m.assetCacheDownloadMS.export() +
		m.assetDownloadSecondsCanonical.export() +
		m.assetCacheVerifyMS.export() +
		m.assetCacheCleanupMS.export() +
		m.assetCacheCleanupSkip.export() +
		m.assetCacheProtectedSkipCanonical.export() +
		m.assetCacheSizeBytes.export() +
		m.assetCacheEntries.export() +
		m.assetCacheDuplicateDownloads.export() +
		m.assetCacheDuplicateDownloadBytes.export() +
		m.leaseAcquires.export() +
		m.leaseReleases.export() +
		m.leaseRenewals.export() +
		m.leaseRetries.export() +
		m.leaseCleanupFailures.export() +
		m.prefetchRequested.export() +
		m.prefetchDownloaded.export() +
		m.prefetchDownloadedBytes.export() +
		m.prefetchUseful.export() +
		m.prefetchWastedBytes.export() +
		m.prefetchQueueWaitSeconds.export() +
		m.prefetchResolveSeconds.export() +
		m.prefetchReadyLeadSeconds.export() +
		m.prefetchActive.export() +
		m.prefetchQueueDepth.export()
}
