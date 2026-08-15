// prometheus_cache.go — cache and asset-download metric methods.
package telemetry

import "time"

func (m *PrometheusMetrics) RecordAssetCacheHit(_ string) {
	m.assetCacheHit.inc("asset")
	m.assetCacheHitsCanonical.inc("total")
}
func (m *PrometheusMetrics) RecordAssetCacheMiss(_ string) {
	m.assetCacheMiss.inc("asset")
	m.assetCacheMissesCanonical.inc("total")
}
func normalizeCacheResult(result string) string {
	switch result {
	case "hit", "miss":
		return result
	default:
		return "other"
	}
}
func normalizeCacheReason(reason string) string {
	switch reason {
	case "pressure", "ttl", "invalid", "protected", "leased", "grace", "in_flight", "stale_snapshot", "no_snapshot", "error":
		return reason
	default:
		return "other"
	}
}
func (m *PrometheusMetrics) RecordCacheRequest(result string) {
	m.assetCacheRequests.inc(normalizeCacheResult(result))
}

// RecordCacheRequestN increments the low-cardinality cache-request counter
// by n. It is the batch projection used by the PrometheusSink (per-attempt
// cache deltas published at attempt Stop); single-count producers keep
// using RecordCacheRequest during the migration window.
func (m *PrometheusMetrics) RecordCacheRequestN(result string, n int64) {
	if n <= 0 {
		return
	}
	m.assetCacheRequests.add(normalizeCacheResult(result), float64(n))
}

// CacheRequestCount returns the current low-cardinality cache-request count.
// It is intended for diagnostics and deterministic tests, not labels.
func (m *PrometheusMetrics) CacheRequestCount(result string) float64 {
	return m.assetCacheRequests.get(normalizeCacheResult(result))
}

// DuplicateDownloadCount returns the AssetDownloadManager-coalesced
// duplicate download counter (diagnostics and deterministic tests only).
func (m *PrometheusMetrics) DuplicateDownloadCount() float64 {
	return m.assetCacheDuplicateDownloads.get("asset")
}

// DuplicateDownloadBytes returns the bytes a duplicate download would
// have consumed (coalesced by AssetDownloadManager; diagnostics/tests only).
func (m *PrometheusMetrics) DuplicateDownloadBytes() float64 {
	return m.assetCacheDuplicateDownloadBytes.get("asset")
}

// WorkerErrorCount returns the worker-side task-failure counter
// (diagnostics and deterministic tests only).
func (m *PrometheusMetrics) CacheEvictionCount(reason string) float64 {
	return m.assetCacheEvictions.get(normalizeCacheReason(reason))
}
func (m *PrometheusMetrics) RecordCacheEviction(reason string) {
	m.assetCacheEvictions.inc(normalizeCacheReason(reason))
}
func (m *PrometheusMetrics) RecordCacheEvictions(reason string, count int) {
	if count > 0 {
		m.assetCacheEvictions.add(normalizeCacheReason(reason), float64(count))
	}
}
func (m *PrometheusMetrics) RecordCacheDownload(bytes int64, duration time.Duration) {
	m.assetCacheDownloads.inc("asset")
	m.assetCacheDownloadBytes.add("asset", float64(bytes))
	m.assetCacheDownloadMS.observe("asset", duration.Seconds())
	m.assetDownloadSecondsCanonical.observe("total", duration.Seconds())
}

// RecordCacheDuplicateDownload counts a concurrent duplicate asset
// request that AssetDownloadManager coalesced onto an in-flight download
// (the worker-side dedup counter the parallelism certification
// harness reads via velox_cache_duplicate_downloads_total). The
// bytes argument is the size of the shared result when it is on
// disk; 0 is accepted (count-only) when the path is unavailable.
func (m *PrometheusMetrics) RecordCacheDuplicateDownload(bytes int64) {
	m.assetCacheDuplicateDownloads.inc("asset")
	if bytes > 0 {
		m.assetCacheDuplicateDownloadBytes.add("asset", float64(bytes))
	}
}

func (m *PrometheusMetrics) RecordPrefetchRequested() { m.prefetchRequested.inc("asset") }
func (m *PrometheusMetrics) RecordPrefetchDownloaded(bytes int64) {
	m.prefetchDownloaded.inc("asset")
	if bytes > 0 {
		m.prefetchDownloadedBytes.add("asset", float64(bytes))
	}
}
func (m *PrometheusMetrics) RecordPrefetchUseful() { m.prefetchUseful.inc("asset") }
func (m *PrometheusMetrics) RecordPrefetchWastedBytes(bytes int64) {
	if bytes > 0 {
		m.prefetchWastedBytes.add("asset", float64(bytes))
	}
}

func prefetchDistanceLabel(distance int) string {
	switch distance {
	case 1:
		return "d1"
	case 2:
		return "d2"
	default:
		return "d3"
	}
}

func (m *PrometheusMetrics) RecordPrefetchQueueWait(distance int, duration time.Duration) {
	m.prefetchQueueWaitSeconds.observe(prefetchDistanceLabel(distance), duration.Seconds())
}

func (m *PrometheusMetrics) RecordPrefetchResolve(distance int, duration time.Duration) {
	m.prefetchResolveSeconds.observe(prefetchDistanceLabel(distance), duration.Seconds())
}

func (m *PrometheusMetrics) RecordPrefetchReadyLead(distance int, lead time.Duration) {
	m.prefetchReadyLeadSeconds.observe(prefetchDistanceLabel(distance), lead.Seconds())
}

func (m *PrometheusMetrics) SetPrefetchOperational(active, queued int) {
	if active < 0 {
		active = 0
	}
	if queued < 0 {
		queued = 0
	}
	m.prefetchActive.set("total", float64(active))
	m.prefetchQueueDepth.set("total", float64(queued))
}

// RecordWorkerError increments the worker-side task-failure counter
// (velox_worker_errors_total) at the same site that bumps
// tasksFailed, so operators can delta it across a batch window.
func (m *PrometheusMetrics) RecordCacheVerify(duration time.Duration) {
	m.assetCacheVerifyMS.observe("asset", duration.Seconds())
}
func (m *PrometheusMetrics) RecordCacheCleanup(duration time.Duration) {
	m.assetCacheCleanupMS.observe("pass", duration.Seconds())
}
func (m *PrometheusMetrics) RecordCacheCleanupSkip(reason string) {
	m.assetCacheCleanupSkip.inc(normalizeCacheReason(reason))
}
func (m *PrometheusMetrics) RecordCacheCleanupSkips(reason string, count int) {
	if count > 0 {
		m.assetCacheCleanupSkip.add(normalizeCacheReason(reason), float64(count))
		if reason == "protected" {
			m.assetCacheProtectedSkipCanonical.add("total", float64(count))
		}
	}
}
func (m *PrometheusMetrics) SetCacheSize(entries int, bytes int64) {
	m.assetCacheEntries.set("total", float64(entries))
	m.assetCacheSizeBytes.set("total", float64(bytes))
}
func (m *PrometheusMetrics) SetAssetDownloadOperational(active, queued, ready, failed, cacheHits int, downloaded, total int64, throughput float64, eta float64, coalesced uint64) {
	m.assetDownloadActive.set("total", float64(active))
	m.assetDownloadQueued.set("total", float64(queued))
	m.assetDownloadReady.set("total", float64(ready))
	m.assetDownloadFailed.set("total", float64(failed))
	m.assetDownloadCacheHits.set("total", float64(cacheHits))
	m.assetDownloadBytes.set("total", float64(downloaded))
	m.assetDownloadTotalBytes.set("total", float64(total))
	m.assetDownloadThroughput.set("total", throughput)
	m.assetDownloadETA.set("total", eta)
	// Counters are monotonic; the manager supplies the current cumulative
	// value, so only move the exported value forward under one lock.
	m.assetDownloadCoalesced.setMonotonic("total", float64(coalesced))
}
// AddAssetDownloadChunksActive adjusts the active-chunk gauge by delta. It
// is additive so concurrent chunked transfers SUM on the shared
// low-cardinality "total" label; every positive delta is balanced by a
// matching negative one when that transfer's chunks settle.
func (m *PrometheusMetrics) AddAssetDownloadChunksActive(delta int) {
	m.assetDownloadChunksActive.add("total", float64(delta))
}

// SetAssetDownloadChunkThroughput publishes the current chunked transfer
// throughput (bytes/s). It is last-writer-wins across concurrent chunked
// transfers; the precise aggregate per-transfer rate stays available on
// velox_asset_download_throughput_bytes_per_second. Producers reset it to
// zero when their transfer ends so a stale rate never lingers.
func (m *PrometheusMetrics) SetAssetDownloadChunkThroughput(bps float64) {
	if bps < 0 {
		bps = 0
	}
	m.assetDownloadChunkThroughput.set("total", bps)
}

// AssetDownloadChunksActive returns the current active-chunk gauge value
// (diagnostics and deterministic tests only).
func (m *PrometheusMetrics) AssetDownloadChunksActive() float64 {
	return m.assetDownloadChunksActive.get("total")
}

// AssetDownloadChunkThroughput returns the current chunked-throughput gauge
// value (diagnostics and deterministic tests only).
func (m *PrometheusMetrics) AssetDownloadChunkThroughput() float64 {
	return m.assetDownloadChunkThroughput.get("total")
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
