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
