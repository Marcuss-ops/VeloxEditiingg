// prometheus_artifact.go — cache-pressure eviction and artifact tmpfs
// staging metric methods.
package telemetry

// RecordCacheEvictedBytes accumulates the physical bytes removed by a cache
// pressure-eviction pass. It is the byte-counter companion of
// velox_cache_evictions_total and is monotonic across passes.
func (m *PrometheusMetrics) RecordCacheEvictedBytes(bytes int64) {
	if bytes > 0 {
		m.assetCacheEvictedBytes.add("total", float64(bytes))
	}
}

// SetCacheDiskUsagePercent publishes the cache-root disk-usage percentage
// observed by the pressure controller. It is a last-writer-wins gauge
// refreshed on every pressure pass (including the below-high-watermark
// no-op pass, so the gauge tracks reality even when nothing is evicted).
func (m *PrometheusMetrics) SetCacheDiskUsagePercent(percent int) {
	if percent < 0 {
		percent = 0
	}
	m.assetCacheDiskUsagePercent.set("total", float64(percent))
}

// SetArtifactTmpfsReservedBytes publishes the RAM bytes currently reserved
// for ARTIFACT_STAGING tmpfs placements. The storage resolver reports it on
// every reserve/release so the gauge tracks the live ledger.
func (m *PrometheusMetrics) SetArtifactTmpfsReservedBytes(reserved int64) {
	if reserved < 0 {
		reserved = 0
	}
	m.artifactTmpfsReservedBytes.set("total", float64(reserved))
}

// RecordArtifactTmpfsSpill counts one tmpfs→NVMe spill and its byte total.
// bytes may be 0 when the size is unavailable (count-only spill).
func (m *PrometheusMetrics) RecordArtifactTmpfsSpill(bytes int64) {
	m.artifactTmpfsSpillTotal.inc("total")
	if bytes > 0 {
		m.artifactTmpfsSpillBytes.add("total", float64(bytes))
	}
}

// normalizeFallbackReason maps an ARTIFACT_STAGING NVMe fallback reason onto
// the low-cardinality set exposed as the velox_artifact_nvme_fallback_total
// `reason` label. Unknown reasons collapse to "other".
func normalizeFallbackReason(reason string) string {
	switch reason {
	case "tmpfs_disabled", "unknown_size", "no_space", "statfs_error", "reservation_failed":
		return reason
	default:
		return "other"
	}
}

// RecordArtifactNvmeFallback counts one ARTIFACT_STAGING placement that fell
// back to durable NVMe, labelled by the reason the tmpfs reservation could
// not be satisfied.
func (m *PrometheusMetrics) RecordArtifactNvmeFallback(reason string) {
	m.artifactNvmeFallback.inc(normalizeFallbackReason(reason))
}
