// Package taskrunner / report_metrics.go
//
// Provider-owned metrics are merged directly into the canonical typed
// RawExecutionMetrics envelope. Executor and provider facts no longer pass
// through a dotted map or a reverse type-coercion adapter.
package taskrunner

import (
	"strings"

	"velox-worker-agent/internal/telemetry"
)

// mergeStatsInto enriches the report's typed raw envelope with facts owned by
// worker-wide providers. Attempt-scoped resolver counters already present in
// RawMetrics remain authoritative and are never overwritten by lifetime
// provider gauges.
func (r *TaskRunner) mergeStatsInto(report *TaskExecutionReport) {
	if report == nil {
		return
	}
	raw := report.RawMetrics
	if raw == nil {
		raw = &telemetry.RawExecutionMetrics{}
		report.RawMetrics = raw
	}

	if r.cacheStats != nil {
		cs := r.cacheStats.Stats()
		cacheHits := cs.Hits
		cacheMisses := cs.Misses
		if report.CacheBaselineSet {
			cacheHits = maxInt64(0, cs.Hits-report.CacheBaseline["hits"])
			cacheMisses = maxInt64(0, cs.Misses-report.CacheBaseline["misses"])
		}
		if raw.BytesFromLocalCache == 0 {
			raw.BytesFromLocalCache = cs.BytesUsed
		}
		if raw.AssetCacheHitCount == 0 && raw.AssetCacheMissCount == 0 {
			raw.AssetCacheHitCount = cacheHits
			raw.AssetCacheMissCount = cacheMisses
		}
		if raw.CacheLookups == 0 {
			raw.CacheLookups = raw.AssetCacheHitCount + raw.AssetCacheMissCount
		}
		raw.JobAssetCacheBytesUsed = cs.BytesUsed
	}

	if r.blobStats != nil {
		bs := r.blobStats.Stats()
		if raw.BlobCacheHitCount == 0 && raw.BlobCacheMissCount == 0 {
			raw.BlobCacheHitCount = bs.Fetch
			raw.BlobCacheMissCount = bs.FetchMiss
		}
	}

	cp := telemetry.DetectCPUCapacity()
	raw.LogicalCpuCount = int32(cp.LogicalCPUCount)
	raw.CpuQuota = cp.CPUQuota
	raw.EffectiveCpuCount = int32(cp.EffectiveCPUCount)
	raw.FinalConcatStreamCopy = concatModeIsStreamCopy(raw.ConcatMode)
	report.TypedMetrics = raw
}

func concatModeIsStreamCopy(mode string) bool {
	return strings.EqualFold(mode, "stream_copy") ||
		strings.EqualFold(mode, "packet_copy") ||
		strings.EqualFold(mode, "mixed_packet")
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
