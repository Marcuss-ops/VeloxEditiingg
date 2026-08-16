// Package taskrunner / report_metrics.go
//
// Report metrics — mergeStatsInto keeps the legacy dotted map as a
// compatibility projection while making the typed raw envelope the internal
// source consumed by the transport path.
//
// Also hosts the small type-coercion helpers (positiveIntegerToInt64,
// stringFromMap, floatFromMap, boolFromMap) used to read dotted-key
// counters robustly across the union of types an Executor might emit.
package taskrunner

import (
	"strings"

	"velox-worker-agent/internal/telemetry"
)

// mergeStatsInto is the bounded legacy compatibility path for the
// remaining unmigrated cache/blob/FFmpeg producers. It keeps the dotted
// projection available while overlaying the same facts onto RawMetrics;
// migrated producers never use the map as their source of truth.
//
// The TypedMetrics fields populated today are limited to what the
// worker's cache + blob stats providers actually expose:
//   - InputBytes / OutputBytes / BytesFromDrive / BytesFromBlobstore:
//     executor-supplied dotted keys (queue_bytes, drive_bytes, ...).
//     Falls back to 0 if absent.
//   - BytesFromLocalCache: cache.bytes (the local cache's authoritative
//     "currently occupied bytes" gauge).
//   - CpuTimeMs / PeakRssBytes / frames*: executor-supplied dotted keys.
//   - FfmpegSpeedRatio / EncodePasses / FinalConcatStreamCopy /
//     ConcatMode: executor-supplied dotted keys.
//   - CpuPricePerSecond / StoragePricePerGb / NetworkPricePerGb: 0 on
//     the worker — the master multiplies utilization × price to derive
//     cost. PR-3.6 will let the worker carry these into the typed
//     envelope once a sampler lands.
//
// Safe under zero-valued providers (noop fallbacks keep the merge
// safe and idempotent for tests).
// TypedMetricsFromMap is the legacy compatibility adapter for reports
// produced by unmigrated executors. New producers must set RawMetrics
// directly; this function exists only at compatibility boundaries.
func TypedMetricsFromMap(m map[string]interface{}) *telemetry.RawExecutionMetrics {
	return (LegacyMetricsAdapter{}).FromMap(m)
}

func (r *TaskRunner) mergeStatsInto(report *TaskExecutionReport, m map[string]interface{}) {
	if r.cacheStats != nil {
		cs := r.cacheStats.Stats()
		m["cache.hits"] = cs.Hits
		m["cache.misses"] = cs.Misses
		m["cache.evictions"] = cs.Evictions
		m["cache.corruptions"] = cs.Corruptions
		if report != nil && report.CacheBaselineSet {
			m["cache.hits"] = maxInt64(0, cs.Hits-report.CacheBaseline["hits"])
			m["cache.misses"] = maxInt64(0, cs.Misses-report.CacheBaseline["misses"])
			m["cache.evictions"] = maxInt64(0, cs.Evictions-report.CacheBaseline["evictions"])
			m["cache.corruptions"] = maxInt64(0, cs.Corruptions-report.CacheBaseline["corruptions"])
		}
		m["cache.entries"] = cs.Entries
		m["cache.bytes"] = cs.BytesUsed
		m["cache.pinned"] = cs.PinnedEntries
	}
	if r.blobStats != nil {
		bs := r.blobStats.Stats()
		m["blob.publish"] = bs.Publish
		m["blob.publish_failed"] = bs.PublishFailed
		m["blob.fetch"] = bs.Fetch
		m["blob.fetch_miss"] = bs.FetchMiss
		m["blob.fetch_corruption"] = bs.FetchCorruption
		m["blob.entries"] = bs.Entries
		m["blob.bytes"] = bs.Bytes
	}

	// Phase B2: per-attempt FFmpeg profile aggregation. Every canonical
	// FFmpegResult produced by the executors in this attempt is folded
	// here once, on success AND failure paths (mergeStatsInto runs from
	// both). "ffmpeg.aggregate" answers the batching question: total
	// spawn/setup (total_spawn_ms + total_first_output_ms) vs total
	// processing (total_processing_ms) across N processes, per operation.
	if report != nil && report.FFmpegProfiles != nil {
		if aggregate := report.FFmpegProfiles.Aggregate(); aggregate.ProcessCount > 0 {
			m["ffmpeg.aggregate"] = aggregate
		}
	}

	// Native SceneComposite metrics use engine/native namespaces while the
	// typed envelope uses the canonical report keys. Preserve an explicitly
	// supplied canonical value and only fill aliases when it is absent.
	for source, target := range map[string]string{
		"engine.frames":                "frames.encoded",
		"engine.frames_decoded":        "frames.decoded",
		"engine.frames_composited":     "frames.composited",
		"engine.speed_x":               "ffmpeg.speed_ratio",
		"engine.encode_passes":         "encode.passes",
		"engine.temp_bytes":            "temp.bytes.written",
		"engine.duration_seconds":      "media.duration.seconds",
		"engine.concat_mode":           "concat.mode",
		"quality.ffprobe.valid":        "ffprobe.valid",
		"quality.has.video.stream":     "has.video.stream",
		"quality.has.audio.stream":     "has.audio.stream",
		"quality.audio.track.count":    "audio.track.count",
		"quality.output.file.size":     "output.file.size",
		"quality.black.frame.ratio":    "black.frame.ratio",
		"quality.duration.diff.sec":    "duration.diff.sec",
		"quality.audio.sync.offset.ms": "audio.sync.offset.ms",
		"io.disk.read.bytes":           "disk.read.bytes",
		"io.disk.write.bytes":          "disk.write.bytes",
		"io.network.rx.bytes":          "network.rx.bytes",
		"io.network.tx.bytes":          "network.tx.bytes",
		"waste.wasted_cpu_ms":          "wasted.cpu.ms",
		"waste.wasted_download_bytes":  "wasted.download.bytes",
		"waste.completed_segments":     "completed.segments",
		"waste.error_component":        "error.component",
		"waste.error_phase":            "error.phase",
	} {
		if value, ok := m[source]; ok {
			if _, exists := m[target]; !exists {
				m[target] = value
			}
		}
	}
	// native.total_ms is wall-clock duration. Convert milliseconds to
	// seconds before projecting it into the typed wall-clock field; never
	// confuse it with CPU time.
	if value, ok := m["native.total_ms"]; ok {
		if _, exists := m["wall.clock.seconds"]; !exists {
			m["wall.clock.seconds"] = floatFromMap(value) / 1000
		}
	}

	// ── Scorecard v1 typed mirror ────────────────────────────────────
	// Built on top of the dotted-key map so the canonical typed shape
	// survives the F3 transition window. If the executor never produced
	// any metric counters, the typed mirror carries the cache.bytes
	// value alone (the only field CacheStatsProvider is authoritative
	// for today) and zeros elsewhere — correct behavior.
	// Asset-operation counters are attempt-scoped and authoritative when the
	// resolver supplied them. In particular, an explicit zero miss count on a
	// warm run must not fall back to the generic cache provider's stale delta.
	// The resolver emits the canonical asset.cache.lookups key; cache.lookups
	// is the legacy alias.
	cacheLookups := firstPresent(m, "asset.cache.lookups", "cache.lookups")
	cacheHits := firstPresent(m, "asset.cache.hit.count", "cache.hits")
	cacheMisses := firstPresent(m, "asset.cache.miss.count", "cache.misses")
	expectedCacheLookups := cacheHits + cacheMisses
	if cacheLookups == 0 || cacheLookups != expectedCacheLookups {
		// Keep the wire contract canonical even when an older executor or
		// an extension reports a stale explicit lookup count.
		cacheLookups = expectedCacheLookups
		m["cache.lookups"] = cacheLookups
	}
	typed := telemetry.TypedExecutionMetrics{
		BytesFromLocalCache: positiveIntegerToInt64(m["cache.bytes"]),
		InputBytes:          positiveIntegerToInt64(m["input.bytes"]),
		OutputBytes:         positiveIntegerToInt64(m["output.bytes"]),
		BytesFromDrive:      positiveIntegerToInt64(m["drive.bytes"]),
		BytesFromBlobstore:  positiveIntegerToInt64(m["blobstore.bytes"]),
		CpuTimeMs:           positiveIntegerToInt64(m["cpu.ms"]),
		PeakRssBytes:        positiveIntegerToInt64(m["rss.peak.bytes"]),
		FramesDecoded:       positiveIntegerToInt64(m["frames.decoded"]),
		FramesComposited:    positiveIntegerToInt64(m["frames.composited"]),
		FramesEncoded:       positiveIntegerToInt64(m["frames.encoded"]),
		ConcatMode:          stringFromMap(m["concat.mode"]),

		// Scorecard v2 resource / cache / quality counters surfaced by
		// executors as dotted keys. Missing keys stay zero.
		GpuTimeMs:              positiveIntegerToInt64(m["gpu.time.ms"]),
		PeakVramBytes:          positiveIntegerToInt64(m["vram.peak.bytes"]),
		TempBytesWritten:       positiveIntegerToInt64(m["temp.bytes.written"]),
		DuplicateDownloadBytes: positiveIntegerToInt64(m["duplicate.download.bytes"]),
		MediaDurationSeconds:   floatFromMap(m["media.duration.seconds"]),
		WallClockSeconds:       floatFromMap(m["wall.clock.seconds"]),

		FfprobeValid:      int32(positiveIntegerToInt64(m["ffprobe.valid"])),
		DurationDiffSec:   floatFromMap(m["duration.diff.sec"]),
		HasVideoStream:    boolFromMap(m["has.video.stream"]),
		HasAudioStream:    boolFromMap(m["has.audio.stream"]),
		AudioTrackCount:   int32(positiveIntegerToInt64(m["audio.track.count"])),
		OutputFileSize:    positiveIntegerToInt64(m["output.file.size"]),
		BlackFrameRatio:   floatFromMap(m["black.frame.ratio"]),
		AudioSyncOffsetMs: positiveIntegerToInt64(m["audio.sync.offset.ms"]),
		OutputSha256:      stringFromMap(m["output.sha256"]),

		CpuPercentPeak: floatFromMap(m["cpu.percent.peak"]),
		DiskReadBytes:  positiveIntegerToInt64(m["disk.read.bytes"]),
		DiskWriteBytes: positiveIntegerToInt64(m["disk.write.bytes"]),
		NetworkRxBytes: positiveIntegerToInt64(m["network.rx.bytes"]),
		NetworkTxBytes: positiveIntegerToInt64(m["network.tx.bytes"]),
		IowaitMs:       positiveIntegerToInt64(m["iowait.ms"]),
		OpenFdsPeak:    positiveIntegerToInt64(m["open.fds.peak"]),

		AssetCacheHitCount:  cacheHits,
		AssetCacheMissCount: cacheMisses,
		BlobCacheHitCount:   firstPresent(m, "blob.cache.hit.count", "blob.fetch"),
		BlobCacheMissCount:  firstPresent(m, "blob.cache.miss.count", "blob.fetch_miss"),
		RenderCacheHitCount: positiveIntegerToInt64(m["render.cache.hit.count"]),

		WastedCpuMs:           positiveIntegerToInt64(m["wasted.cpu.ms"]),
		WastedDownloadBytes:   positiveIntegerToInt64(m["wasted.download.bytes"]),
		CompletedSegments:     int32(positiveIntegerToInt64(m["completed.segments"])),
		ErrorComponent:        stringFromMap(m["error.component"]),
		ErrorPhase:            stringFromMap(m["error.phase"]),
		TelemetryCoverageJSON: stringFromMap(m["telemetry.coverage.json"]),
		TelemetryComplete:     boolFromMap(m["telemetry.complete"]),
		TelemetryCPUSource:    stringFromMap(m["telemetry.cpu.source"]),
		CacheLookups:          cacheLookups,
		UniqueAssetsRequested: positiveIntegerToInt64(m["unique.assets.requested"]),
		// Phase A1: attempt-scoped download volume surfaced by the
		// CacheResolver. asset.cache.download.* is the canonical dotted
		// surface (internal/worker/asset_metrics.go); they ride the typed
		// envelope so the master can persist them in SQL (migration 147).
		CacheDownloadCount: positiveIntegerToInt64(m["asset.cache.download.count"]),
		CacheDownloadBytes: positiveIntegerToInt64(m["asset.cache.download.bytes"]),
	}

	// CPU capacity is a host-level property, not something the executor
	// counter map carries. Detect it once and stamp it on every report.
	cp := telemetry.DetectCPUCapacity()
	typed.LogicalCpuCount = int32(cp.LogicalCPUCount)
	typed.CpuQuota = cp.CPUQuota
	typed.EffectiveCpuCount = int32(cp.EffectiveCPUCount)
	if v, ok := m["ffmpeg.speed_ratio"].(float64); ok {
		typed.FfmpegSpeedRatio = v
	}
	// encode.passes is proto3 int32 — the legacy dotted-key producer
	// may emit it as int32 or int64 depending on platform.
	if v, ok := m["encode.passes"].(int32); ok {
		typed.EncodePasses = v
	} else if v, ok := m["encode.passes"].(int64); ok && v >= 0 && v <= 0x7fffffff {
		typed.EncodePasses = int32(v)
	}
	// final_concat_stream_copy is conventionally a bool in the proto
	// and a JSON-style key in the legacy map. The native sidecar's
	// canonical concat_mode is authoritative when the legacy bool alias is
	// absent; stream_copy and packet_copy both mean the final video stream was
	// never re-encoded. packet_copy is the native engine's canonical value for
	// the strict clip concatenation path.
	// Some producers materialize the legacy boolean with its proto zero value
	// even when the canonical concat mode is packet_copy. A false value must
	// therefore not mask the authoritative mode; explicit true still wins.
	legacyStreamCopy, hasLegacyStreamCopy := m["final.concat.stream_copy"].(bool)
	typed.FinalConcatStreamCopy = legacyStreamCopy ||
		strings.EqualFold(typed.ConcatMode, "stream_copy") ||
		strings.EqualFold(typed.ConcatMode, "packet_copy")
	if !hasLegacyStreamCopy {
		// The mode-derived value above is the complete signal when the legacy
		// alias was not emitted by the producer.
		typed.FinalConcatStreamCopy = strings.EqualFold(typed.ConcatMode, "stream_copy") ||
			strings.EqualFold(typed.ConcatMode, "packet_copy")
	}
	if report.RawMetrics == nil {
		report.RawMetrics = &typed
	} else {
		// A migrated producer may already have supplied facts that do not
		// have a legacy dotted-key representation. Overlay only keys that
		// actually exist in the compatibility map; never replace the raw
		// envelope with zero values manufactured by the adapter.
		overlayLegacyRawMetrics(report.RawMetrics, typed, m)
	}
	report.TypedMetrics = report.RawMetrics
	// ── End typed raw projection ─────────────────────────────────────
}

func overlayLegacyRawMetrics(dst *telemetry.RawExecutionMetrics, typed telemetry.RawExecutionMetrics, m map[string]interface{}) {
	if dst == nil {
		return
	}
	overlay := func(key string, assign func()) {
		if _, ok := m[key]; ok {
			assign()
		}
	}
	overlay("input.bytes", func() { dst.InputBytes = typed.InputBytes })
	overlay("output.bytes", func() { dst.OutputBytes = typed.OutputBytes })
	overlay("drive.bytes", func() { dst.BytesFromDrive = typed.BytesFromDrive })
	overlay("blobstore.bytes", func() { dst.BytesFromBlobstore = typed.BytesFromBlobstore })
	overlay("cache.bytes", func() { dst.BytesFromLocalCache = typed.BytesFromLocalCache })
	overlay("cpu.ms", func() { dst.CpuTimeMs = typed.CpuTimeMs })
	overlay("rss.peak.bytes", func() { dst.PeakRssBytes = typed.PeakRssBytes })
	overlay("frames.decoded", func() { dst.FramesDecoded = typed.FramesDecoded })
	overlay("frames.composited", func() { dst.FramesComposited = typed.FramesComposited })
	overlay("frames.encoded", func() { dst.FramesEncoded = typed.FramesEncoded })
	overlay("ffmpeg.speed_ratio", func() { dst.FfmpegSpeedRatio = typed.FfmpegSpeedRatio })
	overlay("encode.passes", func() { dst.EncodePasses = typed.EncodePasses })
	overlay("concat.mode", func() {
		dst.ConcatMode = typed.ConcatMode
		// The executor may have initialized RawMetrics with the proto zero
		// value before the compatibility projection runs. Keep the canonical
		// concat mode authoritative for the legacy boolean as well.
		dst.FinalConcatStreamCopy = typed.FinalConcatStreamCopy
	})
	overlay("gpu.time.ms", func() { dst.GpuTimeMs = typed.GpuTimeMs })
	overlay("vram.peak.bytes", func() { dst.PeakVramBytes = typed.PeakVramBytes })
	overlay("temp.bytes.written", func() { dst.TempBytesWritten = typed.TempBytesWritten })
	overlay("duplicate.download.bytes", func() { dst.DuplicateDownloadBytes = typed.DuplicateDownloadBytes })
	overlay("media.duration.seconds", func() { dst.MediaDurationSeconds = typed.MediaDurationSeconds })
	overlay("wall.clock.seconds", func() { dst.WallClockSeconds = typed.WallClockSeconds })
	overlay("ffprobe.valid", func() { dst.FfprobeValid = typed.FfprobeValid })
	overlay("duration.diff.sec", func() { dst.DurationDiffSec = typed.DurationDiffSec })
	overlay("has.video.stream", func() { dst.HasVideoStream = typed.HasVideoStream })
	overlay("has.audio.stream", func() { dst.HasAudioStream = typed.HasAudioStream })
	overlay("audio.track.count", func() { dst.AudioTrackCount = typed.AudioTrackCount })
	overlay("output.file.size", func() { dst.OutputFileSize = typed.OutputFileSize })
	overlay("black.frame.ratio", func() { dst.BlackFrameRatio = typed.BlackFrameRatio })
	overlay("audio.sync.offset.ms", func() { dst.AudioSyncOffsetMs = typed.AudioSyncOffsetMs })
	overlay("output.sha256", func() { dst.OutputSha256 = typed.OutputSha256 })
	overlay("cpu.percent.peak", func() { dst.CpuPercentPeak = typed.CpuPercentPeak })
	overlay("disk.read.bytes", func() { dst.DiskReadBytes = typed.DiskReadBytes })
	overlay("disk.write.bytes", func() { dst.DiskWriteBytes = typed.DiskWriteBytes })
	overlay("network.rx.bytes", func() { dst.NetworkRxBytes = typed.NetworkRxBytes })
	overlay("network.tx.bytes", func() { dst.NetworkTxBytes = typed.NetworkTxBytes })
	overlay("iowait.ms", func() { dst.IowaitMs = typed.IowaitMs })
	overlay("open.fds.peak", func() { dst.OpenFdsPeak = typed.OpenFdsPeak })
	overlay("asset.cache.hit.count", func() { dst.AssetCacheHitCount = typed.AssetCacheHitCount })
	overlay("asset.cache.miss.count", func() { dst.AssetCacheMissCount = typed.AssetCacheMissCount })
	overlay("blob.fetch", func() { dst.BlobCacheHitCount = typed.BlobCacheHitCount })
	overlay("blob.fetch_miss", func() { dst.BlobCacheMissCount = typed.BlobCacheMissCount })
	overlay("render.cache.hit.count", func() { dst.RenderCacheHitCount = typed.RenderCacheHitCount })
	overlay("wasted.cpu.ms", func() { dst.WastedCpuMs = typed.WastedCpuMs })
	overlay("wasted.download.bytes", func() { dst.WastedDownloadBytes = typed.WastedDownloadBytes })
	overlay("completed.segments", func() { dst.CompletedSegments = typed.CompletedSegments })
	overlay("error.component", func() { dst.ErrorComponent = typed.ErrorComponent })
	overlay("error.phase", func() { dst.ErrorPhase = typed.ErrorPhase })
	overlay("telemetry.coverage.json", func() { dst.TelemetryCoverageJSON = typed.TelemetryCoverageJSON })
	overlay("telemetry.complete", func() { dst.TelemetryComplete = typed.TelemetryComplete })
	overlay("telemetry.cpu.source", func() { dst.TelemetryCPUSource = typed.TelemetryCPUSource })
	overlay("cache.lookups", func() { dst.CacheLookups = typed.CacheLookups })
	overlay("unique.assets.requested", func() { dst.UniqueAssetsRequested = typed.UniqueAssetsRequested })
	overlay("asset.cache.download.count", func() { dst.CacheDownloadCount = typed.CacheDownloadCount })
	overlay("asset.cache.download.bytes", func() { dst.CacheDownloadBytes = typed.CacheDownloadBytes })
}

// positiveIntegerToInt64 reads dotted-key counters (int64 / int32 /
// int / float64 / uint64 / uint32) and returns a non-negative int64
// compatible with proto3 wire shape. Negatives are floored to 0.
// uint64 inputs are clipped at MaxInt64 to keep varint serialization
// honest rather than silently wrapping at -1.
func positiveIntegerToInt64(v any) int64 {
	const maxInt64 = int64(^uint64(0) >> 1)
	switch x := v.(type) {
	case nil:
		return 0
	case int64:
		if x < 0 {
			return 0
		}
		return x
	case int32:
		if x < 0 {
			return 0
		}
		return int64(x)
	case int:
		if x < 0 {
			return 0
		}
		return int64(x)
	case uint64:
		if x > uint64(maxInt64) {
			return maxInt64
		}
		return int64(x)
	case uint32:
		return int64(x)
	case float64:
		if x <= 0 {
			return 0
		}
		return int64(x)
	}
	return 0
}

func stringFromMap(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func floatFromMap(v any) float64 {
	if v == nil {
		return 0
	}
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int64:
		return float64(x)
	case int32:
		return float64(x)
	case int:
		return float64(x)
	}
	return 0
}

func boolFromMap(v any) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// firstPresent returns the first numerically valid value whose key exists.
// Unlike firstPositive, zero is meaningful here: a producer that reports an
// attempt-scoped counter has authority over the fallback provider even when
// that counter is zero.
func firstPresent(metrics map[string]interface{}, keys ...string) int64 {
	for _, key := range keys {
		value, ok := metrics[key]
		if !ok {
			continue
		}
		return positiveIntegerToInt64(value)
	}
	return 0
}
