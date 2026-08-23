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
// TypedMetricsFromMap constructs a RawExecutionMetrics from a legacy
// dotted-key map. Exists only for test compatibility; production code
// routes through mergeStatsInto(report) which populates RawMetrics from
// cache/blob providers directly.
func TypedMetricsFromMap(m map[string]interface{}) *telemetry.RawExecutionMetrics {
	report := &TaskExecutionReport{Metrics: m}
	// Populate RawMetrics from the dotted-key map so the test surface
	// remains backwards-compatible.
	typed := legacyMapToTyped(m)
	report.RawMetrics = &typed
	(&TaskRunner{}).mergeStatsInto(report)
	return report.RawMetrics
}

// legacyMapToTyped converts the remaining dotted-key map entries into
// a typed RawExecutionMetrics. This is the test-only reverse compatibility
// path; production executors set RawMetrics directly.
func legacyMapToTyped(m map[string]interface{}) telemetry.RawExecutionMetrics {
	typed := telemetry.RawExecutionMetrics{
		BytesFromLocalCache:   positiveIntegerToInt64(firstValue(m, "cache.bytes")),
		InputBytes:            positiveIntegerToInt64(firstValue(m, "input.bytes")),
		OutputBytes:           positiveIntegerToInt64(firstValue(m, "output.bytes")),
		BytesFromDrive:        positiveIntegerToInt64(firstValue(m, "drive.bytes")),
		DiskReadBytes:         positiveIntegerToInt64(firstValue(m, "disk.read.bytes", "io.disk.read.bytes")),
		BytesFromBlobstore:    positiveIntegerToInt64(firstValue(m, "blobstore.bytes")),
		CpuTimeMs:             positiveIntegerToInt64(firstValue(m, "cpu.ms")),
		PeakRssBytes:          positiveIntegerToInt64(firstValue(m, "rss.peak.bytes")),
		FramesDecoded:         positiveIntegerToInt64(firstValue(m, "frames.decoded")),
		FramesComposited:      positiveIntegerToInt64(firstValue(m, "frames.composited")),
		FramesEncoded:         positiveIntegerToInt64(firstValue(m, "frames.encoded", "engine.frames")),
		ConcatMode:            stringFromMap(firstString(m, "concat.mode", "engine.concat_mode")),
		FfmpegSpeedRatio:      floatFromMap(firstValue(m, "ffmpeg.speed_ratio", "engine.speed_x")),
		EncodePasses:          int32(positiveIntegerToInt64(firstValue(m, "encode.passes", "engine.encode_passes"))),
		TempBytesWritten:      positiveIntegerToInt64(firstValue(m, "temp.bytes.written")),
		MediaDurationSeconds:  floatFromMap(firstValue(m, "media.duration.seconds")),
		WallClockSeconds:      floatFromMap(firstValue(m, "wall.clock.seconds")),
		FfprobeValid:          int32(positiveIntegerToInt64(firstValue(m, "ffprobe.valid", "quality.ffprobe.valid"))),
		BlackFrameRatio:       floatFromMap(firstValue(m, "black.frame.ratio", "quality.black.frame.ratio")),
		WastedCpuMs:           positiveIntegerToInt64(firstValue(m, "wasted.cpu.ms")),
		WastedDownloadBytes:   positiveIntegerToInt64(firstValue(m, "wasted.download.bytes")),
		CompletedSegments:     int32(positiveIntegerToInt64(firstValue(m, "completed.segments"))),
		ErrorComponent:        stringFromMap(firstString(m, "error.component")),
		ErrorPhase:            stringFromMap(firstString(m, "error.phase")),
		AssetCacheHitCount:    firstPresent(m, "asset.cache.hit.count", "cache.hits"),
		AssetCacheMissCount:   firstPresent(m, "asset.cache.miss.count", "cache.misses"),
		BlobCacheHitCount:     firstPresent(m, "blob.cache.hit.count", "blob.fetch"),
		BlobCacheMissCount:    firstPresent(m, "blob.cache.miss.count", "blob.fetch_miss"),
		CacheDownloadCount:    positiveIntegerToInt64(m["asset.cache.download.count"]),
		CacheDownloadBytes:    positiveIntegerToInt64(m["asset.cache.download.bytes"]),
		CacheLookups:          firstPresent(m, "asset.cache.lookups", "cache.lookups"),
		UniqueAssetsRequested: positiveIntegerToInt64(m["unique.assets.requested"]),
		RenderCacheHitCount:   positiveIntegerToInt64(m["render.cache.hit.count"]),
	}
	if value := firstValue(m, "native.total_ms"); value != nil {
		typed.WallClockSeconds = floatFromMap(value) / 1000
	}
	if value := firstValue(m, "native.total_ms"); value != nil {
		typed.WallClockSeconds = floatFromMap(value) / 1000
	}
	// Concat mode → stream copy derivation.
	typed.FinalConcatStreamCopy = concatModeIsStreamCopy(typed.ConcatMode)
	return typed
}

// rawMetricsToLegacyMap converts the canonical typed raw envelope into the
// legacy dotted-key map consumed by mergeStatsInto and the remaining
// unmigrated report consumers (asset_metrics, report_observability).
// This is the single reverse-projection point; executors write only
// RawMetrics and the runner round-trips through this map for backward
// compatibility. Phase 3 will eliminate the map entirely.
func rawMetricsToLegacyMap(raw *telemetry.RawExecutionMetrics) map[string]interface{} {
	if raw == nil {
		return make(map[string]interface{})
	}
	m := make(map[string]interface{}, 200)

	// ── Byte accounting ───────────────────────────────────────────────
	setI64(m, "input.bytes", raw.InputBytes)
	setI64(m, "output.bytes", raw.OutputBytes)
	setI64(m, "drive.bytes", raw.BytesFromDrive)
	setI64(m, "blobstore.bytes", raw.BytesFromBlobstore)
	setI64(m, "cache.bytes", raw.BytesFromLocalCache)

	// ── CPU + memory ───────────────────────────────────────────────────
	setI64(m, "cpu.ms", raw.CpuTimeMs)
	setI64(m, "rss.peak.bytes", raw.PeakRssBytes)

	// ── Engine counters ────────────────────────────────────────────────
	setI64(m, "frames.decoded", raw.FramesDecoded)
	setI64(m, "frames.composited", raw.FramesComposited)
	setI64(m, "frames.encoded", raw.FramesEncoded)
	if raw.FfmpegSpeedRatio != 0 {
		m["ffmpeg.speed_ratio"] = raw.FfmpegSpeedRatio
	}
	setI32(m, "encode.passes", raw.EncodePasses)
	if raw.ConcatMode != "" {
		m["concat.mode"] = raw.ConcatMode
	}
	setBool(m, "final.concat.stream_copy", raw.FinalConcatStreamCopy)

	// ── Scorecard v2 resource counters ─────────────────────────────────
	setI64(m, "gpu.time.ms", raw.GpuTimeMs)
	setI64(m, "vram.peak.bytes", raw.PeakVramBytes)
	setI64(m, "temp.bytes.written", raw.TempBytesWritten)
	setI64(m, "duplicate.download.bytes", raw.DuplicateDownloadBytes)
	setF64(m, "media.duration.seconds", raw.MediaDurationSeconds)
	setF64(m, "wall.clock.seconds", raw.WallClockSeconds)
	setF64(m, "realtime.factor", raw.RealTimeFactor)
	setF64(m, "throughput.x", raw.ThroughputX)

	// ── Quality validation ─────────────────────────────────────────────
	setI32(m, "ffprobe.valid", raw.FfprobeValid)
	setF64(m, "duration.diff.sec", raw.DurationDiffSec)
	setBool(m, "has.video.stream", raw.HasVideoStream)
	setBool(m, "has.audio.stream", raw.HasAudioStream)
	setI32(m, "audio.track.count", raw.AudioTrackCount)
	setI64(m, "output.file.size", raw.OutputFileSize)
	setF64(m, "black.frame.ratio", raw.BlackFrameRatio)
	setI64(m, "audio.sync.offset.ms", raw.AudioSyncOffsetMs)
	setStr(m, "output.sha256", raw.OutputSha256)

	// ── Resource snapshot ──────────────────────────────────────────────
	setF64(m, "cpu.percent.peak", raw.CpuPercentPeak)
	setI64(m, "disk.read.bytes", raw.DiskReadBytes)
	setI64(m, "disk.write.bytes", raw.DiskWriteBytes)
	setI64(m, "network.rx.bytes", raw.NetworkRxBytes)
	setI64(m, "network.tx.bytes", raw.NetworkTxBytes)
	setI64(m, "iowait.ms", raw.IowaitMs)
	setI64(m, "open.fds.peak", raw.OpenFdsPeak)

	// ── Cache hit/miss counters ────────────────────────────────────────
	setI64(m, "asset.cache.hit.count", raw.AssetCacheHitCount)
	setI64(m, "asset.cache.miss.count", raw.AssetCacheMissCount)
	setI64(m, "blob.cache.hit.count", raw.BlobCacheHitCount)
	setI64(m, "blob.cache.miss.count", raw.BlobCacheMissCount)
	setI64(m, "render.cache.hit.count", raw.RenderCacheHitCount)

	// ── Failure attribution ────────────────────────────────────────────
	setI64(m, "wasted.cpu.ms", raw.WastedCpuMs)
	setI64(m, "wasted.download.bytes", raw.WastedDownloadBytes)
	setI32(m, "completed.segments", raw.CompletedSegments)
	setStr(m, "error.component", raw.ErrorComponent)
	setStr(m, "error.phase", raw.ErrorPhase)

	// ── Telemetry metadata ─────────────────────────────────────────────
	setStr(m, "telemetry.coverage.json", raw.TelemetryCoverageJSON)
	setBool(m, "telemetry.complete", raw.TelemetryComplete)
	setStr(m, "telemetry.cpu.source", raw.TelemetryCPUSource)
	setI64(m, "cache.lookups", raw.CacheLookups)
	setI64(m, "unique.assets.requested", raw.UniqueAssetsRequested)
	setI64(m, "asset.cache.download.count", raw.CacheDownloadCount)
	setI64(m, "asset.cache.download.bytes", raw.CacheDownloadBytes)

	// ── Fine-grained phase timings ─────────────────────────────────────
	setI64(m, "queue.wait.ms", raw.QueueWaitMs)
	setI64(m, "job.setup.ms", raw.JobSetupMs)
	setI64(m, "asset.resolve.ms", raw.AssetResolveMs)
	setI64(m, "asset.download.ms", raw.AssetDownloadMs)
	setI64(m, "asset.verify.ms", raw.AssetVerifyMs)
	setI64(m, "asset.materialize.ms", raw.AssetMaterializeMs)
	setI64(m, "audio.prepare.ms", raw.AudioPrepareMs)
	setI64(m, "audio.timeline.build.ms", raw.AudioTimelineBuildMs)
	setI64(m, "render.plan.build.ms", raw.RenderPlanBuildMs)
	setI64(m, "video.decode.ms", raw.VideoDecodeMs)
	setI64(m, "video.subtitle.ms", raw.VideoSubtitleMs)
	setI64(m, "video.subtitle.raster.ms", raw.VideoSubtitleRasterMs)
	setI64(m, "video.subtitle.composite.ms", raw.VideoSubtitleCompositeMs)
	setI64(m, "video.watermark.ms", raw.VideoWatermarkMs)
	setI64(m, "video.watermark.upload.ms", raw.VideoWatermarkUploadMs)
	setI64(m, "video.watermark.composite.ms", raw.VideoWatermarkCompositeMs)
	setI64(m, "video.blur.ms", raw.VideoBlurMs)
	setI64(m, "video.filter.ms", raw.VideoFilterMs)
	setI64(m, "video.composite.ms", raw.VideoCompositeMs)
	setI64(m, "video.encode.ms", raw.VideoEncodeMs)
	setI64(m, "video.concat.ms", raw.VideoConcatMs)
	setI64(m, "audio.mux.ms", raw.AudioMuxMs)
	setI64(m, "output.finalize.ms", raw.OutputFinalizeMs)
	setI64(m, "sha256.ms", raw.Sha256Ms)
	setI64(m, "ffprobe.ms", raw.FfprobeMs)
	setI64(m, "artifact.verify.ms", raw.ArtifactVerifyMs)
	setI64(m, "drive.upload.ms", raw.DriveUploadMs)
	setI64(m, "drive.verify.ms", raw.DriveVerifyMs)
	setI64(m, "job.total.ms", raw.JobTotalMs)

	// ── GPU transfers ──────────────────────────────────────────────────
	setI64(m, "frames.downloaded.from.gpu", raw.FramesDownloadedFromGPU)
	setI64(m, "frames.uploaded.to.gpu", raw.FramesUploadedToGPU)
	setI64(m, "gpu.to.cpu.transfer.ms", raw.GpuToCpuTransferMs)
	setI64(m, "cpu.to.gpu.transfer.ms", raw.CpuToGpuTransferMs)
	setI64(m, "gpu.to.cpu.bytes", raw.GpuToCpuBytes)
	setI64(m, "cpu.to.gpu.bytes", raw.CpuToGpuBytes)

	// ── GPU utilization ────────────────────────────────────────────────
	setF64(m, "gpu.util.avg.pct", raw.GpuUtilAvgPct)
	setF64(m, "gpu.util.peak.pct", raw.GpuUtilPeakPct)
	setF64(m, "nvdec.util.avg.pct", raw.NvdecUtilAvgPct)
	setF64(m, "nvdec.util.peak.pct", raw.NvdecUtilPeakPct)
	setF64(m, "nvenc.util.avg.pct", raw.NvencUtilAvgPct)
	setF64(m, "nvenc.util.peak.pct", raw.NvencUtilPeakPct)
	setI64(m, "vram.used.avg.bytes", raw.VramUsedAvgBytes)
	setI64(m, "gpu.idle.during.render.ms", raw.GpuIdleMs)

	// ── CPU attribution ────────────────────────────────────────────────
	setF64(m, "cpu.percent.avg", raw.CpuPercentAvg)
	setI64(m, "cpu.user.ms", raw.CpuUserMs)
	setI64(m, "cpu.system.ms", raw.CpuSystemMs)
	setI64(m, "subtitle.cpu.ms", raw.SubtitleCpuMs)
	setI64(m, "blur.cpu.ms", raw.BlurCpuMs)
	setI64(m, "composite.cpu.ms", raw.CompositeCpuMs)
	setI64(m, "encode.cpu.ms", raw.EncodeCpuMs)
	setI64(m, "hash.cpu.ms", raw.HashCpuMs)

	// ── Segment / packet-copy stats ────────────────────────────────────
	setI32(m, "segments.total", raw.SegmentsTotal)
	setI32(m, "segments.packet.copy", raw.SegmentsPacketCopy)
	setI32(m, "segments.reencoded", raw.SegmentsReencoded)
	setI32(m, "segments.composited", raw.SegmentsComposited)
	setI64(m, "packet.copy.bytes", raw.PacketCopyBytes)
	setI64(m, "reencoded.bytes", raw.ReencodedBytes)
	setI64(m, "packet.copy.duration.ms", raw.PacketCopyDurationMs)
	setI64(m, "reencode.duration.ms", raw.ReencodeDurationMs)
	setF64(m, "packet.copy.ratio", raw.PacketCopyRatio)

	// ── Download / cache / disk timing ─────────────────────────────────
	setI64(m, "drive.download.ms", raw.DriveDownloadMs)
	setI64(m, "blobstore.download.ms", raw.BlobstoreDownloadMs)
	setI64(m, "local.cache.read.ms", raw.LocalCacheReadMs)
	setI64(m, "asset.download.wait.ms", raw.AssetDownloadWaitMs)
	setI64(m, "cache.hit.bytes", raw.CacheHitBytes)
	setI64(m, "cache.miss.bytes", raw.CacheMissBytes)
	setI64(m, "output.write.ms", raw.OutputWriteMs)
	setI64(m, "temp.write.ms", raw.TempWriteMs)
	setI64(m, "final.read.ms", raw.FinalReadMs)
	setI64(m, "disk.read.ms", raw.DiskReadMs)
	setI64(m, "disk.write.ms", raw.DiskWriteMs)

	// ── Bandwidth ──────────────────────────────────────────────────────
	setF64(m, "download.mbps.avg", raw.DownloadMbpsAvg)
	setF64(m, "upload.mbps.avg", raw.UploadMbpsAvg)
	setF64(m, "drive.upload.mbps", raw.DriveUploadMbps)
	setF64(m, "artifact.download.mbps", raw.ArtifactDownloadMbps)

	// ── Process spawn ──────────────────────────────────────────────────
	setI64(m, "ffmpeg.exec.count", raw.FfmpegExecCount)
	setI64(m, "ffprobe.exec.count", raw.FfprobeExecCount)
	setI64(m, "process.spawn.count", raw.ProcessSpawnCount)
	setI64(m, "ffmpeg.process.ms", raw.FfmpegProcessMs)
	setI64(m, "ffprobe.process.ms", raw.FfprobeProcessMs)
	setI64(m, "process.startup.ms", raw.ProcessStartupMs)

	// ── Audio encode/copy ──────────────────────────────────────────────
	setI64(m, "audio.copy.ms", raw.AudioCopyMs)
	setI64(m, "audio.encode.ms", raw.AudioEncodeMs)
	setI64(m, "audio.packet.copy", raw.AudioPacketCopy)
	setI64(m, "audio.reencoded", raw.AudioReencoded)
	setI64(m, "audio.input.bytes", raw.AudioInputBytes)
	setI64(m, "audio.output.bytes", raw.AudioOutputBytes)

	// ── Critical path ──────────────────────────────────────────────────
	setStr(m, "critical.path.component", raw.CriticalPathComponent)
	setI64(m, "critical.path.ms", raw.CriticalPathMs)
	setF64(m, "critical.path.percent", raw.CriticalPathPercent)

	return m
}

func setI64(m map[string]interface{}, key string, v int64) {
	if v != 0 {
		m[key] = v
	}
}
func setI32(m map[string]interface{}, key string, v int32) {
	if v != 0 {
		m[key] = int64(v)
	}
}
func setF64(m map[string]interface{}, key string, v float64) {
	if v != 0 {
		m[key] = v
	}
}
func setBool(m map[string]interface{}, key string, v bool) {
	if v {
		m[key] = v
	}
}
func setStr(m map[string]interface{}, key string, v string) {
	if v != "" {
		m[key] = v
	}
}

// mergeStatsInto enriches the report's RawMetrics with stats from the
// cache, blob store, and FFmpeg profile providers owned by TaskRunner.
// These providers are not executor-scoped and are merged at the report
// boundary after the executor has populated its RawMetrics.
func (r *TaskRunner) mergeStatsInto(report *TaskExecutionReport) {
	raw := report.RawMetrics
	if raw == nil {
		raw = &telemetry.RawExecutionMetrics{}
		report.RawMetrics = raw
	}

	// ── Cache provider ──────────────────────────────────────────────
	if r.cacheStats != nil {
		cs := r.cacheStats.Stats()
		cacheHits := cs.Hits
		cacheMisses := cs.Misses
		if report.CacheBaselineSet {
			cacheHits = maxInt64(0, cs.Hits-report.CacheBaseline["hits"])
			cacheMisses = maxInt64(0, cs.Misses-report.CacheBaseline["misses"])
		}
		raw.BytesFromLocalCache = maxInt64(raw.BytesFromLocalCache, cs.BytesUsed)
		// Wire the per-provider cache counters. The attempt-scoped
		// resolver counters (asset.cache.hit.count etc.) are authoritative
		// when present; the generic cache provider fills gaps.
		if raw.AssetCacheHitCount == 0 && raw.AssetCacheMissCount == 0 {
			raw.AssetCacheHitCount = cacheHits
			raw.AssetCacheMissCount = cacheMisses
		}
		if raw.CacheLookups == 0 {
			raw.CacheLookups = raw.AssetCacheHitCount + raw.AssetCacheMissCount
		}
		// Display projection for downstream dashboards.
		report.Metrics["cache.hits"] = cs.Hits
		report.Metrics["cache.misses"] = cs.Misses
		report.Metrics["cache.evictions"] = cs.Evictions
		report.Metrics["cache.corruptions"] = cs.Corruptions
		report.Metrics["cache.entries"] = cs.Entries
		report.Metrics["cache.bytes"] = cs.BytesUsed
		report.Metrics["cache.pinned"] = cs.PinnedEntries
	}

	// ── Blob store provider ─────────────────────────────────────────
	if r.blobStats != nil {
		bs := r.blobStats.Stats()
		if raw.BlobCacheHitCount == 0 && raw.BlobCacheMissCount == 0 {
			raw.BlobCacheHitCount = bs.Fetch
			raw.BlobCacheMissCount = bs.FetchMiss
		}
		report.Metrics["blob.publish"] = bs.Publish
		report.Metrics["blob.publish_failed"] = bs.PublishFailed
		report.Metrics["blob.fetch"] = bs.Fetch
		report.Metrics["blob.fetch_miss"] = bs.FetchMiss
		report.Metrics["blob.fetch_corruption"] = bs.FetchCorruption
		report.Metrics["blob.entries"] = bs.Entries
		report.Metrics["blob.bytes"] = bs.Bytes
	}

	// ── FFmpeg profile aggregation (display only) ───────────────────
	if report.FFmpegProfiles != nil {
		if aggregate := report.FFmpegProfiles.Aggregate(); aggregate.ProcessCount > 0 {
			report.Metrics["ffmpeg.aggregate"] = aggregate
		}
	}

	// ── CPU capacity ────────────────────────────────────────────────
	cp := telemetry.DetectCPUCapacity()
	raw.LogicalCpuCount = int32(cp.LogicalCPUCount)
	raw.CpuQuota = cp.CPUQuota
	raw.EffectiveCpuCount = int32(cp.EffectiveCPUCount)

	// Concat mode → stream copy derivation.
	raw.FinalConcatStreamCopy = concatModeIsStreamCopy(raw.ConcatMode)

	// ── Engine alias keys on the display map (backward compat) ──────
	report.TypedMetrics = raw
}

// concatModeIsStreamCopy reports whether the engine's concat_mode means the
// final video stream was never re-encoded. Copy-only modes are stream_copy
// (legacy ffmpeg -c:v copy), packet_copy (native copy_only path) and
// mixed_packet (native mixed copy-only path); reencode and frame_pipeline
// mean a re-encode happened.

func concatModeIsStreamCopy(mode string) bool {
	return strings.EqualFold(mode, "stream_copy") ||
		strings.EqualFold(mode, "packet_copy") ||
		strings.EqualFold(mode, "mixed_packet")
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
func firstValue(metrics map[string]interface{}, keys ...string) interface{} {
	for _, key := range keys {
		if value, ok := metrics[key]; ok {
			return value
		}
	}
	return nil
}

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

func firstString(m map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if s, ok := m[key].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
