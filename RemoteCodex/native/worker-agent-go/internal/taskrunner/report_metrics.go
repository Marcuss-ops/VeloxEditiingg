// Package taskrunner / report_metrics.go
//
// Report metrics — mergeStatsInto folds the cache + blob stats provider
// snapshots into the dotted-key report.Metrics map AND projects the same
// data into the typed telemetry.TypedExecutionMetrics envelope used by
// the Scorecard v1 / F3 forward path.
//
// Also hosts the small type-coercion helpers (positiveIntegerToInt64,
// stringFromMap, floatFromMap, boolFromMap) used to read dotted-key
// counters robustly across the union of types an Executor might emit.
package taskrunner

import (
	"velox-worker-agent/internal/telemetry"
)

// mergeStatsInto writes the cache + blob counters into m using
// dotted-key names (legacy PR-3.7 shape) AND populates the typed
// mirror on report.TypedMetrics (Scorecard v1 F3 shape). Both shapes
// are produced until downstream consumers finish adopting the typed
// envelope.
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
func (r *TaskRunner) mergeStatsInto(report *TaskExecutionReport, m map[string]interface{}) {
	if r.cacheStats != nil {
		cs := r.cacheStats.Stats()
		m["cache.hits"] = cs.Hits
		m["cache.misses"] = cs.Misses
		m["cache.evictions"] = cs.Evictions
		m["cache.corruptions"] = cs.Corruptions
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

	// ── Scorecard v1 typed mirror ────────────────────────────────────
	// Built on top of the dotted-key map so the canonical typed shape
	// survives the F3 transition window. If the executor never produced
	// any metric counters, the typed mirror carries the cache.bytes
	// value alone (the only field CacheStatsProvider is authoritative
	// for today) and zeros elsewhere — correct behavior.
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

		AssetCacheHitCount:  positiveIntegerToInt64(m["asset.cache.hit.count"]),
		AssetCacheMissCount: positiveIntegerToInt64(m["asset.cache.miss.count"]),
		BlobCacheHitCount:   positiveIntegerToInt64(m["blob.cache.hit.count"]),
		BlobCacheMissCount:  positiveIntegerToInt64(m["blob.cache.miss.count"]),
		RenderCacheHitCount: positiveIntegerToInt64(m["render.cache.hit.count"]),
	}
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
	// and a JSON-style key in the legacy map.
	if v, ok := m["final.concat.stream_copy"].(bool); ok {
		typed.FinalConcatStreamCopy = v
	}
	report.TypedMetrics = &typed
	// ── End typed mirror ─────────────────────────────────────────────
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
