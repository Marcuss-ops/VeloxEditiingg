// Package telemetry houses the worker's run-time observability layer.
//
// This file — typed_metrics.go — is intentionally narrow: it defines
// the Go-side mirror of proto.TaskExecutionMetrics plus a single
// ToProto() serializer. It does NOT import the prometheus sub-package
// in this directory (telemetry/prometheus.go), nor does it touch the
// existing pkg/cache or pkg/blob stats types. Runners and executors
// populate this struct, then a single hand-off at the transport
// boundary converts it into a *pb.TaskExecutionMetrics onto the
// outgoing TaskResult envelope.
//
// PR-3 invariants (Scorecard v1, F3 worker emit):
//   - Exactly 17 writable fields, parallel 1:1 with the proto schema.
//     Adding a 18th field on the proto side requires editing both this
//     struct and its ToProto() builder at the same time.
//   - Worker/Job/Executor IDs do NOT belong here; the typed envelope
//     is *task-scoped*. Identity lives one level up on TaskResult.//   - All fields are zero-value safe. A worker that produces no
//     ingest/egress traffic simply emits TaskExecutionMetrics{} and
//     protobuf encodes it as an empty message — correct.
//   - ToProto() is infallible; it returns a freshly-allocated proto
//     pointer and never panics. Protobuf setter calls have no error
//     return, so a panic would indicate a proto/struct mismatch bug,
//     not a runtime data error.
//
// Type alignment: proto3 byte-counter fields are SIGNED int64 (the
// proto wire format does not distinguish uint vs int for varint).
// We mirror that with Go int64 / int32 so ToProto() is a direct
// field-by-field setter — no conversion at the boundary. Workers
// must treat the values as non-negative; negative-looking ints are
// rejected upstream by the dotted-key parser before they reach here.
package telemetry

import (
	pb "velox-shared/controltransport/pb"
)

// TypedExecutionMetrics is the worker-side mirror of
// proto.TaskExecutionMetrics. Field-by-field correspondence with the
// proto is enforced by ToProto() — adding a field on one side without
// the other will silently zero-out in F3.x and break Scorecard graphs.
//
// Naming follows the proto (snake_case in proto → PascalCase here) and
// unit suffixes are kept explicit (Ms for milliseconds, Ratio for
// float64 ratio, Bytes for raw bytes, PerSecond / PerGb for prices).
// Number types mirror the proto3 wire schema (int64 / int32 for
// counters, double for ratios and prices). Worker sources never emit
// negative counters; negative values are a worker bug.
type TypedExecutionMetrics struct {
	// ── Byte accounting (raw bytes, not GiB) proto3 int64 ───────────────
	InputBytes          int64 `json:"input_bytes"`
	OutputBytes         int64 `json:"output_bytes"`
	BytesFromDrive      int64 `json:"bytes_from_drive"`       // source: drive
	BytesFromBlobstore  int64 `json:"bytes_from_blobstore"`   // source: blobstore
	BytesFromLocalCache int64 `json:"bytes_from_local_cache"` // source: local cache

	// ── CPU + memory proto3 int64 ────────────────────────────────────────
	CpuTimeMs    int64 `json:"cpu_time_ms"`    // accumulated across all cores
	PeakRssBytes int64 `json:"peak_rss_bytes"` // high-water mark

	// ── Engine counters (ffmpeg / video) proto3 int64 ───────────────────
	FramesDecoded    int64   `json:"frames_decoded"`
	FramesComposited int64   `json:"frames_composited"`
	FramesEncoded    int64   `json:"frames_encoded"`
	FfmpegSpeedRatio float64 `json:"ffmpeg_speed_ratio"` // wall/encoded-time multiplier

	// ── Encode / concat metadata proto3 int32 / bool / string ───────────
	EncodePasses          int32  `json:"encode_passes"`
	FinalConcatStreamCopy bool   `json:"final_concat_stream_copy"`
	ConcatMode            string `json:"concat_mode,omitempty"` // "stream_copy", "reencode", ""

	// ── Cost basis (per-second / per-GiB rates — master multiplies) ─────
	CpuPricePerSecond float64 `json:"cpu_price_per_second"`
	StoragePricePerGb float64 `json:"storage_price_per_gb"`
	NetworkPricePerGb float64 `json:"network_price_per_gb"`

	// ── Scorecard v2 / extra resource counters (migrations 054, 073) ────
	GpuTimeMs              int64   `json:"gpu_time_ms"`
	PeakVramBytes          int64   `json:"peak_vram_bytes"`
	TempBytesWritten       int64   `json:"temp_bytes_written"`
	DuplicateDownloadBytes int64   `json:"duplicate_download_bytes"`
	MediaDurationSeconds   float64 `json:"media_duration_seconds"`
	WallClockSeconds       float64 `json:"wall_clock_seconds"`

	// ── Scorecard v2 / output quality validation (migration 072) ───────
	FfprobeValid      int32   `json:"ffprobe_valid"`
	DurationDiffSec   float64 `json:"duration_diff_sec"`
	HasVideoStream    bool    `json:"has_video_stream"`
	HasAudioStream    bool    `json:"has_audio_stream"`
	OutputFileSize    int64   `json:"output_file_size"`
	BlackFrameRatio   float64 `json:"black_frame_ratio"`
	AudioSyncOffsetMs int64   `json:"audio_sync_offset_ms"`
	OutputSha256      string  `json:"output_sha256"`

	// ── Scorecard v2 / per-attempt resource snapshot (migration 073) ────
	CpuPercentPeak float64 `json:"cpu_percent_peak"`
	DiskReadBytes  int64   `json:"disk_read_bytes"`
	DiskWriteBytes int64   `json:"disk_write_bytes"`
	NetworkRxBytes int64   `json:"network_rx_bytes"`
	NetworkTxBytes int64   `json:"network_tx_bytes"`
	IowaitMs       int64   `json:"iowait_ms"`
	OpenFdsPeak    int64   `json:"open_fds_peak"`

	// ── Scorecard v2 / granular cache hit/miss counters (migration 077) ─
	AssetCacheHitCount  int64 `json:"asset_cache_hit_count"`
	AssetCacheMissCount int64 `json:"asset_cache_miss_count"`
	BlobCacheHitCount   int64 `json:"blob_cache_hit_count"`
	BlobCacheMissCount  int64 `json:"blob_cache_miss_count"`
	RenderCacheHitCount int64 `json:"render_cache_hit_count"`
}

// ToProto serializes a TypedExecutionMetrics onto the typed wire
// envelope. All protobuf field setters are infallible in Go; this
// function returns a *pb.TaskExecutionMetrics and never panics.
//
// Callers typically:
//  1. Build the TypedExecutionMetrics inside TaskRunner.Run /
//     mergeStatsInto from cache.Stats() / blob.Stats() + report.Metrics.
//  2. In worker.job_executor.submitTaskResult, set
//     resultPayload["execution_metrics"] = tm.ToProto() before the
//     transport.Send dispatch.
func (t TypedExecutionMetrics) ToProto() *pb.TaskExecutionMetrics {
	return &pb.TaskExecutionMetrics{
		InputBytes:            t.InputBytes,
		OutputBytes:           t.OutputBytes,
		BytesFromDrive:        t.BytesFromDrive,
		BytesFromBlobstore:    t.BytesFromBlobstore,
		BytesFromLocalCache:   t.BytesFromLocalCache,
		CpuTimeMs:             t.CpuTimeMs,
		PeakRssBytes:          t.PeakRssBytes,
		FramesDecoded:         t.FramesDecoded,
		FramesComposited:      t.FramesComposited,
		FramesEncoded:         t.FramesEncoded,
		FfmpegSpeedRatio:      t.FfmpegSpeedRatio,
		EncodePasses:          t.EncodePasses,
		FinalConcatStreamCopy: t.FinalConcatStreamCopy,
		ConcatMode:            t.ConcatMode,
		CpuPricePerSecond:     t.CpuPricePerSecond,
		StoragePricePerGb:     t.StoragePricePerGb,
		NetworkPricePerGb:     t.NetworkPricePerGb,

		// Scorecard v2 extensions.
		GpuTimeMs:              t.GpuTimeMs,
		PeakVramBytes:          t.PeakVramBytes,
		TempBytesWritten:       t.TempBytesWritten,
		DuplicateDownloadBytes: t.DuplicateDownloadBytes,
		MediaDurationSeconds:   t.MediaDurationSeconds,
		WallClockSeconds:       t.WallClockSeconds,

		FfprobeValid:      t.FfprobeValid,
		DurationDiffSec:   t.DurationDiffSec,
		HasVideoStream:    t.HasVideoStream,
		HasAudioStream:    t.HasAudioStream,
		OutputFileSize:    t.OutputFileSize,
		BlackFrameRatio:   t.BlackFrameRatio,
		AudioSyncOffsetMs: t.AudioSyncOffsetMs,
		OutputSha256:      t.OutputSha256,

		CpuPercentPeak: t.CpuPercentPeak,
		DiskReadBytes:  t.DiskReadBytes,
		DiskWriteBytes: t.DiskWriteBytes,
		NetworkRxBytes: t.NetworkRxBytes,
		NetworkTxBytes: t.NetworkTxBytes,
		IowaitMs:       t.IowaitMs,
		OpenFdsPeak:    t.OpenFdsPeak,

		AssetCacheHitCount:  t.AssetCacheHitCount,
		AssetCacheMissCount: t.AssetCacheMissCount,
		BlobCacheHitCount:   t.BlobCacheHitCount,
		BlobCacheMissCount:  t.BlobCacheMissCount,
		RenderCacheHitCount: t.RenderCacheHitCount,
	}
}

// FromProto accepts a wire payload and mirrors it onto the Go struct.
// Useful for tests, replay tools, and the master-side reverse path
// where a master observer wants the typed view of a wire report.
// Tolerates nil receivers by returning the zero value.
func FromProto(p *pb.TaskExecutionMetrics) TypedExecutionMetrics {
	if p == nil {
		return TypedExecutionMetrics{}
	}
	return TypedExecutionMetrics{
		InputBytes:            p.GetInputBytes(),
		OutputBytes:           p.GetOutputBytes(),
		BytesFromDrive:        p.GetBytesFromDrive(),
		BytesFromBlobstore:    p.GetBytesFromBlobstore(),
		BytesFromLocalCache:   p.GetBytesFromLocalCache(),
		CpuTimeMs:             p.GetCpuTimeMs(),
		PeakRssBytes:          p.GetPeakRssBytes(),
		FramesDecoded:         p.GetFramesDecoded(),
		FramesComposited:      p.GetFramesComposited(),
		FramesEncoded:         p.GetFramesEncoded(),
		FfmpegSpeedRatio:      p.GetFfmpegSpeedRatio(),
		EncodePasses:          p.GetEncodePasses(),
		FinalConcatStreamCopy: p.GetFinalConcatStreamCopy(),
		ConcatMode:            p.GetConcatMode(),
		CpuPricePerSecond:     p.GetCpuPricePerSecond(),
		StoragePricePerGb:     p.GetStoragePricePerGb(),
		NetworkPricePerGb:     p.GetNetworkPricePerGb(),

		GpuTimeMs:              p.GetGpuTimeMs(),
		PeakVramBytes:          p.GetPeakVramBytes(),
		TempBytesWritten:       p.GetTempBytesWritten(),
		DuplicateDownloadBytes: p.GetDuplicateDownloadBytes(),
		MediaDurationSeconds:   p.GetMediaDurationSeconds(),
		WallClockSeconds:       p.GetWallClockSeconds(),

		FfprobeValid:      p.GetFfprobeValid(),
		DurationDiffSec:   p.GetDurationDiffSec(),
		HasVideoStream:    p.GetHasVideoStream(),
		HasAudioStream:    p.GetHasAudioStream(),
		OutputFileSize:    p.GetOutputFileSize(),
		BlackFrameRatio:   p.GetBlackFrameRatio(),
		AudioSyncOffsetMs: p.GetAudioSyncOffsetMs(),
		OutputSha256:      p.GetOutputSha256(),

		CpuPercentPeak: p.GetCpuPercentPeak(),
		DiskReadBytes:  p.GetDiskReadBytes(),
		DiskWriteBytes: p.GetDiskWriteBytes(),
		NetworkRxBytes: p.GetNetworkRxBytes(),
		NetworkTxBytes: p.GetNetworkTxBytes(),
		IowaitMs:       p.GetIowaitMs(),
		OpenFdsPeak:    p.GetOpenFdsPeak(),

		AssetCacheHitCount:  p.GetAssetCacheHitCount(),
		AssetCacheMissCount: p.GetAssetCacheMissCount(),
		BlobCacheHitCount:   p.GetBlobCacheHitCount(),
		BlobCacheMissCount:  p.GetBlobCacheMissCount(),
		RenderCacheHitCount: p.GetRenderCacheHitCount(),
	}
}
