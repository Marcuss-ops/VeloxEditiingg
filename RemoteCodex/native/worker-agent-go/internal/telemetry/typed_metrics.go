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
//   - Writable fields are kept 1:1 with the proto schema. Adding a new
//     field on the proto side requires editing both this struct and its
//     ToProto() builder at the same time.
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
	"encoding/json"
	"sort"
	"strings"

	pb "velox-shared/controltransport/pb"
)

// RawExecutionMetrics is the canonical typed, raw per-attempt metric
// envelope. It contains observed facts only: counters, byte totals,
// resource samples, quality observations, and producer-declared values.
// Derived ratios are calculated by the performance package, never by
// producers writing a metric map.
//
// Field-by-field correspondence with proto.TaskExecutionMetrics is kept
// deliberately stable so the transport projection remains lossless.
// Naming follows the proto (snake_case in proto → PascalCase here) and
// unit suffixes are explicit (Ms for milliseconds, Ratio for float64
// ratios, Bytes for raw bytes, PerSecond / PerGb for prices).
// Number types mirror the proto3 wire schema (int64 / int32 for counters,
// double for ratios and prices). Worker sources never emit negative
// counters; negative values are a producer bug.
type RawExecutionMetrics struct {
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
	ConcatMode            string `json:"concat_mode,omitempty"` // copy-only: "stream_copy", "packet_copy", "mixed_packet" | re-encode: "reencode", "frame_pipeline"

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
	// ── Derived performance ratios ──────────────────────────────────
	RealTimeFactor float64 `json:"realtime_factor"`   // wall / media (lower is better; <1 = faster-than-realtime)
	ThroughputX    float64 `json:"throughput_x"`      // media / wall (higher is better; 2x = 10min in 5min)

	// ── Scorecard v2 / output quality validation (migration 072) ───────
	FfprobeValid      int32   `json:"ffprobe_valid"`
	DurationDiffSec   float64 `json:"duration_diff_sec"`
	HasVideoStream    bool    `json:"has_video_stream"`
	HasAudioStream    bool    `json:"has_audio_stream"`
	AudioTrackCount   int32   `json:"audio_track_count"`
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

	// ── Failure attribution and wasted work (migration 073/step 18) ────
	WastedCpuMs         int64  `json:"wasted_cpu_ms"`
	WastedDownloadBytes int64  `json:"wasted_download_bytes"`
	CompletedSegments   int32  `json:"completed_segments"`
	ErrorComponent      string `json:"error_component,omitempty"`
	ErrorPhase          string `json:"error_phase,omitempty"`

	// ── CPU capacity telemetry (migration 099) ──────────────────────────
	LogicalCpuCount   int32   `json:"logical_cpu_count"`
	CpuQuota          float64 `json:"cpu_quota"`
	EffectiveCpuCount int32   `json:"effective_cpu_count"`

	TelemetryCoverageJSON string `json:"telemetry_coverage_json,omitempty"`
	TelemetryComplete     bool   `json:"telemetry_complete"`
	TelemetryCPUSource    string `json:"telemetry_cpu_source,omitempty"`
	CacheLookups          int64  `json:"cache_lookups"`
	UniqueAssetsRequested int64  `json:"unique_assets_requested"`

	// ── Per-attempt asset download volume (Phase A1 CacheResolver) ─────
	// Attempt-scoped: start from zero on every attempt so job
	// certification never mixes in worker-lifetime counters.
	CacheDownloadCount int64 `json:"cache_download_count"`
	CacheDownloadBytes int64 `json:"cache_download_bytes"`

	// ── Fine-grained phase timings (ms) ────────────────────────────────
	// These decompose the wall clock into the complete job timeline.
	QueueWaitMs          int64 `json:"queue_wait_ms"`
	JobSetupMs           int64 `json:"job_setup_ms"`
	AssetResolveMs       int64 `json:"asset_resolve_ms"`
	AssetDownloadMs      int64 `json:"asset_download_ms"`
	AssetVerifyMs        int64 `json:"asset_verify_ms"`
	AssetMaterializeMs   int64 `json:"asset_materialize_ms"`
	AudioPrepareMs       int64 `json:"audio_prepare_ms"`
	AudioTimelineBuildMs int64 `json:"audio_timeline_build_ms"`
	RenderPlanBuildMs    int64 `json:"render_plan_build_ms"`
	VideoDecodeMs        int64 `json:"video_decode_ms"`
	VideoSubtitleMs      int64 `json:"video_subtitle_ms"`
	VideoSubtitleRasterMs    int64 `json:"video_subtitle_raster_ms"`
	VideoSubtitleCompositeMs int64 `json:"video_subtitle_composite_ms"`
	VideoWatermarkMs         int64 `json:"video_watermark_ms"`
	VideoWatermarkUploadMs   int64 `json:"video_watermark_upload_ms"`
	VideoWatermarkCompositeMs int64 `json:"video_watermark_composite_ms"`
	VideoBlurMs           int64 `json:"video_blur_ms"`
	VideoFilterMs         int64 `json:"video_filter_ms"`
	VideoCompositeMs      int64 `json:"video_composite_ms"`
	VideoEncodeMs         int64 `json:"video_encode_ms"`
	VideoConcatMs         int64 `json:"video_concat_ms"`
	AudioMuxMs            int64 `json:"audio_mux_ms"`
	OutputFinalizeMs      int64 `json:"output_finalize_ms"`
	Sha256Ms              int64 `json:"sha256_ms"`
	FfprobeMs             int64 `json:"ffprobe_ms"`
	ArtifactVerifyMs      int64 `json:"artifact_verify_ms"`
	DriveUploadMs         int64 `json:"drive_upload_ms"`
	DriveVerifyMs         int64 `json:"drive_verify_ms"`
	JobTotalMs            int64 `json:"job_total_ms"`

	// ── GPU transfers (VRAM ↔ RAM) ─────────────────────────────────────
	FramesDownloadedFromGPU int64 `json:"frames_downloaded_from_gpu"`
	FramesUploadedToGPU     int64 `json:"frames_uploaded_to_gpu"`
	GpuToCpuTransferMs      int64 `json:"gpu_to_cpu_transfer_ms"`
	CpuToGpuTransferMs      int64 `json:"cpu_to_gpu_transfer_ms"`
	GpuToCpuBytes           int64 `json:"gpu_to_cpu_bytes"`
	CpuToGpuBytes           int64 `json:"cpu_to_gpu_bytes"`

	// ── GPU utilization sampled averages ────────────────────────────────
	GpuUtilAvgPct    float64 `json:"gpu_util_avg_percent"`
	GpuUtilPeakPct   float64 `json:"gpu_util_peak_percent"`
	NvdecUtilAvgPct  float64 `json:"nvdec_util_avg_percent"`
	NvdecUtilPeakPct float64 `json:"nvdec_util_peak_percent"`
	NvencUtilAvgPct  float64 `json:"nvenc_util_avg_percent"`
	NvencUtilPeakPct float64 `json:"nvenc_util_peak_percent"`
	VramUsedAvgBytes  int64   `json:"vram_used_avg_bytes"`
	GpuIdleMs        int64   `json:"gpu_idle_during_render_ms"`

	// ── CPU attribution to phases ───────────────────────────────────────
	CpuPercentAvg float64 `json:"cpu_percent_avg"`
	CpuUserMs     int64   `json:"cpu_user_ms"`
	CpuSystemMs   int64   `json:"cpu_system_ms"`
	SubtitleCpuMs int64   `json:"subtitle_cpu_ms"`
	BlurCpuMs     int64   `json:"blur_cpu_ms"`
	CompositeCpuMs int64  `json:"composite_cpu_ms"`
	EncodeCpuMs   int64   `json:"encode_cpu_ms"`
	HashCpuMs     int64   `json:"hash_cpu_ms"`

	// ── Segment / packet-copy stats ─────────────────────────────────────
	SegmentsTotal       int32   `json:"segments_total"`
	SegmentsPacketCopy  int32   `json:"segments_packet_copy"`
	SegmentsReencoded   int32   `json:"segments_reencoded"`
	SegmentsComposited  int32   `json:"segments_composited"`
	PacketCopyBytes     int64   `json:"packet_copy_bytes"`
	ReencodedBytes      int64   `json:"reencoded_bytes"`
	PacketCopyDurationMs int64  `json:"packet_copy_duration_ms"`
	ReencodeDurationMs   int64  `json:"reencode_duration_ms"`
	PacketCopyRatio      float64 `json:"packet_copy_ratio"`

	// ── Download / cache timing ─────────────────────────────────────────
	DriveDownloadMs     int64 `json:"drive_download_ms"`
	BlobstoreDownloadMs int64 `json:"blobstore_download_ms"`
	LocalCacheReadMs    int64 `json:"local_cache_read_ms"`
	AssetDownloadWaitMs int64 `json:"asset_download_wait_ms"`
	CacheHitBytes       int64 `json:"cache_hit_bytes"`
	CacheMissBytes      int64 `json:"cache_miss_bytes"`

	// ── Disk I/O timing ─────────────────────────────────────────────────
	OutputWriteMs int64 `json:"output_write_ms"`
	TempWriteMs   int64 `json:"temp_write_ms"`
	FinalReadMs   int64 `json:"final_read_ms"`   // re-read of output for verification
	DiskReadMs    int64 `json:"disk_read_ms"`
	DiskWriteMs   int64 `json:"disk_write_ms"`

	// ── Bandwidth derived ───────────────────────────────────────────────
	DownloadMbpsAvg     float64 `json:"download_mbps_avg"`
	UploadMbpsAvg       float64 `json:"upload_mbps_avg"`
	DriveUploadMbps     float64 `json:"drive_upload_mbps"`
	ArtifactDownloadMbps float64 `json:"artifact_download_mbps"`

	// ── Process spawn ───────────────────────────────────────────────────
	FfmpegExecCount  int64 `json:"ffmpeg_exec_count"`
	FfprobeExecCount int64 `json:"ffprobe_exec_count"`
	ProcessSpawnCount int64 `json:"process_spawn_count"`
	FfmpegProcessMs  int64 `json:"ffmpeg_process_ms"`
	FfprobeProcessMs int64 `json:"ffprobe_process_ms"`
	ProcessStartupMs int64 `json:"process_startup_ms"`

	// ── Audio encode/copy ───────────────────────────────────────────────
	AudioCopyMs      int64 `json:"audio_copy_ms"`
	AudioEncodeMs    int64 `json:"audio_encode_ms"`
	AudioPacketCopy  int64 `json:"audio_packet_copy"`
	AudioReencoded   int64 `json:"audio_reencoded"`
	AudioInputBytes  int64 `json:"audio_input_bytes"`
	AudioOutputBytes int64 `json:"audio_output_bytes"`

	// ── Critical path ───────────────────────────────────────────────────
	CriticalPathComponent string  `json:"critical_path_component,omitempty"`
	CriticalPathMs        int64   `json:"critical_path_ms"`
	CriticalPathPercent   float64 `json:"critical_path_percent"`
}

// TypedExecutionMetrics is retained as a source-compatible name for
// callers that adopted the pre-migration type. New producers must use
// RawExecutionMetrics; both names describe the same canonical typed facts.
type TypedExecutionMetrics = RawExecutionMetrics

// ToProto serializes raw typed metrics onto the typed wire envelope. All
// protobuf field setters are infallible in Go; this function returns a
// *pb.TaskExecutionMetrics and never panics.
//
// Callers typically:
//  1. Build the TypedExecutionMetrics inside TaskRunner.Run /
//     mergeStatsInto from cache.Stats() / blob.Stats() + report.Metrics.
//  2. In worker.job_executor.submitTaskResult, set
//     resultPayload["execution_metrics"] = tm.ToProto() before the
//     transport.Send dispatch.
func (t RawExecutionMetrics) ToProto() *pb.TaskExecutionMetrics {
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
		AudioTrackCount:   t.AudioTrackCount,
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

		WastedCpuMs:         t.WastedCpuMs,
		WastedDownloadBytes: t.WastedDownloadBytes,
		CompletedSegments:   t.CompletedSegments,
		ErrorComponent:      t.ErrorComponent,
		ErrorPhase:          t.ErrorPhase,

		LogicalCpuCount:   t.LogicalCpuCount,
		CpuQuota:          t.CpuQuota,
		EffectiveCpuCount: t.EffectiveCpuCount,

		TelemetryCoverageJson: t.TelemetryCoverageJSON,
		TelemetryComplete:     t.TelemetryComplete,
		TelemetryCpuSource:    t.TelemetryCPUSource,
		CacheLookups:          t.CacheLookups,
		UniqueAssetsRequested: t.UniqueAssetsRequested,
		CacheDownloadCount:    t.CacheDownloadCount,
		CacheDownloadBytes:    t.CacheDownloadBytes,
	}
}

// FromProto accepts a wire payload and mirrors it onto the Go struct.
// Useful for tests, replay tools, and the master-side reverse path
// where a master observer wants the typed view of a wire report.
// Tolerates nil receivers by returning the zero value.
func FromProto(p *pb.TaskExecutionMetrics) RawExecutionMetrics {
	if p == nil {
		return RawExecutionMetrics{}
	}
	return RawExecutionMetrics{
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
		AudioTrackCount:   p.GetAudioTrackCount(),
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

		WastedCpuMs:         p.GetWastedCpuMs(),
		WastedDownloadBytes: p.GetWastedDownloadBytes(),
		CompletedSegments:   p.GetCompletedSegments(),
		ErrorComponent:      p.GetErrorComponent(),
		ErrorPhase:          p.GetErrorPhase(),

		LogicalCpuCount:   p.GetLogicalCpuCount(),
		CpuQuota:          p.GetCpuQuota(),
		EffectiveCpuCount: p.GetEffectiveCpuCount(),

		TelemetryCoverageJSON: p.GetTelemetryCoverageJson(),
		TelemetryComplete:     p.GetTelemetryComplete(),
		TelemetryCPUSource:    p.GetTelemetryCpuSource(),
		CacheLookups:          p.GetCacheLookups(),
		UniqueAssetsRequested: p.GetUniqueAssetsRequested(),
		CacheDownloadCount:    p.GetCacheDownloadCount(),
		CacheDownloadBytes:    p.GetCacheDownloadBytes(),
	}
}

// CoverageMap decodes the optional report coverage block. Invalid or empty
// JSON is intentionally reported as absent rather than as all-false data.
// PopulateFromJobPhaseTimer fills all fine-grained phase timing fields from
// a JobPhaseTimer that was used to instrument the job execution. This is the
// canonical bridge between the runtime timer and the typed metrics envelope.
func (t *RawExecutionMetrics) PopulateFromJobPhaseTimer(timer *JobPhaseTimer) {
	if t == nil || timer == nil {
		return
	}
	phases := timer.PhaseTimings()
	for _, p := range phases {
		ms := p.Timing.DurationMs()
		if ms == 0 {
			continue
		}
		switch p.Name {
		case PhaseQueueWait:
			t.QueueWaitMs = ms
		case PhaseJobSetup:
			t.JobSetupMs = ms
		case PhaseAssetResolve:
			t.AssetResolveMs = ms
		case PhaseAssetDownload:
			t.AssetDownloadMs = ms
		case PhaseAssetVerify:
			t.AssetVerifyMs = ms
		case PhaseAssetMaterialize:
			t.AssetMaterializeMs = ms
		case PhaseAudioPrepare:
			t.AudioPrepareMs = ms
		case PhaseAudioTimelineBuild:
			t.AudioTimelineBuildMs = ms
		case PhaseRenderPlanBuild:
			t.RenderPlanBuildMs = ms
		case PhaseVideoDecode:
			t.VideoDecodeMs = ms
		case PhaseVideoSubtitle:
			t.VideoSubtitleMs = ms
		case PhaseVideoSubtitleRaster:
			t.VideoSubtitleRasterMs = ms
		case PhaseVideoSubtitleComposite:
			t.VideoSubtitleCompositeMs = ms
		case PhaseVideoWatermark:
			t.VideoWatermarkMs = ms
		case PhaseVideoWatermarkUpload:
			t.VideoWatermarkUploadMs = ms
		case PhaseVideoWatermarkComposite:
			t.VideoWatermarkCompositeMs = ms
		case PhaseVideoBlur:
			t.VideoBlurMs = ms
		case PhaseVideoFilter:
			t.VideoFilterMs = ms
		case PhaseVideoComposite:
			t.VideoCompositeMs = ms
		case PhaseVideoEncode:
			t.VideoEncodeMs = ms
		case PhaseVideoConcat:
			t.VideoConcatMs = ms
		case PhaseAudioMux:
			t.AudioMuxMs = ms
		case PhaseOutputFinalize:
			t.OutputFinalizeMs = ms
		case PhaseArtifactHash:
			t.Sha256Ms = ms
		case PhaseArtifactProbe:
			t.FfprobeMs = ms
		case PhaseArtifactVerify:
			t.ArtifactVerifyMs = ms
		case PhaseDriveUpload:
			t.DriveUploadMs = ms
		case PhaseDriveVerify:
			t.DriveVerifyMs = ms
		case PhaseDriveDownload:
			t.DriveDownloadMs = ms
		case PhaseBlobstoreDownload:
			t.BlobstoreDownloadMs = ms
		case PhaseLocalCacheRead:
			t.LocalCacheReadMs = ms
		case PhaseAssetDownloadWait:
			t.AssetDownloadWaitMs = ms
		case PhaseOutputWrite:
			t.OutputWriteMs = ms
		case PhaseTempWrite:
			t.TempWriteMs = ms
		case PhaseFinalRead:
			t.FinalReadMs = ms
		}
	}
	// Total = sum of all phases (wall-clock may differ due to parallelism).
	t.JobTotalMs = timer.TotalDuration().Milliseconds()

	// ── Download / cache byte attribution ────────────────────────────
	// Accumulate per-source byte counters from phase-level data.
	timer.cacheMut.Lock()
	t.CacheHitBytes = timer.cacheHitBytes
	t.CacheMissBytes = timer.cacheMissBytes
	timer.cacheMut.Unlock()

	// ── Per-phase CPU time attribution ────────────────────────────────
	// Map phase-level accumulated CPU milliseconds to the canonical per-phase
	// CPU fields. These are additive: multiple invocations of the same phase
	// (e.g. across segments) are summed by the timer before reaching here.
	for _, p := range phases {
		cpuMs := p.Timing.CPUMs
		if cpuMs == 0 {
			continue
		}
		// Integer truncation from float64: CPUMS is measured in milliseconds
		// and typical values fit int64 without loss.
		cpu := int64(cpuMs)
		switch p.Name {
		case PhaseVideoSubtitle, PhaseVideoSubtitleRaster, PhaseVideoSubtitleComposite:
			t.SubtitleCpuMs += cpu
		case PhaseVideoBlur:
			t.BlurCpuMs += cpu
		case PhaseVideoComposite:
			t.CompositeCpuMs += cpu
		case PhaseVideoEncode:
			t.EncodeCpuMs += cpu
		case PhaseArtifactHash:
			t.HashCpuMs += cpu
		}
	}
}

// ComputeDerivedMetrics fills all derived ratio fields from the raw facts.
// Must be called AFTER MediaDurationSeconds and WallClockSeconds are set.
// Safe to call multiple times; always recomputes from current values.
func (t *RawExecutionMetrics) ComputeDerivedMetrics() {
	if t == nil {
		return
	}
	if t.MediaDurationSeconds > 0 && t.WallClockSeconds > 0 {
		t.RealTimeFactor = t.WallClockSeconds / t.MediaDurationSeconds
		t.ThroughputX = t.MediaDurationSeconds / t.WallClockSeconds
	}
}

// ComputeCriticalPath scans all fine-grained phase durations and identifies
// the single phase that dominates the job wall time — the critical path.
// Must be called AFTER PopulateFromJobPhaseTimer so all phase timings are
// populated. The result is written into CriticalPathComponent,
// CriticalPathMs, and CriticalPathPercent.
//
// Critical path semantics:
//   - In a sequential pipeline, the critical path is the single phase with
//     the highest duration.
//   - In a parallel pipeline, the critical path may span multiple phases
//     that execute on the critical chain. This method sums the durations
//     of the top-N phases until the total exceeds 50% of job wall time;
//     the "critical path component" becomes a concatenation of those
//     phases (e.g. "video.encode + video.composite").
//   - The percentage is computed against the job wall clock, not the sum
//     of all phase durations (which may exceed wall clock due to parallelism).
func (t *RawExecutionMetrics) ComputeCriticalPath() {
	if t == nil {
		return
	}
	// Collect all fine-grained phase durations into a sortable list.
	type namedDuration struct {
		name string
		ms   int64
	}
	phases := []namedDuration{
		{"queue_wait", t.QueueWaitMs},
		{"job_setup", t.JobSetupMs},
		{"asset.resolve", t.AssetResolveMs},
		{"asset.download", t.AssetDownloadMs},
		{"asset.verify", t.AssetVerifyMs},
		{"asset.materialize", t.AssetMaterializeMs},
		{"audio.prepare", t.AudioPrepareMs},
		{"audio.timeline_build", t.AudioTimelineBuildMs},
		{"render_plan_build", t.RenderPlanBuildMs},
		{"video.decode", t.VideoDecodeMs},
		{"video.subtitle", t.VideoSubtitleMs},
		{"video.subtitle_raster", t.VideoSubtitleRasterMs},
		{"video.subtitle_composite", t.VideoSubtitleCompositeMs},
		{"video.watermark", t.VideoWatermarkMs},
		{"video.watermark_upload", t.VideoWatermarkUploadMs},
		{"video.watermark_composite", t.VideoWatermarkCompositeMs},
		{"video.blur", t.VideoBlurMs},
		{"video.filter", t.VideoFilterMs},
		{"video.composite", t.VideoCompositeMs},
		{"video.encode", t.VideoEncodeMs},
		{"video.concat", t.VideoConcatMs},
		{"audio.mux", t.AudioMuxMs},
		{"output_finalize", t.OutputFinalizeMs},
		{"artifact.hash", t.Sha256Ms},
		{"artifact.probe", t.FfprobeMs},
		{"artifact.verify", t.ArtifactVerifyMs},
		{"drive.upload", t.DriveUploadMs},
		{"drive.verify", t.DriveVerifyMs},
		{"asset.download_drive", t.DriveDownloadMs},
		{"asset.download_blobstore", t.BlobstoreDownloadMs},
		{"asset.cache_read", t.LocalCacheReadMs},
	}

	// Sort descending by duration.
	sort.Slice(phases, func(i, j int) bool { return phases[i].ms > phases[j].ms })

	// Single-dominant-phase mode: the top phase IS the critical path.
	// We also accumulate top-N in case no single phase dominates.
	wallMs := int64(t.WallClockSeconds * 1000)
	if wallMs <= 0 {
		return
	}

	top := phases[0]
	if top.ms == 0 {
		return
	}

	// If the top phase alone is >20% of wall time, it's the single
	// dominant bottleneck.
	if float64(top.ms)/float64(wallMs) >= 0.20 {
		t.CriticalPathComponent = top.name
		t.CriticalPathMs = top.ms
		t.CriticalPathPercent = float64(top.ms) / float64(wallMs) * 100
		return
	}

	// Multi-phase critical path: accumulate top phases until >50% of wall.
	var cumulative int64
	var names []string
	for _, p := range phases {
		if p.ms == 0 {
			break
		}
		cumulative += p.ms
		names = append(names, p.name)
		if float64(cumulative)/float64(wallMs) > 0.50 || len(names) >= 3 {
			break
		}
	}
	t.CriticalPathComponent = strings.Join(names, " + ")
	t.CriticalPathMs = cumulative
	if wallMs > 0 {
		t.CriticalPathPercent = float64(cumulative) / float64(wallMs) * 100
	}
}

// ComputeDerivedBandwidth fills bandwidth metrics from byte counts and
// phase durations. Must be called AFTER PopulateFromJobPhaseTimer so
// DriveUploadMs and the disk timings are already populated.
func (t *RawExecutionMetrics) ComputeDerivedBandwidth() {
	if t == nil {
		return
	}
	// download_mbps_avg: total input bytes ÷ total download time.
	dlMs := t.AssetDownloadMs
	if dlMs == 0 {
		dlMs = t.DriveDownloadMs + t.BlobstoreDownloadMs
	}
	if dlMs > 0 && t.InputBytes > 0 {
		t.DownloadMbpsAvg = (float64(t.InputBytes) * 8 / 1_000_000) / (float64(dlMs) / 1000)
	}
	// upload_mbps_avg: output bytes ÷ drive upload time.
	if t.DriveUploadMs > 0 && t.OutputBytes > 0 {
		t.UploadMbpsAvg = (float64(t.OutputBytes) * 8 / 1_000_000) / (float64(t.DriveUploadMs) / 1000)
	}
	// drive_upload_mbps: same as upload but using DriveUpload bytes.
	if t.DriveUploadMs > 0 && t.OutputBytes > 0 {
		t.DriveUploadMbps = (float64(t.OutputBytes) * 8 / 1_000_000) / (float64(t.DriveUploadMs) / 1000)
	}
	// artifact_download_mbps: input bytes on drive download.
	if t.DriveDownloadMs > 0 && t.BytesFromDrive > 0 {
		t.ArtifactDownloadMbps = (float64(t.BytesFromDrive) * 8 / 1_000_000) / (float64(t.DriveDownloadMs) / 1000)
	}
}

// PopulateCentralMetrics is the single entry point for all central
// observability population. It MUST be called by the TaskRunner after the
// executor has returned its result. It sequentially:
//
//  1. Populates fine-grained phase durations from the job phase timer.
//  2. Populates GPU↔CPU transfer metrics from the transfer tracker.
//  3. Populates GPU utilization stats from the background sampler.
//  4. Sets the authoritative wall clock.
//  5. Computes derived ratios (RTF, throughput, bandwidth, critical path).
//
// Executors MUST NOT call this method. It is owned exclusively by the
// TaskRunner — this is the architectural contract that keeps observability
// centralised rather than scattered across twenty executors.
//
// The gpuSamplerStats parameter may be zero-valued when no GPU is present.
func (t *RawExecutionMetrics) PopulateCentralMetrics(
	timer *JobPhaseTimer,
	transfers GPUTransferMetrics,
	gpuSamplerStats GPUStats,
	wallClockSeconds float64,
) {
	if t == nil {
		return
	}
	t.PopulateFromJobPhaseTimer(timer)
	t.PopulateFromGPUTransfers(transfers)
	t.PopulateFromGPUStats(gpuSamplerStats)
	t.WallClockSeconds = wallClockSeconds
	t.ComputeDerivedMetrics()
	t.ComputeDerivedBandwidth()
	t.ComputeCriticalPath()
}

// PopulateFromGPUStats fills GPU utilization metrics from the sampler.
func (t *RawExecutionMetrics) PopulateFromGPUStats(stats GPUStats) {
	if t == nil || stats.SampleCount == 0 {
		return
	}
	t.GpuUtilAvgPct = stats.GPUUtilAvgPct
	t.GpuUtilPeakPct = stats.GPUUtilPeakPct
	t.NvdecUtilAvgPct = stats.NVDECUtilAvgPct
	t.NvdecUtilPeakPct = stats.NVDECUtilPeakPct
	t.NvencUtilAvgPct = stats.NVENCUtilAvgPct
	t.NvencUtilPeakPct = stats.NVENCUtilPeakPct
	t.VramUsedAvgBytes = stats.VRAMUsedAvgBytes
	t.GpuIdleMs = stats.GPUIdleDuringRenderMs
}

// PopulateFromGPUTransfers fills VRAM ↔ RAM transfer metrics.
func (t *RawExecutionMetrics) PopulateFromGPUTransfers(g GPUTransferMetrics) {
	if t == nil {
		return
	}
	t.FramesDownloadedFromGPU = g.FramesDownloadedGPU
	t.FramesUploadedToGPU = g.FramesUploadedGPU
	t.GpuToCpuTransferMs = g.GPUToCPUMs
	t.CpuToGpuTransferMs = g.CPUToGPUMs
	t.GpuToCpuBytes = g.GPUToCPUBytes
	t.CpuToGpuBytes = g.CPUToGPUBytes
}

func (t RawExecutionMetrics) CoverageMap() map[string]bool {
	if t.TelemetryCoverageJSON == "" {
		return nil
	}
	var coverage map[string]bool
	if err := json.Unmarshal([]byte(t.TelemetryCoverageJSON), &coverage); err != nil {
		return nil
	}
	return coverage
}
