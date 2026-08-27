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
	RealTimeFactor float64 `json:"realtime_factor"` // wall / media (lower is better; <1 = faster-than-realtime)
	ThroughputX    float64 `json:"throughput_x"`    // media / wall (higher is better; 2x = 10min in 5min)

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

	// SceneCount is the declared scene count from the RenderPlan Timeline
	// length. The master verifies this against COUNT(DISTINCT scene_id)
	// from segment_timings.
	SceneCount int32 `json:"scene_count"`

	// ── Per-attempt asset download volume (Phase A1 CacheResolver) ─────
	// Attempt-scoped: start from zero on every attempt so job
	// certification never mixes in worker-lifetime counters.
	CacheDownloadCount int64 `json:"cache_download_count"`
	CacheDownloadBytes int64 `json:"cache_download_bytes"`

	// ── Fine-grained phase timings (ms) ────────────────────────────────
	// These decompose the wall clock into the complete job timeline.
	QueueWaitMs               int64 `json:"queue_wait_ms"`
	JobSetupMs                int64 `json:"job_setup_ms"`
	AssetResolveMs            int64 `json:"asset_resolve_ms"`
	AssetDownloadMs           int64 `json:"asset_download_ms"`
	AssetVerifyMs             int64 `json:"asset_verify_ms"`
	AssetMaterializeMs        int64 `json:"asset_materialize_ms"`
	AudioPrepareMs            int64 `json:"audio_prepare_ms"`
	AudioTimelineBuildMs      int64 `json:"audio_timeline_build_ms"`
	RenderPlanBuildMs         int64 `json:"render_plan_build_ms"`
	VideoDecodeMs             int64 `json:"video_decode_ms"`
	VideoSubtitleMs           int64 `json:"video_subtitle_ms"`
	VideoSubtitleRasterMs     int64 `json:"video_subtitle_raster_ms"`
	VideoSubtitleCompositeMs  int64 `json:"video_subtitle_composite_ms"`
	VideoWatermarkMs          int64 `json:"video_watermark_ms"`
	VideoWatermarkUploadMs    int64 `json:"video_watermark_upload_ms"`
	VideoWatermarkCompositeMs int64 `json:"video_watermark_composite_ms"`
	VideoBlurMs               int64 `json:"video_blur_ms"`
	VideoFilterMs             int64 `json:"video_filter_ms"`
	VideoCompositeMs          int64 `json:"video_composite_ms"`
	VideoEncodeMs             int64 `json:"video_encode_ms"`
	VideoConcatMs             int64 `json:"video_concat_ms"`
	AudioMuxMs                int64 `json:"audio_mux_ms"`
	OutputFinalizeMs          int64 `json:"output_finalize_ms"`
	Sha256Ms                  int64 `json:"sha256_ms"`
	FfprobeMs                 int64 `json:"ffprobe_ms"`
	ArtifactVerifyMs          int64 `json:"artifact_verify_ms"`
	DriveUploadMs             int64 `json:"drive_upload_ms"`
	DriveVerifyMs             int64 `json:"drive_verify_ms"`
	CleanupMs                 int64 `json:"cleanup_ms"`
	JobTotalMs                int64 `json:"job_total_ms"`

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
	VramUsedAvgBytes int64   `json:"vram_used_avg_bytes"`
	GpuIdleMs        int64   `json:"gpu_idle_during_render_ms"`

	// ── CPU attribution to phases ───────────────────────────────────────
	CpuPercentAvg  float64 `json:"cpu_percent_avg"`
	CpuUserMs      int64   `json:"cpu_user_ms"`
	CpuSystemMs    int64   `json:"cpu_system_ms"`
	SubtitleCpuMs  int64   `json:"subtitle_cpu_ms"`
	BlurCpuMs      int64   `json:"blur_cpu_ms"`
	CompositeCpuMs int64   `json:"composite_cpu_ms"`
	EncodeCpuMs    int64   `json:"encode_cpu_ms"`
	HashCpuMs      int64   `json:"hash_cpu_ms"`

	CpuStealAvgPct   float64 `json:"cpu_steal_avg_percent"`
	CpuStealPeakPct  float64 `json:"cpu_steal_peak_percent"`
	CpuIOWaitAvgPct  float64 `json:"cpu_iowait_avg_percent"`
	CpuIOWaitPeakPct float64 `json:"cpu_iowait_peak_percent"`
	RunQueueAvg      float64 `json:"run_queue_avg"`
	RunQueuePeak     int32   `json:"run_queue_peak"`

	// ── Segment / packet-copy stats ─────────────────────────────────────
	SegmentsTotal        int32   `json:"segments_total"`
	SegmentsPacketCopy   int32   `json:"segments_packet_copy"`
	SegmentsReencoded    int32   `json:"segments_reencoded"`
	SegmentsComposited   int32   `json:"segments_composited"`
	PacketCopyBytes      int64   `json:"packet_copy_bytes"`
	ReencodedBytes       int64   `json:"reencoded_bytes"`
	PacketCopyDurationMs int64   `json:"packet_copy_duration_ms"`
	ReencodeDurationMs   int64   `json:"reencode_duration_ms"`
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
	FinalReadMs   int64 `json:"final_read_ms"` // re-read of output for verification
	DiskReadMs    int64 `json:"disk_read_ms"`
	DiskWriteMs   int64 `json:"disk_write_ms"`

	// ── Bandwidth derived ───────────────────────────────────────────────
	DownloadMbpsAvg      float64 `json:"download_mbps_avg"`
	UploadMbpsAvg        float64 `json:"upload_mbps_avg"`
	DriveUploadMbps      float64 `json:"drive_upload_mbps"`
	ArtifactDownloadMbps float64 `json:"artifact_download_mbps"`

	// ── Process spawn ───────────────────────────────────────────────────
	FfmpegExecCount   int64 `json:"ffmpeg_exec_count"`
	FfprobeExecCount  int64 `json:"ffprobe_exec_count"`
	ProcessSpawnCount int64 `json:"process_spawn_count"`
	FfmpegProcessMs   int64 `json:"ffmpeg_process_ms"`
	FfprobeProcessMs  int64 `json:"ffprobe_process_ms"`
	ProcessStartupMs  int64 `json:"process_startup_ms"`

	// ── Audio encode/copy ───────────────────────────────────────────────
	AudioCopyMs      int64 `json:"audio_copy_ms"`
	AudioEncodeMs    int64 `json:"audio_encode_ms"`
	AudioPacketCopy  int64 `json:"audio_packet_copy"`
	AudioReencoded   int64 `json:"audio_reencoded"`
	AudioInputBytes  int64 `json:"audio_input_bytes"`
	AudioOutputBytes int64 `json:"audio_output_bytes"`

	// ── Faststart ───────────────────────────────────────────────────────
	FaststartEnabled        bool  `json:"faststart_enabled"`
	FaststartMs             int64 `json:"faststart_ms"`
	FaststartBytesRewritten int64 `json:"faststart_bytes_rewritten"`

	// ── HTTP asset transport ────────────────────────────────────────────
	HttpRequests         int64 `json:"http_requests"`
	HttpConnectionReused int64 `json:"http_connection_reused"`
	HttpNewConnections   int64 `json:"http_new_connections"`
	DnsMs                int64 `json:"dns_ms"`
	TcpConnectMs         int64 `json:"tcp_connect_ms"`
	TlsHandshakeMs       int64 `json:"tls_handshake_ms"`
	TtfbMs               int64 `json:"ttfb_ms"`
	Http2Requests        int64 `json:"http2_requests"`

	ExternalProcessSpawnExact int64 `json:"external_process_spawn_exact"`

	// ── GPU sampler identity ────────────────────────────────────────────
	GpuUUID string `json:"gpu_uuid,omitempty"`

	// ── Critical path ───────────────────────────────────────────────────
	CriticalPathComponent string  `json:"critical_path_component,omitempty"`
	CriticalPathMs        int64   `json:"critical_path_ms"`
	CriticalPathPercent   float64 `json:"critical_path_percent"`

	// ── Per-job resource attribution (migration 160) ────────────────
	JobPeakRssDeltaBytes     int64   `json:"job_peak_rss_delta_bytes"`
	JobCpuCoreSeconds        float64 `json:"job_cpu_core_seconds"`
	JobAssetCacheBytesUsed   int64   `json:"job_asset_cache_bytes_used"`
	JobPrefetchBytes         int64   `json:"job_prefetch_bytes"`
	JobPublishBytes          int64   `json:"job_publish_bytes"` // bytes uploaded/published to master
	JobPageFaults            int64   `json:"job_page_faults"`   // major page faults during attempt
	JobUploadBufferPeakBytes int64   `json:"job_upload_buffer_peak_bytes"`
	JobRenderWallMs          int64   `json:"job_render_wall_ms"`
	JobAssetWallMs           int64   `json:"job_asset_wall_ms"`
	JobPublishWallMs         int64   `json:"job_publish_wall_ms"`

	// ── Progressive upload overlap (migration 161) ───────────────
	// Render/upload overlap metrics surfaced by the progressive upload
	// path. These capture how much upload work was completed while the
	// engine was still rendering, enabling the capacity scorecard to
	// answer: "Did the mux→upload overlap reduce wall-clock time?"
	ProgressiveOverlapFirstPartMs       int64 `json:"progressive_overlap_first_part_ms"`       // time from upload run start to first part sent
	ProgressiveOverlapPartsBeforeRender int64 `json:"progressive_overlap_parts_before_render"` // parts uploaded while render was still running
	ProgressiveOverlapBytesBeforeRender int64 `json:"progressive_overlap_bytes_before_render"` // bytes uploaded while render was still running
	ProgressiveOverlapMs                int64 `json:"progressive_overlap_ms"`                  // render/upload overlap window (ms)
	TrailerToOpenMs                     int64 `json:"trailer_to_open_ms"`                      // C++ trailer_finished → Go file open (ms)
	MuxToOpenUS                         int64 `json:"mux_to_open_us"`                          // first progress event → Go file open (us)
}

// TypedExecutionMetrics is retained as a source-compatible name for
// callers that adopted the pre-migration type. New producers must use
// RawExecutionMetrics; both names describe the same canonical typed facts.
type TypedExecutionMetrics = RawExecutionMetrics
