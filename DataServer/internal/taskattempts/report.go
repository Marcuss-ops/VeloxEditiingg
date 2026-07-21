// Package taskattempts defines types for execution reports and phase metrics.
package taskattempts

import "time"

// Canonical phase names for production rendering pipeline.
// Workers must use these exact strings; free-form identifiers are rejected.
var CanonicalPhases = []string{
	"queue",
	"asset_wait",
	"cache_lookup",
	"download",
	"decode",
	"compile",
	"simulate",
	"render",
	"composite",
	"encode",
	"upload",
	"finalize",
}

// IsCanonicalPhase reports whether the given phase name is a valid canonical phase.
func IsCanonicalPhase(phase string) bool {
	for _, p := range CanonicalPhases {
		if p == phase {
			return true
		}
	}
	return false
}

// PhaseTiming records the duration of a single canonical phase for one attempt.
type PhaseTiming struct {
	AttemptID  string    `json:"attempt_id"`
	Phase      string    `json:"phase"`
	DurationMS int64     `json:"duration_ms"`
	WallStart  time.Time `json:"wall_start"`
	WallEnd    time.Time `json:"wall_end"`
}

// AttemptMetrics stores typed resource counters for one attempt.
//
// Scorecard v1 / PR-5: this struct mirrors TaskExecutionMetrics (proto v3).
// The wire-format struct is the source of truth from the worker; this Go
// struct is the master-side canonical representation. All typed counters
// are 1:1 mapped in PersistenceLayer.PersistMetrics / GetMetrics so SQL
// percentiles / aggregates don't have to walk a JSON blob.
//
// Legacy / 7 base fields (migration 043):
//
//	input_bytes, output_bytes, bytes_from_{drive,blobstore,local_cache},
//	cpu_time_ms, peak_rss_bytes
//
// Scorecard v1 additions (migration 054):
//
//	frames_decoded, frames_composited, frames_encoded,
//	ffmpeg_speed_ratio, encode_passes,
//	final_concat_stream_copy, concat_mode,
//	temp_bytes_written, duplicate_download_bytes,
//	media_duration_seconds, wall_clock_seconds
//
// Project Performance Scorecard (12 metric families) derives from the
// combination of AttemptMetrics + CacheStats + CostBasis on a per-attempt
// basis. The Prometheus text-format exporter on the master side rolls
// these up across attempts into the typed families listed in
// docs/architecture/distributed-rendering/README.md "Required Measurements".
type AttemptMetrics struct {
	AttemptID string `json:"attempt_id"`

	// Legacy 7 carry-over (migration 043). Kept flat (no embedded
	// struct) so scan paths in SQLiteTaskAttemptRepository and other
	// repositories stay 1:1 against the row columns.
	InputBytes          int64 `json:"input_bytes"`
	OutputBytes         int64 `json:"output_bytes"`
	BytesFromDrive      int64 `json:"bytes_from_drive"`
	BytesFromBlobstore  int64 `json:"bytes_from_blobstore"`
	BytesFromLocalCache int64 `json:"bytes_from_local_cache"`
	CPUTimeMS           int64 `json:"cpu_time_ms"`
	GPUTimeMS           int64 `json:"gpu_time_ms"`
	PeakRSSBytes        int64 `json:"peak_rss_bytes"`
	PeakVRAMBytes       int64 `json:"peak_vram_bytes"`

	// Scorecard v1 typed counters (migration 054).
	FramesDecoded          int64   `json:"frames_decoded"`
	FramesComposited       int64   `json:"frames_composited"`
	FramesEncoded          int64   `json:"frames_encoded"`
	FFmpegSpeedRatio       float64 `json:"ffmpeg_speed_ratio"`
	EncodePasses           int32   `json:"encode_passes"`
	FinalConcatStreamCopy  bool    `json:"final_concat_stream_copy"`
	ConcatMode             string  `json:"concat_mode"` // "stream_copy" | "reencode" | "n/a"
	TempBytesWritten       int64   `json:"temp_bytes_written"`
	DuplicateDownloadBytes int64   `json:"duplicate_download_bytes"`
	MediaDurationSeconds   float64 `json:"media_duration_seconds"`
	WallClockSeconds       float64 `json:"wall_clock_seconds"`

	// Scorecard v2 / engine-aggregate phase timing (migration 070).
	PipelineResolveMs     int64 `json:"pipeline_resolve_ms"`
	PipelineValidateMs    int64 `json:"pipeline_validate_ms"`
	PipelineCompileMs     int64 `json:"pipeline_compile_ms"`
	PipelineRenderMs      int64 `json:"pipeline_render_ms"`
	PipelineTotalMs       int64 `json:"pipeline_total_ms"`
	NativeTotalMs         int64 `json:"native_total_ms"`
	NativeProcessWaitMs   int64 `json:"native_process_wait_ms"`
	EngineAssetDownloadMs int64 `json:"engine_asset_download_ms"`
	EngineSegmentBuildMs  int64 `json:"engine_segment_build_ms"`
	EngineConcatMs        int64 `json:"engine_concat_ms"`
	EngineAudioDownloadMs int64 `json:"engine_audio_download_ms"`
	EngineMuxAudioMs      int64 `json:"engine_mux_audio_ms"`
	EngineCopyFinalMs     int64 `json:"engine_copy_final_ms"`

	// Scorecard v2 / Step 9: output quality validation.
	FFprobeValid      int     `json:"ffprobe_valid"`
	DurationDiffSec   float64 `json:"duration_diff_sec"`
	HasVideoStream    bool    `json:"has_video_stream"`
	HasAudioStream    bool    `json:"has_audio_stream"`
	OutputFileSize    int64   `json:"output_file_size"`
	BlackFrameRatio   float64 `json:"black_frame_ratio"`
	AudioSyncOffsetMS int64   `json:"audio_sync_offset_ms"`
	OutputSHA256      string  `json:"output_sha256"`

	// Scorecard v2 / Step 10: per-attempt resource snapshot.
	CPUPercentPeak float64 `json:"cpu_percent_peak"`
	RSSPeakBytes   int64   `json:"rss_peak_bytes"`
	DiskReadBytes  int64   `json:"disk_read_bytes"`
	DiskWriteBytes int64   `json:"disk_write_bytes"`
	NetworkRxBytes int64   `json:"network_rx_bytes"`
	NetworkTxBytes int64   `json:"network_tx_bytes"`
	IOWaitMS       int64   `json:"iowait_ms"`
	OpenFDsPeak    int64   `json:"open_fds_peak"`

	// Scorecard v2 / Step 11: queue and wait-time metrics.
	QueueMS              int64 `json:"queue_ms"`
	LeaseWaitMS          int64 `json:"lease_wait_ms"`
	TimeToFirstWorkerMS  int64 `json:"time_to_first_worker_ms"`
	PendingTasksAtStart  int64 `json:"pending_tasks_at_start"`
	ActiveWorkersAtStart int64 `json:"active_workers_at_start"`

	// Scorecard v2 / migration 099: per-attempt CPU capacity telemetry.
	LogicalCPUCount   int     `json:"logical_cpu_count"`
	CPUQuota          float64 `json:"cpu_quota"`
	EffectiveCPUCount int     `json:"effective_cpu_count"`

	// Scorecard v2 / Step 12: input context for normalization.
	SceneCount            int     `json:"scene_count"`
	SegmentCount          int     `json:"segment_count"`
	CompletedSegments     int     `json:"completed_segments"`
	TotalInputDurationSec float64 `json:"total_input_duration_sec"`
	ResolutionWidth       int     `json:"resolution_width"`
	ResolutionHeight      int     `json:"resolution_height"`
	FPS                   float64 `json:"fps"`
	AudioTrackCount       int     `json:"audio_track_count"`
	SubtitleCount         int     `json:"subtitle_count"`
	TemplateID            string  `json:"template_id"`

	// Scorecard v2 / Step 13: error classification refinement.
	ErrorComponent   string `json:"error_component,omitempty"`
	ErrorPhase       string `json:"error_phase,omitempty"`
	ErrorRetryable   bool   `json:"error_retryable"`
	ErrorMessageHash string `json:"error_message_hash,omitempty"`

	// Scorecard v2 / Step 17: waste/cost attribution.
	RetryCount          int64   `json:"retry_count"`
	WastedCPUMS         int64   `json:"wasted_cpu_ms"`
	WastedDownloadBytes int64   `json:"wasted_download_bytes"`
	WastedCostEstimate  float64 `json:"wasted_cost_estimate"`

	// Scorecard v2 / Step 16: granular cache hit/miss counters.
	AssetCacheHitCount  int64 `json:"asset_cache_hit_count"`
	AssetCacheMissCount int64 `json:"asset_cache_miss_count"`
	BlobCacheHitCount   int64 `json:"blob_cache_hit_count"`
	BlobCacheMissCount  int64 `json:"blob_cache_miss_count"`
	RenderCacheHitCount int64 `json:"render_cache_hit_count"`
}

// AttemptCacheStats is the per-attempt cache snapshot extracted from the
// worker's emit-time merge of cache.hits / cache.misses / cache.evictions /
// cache.corruptions / cache.bytes_used / cache.entries (dotted-key entry
// surface across the executor pipeline, set by taskrunner.mergeStatsInto).
type AttemptCacheStats struct {
	AttemptID        string `json:"attempt_id"`
	CacheHits        int64  `json:"cache_hits"`
	CacheMisses      int64  `json:"cache_misses"`
	CacheEvictions   int64  `json:"cache_evictions"`
	CacheCorruptions int64  `json:"cache_corruptions"`
	CacheBytesUsed   int64  `json:"cache_bytes_used"`
	CacheEntries     int    `json:"cache_entries"`
}

// CacheHitRatio is cache_hits / (cache_hits + cache_misses). Returns 0 when
// no cache activity was recorded.
func (s AttemptCacheStats) CacheHitRatio() float64 {
	total := s.CacheHits + s.CacheMisses
	if total <= 0 {
		return 0
	}
	return float64(s.CacheHits) / float64(total)
}

// CacheByteHitRatio is the canonical scorecard ratio: bytes_from_local_cache
// divided by total bytes (cache + blobstore + drive). Reported as a fraction
// in [0,1] for both the API surface and the Prometheus gauge.
func (m AttemptMetrics) CacheByteHitRatio() float64 {
	total := m.BytesFromDrive + m.BytesFromBlobstore + m.BytesFromLocalCache
	if total == 0 {
		return 0
	}
	return float64(m.BytesFromLocalCache) / float64(total)
}

// DuplicateDownloadRatio is the canonical scorecard ratio:
// duplicate_download_bytes / (duplicate + unique). Returns 0 when no bytes
// were downloaded at all (first-attempt cache hit).
func (m AttemptMetrics) DuplicateDownloadRatio() float64 {
	unique := m.InputBytes - m.DuplicateDownloadBytes
	if unique <= 0 && m.DuplicateDownloadBytes == 0 {
		return 0
	}
	total := m.DuplicateDownloadBytes + unique
	if total == 0 {
		return 0
	}
	return float64(m.DuplicateDownloadBytes) / float64(total)
}

// TempStorageAmplification is temp_bytes_written / output_bytes. Returns 0
// when the attempt produced no output (e.g. failed).
func (m AttemptMetrics) TempStorageAmplification() float64 {
	if m.OutputBytes == 0 {
		return 0
	}
	return float64(m.TempBytesWritten) / float64(m.OutputBytes)
}

// EncodeAmplification is frames_encoded / output_frames. Returns 0 when
// frames_encoded is zero (older workers / pre-PR-2 bridge) OR output is 0.
// Output frames are derived from media_duration_seconds * fps; we keep a
// raw int on the table when the worker surfaces it (PR-2 followup):
// frames_encoded is a strict superset proxy for output_frames, so a value
// >1 here is a real signal of re-encoding.
func (m AttemptMetrics) EncodeAmplification() float64 {
	if m.FramesEncoded == 0 {
		return 0
	}
	// PR-2 followup will introduce OutputFrames as a separate column;
	// until then frames_encoded IS the only signal we have, so the
	// amplification ratio is upper-bounded by 1 — before this worker
	// refactor we conservatively report 1.
	return 1.0
}

// RenderSpeedRatio is media_duration_seconds / wall_clock_seconds. The
// single most important number on the scorecard: >1 means we beat
// realtime. Returns 0 when either side is unknown.
func (m AttemptMetrics) RenderSpeedRatio() float64 {
	if m.WallClockSeconds <= 0 {
		return 0
	}
	if m.MediaDurationSeconds <= 0 {
		return 0
	}
	return m.MediaDurationSeconds / m.WallClockSeconds
}

// RenderFactor is wall_clock_seconds / media_duration_seconds. It is the
// inverse of RenderSpeedRatio: a value of 0.5 means the attempt finished in
// half the media duration; a value of 2 means it took twice as long. Returns
// 0 when either side is unknown.
func (m AttemptMetrics) RenderFactor() float64 {
	if m.MediaDurationSeconds <= 0 {
		return 0
	}
	if m.WallClockSeconds <= 0 {
		return 0
	}
	return m.WallClockSeconds / m.MediaDurationSeconds
}

// EncodeMsPerOutputMinute divides engine_segment_build_ms by output minutes.
// Returns 0 when no output duration is available.
func (m AttemptMetrics) EncodeMsPerOutputMinute() float64 {
	outputMinutes := m.MediaDurationSeconds / 60.0
	if outputMinutes <= 0 {
		return 0
	}
	if m.EngineSegmentBuildMs <= 0 {
		return 0
	}
	return float64(m.EngineSegmentBuildMs) / outputMinutes
}

// CpuMsPerOutputMinute divides cpu_time_ms by output minutes. Returns 0 when
// no output duration is available.
func (m AttemptMetrics) CpuMsPerOutputMinute() float64 {
	outputMinutes := m.MediaDurationSeconds / 60.0
	if outputMinutes <= 0 {
		return 0
	}
	if m.CPUTimeMS <= 0 {
		return 0
	}
	return float64(m.CPUTimeMS) / outputMinutes
}

// DownloadThroughputBytesPerSec is downloaded_bytes / download_seconds.
// Downloaded bytes are approximated by bytes from blobstore plus drive;
// download time is engine_asset_download_ms. Returns 0 when no download
// time was recorded.
func (m AttemptMetrics) DownloadThroughputBytesPerSec() float64 {
	downloadSeconds := float64(m.EngineAssetDownloadMs) / 1000.0
	if downloadSeconds <= 0 {
		return 0
	}
	downloadedBytes := m.BytesFromBlobstore + m.BytesFromDrive
	if downloadedBytes <= 0 {
		return 0
	}
	return float64(downloadedBytes) / downloadSeconds
}

// AttemptCostBasis is the cost-model envelope the worker emits. The proto
// carries a 3-field snapshot (cpu_price + storage_price + network_price);
// on the master side we extend it with the per-attempt totals so
// cost_per_output_minute is a 1-lookup read.
type AttemptCostBasis struct {
	AttemptID           string  `json:"attempt_id"`
	CPUPricePerSecond   float64 `json:"cpu_price_per_second"`
	StoragePricePerGB   float64 `json:"storage_price_per_gb"`
	NetworkPricePerGB   float64 `json:"network_price_per_gb"`
	CPUTimeSecondsTotal float64 `json:"cpu_time_seconds_total"`
	StorageGBWritten    float64 `json:"storage_gb_written"`
	NetworkGBEgressed   float64 `json:"network_gb_egressed"`
	OutputMinutesTotal  float64 `json:"output_minutes_total"`

	// Derived (filled by Compute).
	CostPerOutputMinute float64 `json:"cost_per_output_minute"`
}

// Compute fills Derived fields. OutputMinutesTotal==0 ⇒ no result (we do
// not divide by zero; alternative would be to report cost_per_second).
func (b *AttemptCostBasis) Compute() {
	if b.OutputMinutesTotal <= 0 {
		b.CostPerOutputMinute = 0
		return
	}
	cpuCost := b.CPUTimeSecondsTotal * b.CPUPricePerSecond
	storageCost := b.StorageGBWritten * b.StoragePricePerGB
	networkCost := b.NetworkGBEgressed * b.NetworkPricePerGB
	b.CostPerOutputMinute = (cpuCost + storageCost + networkCost) / b.OutputMinutesTotal
}

// PhaseTimingDetailed extends PhaseTiming with per-phase metadata the
// C++ engine sidecar and Go pipeline runner surface: ordered phase
// index, component/action namespacing, byte/frame counters, and a
// free-form metadata_json for codec/preset/canvas details. The existing
// PhaseTiming struct continues to work for lightweight phase records;
// detailed records are persisted via PersistPhaseTimingsDetailed.
type PhaseTimingDetailed struct {
	AttemptID    string    `json:"attempt_id"`
	JobID        string    `json:"job_id"`
	TaskID       string    `json:"task_id"`
	WorkerID     string    `json:"worker_id"`
	ExecutorID   string    `json:"executor_id"`
	PhaseOrder   int       `json:"phase_order"`
	Component    string    `json:"component"`
	Action       string    `json:"action"`
	StartedAt    time.Time `json:"started_at"`
	CompletedAt  time.Time `json:"completed_at"`
	DurationMS   int64     `json:"duration_ms"`
	Status       string    `json:"status"`
	ErrorCode    string    `json:"error_code,omitempty"`
	ErrorMessage string    `json:"error_message,omitempty"`
	BytesIn      int64     `json:"bytes_in"`
	BytesOut     int64     `json:"bytes_out"`
	Frames       int64     `json:"frames"`
	MetadataJSON string    `json:"metadata_json,omitempty"`
}

// SegmentTiming mirrors the C++ SegmentTiming struct emitted inside the
// sidecar segments[] array. One row per timeline segment.
type SegmentTiming struct {
	AttemptID        string  `json:"attempt_id"`
	JobID            string  `json:"job_id"`
	TaskID           string  `json:"task_id"`
	WorkerID         string  `json:"worker_id"`
	SegmentIndex     int     `json:"segment_index"`
	SceneWorkerIndex int     `json:"scene_worker_index"`
	SourceType       string  `json:"source_type"`
	DurationMS       float64 `json:"duration_ms"`
	AssetDownloadMS  float64 `json:"asset_download_ms"`
	FfmpegEncodeMS   float64 `json:"ffmpeg_encode_ms"`
	SourceBytes      int64   `json:"source_bytes"`
	OutputBytes      int64   `json:"output_bytes"`
	FramesEncoded    int64   `json:"frames_encoded"`
	Codec            string  `json:"codec"`
	Preset           string  `json:"preset"`
	FfmpegThreads    int     `json:"ffmpeg_threads"`
	Status           string  `json:"status"`
	ErrorCode        string  `json:"error_code,omitempty"`
	ErrorMessage     string  `json:"error_message,omitempty"`
	SourceURLHash    string  `json:"source_url_hash,omitempty"`
	CacheKey         string  `json:"cache_key,omitempty"`
	InputDurationMS  float64 `json:"input_duration_ms"`
	OutputDurationMS float64 `json:"output_duration_ms"`
	MetadataJSON     string  `json:"metadata_json,omitempty"`

	// Parallelism telemetry (migration 098).
	StartedOffsetMS  float64 `json:"started_offset_ms"`
	FinishedOffsetMS float64 `json:"finished_offset_ms"`
	WorkerSlot       int     `json:"worker_slot"`
	CPUThreads       int     `json:"cpu_threads"`
	ParallelGroup    string  `json:"parallel_group,omitempty"`
}

// AttemptParallelism stores the master-derived parallelism aggregates
// for a single task attempt. Computed atomically during
// IngestTaskResultAtomic from the segment timing rows.
type AttemptParallelism struct {
	AttemptID string `json:"attempt_id"`

	ConfiguredSegmentWorkers int `json:"configured_segment_workers"`
	FFmpegThreadsPerSegment  int `json:"ffmpeg_threads_per_segment"`
	LogicalCPUCount          int `json:"logical_cpu_count"`
	CPUBudget                int `json:"cpu_budget"`

	SerialWorkMS        float64 `json:"serial_work_ms"`
	RenderWindowMS      float64 `json:"render_window_ms"`
	UnionBusyMS         float64 `json:"union_busy_ms"`
	OverlapMS           float64 `json:"overlap_ms"`
	IdleGapMS           float64 `json:"idle_gap_ms"`
	PeakConcurrency     int     `json:"peak_concurrency"`
	AverageConcurrency  float64 `json:"average_concurrency"`
	SpeedupVsSerial     float64 `json:"speedup_vs_serial"`
	ParallelEfficiency  float64 `json:"parallel_efficiency_ratio"`
	CPUOversubscription float64 `json:"cpu_oversubscription_ratio"`

	BottleneckPhase  string `json:"bottleneck_phase"`
	ParallelStrategy string `json:"parallel_strategy"`
	CalculatedAt     string `json:"calculated_at"`
}

// TaskAttemptReport is the master-side persisted raw worker report for a
// single attempt. It stores the exact JSON payload received from the worker
// plus derived metadata for audit, replay, and forward-compatible metric
// extraction. The typed tables remain the source of truth for fast queries.
type TaskAttemptReport struct {
	AttemptID     string    `json:"attempt_id"`
	ReportSchema  int       `json:"report_schema"`
	ReportHash    string    `json:"report_hash"`
	RawReportJSON string    `json:"raw_report_json"`
	ReceivedAt    time.Time `json:"received_at"`
	PersistedAt   time.Time `json:"persisted_at"`
}

// TaskExecutionReport is the typed, versioned, final report a worker emits
// after completing (or failing) a task attempt. The master validates all
// identity fields before persistence.
type TaskExecutionReport struct {
	ContractVersion int            `json:"contract_version"`
	TaskID          string         `json:"task_id"`
	AttemptID       string         `json:"attempt_id"`
	WorkerID        string         `json:"worker_id"`
	LeaseID         string         `json:"lease_id"`
	Status          AttemptStatus  `json:"status"`
	PhaseTimings    []PhaseTiming  `json:"phase_timings"`
	Metrics         AttemptMetrics `json:"metrics"`
	ErrorCode       string         `json:"error_code,omitempty"`
	ErrorMessage    string         `json:"error_message,omitempty"`
	SubmittedAt     time.Time      `json:"submitted_at"`
	// Scorecard v1 additions:
	CacheStats AttemptCacheStats `json:"cache_stats"`
	CostBasis  AttemptCostBasis  `json:"cost_basis"`
}
