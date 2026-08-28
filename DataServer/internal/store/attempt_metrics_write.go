package store

// attempt_metrics_write.go: PersistMetrics — the standalone (non-atomic)
// insert-or-replace path for task_attempt_metrics.
//
// Split out of attempt_metrics.go per the header comment. The atomic path
// (persistAttemptMetrics in sqlite_task_atomic_persistence_attempt.go)
// writes inside IngestTaskResultAtomic's transaction.

import (
	"context"
	"fmt"

	"velox-server/internal/taskattempts"
)

// PersistMetrics inserts or replaces metrics for an attempt.
//
// Scorecard v1 / migration 054: extended column list (frames_*, ffmpeg_*,
// encode_passes, final_concat_stream_copy, concat_mode, temp_bytes_*,
// duplicate_download_bytes, media/wall_clock_seconds).
// Scorecard v2 / migration 070: engine-aggregate phase columns
// (pipeline_*, native_*, engine_*). All DEFAULT 0 on the migration
// side so older workers that don't emit these fields (zero structs)
// still persist cleanly.
func (r *SQLiteTaskAttemptRepository) PersistMetrics(ctx context.Context, metrics taskattempts.AttemptMetrics) error {
	if metrics.AttemptID == "" {
		return nil
	}
	streamCopy := 0
	if metrics.FinalConcatStreamCopy {
		streamCopy = 1
	}
	concatMode := metrics.ConcatMode
	if concatMode == "" {
		concatMode = "n/a"
	}
	ffprobeValid := 0
	if metrics.FFprobeValid != 0 {
		ffprobeValid = 1
	}
	hasVideo := 0
	if metrics.HasVideoStream {
		hasVideo = 1
	}
	hasAudio := 0
	if metrics.HasAudioStream {
		hasAudio = 1
	}
	errorRetryable := 0
	if metrics.ErrorRetryable {
		errorRetryable = 1
	}
	_, err := r.store.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO task_attempt_metrics (
			attempt_id, input_bytes, output_bytes,
			bytes_from_drive, bytes_from_blobstore, bytes_from_local_cache,
			cpu_time_ms, gpu_time_ms, peak_rss_bytes, peak_vram_bytes,
			frames_decoded, frames_composited, frames_encoded,
			ffmpeg_speed_ratio, encode_passes,
			final_concat_stream_copy, concat_mode,
			temp_bytes_written, duplicate_download_bytes,
			media_duration_seconds, wall_clock_seconds,
			pipeline_resolve_ms, pipeline_validate_ms,
			pipeline_compile_ms, pipeline_render_ms, pipeline_total_ms,
			native_total_ms, native_process_wait_ms,
			engine_asset_download_ms, engine_segment_build_ms,
			engine_concat_ms, engine_audio_download_ms,
			engine_mux_audio_ms, engine_copy_final_ms,
			ffprobe_valid, duration_diff_sec,
			has_video_stream, has_audio_stream,
			output_file_size, black_frame_ratio, audio_sync_offset_ms,
			cpu_percent_peak, rss_peak_bytes,
			disk_read_bytes, disk_write_bytes,
			network_rx_bytes, network_tx_bytes,
			iowait_ms, open_fds_peak,
			queue_ms, lease_wait_ms,
			time_to_first_worker_ms, pending_tasks_at_start,
			active_workers_at_start,
			scene_count, segment_count, total_input_duration_sec,
			resolution_width, resolution_height, fps,
			audio_track_count, subtitle_count, template_id,
			error_component, error_phase,
			error_retryable, error_message_hash,
			retry_count, wasted_cpu_ms, wasted_download_bytes,
			wasted_cost_estimate,
			asset_cache_hit_count, asset_cache_miss_count,
			blob_cache_hit_count, blob_cache_miss_count,
			render_cache_hit_count,
			output_sha256,
			logical_cpu_count, cpu_quota, effective_cpu_count,
			job_peak_rss_delta_bytes, job_cpu_core_seconds,
			job_asset_cache_bytes_used, job_prefetch_bytes,
			job_upload_buffer_peak_bytes,
			job_render_wall_ms, job_asset_wall_ms, job_publish_wall_ms,
			progressive_overlap_first_part_ms, progressive_overlap_parts_before_render,
			progressive_overlap_bytes_before_render, progressive_overlap_ms,
			trailer_to_open_ms, mux_to_open_us,
			job_publish_bytes, job_page_faults, job_scratch_peak_bytes
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		          ?, ?, ?, ?, ?, ?, ?
		)`,
		metrics.AttemptID, metrics.InputBytes, metrics.OutputBytes,
		metrics.BytesFromDrive, metrics.BytesFromBlobstore, metrics.BytesFromLocalCache,
		metrics.CPUTimeMS, metrics.GPUTimeMS, metrics.PeakRSSBytes, metrics.PeakVRAMBytes,
		metrics.FramesDecoded, metrics.FramesComposited, metrics.FramesEncoded,
		metrics.FFmpegSpeedRatio, metrics.EncodePasses,
		streamCopy, concatMode,
		metrics.TempBytesWritten, metrics.DuplicateDownloadBytes,
		metrics.MediaDurationSeconds, metrics.WallClockSeconds,
		metrics.PipelineResolveMs, metrics.PipelineValidateMs,
		metrics.PipelineCompileMs, metrics.PipelineRenderMs, metrics.PipelineTotalMs,
		metrics.NativeTotalMs, metrics.NativeProcessWaitMs,
		metrics.EngineAssetDownloadMs, metrics.EngineSegmentBuildMs,
		metrics.EngineConcatMs, metrics.EngineAudioDownloadMs,
		metrics.EngineMuxAudioMs, metrics.EngineCopyFinalMs,
		ffprobeValid, metrics.DurationDiffSec,
		hasVideo, hasAudio,
		metrics.OutputFileSize, metrics.BlackFrameRatio, metrics.AudioSyncOffsetMS,
		metrics.CPUPercentPeak, metrics.RSSPeakBytes,
		metrics.DiskReadBytes, metrics.DiskWriteBytes,
		metrics.NetworkRxBytes, metrics.NetworkTxBytes,
		metrics.IOWaitMS, metrics.OpenFDsPeak,
		metrics.QueueMS, metrics.LeaseWaitMS,
		metrics.TimeToFirstWorkerMS, metrics.PendingTasksAtStart,
		metrics.ActiveWorkersAtStart,
		metrics.SceneCount, metrics.SegmentCount, metrics.TotalInputDurationSec,
		metrics.ResolutionWidth, metrics.ResolutionHeight, metrics.FPS,
		metrics.AudioTrackCount, metrics.SubtitleCount, metrics.TemplateID,
		metrics.ErrorComponent, metrics.ErrorPhase,
		errorRetryable, metrics.ErrorMessageHash,
		metrics.RetryCount, metrics.WastedCPUMS, metrics.WastedDownloadBytes,
		metrics.WastedCostEstimate,
		metrics.AssetCacheHitCount, metrics.AssetCacheMissCount,
		metrics.BlobCacheHitCount, metrics.BlobCacheMissCount,
		metrics.RenderCacheHitCount,
		metrics.OutputSHA256,
		metrics.LogicalCPUCount, metrics.CPUQuota, metrics.EffectiveCPUCount,
		metrics.JobPeakRssDeltaBytes, metrics.JobCpuCoreSeconds,
		metrics.JobAssetCacheBytesUsed, metrics.JobPrefetchBytes,
		metrics.JobUploadBufferPeakBytes,
		metrics.JobRenderWallMs, metrics.JobAssetWallMs, metrics.JobPublishWallMs,
		metrics.ProgressiveOverlapFirstPartMs, metrics.ProgressiveOverlapPartsBeforeRender,
		metrics.ProgressiveOverlapBytesBeforeRender, metrics.ProgressiveOverlapMs,
		metrics.TrailerToOpenMs, metrics.MuxToOpenUS,
		metrics.JobPublishBytes, metrics.JobPageFaults, metrics.JobScratchPeakBytes,
	)
	if err != nil {
		return fmt.Errorf("metrics persist: %w", err)
	}
	return nil
}
