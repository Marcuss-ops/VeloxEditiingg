package store

import (
	"context"
	"database/sql"
	"fmt"

	"velox-server/internal/observability"
	"velox-server/internal/taskattempts"
)

// GetMetrics returns metrics for an attempt, or nil if not found.
func (r *SQLiteTaskAttemptRepository) GetMetrics(ctx context.Context, attemptID string) (*taskattempts.AttemptMetrics, error) {
	if attemptID == "" {
		return nil, nil
	}
	row := r.store.db.QueryRowContext(ctx,
		`SELECT attempt_id, input_bytes, output_bytes,
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
		 FROM task_attempt_metrics WHERE attempt_id = ?`,
		attemptID,
	)
	var m taskattempts.AttemptMetrics
	var concatMode string
	var streamCopy int
	var ffprobeValid, hasVideo, hasAudio, errorRetryable int
	err := row.Scan(
		&m.AttemptID, &m.InputBytes, &m.OutputBytes,
		&m.BytesFromDrive, &m.BytesFromBlobstore, &m.BytesFromLocalCache,
		&m.CPUTimeMS, &m.GPUTimeMS, &m.PeakRSSBytes, &m.PeakVRAMBytes,
		&m.FramesDecoded, &m.FramesComposited, &m.FramesEncoded,
		&m.FFmpegSpeedRatio, &m.EncodePasses,
		&streamCopy, &concatMode,
		&m.TempBytesWritten, &m.DuplicateDownloadBytes,
		&m.MediaDurationSeconds, &m.WallClockSeconds,
		&m.PipelineResolveMs, &m.PipelineValidateMs,
		&m.PipelineCompileMs, &m.PipelineRenderMs, &m.PipelineTotalMs,
		&m.NativeTotalMs, &m.NativeProcessWaitMs,
		&m.EngineAssetDownloadMs, &m.EngineSegmentBuildMs,
		&m.EngineConcatMs, &m.EngineAudioDownloadMs,
		&m.EngineMuxAudioMs, &m.EngineCopyFinalMs,
		&ffprobeValid, &m.DurationDiffSec,
		&hasVideo, &hasAudio,
		&m.OutputFileSize, &m.BlackFrameRatio, &m.AudioSyncOffsetMS,
		&m.CPUPercentPeak, &m.RSSPeakBytes,
		&m.DiskReadBytes, &m.DiskWriteBytes,
		&m.NetworkRxBytes, &m.NetworkTxBytes,
		&m.IOWaitMS, &m.OpenFDsPeak,
		&m.QueueMS, &m.LeaseWaitMS,
		&m.TimeToFirstWorkerMS, &m.PendingTasksAtStart,
		&m.ActiveWorkersAtStart,
		&m.SceneCount, &m.SegmentCount, &m.TotalInputDurationSec,
		&m.ResolutionWidth, &m.ResolutionHeight, &m.FPS,
		&m.AudioTrackCount, &m.SubtitleCount, &m.TemplateID,
		&m.ErrorComponent, &m.ErrorPhase,
		&errorRetryable, &m.ErrorMessageHash,
		&m.RetryCount, &m.WastedCPUMS, &m.WastedDownloadBytes,
		&m.WastedCostEstimate,
		&m.AssetCacheHitCount, &m.AssetCacheMissCount,
		&m.BlobCacheHitCount, &m.BlobCacheMissCount,
		&m.RenderCacheHitCount,
		&m.OutputSHA256,
		&m.LogicalCPUCount, &m.CPUQuota, &m.EffectiveCPUCount,
		&m.JobPeakRssDeltaBytes, &m.JobCpuCoreSeconds,
		&m.JobAssetCacheBytesUsed, &m.JobPrefetchBytes,
		&m.JobUploadBufferPeakBytes,
		&m.JobRenderWallMs, &m.JobAssetWallMs, &m.JobPublishWallMs,
		&m.ProgressiveOverlapFirstPartMs, &m.ProgressiveOverlapPartsBeforeRender,
		&m.ProgressiveOverlapBytesBeforeRender, &m.ProgressiveOverlapMs,
		&m.TrailerToOpenMs, &m.MuxToOpenUS,
		&m.JobPublishBytes, &m.JobPageFaults, &m.JobScratchPeakBytes,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("metrics get: %w", err)
	}
	m.FinalConcatStreamCopy = streamCopy != 0
	m.ConcatMode = concatMode
	m.FFprobeValid = ffprobeValid
	m.HasVideoStream = hasVideo != 0
	m.HasAudioStream = hasAudio != 0
	m.ErrorRetryable = errorRetryable != 0
	return &m, nil
}

// ── Version Metrics Query (Step 4 / Velox Metrics Center) ─────────────────

// ListMetricsByGitSHA returns metric snapshots for all terminal attempts
// with the given git_sha. Joins task_attempts with task_attempt_metrics
// to fetch the key engine/pipeline metric columns.
//
// Implements observability.VersionMetricsReader.
func (r *SQLiteTaskAttemptRepository) ListMetricsByGitSHA(ctx context.Context, gitSHA string) ([]observability.VersionMetricSnapshot, error) {
	if gitSHA == "" {
		return nil, nil
	}

	rows, err := r.store.db.QueryContext(ctx, `
		SELECT
			a.id,
			a.worker_id,
			COALESCE(t.executor_id, ''),
			m.engine_asset_download_ms,
			m.engine_segment_build_ms,
			m.engine_concat_ms,
			m.engine_mux_audio_ms,
			m.engine_copy_final_ms,
			m.engine_audio_download_ms,
			m.pipeline_resolve_ms,
			m.pipeline_validate_ms,
			m.pipeline_compile_ms,
			m.pipeline_render_ms,
			m.pipeline_total_ms,
			m.native_total_ms,
			m.native_process_wait_ms,
			m.output_bytes,
			m.ffmpeg_speed_ratio,
			m.queue_ms,
			COALESCE(m.wall_clock_seconds, 0) * 1000,
			m.cpu_time_ms,
			m.input_bytes
		FROM task_attempts a
		JOIN task_attempt_metrics m ON m.attempt_id = a.id
		LEFT JOIN tasks t ON t.task_id = a.task_id
		WHERE a.git_sha = ?
		  AND a.status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
		ORDER BY a.updated_at DESC
		LIMIT 500`,
		gitSHA,
	)
	if err != nil {
		return nil, fmt.Errorf("ListMetricsByGitSHA query: %w", err)
	}
	defer rows.Close()

	var results []observability.VersionMetricSnapshot
	for rows.Next() {
		var (
			attemptID, workerID, executorID                             string
			engineAssetDownloadMs, engineSegmentBuildMs, engineConcatMs float64
			engineMuxAudioMs, engineCopyFinalMs, engineAudioDownloadMs  float64
			pipelineResolveMs, pipelineValidateMs, pipelineCompileMs    float64
			pipelineRenderMs, pipelineTotalMs                           float64
			nativeTotalMs, nativeProcessWaitMs                          float64
			outputBytes, ffmpegSpeedRatio, queueMs                      float64
			wallClockMs, cpuTimeMs, inputBytes                          float64
		)
		if err := rows.Scan(
			&attemptID, &workerID, &executorID,
			&engineAssetDownloadMs, &engineSegmentBuildMs, &engineConcatMs,
			&engineMuxAudioMs, &engineCopyFinalMs, &engineAudioDownloadMs,
			&pipelineResolveMs, &pipelineValidateMs, &pipelineCompileMs,
			&pipelineRenderMs, &pipelineTotalMs,
			&nativeTotalMs, &nativeProcessWaitMs,
			&outputBytes, &ffmpegSpeedRatio, &queueMs,
			&wallClockMs, &cpuTimeMs, &inputBytes,
		); err != nil {
			continue
		}

		snap := observability.VersionMetricSnapshot{
			AttemptID:  attemptID,
			WorkerID:   workerID,
			ExecutorID: executorID,
			Metrics: map[string]float64{
				"engine.asset_download_ms": engineAssetDownloadMs,
				"engine.segment_build_ms":  engineSegmentBuildMs,
				"engine.concat_ms":         engineConcatMs,
				"engine.mux_audio_ms":      engineMuxAudioMs,
				"engine.copy_final_ms":     engineCopyFinalMs,
				"engine.audio_download_ms": engineAudioDownloadMs,
				"pipeline.resolve_ms":      pipelineResolveMs,
				"pipeline.validate_ms":     pipelineValidateMs,
				"pipeline.compile_ms":      pipelineCompileMs,
				"pipeline.render_ms":       pipelineRenderMs,
				"pipeline.total_ms":        pipelineTotalMs,
				"native.total_ms":          nativeTotalMs,
				"native.process_wait_ms":   nativeProcessWaitMs,
				"output.bytes":             outputBytes,
				"ffmpeg.speed_ratio":       ffmpegSpeedRatio,
				"queue.ms":                 queueMs,
				"task.wall_clock_ms":       wallClockMs,
				"task.cpu_time_ms":         cpuTimeMs,
				"input.bytes":              inputBytes,
			},
		}
		results = append(results, snap)
	}
	return results, rows.Err()
}
