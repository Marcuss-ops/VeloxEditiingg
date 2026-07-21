package store

// attempt_metrics.go: typed metrics / cache-stats / cost-basis persistence
// + drift-snapshot reads for the attempts side. Single-row, non-tx
// writes against three flat read-models (`task_attempt_metrics`,
// `task_attempt_cache_stats`, `task_attempt_cost_basis`); the only
// Tx-encapsulated writes on the repo live in attempt_reports.go
// (per-phase / per-segment sidecars). Lifecycle row-CRUD lives in
// attempt_lifecycle.go.
// Extracted from sqlite_task_attempt_repository.go.

import (
	"context"
	"database/sql"
	"fmt"

	"velox-server/internal/observability"
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
			logical_cpu_count, cpu_quota, effective_cpu_count
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		          ?, ?, ?, ?, ?, ?, ?,
		          ?, ?, ?, ?, ?, ?,
		          ?, ?, ?, ?, ?, ?, ?,
		          ?, ?, ?, ?, ?, ?, ?, ?,
		          ?, ?, ?, ?, ?,
		          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
		          ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
	)
	if err != nil {
		return fmt.Errorf("metrics persist: %w", err)
	}
	return nil
}

// PersistCacheStats hoists the worker's dotted-key cache counters into a
// typed row so the byte_hit_ratio can be computed in SQL. Idempotent
// INSERT OR REPLACE keyed by attempt_id.
func (r *SQLiteTaskAttemptRepository) PersistCacheStats(ctx context.Context, stats taskattempts.AttemptCacheStats) error {
	if stats.AttemptID == "" {
		return nil
	}
	_, err := r.store.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO task_attempt_cache_stats (
			attempt_id, cache_hits, cache_misses, cache_evictions,
			cache_corruptions, cache_bytes_used, cache_entries
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		stats.AttemptID, stats.CacheHits, stats.CacheMisses, stats.CacheEvictions,
		stats.CacheCorruptions, stats.CacheBytesUsed, stats.CacheEntries,
	)
	if err != nil {
		return fmt.Errorf("cache stats persist: %w", err)
	}
	return nil
}

// GetCacheStats returns the typed cache snapshot for an attempt, or
// (nil, nil) on miss.
func (r *SQLiteTaskAttemptRepository) GetCacheStats(ctx context.Context, attemptID string) (*taskattempts.AttemptCacheStats, error) {
	if attemptID == "" {
		return nil, nil
	}
	row := r.store.db.QueryRowContext(ctx,
		`SELECT attempt_id, cache_hits, cache_misses, cache_evictions,
		        cache_corruptions, cache_bytes_used, cache_entries
		 FROM task_attempt_cache_stats WHERE attempt_id = ?`,
		attemptID,
	)
	var s taskattempts.AttemptCacheStats
	err := row.Scan(
		&s.AttemptID, &s.CacheHits, &s.CacheMisses, &s.CacheEvictions,
		&s.CacheCorruptions, &s.CacheBytesUsed, &s.CacheEntries,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cache stats get: %w", err)
	}
	return &s, nil
}

// PersistCostBasis hoists the cost-model envelope for one attempt; the
// master derives cost_per_output_minute from this row via ComputeCostBasis.
func (r *SQLiteTaskAttemptRepository) PersistCostBasis(ctx context.Context, basis taskattempts.AttemptCostBasis) error {
	if basis.AttemptID == "" {
		return nil
	}
	_, err := r.store.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO task_attempt_cost_basis (
			attempt_id, cpu_price_per_second, storage_price_per_gb, network_price_per_gb,
			cpu_time_seconds_total, storage_gb_written, network_gb_egressed, output_minutes_total
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		basis.AttemptID, basis.CPUPricePerSecond, basis.StoragePricePerGB, basis.NetworkPricePerGB,
		basis.CPUTimeSecondsTotal, basis.StorageGBWritten, basis.NetworkGBEgressed, basis.OutputMinutesTotal,
	)
	if err != nil {
		return fmt.Errorf("cost basis persist: %w", err)
	}
	return nil
}

// GetCostBasis returns the typed cost envelope for an attempt, or
// (nil, nil) on miss.
func (r *SQLiteTaskAttemptRepository) GetCostBasis(ctx context.Context, attemptID string) (*taskattempts.AttemptCostBasis, error) {
	if attemptID == "" {
		return nil, nil
	}
	row := r.store.db.QueryRowContext(ctx,
		`SELECT attempt_id, cpu_price_per_second, storage_price_per_gb, network_price_per_gb,
		        cpu_time_seconds_total, storage_gb_written, network_gb_egressed, output_minutes_total
		 FROM task_attempt_cost_basis WHERE attempt_id = ?`,
		attemptID,
	)
	var b taskattempts.AttemptCostBasis
	err := row.Scan(
		&b.AttemptID, &b.CPUPricePerSecond, &b.StoragePricePerGB, &b.NetworkPricePerGB,
		&b.CPUTimeSecondsTotal, &b.StorageGBWritten, &b.NetworkGBEgressed, &b.OutputMinutesTotal,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cost basis get: %w", err)
	}
	b.Compute()
	return &b, nil
}

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
		        logical_cpu_count, cpu_quota, effective_cpu_count
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
