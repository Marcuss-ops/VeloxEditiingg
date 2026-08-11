package metrics

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"velox-server/internal/taskattempts"
)

// supervisor_sqlite_metrics.go owns the six per-attempt metric
// queries of SQLiteLabelResolver (GetMetrics / GetCacheStats /
// GetCostBasis / GetPhaseTimingsDetailed / GetSegmentTimings /
// GetParallelism). The resolver struct, constructor and the
// identity queries (RecentAttemptIDs / Labels / GetStatus) live in
// supervisor_sqlite.go.

// GetMetrics mirrors the SQLiteTaskAttemptRepository contract. It is
// kept inline (rather than wrapping the repository struct) so the
// supervisor can compile in unit tests without a fully-wired store
// bundle — see supervisor_test.go.
func (r *SQLiteLabelResolver) GetMetrics(ctx context.Context, attemptID string) (*taskattempts.AttemptMetrics, error) {
	if attemptID == "" {
		return nil, nil
	}
	var m taskattempts.AttemptMetrics
	var concatMode string
	var streamCopy int
	var ffprobeValid, hasVideo, hasAudio, errorRetryable int
	err := r.DB.QueryRowContext(ctx, `
		SELECT attempt_id, input_bytes, output_bytes,
		       bytes_from_drive, bytes_from_blobstore, bytes_from_local_cache,
		       cpu_time_ms, gpu_time_ms, peak_rss_bytes, peak_vram_bytes,
		       frames_decoded, frames_composited, frames_encoded,
		       ffmpeg_speed_ratio, encode_passes,
		       final_concat_stream_copy, concat_mode,
		       temp_bytes_written, duplicate_download_bytes,
		       media_duration_seconds, wall_clock_seconds,
		       pipeline_resolve_ms, pipeline_validate_ms, pipeline_compile_ms,
		       pipeline_render_ms, pipeline_total_ms,
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
		       output_sha256
		FROM task_attempt_metrics WHERE attempt_id = ?`,
		attemptID,
	).Scan(
		&m.AttemptID, &m.InputBytes, &m.OutputBytes,
		&m.BytesFromDrive, &m.BytesFromBlobstore, &m.BytesFromLocalCache,
		&m.CPUTimeMS, &m.GPUTimeMS, &m.PeakRSSBytes, &m.PeakVRAMBytes,
		&m.FramesDecoded, &m.FramesComposited, &m.FramesEncoded,
		&m.FFmpegSpeedRatio, &m.EncodePasses,
		&streamCopy, &concatMode,
		&m.TempBytesWritten, &m.DuplicateDownloadBytes,
		&m.MediaDurationSeconds, &m.WallClockSeconds,
		&m.PipelineResolveMs, &m.PipelineValidateMs, &m.PipelineCompileMs,
		&m.PipelineRenderMs, &m.PipelineTotalMs,
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
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("supervisor: get metrics: %w", err)
	}
	m.FinalConcatStreamCopy = streamCopy != 0
	m.ConcatMode = concatMode
	m.FFprobeValid = ffprobeValid
	m.HasVideoStream = hasVideo != 0
	m.HasAudioStream = hasAudio != 0
	m.ErrorRetryable = errorRetryable != 0
	return &m, nil
}

func (r *SQLiteLabelResolver) GetCacheStats(ctx context.Context, attemptID string) (*taskattempts.AttemptCacheStats, error) {
	if attemptID == "" {
		return nil, nil
	}
	var s taskattempts.AttemptCacheStats
	err := r.DB.QueryRowContext(ctx, `
		SELECT attempt_id, cache_hits, cache_misses, cache_evictions,
		       cache_corruptions, cache_bytes_used, cache_entries,
		       cache_lookups, unique_assets_requested,
		       cache_download_count, cache_download_bytes
		FROM task_attempt_cache_stats WHERE attempt_id = ?`,
		attemptID,
	).Scan(&s.AttemptID, &s.CacheHits, &s.CacheMisses, &s.CacheEvictions,
		&s.CacheCorruptions, &s.CacheBytesUsed, &s.CacheEntries,
		&s.CacheLookups, &s.UniqueAssetsRequested,
		&s.CacheDownloadCount, &s.CacheDownloadBytes)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("supervisor: get cache stats: %w", err)
	}
	if err := s.NormalizeCacheAccounting(); err != nil {
		return nil, fmt.Errorf("supervisor: validate cache stats: %w", err)
	}
	return &s, nil
}

func (r *SQLiteLabelResolver) GetCostBasis(ctx context.Context, attemptID string) (*taskattempts.AttemptCostBasis, error) {
	if attemptID == "" {
		return nil, nil
	}
	var b taskattempts.AttemptCostBasis
	err := r.DB.QueryRowContext(ctx, `
		SELECT attempt_id, cpu_price_per_second, storage_price_per_gb, network_price_per_gb,
		       cpu_time_seconds_total, storage_gb_written, network_gb_egressed, output_minutes_total
		FROM task_attempt_cost_basis WHERE attempt_id = ?`,
		attemptID,
	).Scan(&b.AttemptID, &b.CPUPricePerSecond, &b.StoragePricePerGB, &b.NetworkPricePerGB,
		&b.CPUTimeSecondsTotal, &b.StorageGBWritten, &b.NetworkGBEgressed, &b.OutputMinutesTotal)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("supervisor: get cost basis: %w", err)
	}
	b.Compute()
	return &b, nil
}

// GetPhaseTimingsDetailed returns all detailed phase timing rows for an
// attempt from the extended task_phase_timings table (migration 070).
// Returns an empty slice when no rows exist (not an error — older
// attempts predating migration 070 have no detailed rows).
func (r *SQLiteLabelResolver) GetPhaseTimingsDetailed(ctx context.Context, attemptID string) ([]taskattempts.PhaseTimingDetailed, error) {
	if attemptID == "" {
		return nil, nil
	}
	rows, err := r.DB.QueryContext(ctx, `
		SELECT attempt_id, phase, duration_ms, wall_start, wall_end,
		       phase_order, component, action,
		       status, error_code, error_message,
		       bytes_in, bytes_out, frames, metadata_json,
		       job_id, task_id, worker_id, worker_snapshot_id,
		       executor_id, executor_version
		FROM task_phase_timings WHERE attempt_id = ? ORDER BY phase_order ASC, wall_start ASC`,
		attemptID,
	)
	if err != nil {
		return nil, fmt.Errorf("supervisor: get phase timings detailed: %w", err)
	}
	defer rows.Close()

	var results []taskattempts.PhaseTimingDetailed
	for rows.Next() {
		var pt taskattempts.PhaseTimingDetailed
		pt.AttemptID = attemptID
		var wallStart, wallEnd string
		var phase string
		if err := rows.Scan(&pt.AttemptID, &phase, &pt.DurationMS, &wallStart, &wallEnd,
			&pt.PhaseOrder, &pt.Component, &pt.Action,
			&pt.Status, &pt.ErrorCode, &pt.ErrorMessage,
			&pt.BytesIn, &pt.BytesOut, &pt.Frames, &pt.MetadataJSON,
			&pt.JobID, &pt.TaskID, &pt.WorkerID, &pt.WorkerSnapshotID,
			&pt.ExecutorID, &pt.ExecutorVersion); err != nil {
			continue
		}
		pt.StartedAt, _ = time.Parse(time.RFC3339, wallStart)
		pt.CompletedAt, _ = time.Parse(time.RFC3339, wallEnd)
		results = append(results, pt)
	}
	return results, rows.Err()
}

// GetSegmentTimings returns all segment timing rows for an attempt from
// the task_attempt_segment_timings table (migration 070). Returns an
// empty slice when no rows exist.
func (r *SQLiteLabelResolver) GetSegmentTimings(ctx context.Context, attemptID string) ([]taskattempts.SegmentTiming, error) {
	if attemptID == "" {
		return nil, nil
	}
	rows, err := r.DB.QueryContext(ctx, `
		SELECT attempt_id, job_id, task_id, worker_id,
		       segment_index, scene_worker_index, source_type, scene_id,
		       duration_ms, asset_download_ms, ffmpeg_encode_ms,
		       source_bytes, output_bytes, frames_encoded,
		       frames_decoded, frames_composited, ffmpeg_speed_x,
		       codec, preset, ffmpeg_threads,
		       status, error_code, error_message,
		       source_url_hash, cache_key,
		       input_duration_ms, output_duration_ms,
		       metadata_json
		FROM task_attempt_segment_timings WHERE attempt_id = ? ORDER BY segment_index ASC`,
		attemptID,
	)
	if err != nil {
		return nil, fmt.Errorf("supervisor: get segment timings: %w", err)
	}
	defer rows.Close()

	var results []taskattempts.SegmentTiming
	for rows.Next() {
		var seg taskattempts.SegmentTiming
		if err := rows.Scan(&seg.AttemptID, &seg.JobID, &seg.TaskID, &seg.WorkerID,
			&seg.SegmentIndex, &seg.SceneWorkerIndex, &seg.SourceType, &seg.SceneID,
			&seg.DurationMS, &seg.AssetDownloadMS, &seg.FfmpegEncodeMS,
			&seg.SourceBytes, &seg.OutputBytes, &seg.FramesEncoded,
			&seg.FramesDecoded, &seg.FramesComposited, &seg.FfmpegSpeedX,
			&seg.Codec, &seg.Preset, &seg.FfmpegThreads,
			&seg.Status, &seg.ErrorCode, &seg.ErrorMessage,
			&seg.SourceURLHash, &seg.CacheKey,
			&seg.InputDurationMS, &seg.OutputDurationMS,
			&seg.MetadataJSON); err != nil {
			continue
		}
		results = append(results, seg)
	}
	return results, rows.Err()
}

// GetParallelism returns the derived parallelism aggregates for an attempt,
// or nil if no parallelism row exists (pre-migration or zero-segment attempts).
func (r *SQLiteLabelResolver) GetParallelism(ctx context.Context, attemptID string) (*taskattempts.AttemptParallelism, error) {
	if attemptID == "" {
		return nil, nil
	}
	var p taskattempts.AttemptParallelism
	err := r.DB.QueryRowContext(ctx,
		`SELECT attempt_id,
			configured_segment_workers, ffmpeg_threads_per_segment,
			logical_cpu_count, cpu_budget,
			serial_work_ms, render_window_ms, union_busy_ms,
			overlap_ms, idle_gap_ms,
			peak_concurrency, average_concurrency,
			speedup_vs_serial, parallel_efficiency_ratio,
			cpu_oversubscription_ratio,
			bottleneck_phase, parallel_strategy, calculated_at
		 FROM task_attempt_parallelism WHERE attempt_id = ?`, attemptID,
	).Scan(
		&p.AttemptID,
		&p.ConfiguredSegmentWorkers, &p.FFmpegThreadsPerSegment,
		&p.LogicalCPUCount, &p.CPUBudget,
		&p.SerialWorkMS, &p.RenderWindowMS, &p.UnionBusyMS,
		&p.OverlapMS, &p.IdleGapMS,
		&p.PeakConcurrency, &p.AverageConcurrency,
		&p.SpeedupVsSerial, &p.ParallelEfficiency,
		&p.CPUOversubscription,
		&p.BottleneckPhase, &p.ParallelStrategy, &p.CalculatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("supervisor: get parallelism: %w", err)
	}
	return &p, nil
}
