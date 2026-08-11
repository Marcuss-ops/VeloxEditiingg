package store

// sqlite_task_atomic_persistence_attempt.go contains the attempt-entity
// write helpers used by IngestTaskResultAtomic. Each helper receives the
// coordinator-owned transaction; none opens, commits, or rolls back a
// transaction. Split out of sqlite_task_atomic_persistence_helpers.go.
//
// The file is split by responsibility:
//   - sqlite_task_atomic_persistence_attempt.go → versioning + tracing + metrics row
//   - sqlite_task_atomic_persistence_cache.go   → cache stats + cost basis
//   - sqlite_task_atomic_persistence_phase.go   → partial phase metrics + raw report
//   - sqlite_task_atomic_persistence_parallelism.go → segment timings + parallelism

import (
	"context"
	"database/sql"
	"fmt"

	"velox-server/internal/taskgraph"
)

// persistAttemptVersioning writes the software-versioning columns on the
// attempt row when at least one field is non-empty.
func persistAttemptVersioning(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand, now string) error {
	if cmd.GitSHA == "" && cmd.WorkerVersion == "" && cmd.EngineVersion == "" &&
		cmd.FFmpegVersion == "" && cmd.ConfigHash == "" && cmd.DockerImageDigest == "" {
		return nil
	}
	_, err := tx.ExecContext(ctx,
		`UPDATE task_attempts
		 SET git_sha = ?, worker_version = ?, engine_version = ?,
		     ffmpeg_version = ?, config_hash = ?, docker_image_digest = ?,
		     updated_at = ?
		 WHERE task_id = ? AND worker_id = ? AND lease_id = ?`,
		cmd.GitSHA, cmd.WorkerVersion, cmd.EngineVersion,
		cmd.FFmpegVersion, cmd.ConfigHash, cmd.DockerImageDigest,
		now,
		cmd.TaskID, cmd.WorkerID, cmd.LeaseID,
	)
	if err != nil {
		return fmt.Errorf("task ingest atomic versioning: %w", err)
	}
	return nil
}

// persistAttemptRenderIdentity stamps the determinism-chain tail
// (renderer_version + artifact_sha256, migration 148) on the attempt row
// when the worker report arrives. renderer_version falls back to the
// worker engine version when not carried explicitly. artifact_sha256 is
// the worker-declared primary artifact SHA, stamped as a GAP-FILL ONLY:
// finalization (store.FinalizeVerified) writes the master-computed
// authoritative value first in the production ordering, and the
// CASE-guard below never overwrites it with a worker hint. Both are
// best-effort: no-op when neither is known.
func persistAttemptRenderIdentity(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand, now string) error {
	rendererVersion := cmd.RendererVersion
	if rendererVersion == "" {
		rendererVersion = cmd.EngineVersion
	}
	if rendererVersion == "" && cmd.ArtifactSHA256 == "" {
		return nil
	}
	_, err := tx.ExecContext(ctx,
		`UPDATE task_attempts
		 SET renderer_version = ?,
		     artifact_sha256 = CASE WHEN artifact_sha256 = '' THEN ? ELSE artifact_sha256 END,
		     updated_at = ?
		 WHERE task_id = ? AND worker_id = ? AND lease_id = ?`,
		rendererVersion, cmd.ArtifactSHA256, now,
		cmd.TaskID, cmd.WorkerID, cmd.LeaseID,
	)
	if err != nil {
		return fmt.Errorf("task ingest atomic render identity: %w", err)
	}
	return nil
}

// persistAttemptTracing writes OpenTelemetry trace context on the attempt row.
func persistAttemptTracing(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand, now string) error {
	if cmd.TraceID == "" && cmd.SpanID == "" {
		return nil
	}
	_, err := tx.ExecContext(ctx,
		`UPDATE task_attempts
		 SET trace_id = ?, span_id = ?, updated_at = ?
		 WHERE task_id = ? AND worker_id = ? AND lease_id = ?`,
		cmd.TraceID, cmd.SpanID,
		now,
		cmd.TaskID, cmd.WorkerID, cmd.LeaseID,
	)
	if err != nil {
		return fmt.Errorf("task ingest atomic tracing: %w", err)
	}
	return nil
}

// persistAttemptMetrics persists the typed execution metrics row.
func persistAttemptMetrics(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand) error {
	if cmd.Metrics.AttemptID == "" {
		return nil
	}
	m := cmd.Metrics
	streamCopy := boolToInt(m.FinalConcatStreamCopy)
	concatMode := m.ConcatMode
	if concatMode == "" {
		concatMode = "n/a"
	}
	ffprobeValid := boolToInt(m.FFprobeValid != 0)
	hasVideo := boolToInt(m.HasVideoStream)
	hasAudio := boolToInt(m.HasAudioStream)
	errorRetryable := boolToInt(m.ErrorRetryable)

	_, err := tx.ExecContext(ctx,
		`INSERT OR REPLACE INTO task_attempt_metrics (
			attempt_id, input_bytes, output_bytes,
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
			output_sha256,
			completed_segments,
			logical_cpu_count, cpu_quota, effective_cpu_count
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?
		)`,
		m.AttemptID, m.InputBytes, m.OutputBytes,
		m.BytesFromDrive, m.BytesFromBlobstore, m.BytesFromLocalCache,
		m.CPUTimeMS, m.GPUTimeMS, m.PeakRSSBytes, m.PeakVRAMBytes,
		m.FramesDecoded, m.FramesComposited, m.FramesEncoded,
		m.FFmpegSpeedRatio, m.EncodePasses,
		streamCopy, concatMode,
		m.TempBytesWritten, m.DuplicateDownloadBytes,
		m.MediaDurationSeconds, m.WallClockSeconds,
		m.PipelineResolveMs, m.PipelineValidateMs, m.PipelineCompileMs,
		m.PipelineRenderMs, m.PipelineTotalMs,
		m.NativeTotalMs, m.NativeProcessWaitMs,
		m.EngineAssetDownloadMs, m.EngineSegmentBuildMs,
		m.EngineConcatMs, m.EngineAudioDownloadMs,
		m.EngineMuxAudioMs, m.EngineCopyFinalMs,
		ffprobeValid, m.DurationDiffSec,
		hasVideo, hasAudio,
		m.OutputFileSize, m.BlackFrameRatio, m.AudioSyncOffsetMS,
		m.CPUPercentPeak, m.RSSPeakBytes,
		m.DiskReadBytes, m.DiskWriteBytes,
		m.NetworkRxBytes, m.NetworkTxBytes,
		m.IOWaitMS, m.OpenFDsPeak,
		m.QueueMS, m.LeaseWaitMS,
		m.TimeToFirstWorkerMS, m.PendingTasksAtStart,
		m.ActiveWorkersAtStart,
		m.SceneCount, m.SegmentCount, m.TotalInputDurationSec,
		m.ResolutionWidth, m.ResolutionHeight, m.FPS,
		m.AudioTrackCount, m.SubtitleCount, m.TemplateID,
		m.ErrorComponent, m.ErrorPhase,
		errorRetryable, m.ErrorMessageHash,
		m.RetryCount, m.WastedCPUMS,
		m.WastedDownloadBytes, m.WastedCostEstimate,
		m.AssetCacheHitCount, m.AssetCacheMissCount,
		m.BlobCacheHitCount, m.BlobCacheMissCount,
		m.RenderCacheHitCount,
		m.OutputSHA256,
		m.CompletedSegments,
		m.LogicalCPUCount, m.CPUQuota, m.EffectiveCPUCount,
	)
	if err != nil {
		return fmt.Errorf("task ingest atomic metrics: %w", err)
	}
	return nil
}
