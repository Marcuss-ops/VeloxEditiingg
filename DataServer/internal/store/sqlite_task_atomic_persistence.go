package store

// sqlite_task_atomic_persistence.go: persistence helpers used by
// IngestTaskResultAtomic. Keeping them in a separate file makes the
// critical transaction orchestration in sqlite_task_atomic.go easier to
// follow while preserving the exact same atomic semantics.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"time"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// ingestTaskCAS performs the Task-side CAS for IngestTaskResultAtomic.
// See IngestTaskResultAtomic for the full invariant documentation.
func ingestTaskCAS(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand, now string) error {
	alreadyTerminalForThisAttempt, probeErr := probeTaskAlreadyTerminalForAttempt(ctx, tx, cmd.TaskID, cmd.WorkerID, cmd.LeaseID, cmd.AttemptID)
	if probeErr != nil {
		return probeErr
	}
	if alreadyTerminalForThisAttempt {
		return nil
	}

	var (
		taskRes sql.Result
		errCas  error
	)
	if cmd.TaskStatus == taskgraph.StatusSucceeded {
		taskRes, errCas = tx.ExecContext(ctx,
			`UPDATE tasks
			 SET winning_attempt_terminal_pending = 1,
			     completed_at = ?, updated_at = ?
			 WHERE task_id = ? AND status = 'RUNNING'
			   AND attempt_id = ? AND worker_id = ? AND lease_id = ?`,
			now, now,
			cmd.TaskID, cmd.AttemptID, cmd.WorkerID, cmd.LeaseID,
		)
	} else {
		taskRes, errCas = tx.ExecContext(ctx,
			`UPDATE tasks
			 SET status = ?, completed_at = ?, revision = revision + 1, updated_at = ?
			 WHERE task_id = ? AND status IN ('LEASED', 'RUNNING', 'READY')
			   AND worker_id = ? AND lease_id = ?`,
			string(cmd.TaskStatus), now, now,
			cmd.TaskID, cmd.WorkerID, cmd.LeaseID,
		)
	}
	if errCas != nil {
		return fmt.Errorf("task ingest atomic task cas: %w", errCas)
	}
	if n, _ := taskRes.RowsAffected(); n != 1 {
		return fmt.Errorf("task ingest atomic %s: %w", cmd.TaskID, taskgraph.ErrTransitionConflict)
	}
	return nil
}

// probeTaskAlreadyTerminalForAttempt returns true when the task is already
// SUCCEEDED for the same attempt_id. In that case the downstream
// metric/cache/cost/artifact writes must still commit, so the CAS is skipped.
func probeTaskAlreadyTerminalForAttempt(ctx context.Context, tx *sql.Tx, taskID, workerID, leaseID, attemptID string) (bool, error) {
	var cs, ca string
	probeErr := tx.QueryRowContext(ctx,
		`SELECT status, COALESCE(attempt_id, '') FROM tasks WHERE task_id = ? AND worker_id = ? AND lease_id = ?`,
		taskID, workerID, leaseID,
	).Scan(&cs, &ca)
	if probeErr == sql.ErrNoRows {
		return false, fmt.Errorf("task ingest atomic %s: %w", taskID, taskgraph.ErrTransitionConflict)
	}
	if probeErr != nil {
		return false, fmt.Errorf("task ingest atomic probe: %w", probeErr)
	}
	return cs == "SUCCEEDED" && ca == attemptID, nil
}

// ingestAttemptCAS performs the Attempt-side CAS for IngestTaskResultAtomic.
// It returns nil when the attempt is already terminal (replay-safe) and
// ErrStaleReport when no attempt row exists at all.
func ingestAttemptCAS(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand, now string) error {
	attRes, err := tx.ExecContext(ctx,
		`UPDATE task_attempts
		 SET status = ?, completed_at = ?, error_code = ?, error_message = ?,
		     report_version = report_version + 1, updated_at = ?
		 WHERE task_id = ?
		   AND worker_id = ? AND lease_id = ?
		   AND status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
		string(cmd.AttemptStatus), now, cmd.ErrorCode, cmd.ErrorMsg, now,
		cmd.TaskID, cmd.WorkerID, cmd.LeaseID,
	)
	if err != nil {
		return fmt.Errorf("task ingest atomic attempt cas: %w", err)
	}
	attemptRows, _ := attRes.RowsAffected()
	if attemptRows == 0 {
		return handleAttemptCASMiss(ctx, tx, cmd.TaskID, cmd.WorkerID, cmd.LeaseID)
	}
	return nil
}

// handleAttemptCASMiss distinguishes a replay-safe already-terminal attempt
// from a missing attempt row. The latter is a §9.5 invariant violation.
func handleAttemptCASMiss(ctx context.Context, tx *sql.Tx, taskID, workerID, leaseID string) error {
	existingTerminal, err := countTerminalAttempts(ctx, tx, taskID, workerID, leaseID)
	if err != nil {
		return fmt.Errorf("task ingest atomic attempt probe: %w", err)
	}
	if existingTerminal == 0 {
		return fmt.Errorf("task ingest atomic %s: missing attempt row for worker=%s lease=%s (§9.5 invariant guard): %w",
			taskID, workerID, leaseID, taskattempts.ErrStaleReport)
	}
	return nil
}

// countTerminalAttempts returns the number of terminal attempts for the
// identity tuple.
func countTerminalAttempts(ctx context.Context, tx *sql.Tx, taskID, workerID, leaseID string) (int, error) {
	var existingTerminal int
	probeErr := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_attempts
		 WHERE task_id = ? AND worker_id = ? AND lease_id = ?
		   AND status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
		taskID, workerID, leaseID,
	).Scan(&existingTerminal)
	if probeErr != nil {
		return 0, probeErr
	}
	return existingTerminal, nil
}

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
			completed_segments
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?
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
	)
	if err != nil {
		return fmt.Errorf("task ingest atomic metrics: %w", err)
	}
	return nil
}

// persistAttemptCacheStats persists the per-attempt cache snapshot.
func persistAttemptCacheStats(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand) error {
	if cmd.CacheStats.AttemptID == "" {
		return nil
	}
	_, err := tx.ExecContext(ctx,
		`INSERT OR REPLACE INTO task_attempt_cache_stats (
			attempt_id, cache_hits, cache_misses, cache_evictions,
			cache_corruptions, cache_bytes_used, cache_entries
		) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		cmd.CacheStats.AttemptID, cmd.CacheStats.CacheHits, cmd.CacheStats.CacheMisses,
		cmd.CacheStats.CacheEvictions, cmd.CacheStats.CacheCorruptions,
		cmd.CacheStats.CacheBytesUsed, cmd.CacheStats.CacheEntries,
	)
	if err != nil {
		return fmt.Errorf("task ingest atomic cache stats: %w", err)
	}
	return nil
}

// persistAttemptCostBasis persists the cost-model envelope.
func persistAttemptCostBasis(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand) error {
	if cmd.CostBasis.AttemptID == "" {
		return nil
	}
	_, err := tx.ExecContext(ctx,
		`INSERT OR REPLACE INTO task_attempt_cost_basis (
			attempt_id, cpu_price_per_second, storage_price_per_gb, network_price_per_gb,
			cpu_time_seconds_total, storage_gb_written, network_gb_egressed, output_minutes_total
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		cmd.CostBasis.AttemptID, cmd.CostBasis.CPUPricePerSecond,
		cmd.CostBasis.StoragePricePerGB, cmd.CostBasis.NetworkPricePerGB,
		cmd.CostBasis.CPUTimeSecondsTotal, cmd.CostBasis.StorageGBWritten,
		cmd.CostBasis.NetworkGBEgressed, cmd.CostBasis.OutputMinutesTotal,
	)
	if err != nil {
		return fmt.Errorf("task ingest atomic cost basis: %w", err)
	}
	return nil
}

// persistOutputArtifacts registers declared output artifacts, skipping
// duplicates on UNIQUE conflict.
func persistOutputArtifacts(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand, now string) error {
	for _, a := range cmd.Artifacts {
		if a.ArtifactID == "" {
			continue
		}
		metadata := a.MetadataJSON
		if metadata == "" {
			metadata = "{}"
		}
		_, artErr := tx.ExecContext(ctx,
			`INSERT INTO task_output_artifacts
			 (task_id, attempt_id, artifact_id, artifact_type, declared_path,
			  declared_size, declared_sha256, metadata_json, registered_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			a.TaskID, a.AttemptID, a.ArtifactID, a.ArtifactType, a.DeclaredPath,
			a.DeclaredSize, a.DeclaredSHA256, metadata, now,
		)
		if artErr != nil {
			if isUniqueConflict(artErr) {
				continue
			}
			return fmt.Errorf("task ingest atomic artifact %s: %w", a.ArtifactID, artErr)
		}
	}
	return nil
}

// persistSegmentTimings replaces per-segment sidecar timings for the attempt.
func persistSegmentTimings(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand) error {
	if len(cmd.SegmentTimings) == 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM task_attempt_segment_timings WHERE attempt_id = ?`, cmd.AttemptID); err != nil {
		return fmt.Errorf("task ingest atomic segment timings delete: %w", err)
	}
	nowSeg := time.Now().UTC().Format(time.RFC3339)
	for _, seg := range cmd.SegmentTimings {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO task_attempt_segment_timings (
				attempt_id, job_id, task_id, worker_id,
				segment_index, scene_worker_index, source_type,
				duration_ms, asset_download_ms, ffmpeg_encode_ms,
				source_bytes, output_bytes, frames_encoded,
				codec, preset, ffmpeg_threads,
				status, error_code, error_message,
				source_url_hash, cache_key,
				input_duration_ms, output_duration_ms,
				metadata_json, created_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			cmd.AttemptID, seg.JobID, seg.TaskID, seg.WorkerID,
			seg.SegmentIndex, seg.SceneWorkerIndex, seg.SourceType,
			seg.DurationMS, seg.AssetDownloadMS, seg.FfmpegEncodeMS,
			seg.SourceBytes, seg.OutputBytes, seg.FramesEncoded,
			seg.Codec, seg.Preset, seg.FfmpegThreads,
			seg.Status, seg.ErrorCode, seg.ErrorMessage,
			seg.SourceURLHash, seg.CacheKey,
			seg.InputDurationMS, seg.OutputDurationMS,
			seg.MetadataJSON, nowSeg,
		)
		if err != nil {
			return fmt.Errorf("task ingest atomic segment timing insert %d: %w", seg.SegmentIndex, err)
		}
	}
	return nil
}

// persistPartialPhaseMetrics replaces partial phase metrics for FAILED attempts.
func persistPartialPhaseMetrics(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand) error {
	if len(cmd.PartialPhaseMetrics) == 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM task_phase_timings WHERE attempt_id = ?`, cmd.AttemptID); err != nil {
		return fmt.Errorf("task ingest atomic partial phase timings delete: %w", err)
	}
	nowPhase := time.Now().UTC().Format(time.RFC3339)
	for _, pt := range cmd.PartialPhaseMetrics {
		startedAt := nowPhase
		completedAt := nowPhase
		if !pt.StartedAt.IsZero() {
			startedAt = pt.StartedAt.UTC().Format(time.RFC3339)
		}
		if !pt.CompletedAt.IsZero() {
			completedAt = pt.CompletedAt.UTC().Format(time.RFC3339)
		}
		phase := pt.Component + "." + pt.Action
		if phase == "." {
			phase = "unknown"
		}
		_, err := tx.ExecContext(ctx,
			`INSERT INTO task_phase_timings (
				attempt_id, phase, duration_ms, wall_start, wall_end,
				phase_order, component, action,
				status, error_code, error_message,
				bytes_in, bytes_out, frames, metadata_json
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			cmd.AttemptID, phase, pt.DurationMS, startedAt, completedAt,
			pt.PhaseOrder, pt.Component, pt.Action,
			pt.Status, pt.ErrorCode, pt.ErrorMessage,
			pt.BytesIn, pt.BytesOut, pt.Frames, pt.MetadataJSON,
		)
		if err != nil {
			return fmt.Errorf("task ingest atomic partial phase timing insert %s: %w", phase, err)
		}
	}
	return nil
}

// persistRawReport persists the raw worker report payload for audit/replay,
// enforcing idempotency via report_hash.
func persistRawReport(ctx context.Context, tx *sql.Tx, cmd taskgraph.IngestResultCommand, now string) error {
	if cmd.RawReportJSON == "" {
		return nil
	}
	rawHash := cmd.ReportHash
	if rawHash == "" {
		rawHash = fmt.Sprintf("%x", sha256.Sum256([]byte(cmd.RawReportJSON)))
	}

	var existingHash string
	err := tx.QueryRowContext(ctx,
		`SELECT report_hash FROM task_attempt_reports WHERE attempt_id = ?`,
		cmd.AttemptID,
	).Scan(&existingHash)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("task ingest atomic raw report conflict check: %w", err)
	}
	if existingHash != "" && existingHash != rawHash {
		return fmt.Errorf("task ingest atomic raw report conflict: attempt_id=%s existing_hash=%s new_hash=%s: %w",
			cmd.AttemptID, existingHash, rawHash, taskattempts.ErrReportConflict)
	}

	if existingHash != "" {
		return nil
	}

	receivedAt := now
	if !cmd.RawReportReceivedAt.IsZero() {
		receivedAt = cmd.RawReportReceivedAt.UTC().Format(time.RFC3339)
	}

	reportSchema := cmd.ReportSchemaVersion
	if reportSchema <= 0 {
		reportSchema = 1
	}
	reportVersion := cmd.ReportVersion
	if reportVersion <= 0 {
		reportVersion = 1
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO task_attempt_reports
		 (attempt_id, report_schema, report_version, report_hash, raw_report_json, received_at, persisted_at)
		 VALUES (
			?, ?, ?, ?, ?, ?, ?
		)`,
		cmd.AttemptID, reportSchema, reportVersion, rawHash, cmd.RawReportJSON, receivedAt, now,
	)
	if err != nil {
		return fmt.Errorf("task ingest atomic raw report: %w", err)
	}
	return nil
}
