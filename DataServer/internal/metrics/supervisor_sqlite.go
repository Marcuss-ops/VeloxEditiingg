// Package metrics / supervisor_sqlite.go
//
// SQLiteLabelResolver production implementation of AttemptsDataSource
// extracted from supervisor.go so the per-tick loop stays focused on
// lifecycle + supervision. Pure read-only SQL queries on the canonical
// velox schema (task_attempts + task_attempt_metrics +
// task_attempt_cache_stats + task_attempt_cost_basis +
// task_phase_timings + task_attempt_segment_timings + tasks +
// workers). No state of its own beyond the *sql.DB handle.
package metrics

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/taskattempts"
)

// workerClassFromExecutorID is the heuristic the supervisor /
// SQLiteLabelResolver fall back to when the workers table has no
// resource_class column or the JOIN misses. Pure string-match —
// matches the canonical costmodel enum verbatim (cpu | mixed | io
// | gpu). Empty / unknown → "default". This operator-friendly
// compromise keeps the supervisor running on legacy schemas that
// predate the typed resource_class column.
func workerClassFromExecutorID(executorID string) string {
	id := strings.ToLower(strings.TrimSpace(executorID))
	switch {
	case id == "":
		return "default"
	case strings.Contains(id, "gpu"):
		return "gpu"
	case strings.Contains(id, "scene.composite") || strings.Contains(id, "composite"):
		return "mixed"
	case strings.Contains(id, "io"):
		return "io"
	case strings.Contains(id, "transcode") || strings.Contains(id, "process"):
		return "cpu"
	default:
		return "default"
	}
}

func isNoSuchColumnErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "no such column")
}

// SQLiteLabelResolver is the production-grade AttemptsDataSource
// implementation. Backed by a raw *sql.DB on the canonical velox
// schema (task_attempts + tasks + workers). One humble query per
// method — the resolver is read-only and pure, so it can be shared
// across multiple supervisors if necessary.
type SQLiteLabelResolver struct {
	DB *sql.DB
}

// Compile-time guard: SQLiteLabelResolver satisfies
// AttemptsDataSource. Wiring mistakes break loudly.
var _ AttemptsDataSource = (*SQLiteLabelResolver)(nil)

// NewSQLiteLabelResolver builds the default resolver backed by
// `db`. Bootstrap wires this: velmetrics.NewSQLiteLabelResolver(p.SQLite.DB()).
func NewSQLiteLabelResolver(db *sql.DB) *SQLiteLabelResolver {
	if db == nil {
		panic("metrics.NewSQLiteLabelResolver: db is nil")
	}
	return &SQLiteLabelResolver{DB: db}
}

// RecentAttemptIDs returns IDs of attempts whose status is terminal
// (SUCCEEDED, FAILED, CANCELLED, TIMED_OUT) AND whose updated_at is
// >= since. limit caps the response (0/negative ⇒ defaultCap).
//
// Order is updated_at ASC so older newly-terminal attempts are
// processed first within a tick — protects the dedup map against
// a long backlog at startup where the wall-clock watermark is
// initialised to "now" and RecentAttemptIDs picks up attempts
// that completed BEFORE the supervisor ever started.
func (r *SQLiteLabelResolver) RecentAttemptIDs(ctx context.Context, since time.Time, limit int) ([]string, error) {
	if limit <= 0 {
		limit = defaultSupervisorAttemptCap
	}
	sinceStr := since.UTC().Format(time.RFC3339Nano)
	rows, err := r.DB.QueryContext(ctx, `
		SELECT id FROM task_attempts
		WHERE status IN ('SUCCEEDED','FAILED','CANCELLED','TIMED_OUT')
		  AND updated_at >= ?
		ORDER BY updated_at ASC
		LIMIT ?`,
		sinceStr, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("supervisor: recent attempts query: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("supervisor: recent attempts scan: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("supervisor: recent attempts rows: %w", err)
	}
	return ids, nil
}

// Labels resolves (execID, execVer, workerClass) via a single JOIN
// over task_attempts + tasks + workers. The JOIN returns the
// executor identity from the canonical taskgraph row (PR-5
// typed-metrics cutover contract) and the resource classification
// from the workers row / executor_id heuristic when the workers
// schema lacks a typed column.
//
// On SQL miss (DELETE before supervisor query) the resolver
// returns the historical defaults (unknown / 0 / default) so the
// downstream ScanAttemptWithLabels call stamps with non-empty
// labels (collector.go's label-len panic is never triggered).
func (r *SQLiteLabelResolver) Labels(ctx context.Context, attemptID string) (string, string, string, error) {
	if attemptID == "" {
		return "unknown", "0", "default", nil
	}
	var execID, execVer, resourceClass sql.NullString
	err := r.DB.QueryRowContext(ctx, `
		SELECT
		    COALESCE(t.executor_id, ''),
		    COALESCE(CAST(t.executor_version AS TEXT), '0'),
		    COALESCE(w.resource_class, '')
		FROM task_attempts a
		LEFT JOIN tasks t ON t.task_id = a.task_id
		LEFT JOIN workers w ON w.worker_id = a.worker_id
		WHERE a.id = ?`,
		attemptID,
	).Scan(&execID, &execVer, &resourceClass)
	if isNoSuchColumnErr(err) {
		err = r.DB.QueryRowContext(ctx, `
			SELECT
			    COALESCE(t.executor_id, ''),
			    COALESCE(CAST(t.executor_version AS TEXT), '0'),
			    COALESCE(w.worker_class, '')
			FROM task_attempts a
			LEFT JOIN tasks t ON t.task_id = a.task_id
			LEFT JOIN workers w ON w.worker_id = a.worker_id
			WHERE a.id = ?`,
			attemptID,
		).Scan(&execID, &execVer, &resourceClass)
	}
	if err == sql.ErrNoRows {
		return "unknown", "0", "default", nil
	}
	if err != nil {
		return "", "", "", fmt.Errorf("supervisor: labels query: %w", err)
	}
	execIDStr := execID.String
	if execIDStr == "" {
		execIDStr = "unknown"
	}
	execVerStr := execVer.String
	if execVerStr == "" {
		execVerStr = "0"
	}
	class := resourceClass.String
	if class == "" {
		// Fall back to the executor-id heuristic — operators
		// with a typed resource_class column rarely hit this
		// path, and the fallback keeps legacy schemas running.
		class = workerClassFromExecutorID(execIDStr)
	}
	return execIDStr, execVerStr, class, nil
}

// GetStatus / GetMetrics / GetCacheStats / GetCostBasis mirror
// the SQLiteTaskAttemptRepository contract. They are kept inline
// (rather than wrapping the repository struct) so the supervisor
// can compile in unit tests without a fully-wired store bundle —
// see supervisor_test.go.
func (r *SQLiteLabelResolver) GetStatus(ctx context.Context, attemptID string) (taskattempts.AttemptStatus, error) {
	if attemptID == "" {
		return taskattempts.AttemptStatusPending, nil
	}
	var status string
	err := r.DB.QueryRowContext(ctx,
		`SELECT status FROM task_attempts WHERE id = ?`, attemptID,
	).Scan(&status)
	if err == sql.ErrNoRows {
		return taskattempts.AttemptStatusPending, nil
	}
	if err != nil {
		return taskattempts.AttemptStatusPending, fmt.Errorf("supervisor: get status: %w", err)
	}
	return taskattempts.AttemptStatus(status), nil
}

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
		       cache_corruptions, cache_bytes_used, cache_entries
		FROM task_attempt_cache_stats WHERE attempt_id = ?`,
		attemptID,
	).Scan(&s.AttemptID, &s.CacheHits, &s.CacheMisses, &s.CacheEvictions,
		&s.CacheCorruptions, &s.CacheBytesUsed, &s.CacheEntries)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("supervisor: get cache stats: %w", err)
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
		       bytes_in, bytes_out, frames, metadata_json
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
			&pt.BytesIn, &pt.BytesOut, &pt.Frames, &pt.MetadataJSON); err != nil {
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
		       segment_index, scene_worker_index, source_type,
		       duration_ms, asset_download_ms, ffmpeg_encode_ms,
		       source_bytes, output_bytes, frames_encoded,
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
			&seg.SegmentIndex, &seg.SceneWorkerIndex, &seg.SourceType,
			&seg.DurationMS, &seg.AssetDownloadMS, &seg.FfmpegEncodeMS,
			&seg.SourceBytes, &seg.OutputBytes, &seg.FramesEncoded,
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
