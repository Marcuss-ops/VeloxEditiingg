package store

import (
	"strings"
	"testing"
)

// TestAttemptMetricsColumnOrderConsistency verifies that the canonical
// column count in attempt_metrics_scan.go matches the INSERT OR REPLACE
// placeholder count in attempt_metrics_write.go.
//
// This catches the most common bug when adding a new metric column:
// the writer adds a column to INSERT but forgets to add it to SELECT
// (or vice versa), producing silently wrong data.
func TestAttemptMetricsColumnOrderConsistency(t *testing.T) {
	if attemptMetricsColCount == 0 {
		t.Fatal("attemptMetricsColCount is 0 — canonical column list is empty")
	}

	// The INSERT OR REPLACE statement from attempt_metrics_write.go.
	// The column list and VALUES placeholder count MUST both equal
	// attemptMetricsColCount.
	insertSQL := `INSERT OR REPLACE INTO task_attempt_metrics (
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
		          ?, ?, ?, ?, ?, ?, ?`

	// Count ? in the VALUES clause.
	valsIdx := strings.Index(insertSQL, "VALUES")
	if valsIdx < 0 {
		t.Fatal("VALUES clause not found in INSERT SQL")
	}
	placeholders := strings.Count(insertSQL[valsIdx:], "?")

	if placeholders != attemptMetricsColCount {
		t.Fatalf("INSERT placeholder count = %d, want %d (attemptMetricsColCount)\n"+
			"Fix: add or remove ? placeholders in the INSERT VALUES clause to match attemptMetricsColumns",
			placeholders, attemptMetricsColCount)
	}

	t.Logf("attempt_metrics_scan.go: canonical column count = %d, INSERT placeholders = %d ✓",
		attemptMetricsColCount, placeholders)
}
