package store

import (
	"context"
	"fmt"
	"time"

	"velox-server/internal/taskattempts"
)

// ── Phase Timings ──────────────────────────────────────────────────────────

// PersistPhaseTimings inserts or replaces phase timing rows for an attempt.
func (r *SQLiteTaskAttemptRepository) PersistPhaseTimings(ctx context.Context, attemptID string, timings []taskattempts.PhaseTiming) error {
	if attemptID == "" || len(timings) == 0 {
		return nil
	}
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("phase timings begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, pt := range timings {
		_, err := tx.ExecContext(ctx,
			`INSERT OR REPLACE INTO task_phase_timings (attempt_id, phase, duration_ms, wall_start, wall_end)
			 VALUES (?, ?, ?, ?, ?)`,
			attemptID, pt.Phase, pt.DurationMS,
			pt.WallStart.Format(time.RFC3339), pt.WallEnd.Format(time.RFC3339),
		)
		if err != nil {
			return fmt.Errorf("phase timing insert: %w", err)
		}
	}
	return tx.Commit()
}

// GetPhaseTimings returns all phase timings for an attempt.
func (r *SQLiteTaskAttemptRepository) GetPhaseTimings(ctx context.Context, attemptID string) ([]taskattempts.PhaseTiming, error) {
	if attemptID == "" {
		return nil, nil
	}
	rows, err := r.store.db.QueryContext(ctx,
		`SELECT attempt_id, phase, duration_ms, wall_start, wall_end
		 FROM task_phase_timings WHERE attempt_id = ? ORDER BY wall_start ASC`,
		attemptID,
	)
	if err != nil {
		return nil, fmt.Errorf("phase timings get: %w", err)
	}
	defer rows.Close()

	var results []taskattempts.PhaseTiming
	for rows.Next() {
		var pt taskattempts.PhaseTiming
		var wallStart, wallEnd string
		if err := rows.Scan(&pt.AttemptID, &pt.Phase, &pt.DurationMS, &wallStart, &wallEnd); err != nil {
			continue
		}
		pt.WallStart, _ = time.Parse(time.RFC3339, wallStart)
		pt.WallEnd, _ = time.Parse(time.RFC3339, wallEnd)
		results = append(results, pt)
	}
	return results, rows.Err()
}

// ── Detailed Phase Timings (migration 070) ───────────────────────────────

// PersistPhaseTimingsDetailed inserts or replaces detailed phase timing
// rows keyed by (attempt_id, component, action). Replaces the simpler
// PersistPhaseTimings contract when the worker surfaces the richer
// Scorecard v2 shape (component/action namespace + byte/frame counters).
func (r *SQLiteTaskAttemptRepository) PersistPhaseTimingsDetailed(ctx context.Context, attemptID string, timings []taskattempts.PhaseTimingDetailed) error {
	if attemptID == "" || len(timings) == 0 {
		return nil
	}
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("phase timings detailed begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, pt := range timings {
		_, err := tx.ExecContext(ctx,
			`INSERT OR REPLACE INTO task_phase_timings (
				attempt_id, phase, duration_ms, wall_start, wall_end,
				phase_order, component, action,
				status, error_code, error_message,
				bytes_in, bytes_out, frames, metadata_json
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			attemptID, pt.Component+"."+pt.Action, pt.DurationMS,
			pt.StartedAt.Format(time.RFC3339), pt.CompletedAt.Format(time.RFC3339),
			pt.PhaseOrder, pt.Component, pt.Action,
			pt.Status, pt.ErrorCode, pt.ErrorMessage,
			pt.BytesIn, pt.BytesOut, pt.Frames, pt.MetadataJSON,
		)
		if err != nil {
			return fmt.Errorf("phase timing detailed insert: %w", err)
		}
	}
	return tx.Commit()
}

// ── Segment Timings (migration 070) ──────────────────────────────────────

// PersistSegmentTimings replaces all segment rows for an attempt with
// the authoritative sidecar records from the C++ engine. Delete-then-insert
// under a transaction so the table never contains stale segments.
func (r *SQLiteTaskAttemptRepository) PersistSegmentTimings(ctx context.Context, attemptID string, segments []taskattempts.SegmentTiming) error {
	if attemptID == "" {
		return nil
	}
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("segment timings begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Delete any prior rows for this attempt so the table mirrors the
	// authoritative sidecar exactly.
	if _, err := tx.ExecContext(ctx, `DELETE FROM task_attempt_segment_timings WHERE attempt_id = ?`, attemptID); err != nil {
		return fmt.Errorf("segment timings delete: %w", err)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	for _, seg := range segments {
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
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			attemptID, seg.JobID, seg.TaskID, seg.WorkerID,
			seg.SegmentIndex, seg.SceneWorkerIndex, seg.SourceType,
			seg.DurationMS, seg.AssetDownloadMS, seg.FfmpegEncodeMS,
			seg.SourceBytes, seg.OutputBytes, seg.FramesEncoded,
			seg.Codec, seg.Preset, seg.FfmpegThreads,
			seg.Status, seg.ErrorCode, seg.ErrorMessage,
			seg.SourceURLHash, seg.CacheKey,
			seg.InputDurationMS, seg.OutputDurationMS,
			seg.MetadataJSON, now,
		)
		if err != nil {
			return fmt.Errorf("segment timing insert %d: %w", seg.SegmentIndex, err)
		}
	}
	return tx.Commit()
}
