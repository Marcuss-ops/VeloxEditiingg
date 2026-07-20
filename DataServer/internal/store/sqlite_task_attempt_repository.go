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
// the authoritative sidecar records from the C++ engine and computes
// parallelism aggregates. Uses the shared insertSegmentTimingsAndParallelism
// to ensure identical behavior with the atomic IngestTaskResultAtomic path.
func (r *SQLiteTaskAttemptRepository) PersistSegmentTimings(ctx context.Context, attemptID string, segments []taskattempts.SegmentTiming) error {
	if attemptID == "" {
		return nil
	}
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("segment timings begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Load attempt metrics for parallelism computation. If the metrics row
	// doesn't exist yet (e.g. called before PersistMetrics), use zero-value
	// metrics — the segment timing offsets are still authoritative.
	var metrics taskattempts.AttemptMetrics
	err = tx.QueryRowContext(ctx,
		`SELECT COALESCE(active_workers_at_start, 0)
		 FROM task_attempt_metrics WHERE attempt_id = ?`, attemptID,
	).Scan(&metrics.ActiveWorkersAtStart)
	if err != nil && err.Error() != "sql: no rows in result set" {
		// Non-fatal: proceed with zero metrics.
		metrics = taskattempts.AttemptMetrics{}
	}
	metrics.AttemptID = attemptID

	if err := insertSegmentTimingsAndParallelism(ctx, tx, attemptID, segments, metrics); err != nil {
		return err
	}
	return tx.Commit()
}

// GetParallelism returns the derived parallelism aggregates for an attempt,
// or nil if no parallelism row exists.
func (r *SQLiteTaskAttemptRepository) GetParallelism(ctx context.Context, attemptID string) (*taskattempts.AttemptParallelism, error) {
	if attemptID == "" {
		return nil, nil
	}
	var p taskattempts.AttemptParallelism
	err := r.store.db.QueryRowContext(ctx,
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
		return nil, err
	}
	return &p, nil
}
