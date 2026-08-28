// worker_benchmark_results.go owns the canonical capacity_benchmark_runs
// lifecycle: persist a completed benchmark run, its normalized level/gain
// facts, query history, and store validation results for scorecard accuracy.

package store

import (
	"context"
	"database/sql"
	"fmt"

	"velox-server/internal/performance"
)

// BenchmarkResultRow is the persisted projection of a ConcurrentBenchmarkResult.
type BenchmarkResultRow struct {
	ID                     int64
	WorkerID               string
	BenchmarkRunID         string
	FixtureID              string
	MaxConcurrency         int
	RunsPerLevel           int
	CacheMode              string
	SweetSpot              int
	LimitingFactor         string
	Levels                 []performance.ConcurrencyLevelResult
	Gains                  []performance.ThroughputGain
	Summary                string
	SnapshotRAMBytes       int64
	SnapshotCPUCores       int
	SnapshotDiskReadMbps   float64
	SnapshotUploadMbps     float64
	PredictedRenderSlots   *int
	PredictedSweetSpot     *int
	PredictionAccuracy     *string
	SuggestedRAMSafety     *float64
	SuggestedDiskSafety    *float64
	SuggestedNetworkSafety *float64
	TuningRationale        *string
	StartedAt              string
	CompletedAt            string
	ValidatedAt            *string
}

// UpsertBenchmarkResult persists a completed benchmark run.
func (s *SQLiteStore) UpsertBenchmarkResult(ctx context.Context, row BenchmarkResultRow) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("upsert benchmark result: store not initialized")
	}
	if row.WorkerID == "" || row.BenchmarkRunID == "" {
		return fmt.Errorf("upsert benchmark result: worker_id and benchmark_run_id are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO capacity_benchmark_runs (
		worker_id, benchmark_run_id, fixture_id, max_concurrency, runs_per_level,
		cache_mode, sweet_spot, limiting_factor, summary,
		snapshot_ram_bytes, snapshot_cpu_cores, snapshot_disk_read_mbps, snapshot_upload_mbps,
		started_at, completed_at
	) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
	ON CONFLICT(benchmark_run_id) DO UPDATE SET
		sweet_spot=excluded.sweet_spot,
		limiting_factor=excluded.limiting_factor,
		summary=excluded.summary,
		completed_at=excluded.completed_at`,
		row.WorkerID, row.BenchmarkRunID, row.FixtureID, row.MaxConcurrency, row.RunsPerLevel,
		row.CacheMode, row.SweetSpot, row.LimitingFactor, row.Summary,
		row.SnapshotRAMBytes, row.SnapshotCPUCores, row.SnapshotDiskReadMbps, row.SnapshotUploadMbps,
		row.StartedAt, row.CompletedAt,
	)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM capacity_benchmark_levels WHERE benchmark_run_id=?`, row.BenchmarkRunID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM capacity_benchmark_gains WHERE benchmark_run_id=?`, row.BenchmarkRunID); err != nil {
		return err
	}
	for _, level := range row.Levels {
		_, err = tx.ExecContext(ctx, `INSERT OR REPLACE INTO capacity_benchmark_levels (
			benchmark_run_id, level, total_runs, successful_runs, failed_runs, total_wall_ms,
			avg_wall_ms, p50_wall_ms, p95_wall_ms, throughput, peak_ram_bytes, avg_ram_bytes,
			render_wall_ms, upload_wall_ms, render_per_job_ms, upload_per_job_ms,
			render_jobs_active, prefetch_jobs_active, publisher_active
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			row.BenchmarkRunID, level.Level, level.TotalRuns, level.SuccessfulRuns, level.FailedRuns,
			level.TotalWallMS, level.AvgWallMS, level.P50WallMS, level.P95WallMS, level.Throughput,
			level.PeakRAMBytes, level.AvgRAMBytes, level.RenderWallMS, level.UploadWallMS,
			level.RenderPerJobMS, level.UploadPerJobMS, level.RenderJobsActive, level.PrefetchActive,
			level.PublisherActive)
		if err != nil {
			return err
		}
	}
	for _, gain := range row.Gains {
		efficient := 0
		if gain.IsEfficient {
			efficient = 1
		}
		_, err = tx.ExecContext(ctx, `INSERT OR REPLACE INTO capacity_benchmark_gains
			(benchmark_run_id, from_level, to_level, gain_percent, absolute_gain, is_efficient)
			VALUES (?,?,?,?,?,?)`, row.BenchmarkRunID, gain.FromLevel, gain.ToLevel,
			gain.GainPercent, gain.AbsoluteGain, efficient)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// UpdateBenchmarkValidation stores the scorecard validation and threshold
// tuning results for a benchmark run.
func (s *SQLiteStore) UpdateBenchmarkValidation(ctx context.Context, runID string, predictedRenderSlots, predictedSweetSpot *int, accuracy *string, ramSafety, diskSafety, networkSafety *float64, rationale *string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("update benchmark validation: store not initialized")
	}
	if runID == "" {
		return fmt.Errorf("update benchmark validation: benchmark_run_id is required")
	}
	_, err := s.db.ExecContext(ctx, `UPDATE capacity_benchmark_runs SET
		predicted_render_slots=?, predicted_sweet_spot=?, prediction_accuracy=?,
		suggested_ram_safety=?, suggested_disk_safety=?, suggested_network_safety=?,
		tuning_rationale=?, validated_at=?
	WHERE benchmark_run_id=?`,
		predictedRenderSlots, predictedSweetSpot, accuracy,
		ramSafety, diskSafety, networkSafety,
		rationale, nowRFC3339(), runID,
	)
	return err
}

// GetBenchmarkResults returns the most recent benchmark results for a worker.
func (s *SQLiteStore) GetBenchmarkResults(ctx context.Context, workerID string, limit int) ([]BenchmarkResultRow, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("get benchmark results: store not initialized")
	}
	if workerID == "" {
		return nil, fmt.Errorf("get benchmark results: worker_id is required")
	}
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.QueryContext(ctx, `SELECT
		id, worker_id, benchmark_run_id, fixture_id, max_concurrency, runs_per_level,
		cache_mode, sweet_spot, limiting_factor, summary,
		snapshot_ram_bytes, snapshot_cpu_cores, snapshot_disk_read_mbps, snapshot_upload_mbps,
		predicted_render_slots, predicted_sweet_spot, prediction_accuracy,
		suggested_ram_safety, suggested_disk_safety, suggested_network_safety,
		tuning_rationale, started_at, completed_at, validated_at
	FROM capacity_benchmark_runs
	WHERE worker_id = ?
	ORDER BY completed_at DESC
	LIMIT ?`, workerID, limit)
	if err != nil {
		return nil, fmt.Errorf("get benchmark results: %w", err)
	}
	defer rows.Close()
	var out []BenchmarkResultRow
	for rows.Next() {
		var r BenchmarkResultRow
		if err := rows.Scan(
			&r.ID, &r.WorkerID, &r.BenchmarkRunID, &r.FixtureID, &r.MaxConcurrency, &r.RunsPerLevel,
			&r.CacheMode, &r.SweetSpot, &r.LimitingFactor, &r.Summary,
			&r.SnapshotRAMBytes, &r.SnapshotCPUCores, &r.SnapshotDiskReadMbps, &r.SnapshotUploadMbps,
			&r.PredictedRenderSlots, &r.PredictedSweetSpot, &r.PredictionAccuracy,
			&r.SuggestedRAMSafety, &r.SuggestedDiskSafety, &r.SuggestedNetworkSafety,
			&r.TuningRationale, &r.StartedAt, &r.CompletedAt, &r.ValidatedAt,
		); err != nil {
			return nil, fmt.Errorf("get benchmark results scan: %w", err)
		}
		levelRows, err := s.db.QueryContext(ctx, `SELECT
			level, total_runs, successful_runs, failed_runs, total_wall_ms,
			avg_wall_ms, p50_wall_ms, p95_wall_ms, throughput, peak_ram_bytes,
			avg_ram_bytes, render_wall_ms, upload_wall_ms, render_per_job_ms,
			upload_per_job_ms, render_jobs_active, prefetch_jobs_active, publisher_active
			FROM capacity_benchmark_levels WHERE benchmark_run_id=? ORDER BY level`, r.BenchmarkRunID)
		if err != nil {
			return nil, fmt.Errorf("get benchmark levels: %w", err)
		}
		for levelRows.Next() {
			var level performance.ConcurrencyLevelResult
			if err := levelRows.Scan(&level.Level, &level.TotalRuns, &level.SuccessfulRuns,
				&level.FailedRuns, &level.TotalWallMS, &level.AvgWallMS, &level.P50WallMS,
				&level.P95WallMS, &level.Throughput, &level.PeakRAMBytes, &level.AvgRAMBytes,
				&level.RenderWallMS, &level.UploadWallMS, &level.RenderPerJobMS,
				&level.UploadPerJobMS, &level.RenderJobsActive, &level.PrefetchActive,
				&level.PublisherActive); err != nil {
				levelRows.Close()
				return nil, fmt.Errorf("scan benchmark level: %w", err)
			}
			r.Levels = append(r.Levels, level)
		}
		if err := levelRows.Err(); err != nil {
			levelRows.Close()
			return nil, err
		}
		levelRows.Close()
		gainRows, err := s.db.QueryContext(ctx, `SELECT from_level, to_level, gain_percent, absolute_gain, is_efficient
			FROM capacity_benchmark_gains WHERE benchmark_run_id=? ORDER BY to_level`, r.BenchmarkRunID)
		if err != nil {
			return nil, fmt.Errorf("get benchmark gains: %w", err)
		}
		for gainRows.Next() {
			var gain performance.ThroughputGain
			var efficient int
			if err := gainRows.Scan(&gain.FromLevel, &gain.ToLevel, &gain.GainPercent, &gain.AbsoluteGain, &efficient); err != nil {
				gainRows.Close()
				return nil, fmt.Errorf("scan benchmark gain: %w", err)
			}
			gain.IsEfficient = efficient != 0
			r.Gains = append(r.Gains, gain)
		}
		if err := gainRows.Err(); err != nil {
			gainRows.Close()
			return nil, err
		}
		gainRows.Close()
		out = append(out, r)
	}
	return out, rows.Err()
}

// GetLatestBenchmarkSweetSpot returns the most recent sweet spot for a worker,
// or 0 if no benchmark has been run.
func (s *SQLiteStore) GetLatestBenchmarkSweetSpot(ctx context.Context, workerID string) (int, string, error) {
	if s == nil || s.db == nil {
		return 0, "", fmt.Errorf("get latest benchmark sweet spot: store not initialized")
	}
	var sweetSpot int
	var limiting string
	err := s.db.QueryRowContext(ctx, `SELECT sweet_spot, limiting_factor
	FROM capacity_benchmark_runs
	WHERE worker_id = ?
	ORDER BY completed_at DESC
	LIMIT 1`, workerID).Scan(&sweetSpot, &limiting)
	if err == sql.ErrNoRows {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", fmt.Errorf("get latest benchmark sweet spot: %w", err)
	}
	return sweetSpot, limiting, nil
}
