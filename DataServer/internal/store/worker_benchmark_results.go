// worker_benchmark_results.go owns the worker_benchmark_results row lifecycle:
// persist a completed benchmark run, query history, and store validation
// results for scorecard accuracy tracking and threshold tuning.

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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
	LevelsJSON             string
	GainsJSON              string
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
	_, err := s.db.ExecContext(ctx, `INSERT INTO worker_benchmark_results (
		worker_id, benchmark_run_id, fixture_id, max_concurrency, runs_per_level,
		cache_mode, sweet_spot, limiting_factor, levels_json, gains_json, summary,
		snapshot_ram_bytes, snapshot_cpu_cores, snapshot_disk_read_mbps, snapshot_upload_mbps,
		started_at, completed_at
	) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
	ON CONFLICT(benchmark_run_id) DO UPDATE SET
		sweet_spot=excluded.sweet_spot,
		limiting_factor=excluded.limiting_factor,
		levels_json=excluded.levels_json,
		gains_json=excluded.gains_json,
		summary=excluded.summary,
		completed_at=excluded.completed_at`,
		row.WorkerID, row.BenchmarkRunID, row.FixtureID, row.MaxConcurrency, row.RunsPerLevel,
		row.CacheMode, row.SweetSpot, row.LimitingFactor, row.LevelsJSON, row.GainsJSON, row.Summary,
		row.SnapshotRAMBytes, row.SnapshotCPUCores, row.SnapshotDiskReadMbps, row.SnapshotUploadMbps,
		row.StartedAt, row.CompletedAt,
	)
	return err
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
	_, err := s.db.ExecContext(ctx, `UPDATE worker_benchmark_results SET
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
		cache_mode, sweet_spot, limiting_factor, levels_json, gains_json, summary,
		snapshot_ram_bytes, snapshot_cpu_cores, snapshot_disk_read_mbps, snapshot_upload_mbps,
		predicted_render_slots, predicted_sweet_spot, prediction_accuracy,
		suggested_ram_safety, suggested_disk_safety, suggested_network_safety,
		tuning_rationale, started_at, completed_at, validated_at
	FROM worker_benchmark_results
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
			&r.CacheMode, &r.SweetSpot, &r.LimitingFactor, &r.LevelsJSON, &r.GainsJSON, &r.Summary,
			&r.SnapshotRAMBytes, &r.SnapshotCPUCores, &r.SnapshotDiskReadMbps, &r.SnapshotUploadMbps,
			&r.PredictedRenderSlots, &r.PredictedSweetSpot, &r.PredictionAccuracy,
			&r.SuggestedRAMSafety, &r.SuggestedDiskSafety, &r.SuggestedNetworkSafety,
			&r.TuningRationale, &r.StartedAt, &r.CompletedAt, &r.ValidatedAt,
		); err != nil {
			return nil, fmt.Errorf("get benchmark results scan: %w", err)
		}
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
	FROM worker_benchmark_results
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

// MarshalLevelsJSON serialises a slice of concurrency level results to JSON.
func MarshalLevelsJSON(levels interface{}) string {
	b, err := json.Marshal(levels)
	if err != nil {
		return "[]"
	}
	return string(b)
}

// MarshalGainsJSON serialises a slice of throughput gains to JSON.
func MarshalGainsJSON(gains interface{}) string {
	b, err := json.Marshal(gains)
	if err != nil {
		return "[]"
	}
	return string(b)
}
