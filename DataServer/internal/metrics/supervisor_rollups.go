// Package metrics / supervisor_rollups.go
//
// Daily metric rollups sliced out of supervisor.go so the per-tick
// orchestrator (supervisor.go) stays focused on the lifecycle loop.
//
// Scorecard v2 / Metrics Center / Step 2: each Metrics Center day
// produces per-(day, metric_name, executor_id, worker_id) rollup
// rows with avg / p50 / p95 / p99 / min / max / sample_count.
// Computed by the supervisor's tryDailyRollup hook once the
// midnight boundary crosses; idempotent via INSERT OR REPLACE on
// the unique index.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"sort"
	"time"
)

// ── Daily Metric Rollups (Step 2 / Velox Metrics Center) ─────────────────

// dailyRollupDef maps a metric catalog name to its source column in
// task_attempt_metrics. Expand this when new engine phase columns are added.
var dailyRollupMetrics = []struct {
	CatalogName string
	Column      string // SQL column in task_attempt_metrics
}{
	// Engine phases.
	{"engine.asset_download_ms", "engine_asset_download_ms"},
	{"engine.segment_build_ms", "engine_segment_build_ms"},
	{"engine.concat_ms", "engine_concat_ms"},
	{"engine.mux_audio_ms", "engine_mux_audio_ms"},
	{"engine.copy_final_ms", "engine_copy_final_ms"},
	{"engine.audio_download_ms", "engine_audio_download_ms"},
	// Pipeline phases.
	{"pipeline.resolve_ms", "pipeline_resolve_ms"},
	{"pipeline.validate_ms", "pipeline_validate_ms"},
	{"pipeline.compile_ms", "pipeline_compile_ms"},
	{"pipeline.render_ms", "pipeline_render_ms"},
	{"pipeline.total_ms", "pipeline_total_ms"},
	// Native process.
	{"native.total_ms", "native_total_ms"},
	{"native.process_wait_ms", "native_process_wait_ms"},
	// Output.
	{"output.bytes", "output_bytes"},
	// FFmpeg.
	{"ffmpeg.speed_ratio", "ffmpeg_speed_ratio"},
	// Queue.
	{"queue.ms", "queue_ms"},
}

// ComputeDailyRollups aggregates attempt metrics into daily_metric_rollups
// for the given UTC day. One row per (day, metric_name, executor_id, worker_id).
// Idempotent via INSERT OR REPLACE on the unique index.
//
// Errors from individual metric rollups are collected via errors.Join;
// one metric failure doesn't block others, but callers can distinguish
// partial failure from total success.
func (r *SQLiteLabelResolver) ComputeDailyRollups(ctx context.Context, day string) error {
	if day == "" {
		return fmt.Errorf("supervisor: daily rollup: day is empty")
	}

	// For each registered metric, query the column values joined with
	// executor and worker context, compute percentiles, and insert.
	var errs []error
	for _, dm := range dailyRollupMetrics {
		if err := r.rollupOneMetric(ctx, day, dm.CatalogName, dm.Column); err != nil {
			log.Printf("[METRICS-SUPERVISOR] daily rollup metric %s for %s: %v", dm.CatalogName, day, err)
			errs = append(errs, err)
			// Continue — one metric failure shouldn't block others.
		}
	}
	return errors.Join(errs...)
}

// rollupOneMetric computes and persists the daily rollup for a single metric.
//
// The column parameter is interpolated directly into the SQL query.
// This is safe because column values come exclusively from the
// package-level dailyRollupMetrics var — they are hardcoded string
// literals, never user input. If this contract changes (e.g. dynamic
// column registration), switch to a whitelist or prepared-statement
// approach.
func (r *SQLiteLabelResolver) rollupOneMetric(ctx context.Context, day, catalogName, column string) error {
	nextDay, err := dateAddDay(day, 1)
	if err != nil {
		return fmt.Errorf("rollup %s: invalid day %q: %w", column, day, err)
	}
	rows, err := r.DB.QueryContext(ctx, `
		SELECT
			COALESCE(t.executor_id, '')        AS executor_id,
			a.worker_id                         AS worker_id,
			m.`+column+`                        AS val
		FROM task_attempt_metrics m
		JOIN task_attempts a ON a.id = m.attempt_id
		LEFT JOIN tasks t ON t.task_id = a.task_id
		WHERE a.updated_at >= ? || 'T00:00:00Z'
		  AND a.updated_at <  ? || 'T00:00:00Z'
		  AND m.`+column+` > 0
		ORDER BY executor_id, worker_id`,
		day, nextDay,
	)
	if err != nil {
		return fmt.Errorf("query %s: %w", column, err)
	}
	defer rows.Close()

	// Separate maps for per-dimension aggregation.
	byExecutor := make(map[string][]float64)
	byWorker := make(map[string][]float64)
	var globalVals []float64

	for rows.Next() {
		var execID, workerID string
		var val float64
		if err := rows.Scan(&execID, &workerID, &val); err != nil {
			continue
		}
		if execID != "" {
			byExecutor[execID] = append(byExecutor[execID], val)
		}
		if workerID != "" {
			byWorker[workerID] = append(byWorker[workerID], val)
		}
		globalVals = append(globalVals, val)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("scan %s: %w", column, err)
	}

	// Global rollup (executor_id='', worker_id='').
	if len(globalVals) > 0 {
		if err := r.insertRollupRow(ctx, day, catalogName, "", "", globalVals); err != nil {
			// Log and continue — one dimension failure shouldn't block others.
			log.Printf("[METRICS-SUPERVISOR] daily rollup insert global %s: %v", catalogName, err)
		}
	}

	// Per-executor rollups.
	for execID, vals := range byExecutor {
		if len(vals) == 0 {
			continue
		}
		if err := r.insertRollupRow(ctx, day, catalogName, execID, "", vals); err != nil {
			log.Printf("[METRICS-SUPERVISOR] daily rollup insert executor %s/%s: %v", catalogName, execID, err)
		}
	}

	// Per-worker rollups.
	for workerID, vals := range byWorker {
		if len(vals) == 0 {
			continue
		}
		if err := r.insertRollupRow(ctx, day, catalogName, "", workerID, vals); err != nil {
			log.Printf("[METRICS-SUPERVISOR] daily rollup insert worker %s/%s: %v", catalogName, workerID, err)
		}
	}

	return nil
}

// insertRollupRow computes percentiles and inserts/updates one rollup row.
func (r *SQLiteLabelResolver) insertRollupRow(ctx context.Context, day, metricName, executorID, workerID string, vals []float64) error {
	sort.Float64s(vals)
	n := len(vals)
	avg := avgFloat64(vals)
	p50 := percentileFloat64(vals, 0.50)
	p95 := percentileFloat64(vals, 0.95)
	p99 := percentileFloat64(vals, 0.99)
	minv := vals[0]
	maxv := vals[n-1]

	_, err := r.DB.ExecContext(ctx, `
		INSERT OR REPLACE INTO daily_metric_rollups
			(day, metric_name, executor_id, worker_id,
			 avg_value, p50_value, p95_value, p99_value,
			 min_value, max_value, sample_count, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))`,
		day, metricName, executorID, workerID,
		avg, p50, p95, p99, minv, maxv, n,
	)
	if err != nil {
		return fmt.Errorf("insert rollup %s/%s/%s: %w", metricName, executorID, workerID, err)
	}
	return nil
}

// avgFloat64 computes the arithmetic mean.
func avgFloat64(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	var sum float64
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

// percentileFloat64 returns the value at the given percentile (0-1)
// from a sorted slice. Uses linear interpolation for non-integer indices.
func percentileFloat64(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := p * float64(len(sorted)-1)
	lower := int(math.Floor(idx))
	upper := int(math.Ceil(idx))
	if lower < 0 {
		lower = 0
	}
	if upper >= len(sorted) {
		upper = len(sorted) - 1
	}
	if lower == upper {
		return sorted[lower]
	}
	frac := idx - float64(lower)
	return sorted[lower]*(1-frac) + sorted[upper]*frac
}

// dateAddDay adds n days to a YYYY-MM-DD date string and returns the
// result in the same format. n can be negative. Returns an error when
// day is not a valid YYYY-MM-DD date.
func dateAddDay(day string, n int) (string, error) {
	t, err := time.Parse("2006-01-02", day)
	if err != nil {
		return "", fmt.Errorf("dateAddDay: invalid date %q: %w", day, err)
	}
	return t.Add(time.Duration(n) * 24 * time.Hour).Format("2006-01-02"), nil
}
