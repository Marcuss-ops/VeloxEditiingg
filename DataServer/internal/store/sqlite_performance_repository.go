package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"velox-server/internal/performance"
)

// SQLitePerformanceRepository implements performance.Repository against
// *SQLiteStore.
type SQLitePerformanceRepository struct {
	store *SQLiteStore
}

// NewSQLitePerformanceRepository wraps a SQLiteStore as a performance.Repository.
func NewSQLitePerformanceRepository(store *SQLiteStore) *SQLitePerformanceRepository {
	return &SQLitePerformanceRepository{store: store}
}

// Compile-time assertion.
var _ performance.Repository = (*SQLitePerformanceRepository)(nil)

// Upsert inserts a new baseline or replaces an existing baseline with the
// same (workload_key, git_sha, config_hash, worker_class) tuple.
func (r *SQLitePerformanceRepository) Upsert(ctx context.Context, baseline *performance.Baseline) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("performance repository: store not initialized")
	}
	if baseline == nil {
		return fmt.Errorf("performance repository: baseline is nil")
	}
	if baseline.BaselineID == "" {
		baseline.BaselineID = uuid.NewString()
	}
	if baseline.CreatedAt.IsZero() {
		baseline.CreatedAt = time.Now().UTC()
	}

	_, err := r.store.db.ExecContext(ctx,
		`INSERT INTO performance_baselines (
			baseline_id, workload_key, git_sha, config_hash, worker_class,
			sample_count, p50_wall_ms, p95_wall_ms,
			p50_render_factor, p95_render_factor, error_rate, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(workload_key, git_sha, config_hash, worker_class) DO UPDATE SET
			sample_count = excluded.sample_count,
			p50_wall_ms = excluded.p50_wall_ms,
			p95_wall_ms = excluded.p95_wall_ms,
			p50_render_factor = excluded.p50_render_factor,
			p95_render_factor = excluded.p95_render_factor,
			error_rate = excluded.error_rate`,
		baseline.BaselineID, baseline.WorkloadKey, baseline.GitSHA, baseline.ConfigHash, baseline.WorkerClass,
		baseline.SampleCount, baseline.P50WallMs, baseline.P95WallMs,
		baseline.P50RenderFactor, baseline.P95RenderFactor, baseline.ErrorRate,
		baseline.CreatedAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("performance baseline upsert: %w", err)
	}
	return nil
}

// Get returns the baseline matching the unique tuple, or (nil, nil) when no
// baseline exists.
func (r *SQLitePerformanceRepository) Get(ctx context.Context, workloadKey, gitSHA, configHash, workerClass string) (*performance.Baseline, error) {
	if r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("performance repository: store not initialized")
	}
	row := r.store.db.QueryRowContext(ctx,
		`SELECT baseline_id, workload_key, git_sha, config_hash, worker_class,
		        sample_count, p50_wall_ms, p95_wall_ms,
		        p50_render_factor, p95_render_factor, error_rate, created_at
		 FROM performance_baselines
		 WHERE workload_key = ? AND git_sha = ? AND config_hash = ? AND worker_class = ?`,
		workloadKey, gitSHA, configHash, workerClass,
	)
	return scanBaseline(row)
}

// ListByWorkloadKey returns all baselines for a given workload class,
// ordered by created_at descending.
func (r *SQLitePerformanceRepository) ListByWorkloadKey(ctx context.Context, workloadKey string) ([]performance.Baseline, error) {
	if r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("performance repository: store not initialized")
	}
	rows, err := r.store.db.QueryContext(ctx,
		`SELECT baseline_id, workload_key, git_sha, config_hash, worker_class,
		        sample_count, p50_wall_ms, p95_wall_ms,
		        p50_render_factor, p95_render_factor, error_rate, created_at
		 FROM performance_baselines
		 WHERE workload_key = ?
		 ORDER BY created_at DESC`,
		workloadKey,
	)
	if err != nil {
		return nil, fmt.Errorf("performance baseline list by workload: %w", err)
	}
	defer rows.Close()

	var results []performance.Baseline
	for rows.Next() {
		b, err := scanBaseline(rows)
		if err != nil {
			return nil, fmt.Errorf("performance baseline list by workload row: %w", err)
		}
		results = append(results, *b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("performance baseline list by workload rows: %w", err)
	}
	return results, nil
}

// ListByGitSHA returns all baselines for a given git SHA, ordered by
// created_at descending.
func (r *SQLitePerformanceRepository) ListByGitSHA(ctx context.Context, gitSHA string) ([]performance.Baseline, error) {
	if r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("performance repository: store not initialized")
	}
	rows, err := r.store.db.QueryContext(ctx,
		`SELECT baseline_id, workload_key, git_sha, config_hash, worker_class,
		        sample_count, p50_wall_ms, p95_wall_ms,
		        p50_render_factor, p95_render_factor, error_rate, created_at
		 FROM performance_baselines
		 WHERE git_sha = ?
		 ORDER BY created_at DESC`,
		gitSHA,
	)
	if err != nil {
		return nil, fmt.Errorf("performance baseline list by git sha: %w", err)
	}
	defer rows.Close()

	var results []performance.Baseline
	for rows.Next() {
		b, err := scanBaseline(rows)
		if err != nil {
			return nil, fmt.Errorf("performance baseline list by git sha row: %w", err)
		}
		results = append(results, *b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("performance baseline list by git sha rows: %w", err)
	}
	return results, nil
}

// RecordBenchmarkRun persists immutable benchmark evidence. An identical replay
// is accepted; a divergent payload using the same run_id returns a conflict.
func (r *SQLitePerformanceRepository) RecordBenchmarkRun(ctx context.Context, run *performance.BenchmarkRun) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("performance repository: store not initialized")
	}
	if err := run.Validate(); err != nil {
		return err
	}
	hash, err := performance.ComputeBenchmarkRunPayloadHash(run)
	if err != nil {
		return err
	}
	run.PayloadHash = hash
	if run.CreatedAt.IsZero() {
		run.CreatedAt = time.Now().UTC()
	}

	_, err = r.store.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO performance_benchmark_runs (
			run_id, payload_hash, benchmark_case_id, job_id, task_id, attempt_id,
			worker_id, worker_snapshot_id, cache_mode, git_sha, engine_version,
			ffmpeg_version, config_hash, docker_image_digest, status, render_factor,
			wall_ms, output_duration_ms, output_sha256, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		run.RunID, run.PayloadHash, run.BenchmarkCaseID, run.JobID, run.TaskID, run.AttemptID,
		run.WorkerID, run.WorkerSnapshotID, run.CacheMode, run.GitSHA, run.EngineVersion,
		run.FFmpegVersion, run.ConfigHash, run.DockerImageDigest, run.Status, run.RenderFactor,
		run.WallMS, run.OutputDurationMS, run.OutputSHA256, run.CreatedAt.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("performance benchmark run insert: %w", err)
	}

	existing, err := r.GetBenchmarkRun(ctx, run.RunID)
	if err != nil {
		return err
	}
	if existing == nil {
		return fmt.Errorf("performance benchmark run %q was not persisted", run.RunID)
	}
	if existing.PayloadHash != run.PayloadHash {
		return &performance.BenchmarkRunConflictError{RunID: run.RunID}
	}
	return nil
}

// GetBenchmarkRun returns one persisted benchmark run, or nil when absent.
func (r *SQLitePerformanceRepository) GetBenchmarkRun(ctx context.Context, runID string) (*performance.BenchmarkRun, error) {
	if r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("performance repository: store not initialized")
	}
	row := r.store.db.QueryRowContext(ctx, `
		SELECT run_id, payload_hash, benchmark_case_id, job_id, task_id, attempt_id,
		       worker_id, worker_snapshot_id, cache_mode, git_sha, engine_version,
		       ffmpeg_version, config_hash, docker_image_digest, status, render_factor,
		       wall_ms, output_duration_ms, output_sha256, created_at
		FROM performance_benchmark_runs WHERE run_id = ?`, runID)
	var run performance.BenchmarkRun
	var createdAt string
	if err := row.Scan(
		&run.RunID, &run.PayloadHash, &run.BenchmarkCaseID, &run.JobID, &run.TaskID, &run.AttemptID,
		&run.WorkerID, &run.WorkerSnapshotID, &run.CacheMode, &run.GitSHA, &run.EngineVersion,
		&run.FFmpegVersion, &run.ConfigHash, &run.DockerImageDigest, &run.Status, &run.RenderFactor,
		&run.WallMS, &run.OutputDurationMS, &run.OutputSHA256, &createdAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("scan performance benchmark run: %w", err)
	}
	if createdAt != "" {
		parsed, parseErr := time.Parse(time.RFC3339Nano, createdAt)
		if parseErr != nil {
			return nil, fmt.Errorf("parse performance benchmark run created_at: %w", parseErr)
		}
		run.CreatedAt = parsed
	}
	return &run, nil
}

func scanBaseline(row interface{ Scan(...interface{}) error }) (*performance.Baseline, error) {
	var b performance.Baseline
	var createdAt string
	err := row.Scan(
		&b.BaselineID, &b.WorkloadKey, &b.GitSHA, &b.ConfigHash, &b.WorkerClass,
		&b.SampleCount, &b.P50WallMs, &b.P95WallMs,
		&b.P50RenderFactor, &b.P95RenderFactor, &b.ErrorRate, &createdAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan performance baseline: %w", err)
	}
	if createdAt != "" {
		pt, parseErr := time.Parse(time.RFC3339Nano, createdAt)
		if parseErr != nil {
			return nil, fmt.Errorf("parse performance baseline created_at: %w", parseErr)
		}
		b.CreatedAt = pt
	}
	return &b, nil
}
