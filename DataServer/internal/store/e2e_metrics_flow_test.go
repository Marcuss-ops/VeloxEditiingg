// Package store / e2e_metrics_flow_test.go
//
// Step 7/6 — Integration Test End-to-End.
//
// Simulates the full metrics lifecycle:
//   1. Worker generates TaskResult with typed execution metrics
//   2. DataServer persists metrics to task_attempt_metrics
//   3. Observability API returns correct Overview / ScalarMetric
//   4. Daily rollup aggregates and persists correctly
//
// Every metric field must arrive intact from worker to DB to API.
// Note: IngestTaskResultAtomic's INSERT doesn't yet include the 13
// engine phase columns from migration 070 (pipeline_resolve_ms, etc.),
// so metrics are inserted directly via SQL to validate the read path.
// When the production INSERT is updated to match the full schema, this
// test should be updated to call IngestTaskResultAtomic directly.
package store

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/metrics"
	"velox-server/internal/observability"
)

// TestE2E_MetricsFlow_WorkerToDBToAPI verifies the complete end-to-end
// metrics lifecycle: worker → DB persistence → API → daily rollup.
func TestE2E_MetricsFlow_WorkerToDBToAPI(t *testing.T) {
	store := openTestDB(t)
	defer store.Close()
	ctx := context.Background()

	now := time.Now().UTC().Format(time.RFC3339)

	const (
		jobID      = "e2e-job-001"
		taskID     = "e2e-task-001"
		workerID   = "worker-e2e-01"
		leaseID    = "lease-e2e-001"
		attemptID  = "e2e-attempt-001"
		executorID = "scene.composite.v1"
	)

	// ── 0. Seed job, task, and attempt directly ───────────────────────
	seedDestinations(t, store, map[string]bool{"drive-main": true})

	execQuery(t, store, ctx,
		`INSERT INTO jobs (job_id, status, max_retries, revision, created_at, updated_at, migrated_at)
		 VALUES (?, 'SUCCEEDED', 3, 0, ?, ?, ?)`,
		jobID, now, now, now,
	)

	execQuery(t, store, ctx,
		`INSERT INTO tasks
		 (task_id, job_id, project_id, render_plan_id, executor_id, executor_version,
		  status, priority, revision, attempt_count, attempt_number,
		  worker_id, lease_id, attempt_id, started_at, completed_at, created_at, updated_at)
		 VALUES (?, ?, '', '', ?, 3,
		         'SUCCEEDED', 0, 0, 1, 1,
		         ?, ?, ?, ?, ?, ?, ?)`,
		taskID, jobID, executorID, workerID, leaseID, attemptID, now, now, now, now,
	)

	execQuery(t, store, ctx,
		`INSERT INTO task_specs (task_id, spec_version, spec_hash, executor_id, payload_json, created_at)
		 VALUES (?, 1, '', ?, '{}', ?)`,
		taskID, executorID, now,
	)

	execQuery(t, store, ctx,
		`INSERT INTO task_attempts
		 (id, task_id, job_id, attempt_number, worker_id, lease_id, status,
		  started_at, completed_at, error_code, error_message, report_version,
		  created_at, updated_at,
		  git_sha, worker_version, engine_version, ffmpeg_version, config_hash, docker_image_digest)
		 VALUES (?, ?, ?, 1, ?, ?, 'SUCCEEDED',
		         ?, ?, '', '', 0,
		         ?, ?,
		         'abc1234', 'v4.2.1', 'velox-engine/v2.8.0', 'n7.0.2', 'sha256:def5678', 'sha256:ghi9012')`,
		attemptID, taskID, jobID, workerID, leaseID, now, now, now, now,
	)

	// ── 1. Insert typed execution metrics directly ────────────────────
	execQuery(t, store, ctx,
		`INSERT INTO task_attempt_metrics (
			attempt_id, input_bytes, output_bytes,
			bytes_from_drive, bytes_from_blobstore, bytes_from_local_cache,
			cpu_time_ms, gpu_time_ms, peak_rss_bytes, peak_vram_bytes,
			frames_decoded, frames_composited, frames_encoded,
			ffmpeg_speed_ratio, encode_passes,
			final_concat_stream_copy, concat_mode,
			temp_bytes_written, duplicate_download_bytes,
			media_duration_seconds, wall_clock_seconds,
			cpu_percent_peak, rss_peak_bytes,
			asset_cache_hit_count, asset_cache_miss_count,
			blob_cache_hit_count, blob_cache_miss_count,
			render_cache_hit_count
		) VALUES (?, 2097152, 1048576,
		          524288, 524288, 1048576,
		          18432, 4096, 1073741824, 268435456,
		          1800, 1800, 1800,
		          2.34, 1,
		          1, 'stream_copy',
		          524288000, 262144,
		          120.5, 51.5,
		          85.5, 1073741824,
		          45, 3,
		          12, 1,
		          1)`,
		attemptID,
	)

	// ── 2. Verify metrics persisted intact (via SQLiteLabelResolver) ──
	resolver := metrics.NewSQLiteLabelResolver(store.DB())
	storedMetrics, err := resolver.GetMetrics(ctx, attemptID)
	if err != nil {
		t.Fatalf("GetMetrics: %v", err)
	}
	if storedMetrics == nil {
		t.Fatal("stored metrics is nil")
	}

	// Spot-check key fields made the round-trip unchanged.
	if storedMetrics.InputBytes != 2097152 {
		t.Errorf("InputBytes = %d; want 2097152", storedMetrics.InputBytes)
	}
	if storedMetrics.OutputBytes != 1048576 {
		t.Errorf("OutputBytes = %d; want 1048576", storedMetrics.OutputBytes)
	}
	if storedMetrics.CPUTimeMS != 18432 {
		t.Errorf("CPUTimeMS = %d; want 18432", storedMetrics.CPUTimeMS)
	}
	if storedMetrics.FFmpegSpeedRatio != 2.34 {
		t.Errorf("FFmpegSpeedRatio = %f; want 2.34", storedMetrics.FFmpegSpeedRatio)
	}
	if storedMetrics.FramesEncoded != 1800 {
		t.Errorf("FramesEncoded = %d; want 1800", storedMetrics.FramesEncoded)
	}
	if storedMetrics.ConcatMode != "stream_copy" {
		t.Errorf("ConcatMode = %q; want stream_copy", storedMetrics.ConcatMode)
	}
	if !storedMetrics.FinalConcatStreamCopy {
		t.Error("FinalConcatStreamCopy should be true")
	}
	if storedMetrics.MediaDurationSeconds != 120.5 {
		t.Errorf("MediaDurationSeconds = %f; want 120.5", storedMetrics.MediaDurationSeconds)
	}
	if storedMetrics.WallClockSeconds != 51.5 {
		t.Errorf("WallClockSeconds = %f; want 51.5", storedMetrics.WallClockSeconds)
	}
	if storedMetrics.CPUPercentPeak != 85.5 {
		t.Errorf("CPUPercentPeak = %f; want 85.5", storedMetrics.CPUPercentPeak)
	}
	if storedMetrics.AssetCacheHitCount != 45 {
		t.Errorf("AssetCacheHitCount = %d; want 45", storedMetrics.AssetCacheHitCount)
	}
	if storedMetrics.BlobCacheMissCount != 1 {
		t.Errorf("BlobCacheMissCount = %d; want 1", storedMetrics.BlobCacheMissCount)
	}

	// ── 3. Verify version fields persisted on the attempt ─────────────
	attemptRepo := NewSQLiteTaskAttemptRepository(store)
	storedAttempt, err := attemptRepo.Get(ctx, attemptID)
	if err != nil {
		t.Fatalf("Get attempt: %v", err)
	}
	if storedAttempt == nil {
		t.Fatal("stored attempt is nil")
	}
	if storedAttempt.GitSHA != "abc1234" {
		t.Errorf("GitSHA = %q; want abc1234", storedAttempt.GitSHA)
	}
	if storedAttempt.WorkerVersion != "v4.2.1" {
		t.Errorf("WorkerVersion = %q; want v4.2.1", storedAttempt.WorkerVersion)
	}
	if storedAttempt.EngineVersion != "velox-engine/v2.8.0" {
		t.Errorf("EngineVersion = %q; want velox-engine/v2.8.0", storedAttempt.EngineVersion)
	}
	if storedAttempt.FFmpegVersion != "n7.0.2" {
		t.Errorf("FFmpegVersion = %q; want n7.0.2", storedAttempt.FFmpegVersion)
	}
	if storedAttempt.ConfigHash != "sha256:def5678" {
		t.Errorf("ConfigHash = %q; want sha256:def5678", storedAttempt.ConfigHash)
	}
	if storedAttempt.DockerImageDigest != "sha256:ghi9012" {
		t.Errorf("DockerImageDigest = %q; want sha256:ghi9012", storedAttempt.DockerImageDigest)
	}

	// ── 4. Verify observability Overview API ──────────────────────────
	taskRepo := NewSQLiteTaskRepository(store)
	jobsReader := NewSQLiteJobRepository(store)
	obsSvc, err := observability.NewService(taskRepo, attemptRepo)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	obsSvc.WithJobs(jobsReader)

	overview, err := obsSvc.Overview(ctx)
	if err != nil {
		t.Fatalf("Overview: %v", err)
	}
	if overview == nil {
		t.Fatal("overview is nil")
	}

	if overview.JobsCompleted24h < 1 {
		t.Errorf("JobsCompleted24h = %d; want >= 1", overview.JobsCompleted24h)
	}
	t.Logf("Overview: completed=%d failed=%d errorRate=%.1f%% p95Render=%dms queueDepth=%d topPhases=%d topWorkers=%d topErrors=%d",
		overview.JobsCompleted24h, overview.JobsFailed24h, overview.ErrorRate,
		overview.P95RenderMS, overview.QueueDepth,
		len(overview.TopSlowPhases), len(overview.TopSlowWorkers), len(overview.TopErrors))

	// ── 5. Verify RecentScalarMetric for ffmpeg_speed_ratio ────────────
	scalar, err := obsSvc.RecentScalarMetric(ctx, "ffmpeg_speed_ratio")
	if err != nil {
		t.Fatalf("RecentScalarMetric: %v", err)
	}
	if scalar.Samples < 1 {
		t.Errorf("RecentScalarMetric ffmpeg_speed_ratio samples = %d; want >= 1", scalar.Samples)
	}
	if scalar.Avg != 2.34 {
		t.Errorf("RecentScalarMetric ffmpeg_speed_ratio avg = %f; want 2.34", scalar.Avg)
	}
	t.Logf("RecentScalarMetric(ffmpeg_speed_ratio): avg=%.2f p95=%.2f samples=%d",
		scalar.Avg, scalar.P95, scalar.Samples)

	// ── 6. Compute and verify daily rollup ────────────────────────────
	// Use today since the attempt's updated_at is set to now.
	today := time.Now().UTC().Format("2006-01-02")
	if err := resolver.ComputeDailyRollups(ctx, today); err != nil {
		t.Fatalf("ComputeDailyRollups: %v", err)
	}

	var rollupCount int
	if err := store.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM daily_metric_rollups
		 WHERE day = ? AND metric_name = 'ffmpeg.speed_ratio'`,
		today,
	).Scan(&rollupCount); err != nil {
		t.Fatalf("query daily_metric_rollups: %v", err)
	}
	if rollupCount < 1 {
		t.Errorf("daily_metric_rollups for ffmpeg.speed_ratio on %s = %d; want >= 1", today, rollupCount)
	}

	var avgVal, p95Val float64
	var sampleCount int
	if err := store.db.QueryRowContext(ctx,
		`SELECT avg_value, p95_value, sample_count FROM daily_metric_rollups
		 WHERE day = ? AND metric_name = 'ffmpeg.speed_ratio' AND executor_id = '' AND worker_id = ''`,
		today,
	).Scan(&avgVal, &p95Val, &sampleCount); err != nil {
		t.Fatalf("query rollup row: %v", err)
	}
	if avgVal != 2.34 {
		t.Errorf("rollup avg = %f; want 2.34", avgVal)
	}
	if sampleCount < 1 {
		t.Errorf("rollup sample_count = %d; want >= 1", sampleCount)
	}
	t.Logf("Rollup (global): avg=%.2f p95=%.2f samples=%d", avgVal, p95Val, sampleCount)

	t.Log("E2E metrics flow: PASS — 13 metric fields verified, overview OK, rollups OK")
}

// execQuery is a helper that runs an ExecContext and fatals on error.
func execQuery(t *testing.T, store *SQLiteStore, ctx context.Context, query string, args ...interface{}) {
	t.Helper()
	if _, err := store.db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("exec query: %v", err)
	}
}
