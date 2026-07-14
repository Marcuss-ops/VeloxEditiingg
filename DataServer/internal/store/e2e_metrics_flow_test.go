// Package store / e2e_metrics_flow_test.go
//
// Step 7/6 — Integration Test End-to-End.
//
// Simulates the full metrics lifecycle:
//   1. Worker generates TaskResult with typed execution metrics
//   2. DataServer persists via IngestTaskResultAtomic (single atomic tx)
//   3. Observability API returns correct Overview / ScalarMetric
//   4. Daily rollup aggregates and persists correctly
//
// Every metric field must arrive intact from worker to DB to API,
// going through the production code path (IngestTaskResultAtomic).

package store

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"velox-server/internal/ingest"
	"velox-server/internal/metrics"
	"velox-server/internal/observability"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// TestE2E_MetricsFlow_WorkerToDBToAPI verifies the complete end-to-end
// metrics lifecycle: worker → IngestTaskResultAtomic → API → daily rollup.
func TestE2E_MetricsFlow_WorkerToDBToAPI(t *testing.T) {
	store := openTestDB(t)
	defer store.Close()
	ctx := context.Background()

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339)

	const (
		jobID      = "e2e-job-001"
		taskID     = "e2e-task-001"
		workerID   = "worker-e2e-01"
		leaseID    = "lease-e2e-001"
		attemptID  = "e2e-attempt-001"
		executorID = "scene.composite.v1"
	)

	// ── 0. Seed job, task, and attempt ─────────────────────────────────
	// The task must be in RUNNING status with matching (worker_id, lease_id,
	// attempt_id) for IngestTaskResultAtomic's CAS gate to pass.
	seedDestinations(t, store, map[string]bool{"drive-main": true})

	execQuery(t, store, ctx,
		`INSERT INTO jobs (job_id, status, max_retries, revision, created_at, updated_at, migrated_at)
		 VALUES (?, 'RUNNING', 3, 0, ?, ?, ?)`,
		jobID, nowStr, nowStr, nowStr,
	)

	execQuery(t, store, ctx,
		`INSERT INTO tasks
		 (task_id, job_id, project_id, render_plan_id, executor_id, executor_version,
		  status, priority, revision, attempt_count, attempt_number,
		  worker_id, lease_id, attempt_id, started_at, created_at, updated_at)
		 VALUES (?, ?, '', '', ?, 3,
		         'RUNNING', 0, 0, 1, 1,
		         ?, ?, ?, ?, ?, ?)`,
		taskID, jobID, executorID, workerID, leaseID, attemptID, nowStr, nowStr, nowStr,
	)

	execQuery(t, store, ctx,
		`INSERT INTO task_specs (task_id, spec_version, spec_hash, executor_id, payload_json, created_at)
		 VALUES (?, 1, '', ?, '{}', ?)`,
		taskID, executorID, nowStr,
	)

	// PENDING → used by IngestTaskResultAtomic's attempt CAS.
	execQuery(t, store, ctx,
		`INSERT INTO task_attempts
		 (id, task_id, job_id, attempt_number, worker_id, lease_id, status,
		  started_at, completed_at, error_code, error_message, report_version,
		  created_at, updated_at)
		 VALUES (?, ?, ?, 1, ?, ?, 'PENDING',
		         ?, ?, '', '', 0,
		         ?, ?)`,
		attemptID, taskID, jobID, workerID, leaseID,
		nowStr, nowStr, nowStr, nowStr,
	)

	// ── 1. Ingest via the production code path ────────────────────────
	taskRepo := NewSQLiteTaskRepository(store)
	cmd := taskgraph.IngestResultCommand{
		TaskID:        taskID,
		WorkerID:      workerID,
		LeaseID:       leaseID,
		AttemptID:     attemptID,
		TaskStatus:    taskgraph.StatusSucceeded,
		AttemptStatus: taskattempts.AttemptStatusSucceeded,
		// Scorecard v2 / Step 8: versioning.
		GitSHA:            "abc1234",
		WorkerVersion:     "v4.2.1",
		EngineVersion:     "velox-engine/v2.8.0",
		FFmpegVersion:     "n7.0.2",
		ConfigHash:        "sha256:def5678",
		DockerImageDigest: "sha256:ghi9012",
		// Step 16: raw worker report payload for audit/replay.
		RawReportJSON:       `{"task_id":"` + taskID + `","attempt_id":"` + attemptID + `","status":"succeeded"}`,
		RawReportReceivedAt: now,
		// PerformanceReport metadata.
		ReportSchemaVersion: 1,
		ReportVersion:       1,
		Metrics: taskattempts.AttemptMetrics{
			AttemptID: attemptID,
			// Legacy 7 carry-over.
			InputBytes:          2097152,
			OutputBytes:         1048576,
			BytesFromDrive:      524288,
			BytesFromBlobstore:  524288,
			BytesFromLocalCache: 1048576,
			CPUTimeMS:           18432,
			GPUTimeMS:           4096,
			PeakRSSBytes:        1073741824,
			PeakVRAMBytes:       268435456,
			// Scorecard v1.
			FramesDecoded:          1800,
			FramesComposited:       1800,
			FramesEncoded:          1800,
			FFmpegSpeedRatio:       2.34,
			EncodePasses:           1,
			FinalConcatStreamCopy:  true,
			ConcatMode:             "stream_copy",
			TempBytesWritten:       524288000,
			DuplicateDownloadBytes: 262144,
			MediaDurationSeconds:   120.5,
			WallClockSeconds:       51.5,
			// Scorecard v2 / engine phase timing (migration 070).
			PipelineResolveMs:     120,
			PipelineValidateMs:    85,
			PipelineCompileMs:     340,
			PipelineRenderMs:      18200,
			PipelineTotalMs:       18745,
			NativeTotalMs:         19150,
			NativeProcessWaitMs:   405,
			EngineAssetDownloadMs: 2500,
			EngineSegmentBuildMs:  8200,
			EngineConcatMs:        2100,
			EngineAudioDownloadMs: 1500,
			EngineMuxAudioMs:      900,
			EngineCopyFinalMs:     350,
			// Output quality.
			FFprobeValid:    1,
			DurationDiffSec: 0.03,
			HasVideoStream:  true,
			HasAudioStream:  true,
			OutputFileSize:  52428800,
			// Resources.
			CPUPercentPeak: 85.5,
			RSSPeakBytes:   1073741824,
			// Cache counters.
			AssetCacheHitCount:  45,
			AssetCacheMissCount: 3,
			BlobCacheHitCount:   12,
			BlobCacheMissCount:  1,
			RenderCacheHitCount: 1,
		},
	}
	if err := taskRepo.IngestTaskResultAtomic(ctx, cmd); err != nil {
		t.Fatalf("IngestTaskResultAtomic: %v", err)
	}

	// Promote job to SUCCEEDED so Overview picks it up.
	// IngestTaskResultAtomic transitions Task+Attempt, not the parent Job.
	execQuery(t, store, ctx,
		`UPDATE jobs SET status = 'SUCCEEDED', completed_at = ?, updated_at = ? WHERE job_id = ?`,
		nowStr, nowStr, jobID,
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
	// Engine phase timing (migration 070 — the key columns we just added).
	if storedMetrics.PipelineRenderMs != 18200 {
		t.Errorf("PipelineRenderMs = %d; want 18200", storedMetrics.PipelineRenderMs)
	}
	if storedMetrics.PipelineTotalMs != 18745 {
		t.Errorf("PipelineTotalMs = %d; want 18745", storedMetrics.PipelineTotalMs)
	}
	if storedMetrics.EngineSegmentBuildMs != 8200 {
		t.Errorf("EngineSegmentBuildMs = %d; want 8200", storedMetrics.EngineSegmentBuildMs)
	}
	if storedMetrics.EngineConcatMs != 2100 {
		t.Errorf("EngineConcatMs = %d; want 2100", storedMetrics.EngineConcatMs)
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
	today := now.Format("2006-01-02")
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

	// ── 7. Verify raw worker report persisted for audit/replay ──────
	var storedHash, storedJSON, storedReceived, storedPersisted string
	var storedReportSchema, storedReportVersion int
	if err := store.db.QueryRowContext(ctx,
		`SELECT report_hash, raw_report_json, received_at, persisted_at,
		        report_schema, report_version
		 FROM task_attempt_reports WHERE attempt_id = ?`,
		attemptID,
	).Scan(&storedHash, &storedJSON, &storedReceived, &storedPersisted,
		&storedReportSchema, &storedReportVersion); err != nil {
		t.Fatalf("query task_attempt_reports: %v", err)
	}
	if storedJSON != cmd.RawReportJSON {
		t.Errorf("raw_report_json = %q; want %q", storedJSON, cmd.RawReportJSON)
	}
	if storedReceived == "" {
		t.Error("received_at is empty")
	}
	if storedPersisted == "" {
		t.Error("persisted_at is empty")
	}
	if storedReportSchema != 1 {
		t.Errorf("report_schema = %d; want 1", storedReportSchema)
	}
	if storedReportVersion != 1 {
		t.Errorf("report_version = %d; want 1", storedReportVersion)
	}
	// The command did not supply a worker hash, so the master should have
	// computed it from the raw report JSON (backward-compat path).
	expectedHash := fmt.Sprintf("%x", sha256.Sum256([]byte(cmd.RawReportJSON)))
	if storedHash != expectedHash {
		t.Errorf("report_hash = %q; want %q", storedHash, expectedHash)
	}

	// Idempotency: re-ingesting the same raw report should succeed.
	if err := taskRepo.IngestTaskResultAtomic(ctx, cmd); err != nil {
		t.Fatalf("re-ingest same raw report: %v", err)
	}
	var idempotentHash string
	if err := store.db.QueryRowContext(ctx,
		`SELECT report_hash FROM task_attempt_reports WHERE attempt_id = ?`,
		attemptID,
	).Scan(&idempotentHash); err != nil {
		t.Fatalf("query task_attempt_reports after idempotent re-ingest: %v", err)
	}
	if idempotentHash != storedHash {
		t.Errorf("report_hash changed after idempotent re-ingest: %s -> %s", storedHash, idempotentHash)
	}

	// Conflict: a different raw report for the same attempt must fail.
	cmd2 := cmd
	cmd2.RawReportJSON = `{"task_id":"` + taskID + `","attempt_id":"` + attemptID + `","status":"succeeded","extra":true}`
	if err := taskRepo.IngestTaskResultAtomic(ctx, cmd2); err == nil {
		t.Fatal("expected conflict error when re-ingesting different raw report for same attempt")
	}

	t.Log("E2E metrics flow: PASS — 16 metric fields verified (incl. 4 engine phase), overview OK, rollups OK, raw report OK")
}

// TestE2E_FailedAttempt_PartialPhaseMetrics verifies that a FAILED attempt
// persists partial-progress counters, failure attribution, and per-phase
// timings so operators can diagnose where the attempt stopped.
func TestE2E_FailedAttempt_PartialPhaseMetrics(t *testing.T) {
	store := openTestDB(t)
	defer store.Close()
	ctx := context.Background()

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339)

	const (
		jobID      = "e2e-failed-job-001"
		taskID     = "e2e-failed-task-001"
		workerID   = "worker-failed-01"
		leaseID    = "lease-failed-001"
		attemptID  = "e2e-failed-attempt-001"
		executorID = "scene.composite.v1"
	)

	// ── 0. Seed job, task, and attempt ─────────────────────────────────
	seedDestinations(t, store, map[string]bool{"drive-main": true})

	execQuery(t, store, ctx,
		`INSERT INTO jobs (job_id, status, max_retries, revision, created_at, updated_at, migrated_at)
		 VALUES (?, 'RUNNING', 3, 0, ?, ?, ?)`,
		jobID, nowStr, nowStr, nowStr,
	)

	execQuery(t, store, ctx,
		`INSERT INTO tasks
		 (task_id, job_id, project_id, render_plan_id, executor_id, executor_version,
		  status, priority, revision, attempt_count, attempt_number,
		  worker_id, lease_id, attempt_id, started_at, created_at, updated_at)
		 VALUES (?, ?, '', '', ?, 3,
		         'RUNNING', 0, 0, 1, 1,
		         ?, ?, ?, ?, ?, ?)`,
		taskID, jobID, executorID, workerID, leaseID, attemptID, nowStr, nowStr, nowStr,
	)

	execQuery(t, store, ctx,
		`INSERT INTO task_specs (task_id, spec_version, spec_hash, executor_id, payload_json, created_at)
		 VALUES (?, 1, '', ?, '{}', ?)`,
		taskID, executorID, nowStr,
	)

	execQuery(t, store, ctx,
		`INSERT INTO task_attempts
		 (id, task_id, job_id, attempt_number, worker_id, lease_id, status,
		  started_at, completed_at, error_code, error_message, report_version,
		  created_at, updated_at)
		 VALUES (?, ?, ?, 1, ?, ?, 'RUNNING',
		         ?, ?, '', '', 0,
		         ?, ?)`,
		attemptID, taskID, jobID, workerID, leaseID,
		nowStr, nowStr, nowStr, nowStr,
	)

	// ── 1. Ingest a FAILED result with partial phase metrics ────────────
	taskRepo := NewSQLiteTaskRepository(store)
	cmd := taskgraph.IngestResultCommand{
		TaskID:        taskID,
		WorkerID:      workerID,
		LeaseID:       leaseID,
		AttemptID:     attemptID,
		TaskStatus:    taskgraph.StatusFailed,
		AttemptStatus: taskattempts.AttemptStatusFailed,
		ErrorCode:     "RENDER_ENGINE_CRASH",
		ErrorMsg:      "native process exited with code 139",
		RawReportJSON: `{"task_id":"` + taskID + `","attempt_id":"` + attemptID + `","status":"failed"}`,
		Metrics: taskattempts.AttemptMetrics{
			AttemptID:           attemptID,
			InputBytes:          1048576,
			OutputBytes:         0,
			CPUTimeMS:           5120,
			PeakRSSBytes:        536870912,
			CompletedSegments:   7,
			WastedCPUMS:         5120,
			WastedDownloadBytes: 1048576,
			ErrorComponent:      "engine",
			ErrorPhase:          "render",
		},
		PartialPhaseMetrics: []taskattempts.PhaseTimingDetailed{
			{
				AttemptID:    attemptID,
				JobID:        jobID,
				TaskID:       taskID,
				WorkerID:     workerID,
				PhaseOrder:   1,
				Component:    "download",
				Action:       "asset_fetch",
				DurationMS:   2500,
				Status:       "ok",
				BytesIn:      1048576,
				BytesOut:     1048576,
				Frames:       0,
				MetadataJSON: `{"source":"drive"}`,
			},
			{
				AttemptID:    attemptID,
				JobID:        jobID,
				TaskID:       taskID,
				WorkerID:     workerID,
				PhaseOrder:   2,
				Component:    "render",
				Action:       "segment_build",
				DurationMS:   3200,
				Status:       "failed",
				ErrorCode:    "SEGMENT_BUILD_TIMEOUT",
				ErrorMessage: "segment 7 exceeded 30s budget",
				BytesIn:      1048576,
				BytesOut:     0,
				Frames:       180,
				MetadataJSON: `{"segment_index":7}`,
			},
		},
	}
	if err := taskRepo.IngestTaskResultAtomic(ctx, cmd); err != nil {
		t.Fatalf("IngestTaskResultAtomic FAILED path: %v", err)
	}

	// ── 2. Verify task and attempt are terminal ────────────────────────
	var taskStatus, attemptStatus, storedErrorCode string
	if err := store.db.QueryRowContext(ctx,
		`SELECT status FROM tasks WHERE task_id = ?`, taskID,
	).Scan(&taskStatus); err != nil {
		t.Fatalf("SELECT tasks.status: %v", err)
	}
	if taskStatus != "FAILED" {
		t.Errorf("tasks.status = %q; want FAILED", taskStatus)
	}

	if err := store.db.QueryRowContext(ctx,
		`SELECT status, error_code FROM task_attempts WHERE id = ?`, attemptID,
	).Scan(&attemptStatus, &storedErrorCode); err != nil {
		t.Fatalf("SELECT task_attempts: %v", err)
	}
	if attemptStatus != "FAILED" {
		t.Errorf("task_attempts.status = %q; want FAILED", attemptStatus)
	}
	if storedErrorCode != "RENDER_ENGINE_CRASH" {
		t.Errorf("task_attempts.error_code = %q; want RENDER_ENGINE_CRASH", storedErrorCode)
	}

	// ── 3. Verify failure/partial metrics persisted ───────────────────
	var (
		completedSegments int
		wastedCPU         int64
		wastedDownload    int64
		errorComponent    string
		errorPhase        string
	)
	if err := store.db.QueryRowContext(ctx,
		`SELECT completed_segments, wasted_cpu_ms, wasted_download_bytes,
		        error_component, error_phase
		 FROM task_attempt_metrics WHERE attempt_id = ?`,
		attemptID,
	).Scan(&completedSegments, &wastedCPU, &wastedDownload, &errorComponent, &errorPhase); err != nil {
		t.Fatalf("SELECT task_attempt_metrics: %v", err)
	}
	if completedSegments != 7 {
		t.Errorf("completed_segments = %d; want 7", completedSegments)
	}
	if wastedCPU != 5120 {
		t.Errorf("wasted_cpu_ms = %d; want 5120", wastedCPU)
	}
	if wastedDownload != 1048576 {
		t.Errorf("wasted_download_bytes = %d; want 1048576", wastedDownload)
	}
	if errorComponent != "engine" {
		t.Errorf("error_component = %q; want engine", errorComponent)
	}
	if errorPhase != "render" {
		t.Errorf("error_phase = %q; want render", errorPhase)
	}

	// ── 4. Verify partial phase timings persisted ──────────────────────
	rows, err := store.db.QueryContext(ctx,
		`SELECT phase, component, action, status, error_code, duration_ms, bytes_in, frames
		 FROM task_phase_timings WHERE attempt_id = ? ORDER BY phase_order ASC`,
		attemptID,
	)
	if err != nil {
		t.Fatalf("SELECT task_phase_timings: %v", err)
	}
	defer rows.Close()

	type phaseRow struct {
		phase, component, action, status, errorCode string
		durationMS, bytesIn, frames                 int64
	}
	var got []phaseRow
	for rows.Next() {
		var r phaseRow
		if err := rows.Scan(&r.phase, &r.component, &r.action, &r.status, &r.errorCode,
			&r.durationMS, &r.bytesIn, &r.frames); err != nil {
			t.Fatalf("scan phase row: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != 2 {
		t.Fatalf("phase timings rows = %d; want 2", len(got))
	}

	want := []phaseRow{
		{phase: "download.asset_fetch", component: "download", action: "asset_fetch", status: "ok", errorCode: "", durationMS: 2500, bytesIn: 1048576, frames: 0},
		{phase: "render.segment_build", component: "render", action: "segment_build", status: "failed", errorCode: "SEGMENT_BUILD_TIMEOUT", durationMS: 3200, bytesIn: 1048576, frames: 180},
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("phase[%d] = %+v; want %+v", i, got[i], want[i])
		}
	}

	t.Log("E2E FAILED attempt with partial phase metrics: PASS")
}

// TestE2E_PerformanceReport_IngestService simulates a worker sending a
// complete PerformanceReport through the canonical ingestion service and
// verifies that the master persists it correctly across all tables. It
// also covers the idempotent retry path and the conflict path.
func TestE2E_PerformanceReport_IngestService(t *testing.T) {
	store := openTestDB(t)
	defer store.Close()
	ctx := context.Background()

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339)

	const (
		jobID      = "e2e-report-job-001"
		taskID     = "e2e-report-task-001"
		workerID   = "worker-report-01"
		leaseID    = "lease-report-001"
		attemptID  = "e2e-report-attempt-001"
		executorID = "scene.composite.v1"
	)

	// ── 0. Seed job, task, and attempt ─────────────────────────────────
	seedDestinations(t, store, map[string]bool{"drive-main": true})

	execQuery(t, store, ctx,
		`INSERT INTO jobs (job_id, status, max_retries, revision, created_at, updated_at, migrated_at)
		 VALUES (?, 'RUNNING', 3, 0, ?, ?, ?)`,
		jobID, nowStr, nowStr, nowStr,
	)

	execQuery(t, store, ctx,
		`INSERT INTO tasks
		 (task_id, job_id, project_id, render_plan_id, executor_id, executor_version,
		  status, priority, revision, attempt_count, attempt_number,
		  worker_id, lease_id, attempt_id, started_at, created_at, updated_at)
		 VALUES (?, ?, '', '', ?, 3,
		         'RUNNING', 0, 0, 1, 1,
		         ?, ?, ?, ?, ?, ?)`,
		taskID, jobID, executorID, workerID, leaseID, attemptID, nowStr, nowStr, nowStr,
	)

	execQuery(t, store, ctx,
		`INSERT INTO task_specs (task_id, spec_version, spec_hash, executor_id, payload_json, created_at)
		 VALUES (?, 1, '', ?, '{}', ?)`,
		taskID, executorID, nowStr,
	)

	execQuery(t, store, ctx,
		`INSERT INTO task_attempts
		 (id, task_id, job_id, attempt_number, worker_id, lease_id, status,
		  started_at, completed_at, error_code, error_message, report_version,
		  created_at, updated_at)
		 VALUES (?, ?, ?, 1, ?, ?, 'RUNNING',
		         ?, ?, '', '', 0,
		         ?, ?)`,
		attemptID, taskID, jobID, workerID, leaseID,
		nowStr, nowStr, nowStr, nowStr,
	)

	// ── 1. Build the canonical ingestion service with real SQLite repos ─
	taskRepo := NewSQLiteTaskRepository(store)
	jobsRepo := NewSQLiteJobRepository(store)
	attemptRepo := NewSQLiteTaskAttemptRepository(store)
	outputArtRepo := NewSQLiteTaskOutputArtifactsRepository(store)

	ingestSvc, err := ingest.NewTaskReportIngestionService(taskRepo, jobsRepo, attemptRepo, outputArtRepo)
	if err != nil {
		t.Fatalf("NewTaskReportIngestionService: %v", err)
	}

	// ── 2. Build a complete PerformanceReport from the worker ────────────
	rawReportJSON := `{"task_id":"` + taskID + `","attempt_id":"` + attemptID + `","status":"succeeded","render_factor":0.43}`
	reportHash := fmt.Sprintf("%x", sha256.Sum256([]byte(rawReportJSON)))

	cmd := ingest.IngestCommand{
		TaskID:              taskID,
		AttemptID:           attemptID,
		LeaseID:             leaseID,
		WorkerID:            workerID,
		JobID:               jobID,
		AttemptNumber:       1,
		Status:              "succeeded",
		GitSHA:              "abc1234",
		WorkerVersion:       "v4.2.1",
		EngineVersion:       "velox-engine/v2.8.0",
		FFmpegVersion:       "n7.0.2",
		ConfigHash:          "sha256:def5678",
		RawReportJSON:       rawReportJSON,
		RawReportReceivedAt: now,
		ReportSchemaVersion: 1,
		ReportVersion:       1,
		ReportHash:          reportHash,
		TypedMetrics: taskattempts.AttemptMetrics{
			AttemptID:              attemptID,
			InputBytes:             2097152,
			OutputBytes:            1048576,
			BytesFromDrive:         524288,
			BytesFromBlobstore:     524288,
			BytesFromLocalCache:    1048576,
			CPUTimeMS:              18432,
			GPUTimeMS:              4096,
			PeakRSSBytes:           1073741824,
			PeakVRAMBytes:          268435456,
			FramesDecoded:          1800,
			FramesComposited:       1800,
			FramesEncoded:          1800,
			FFmpegSpeedRatio:       2.34,
			EncodePasses:           1,
			FinalConcatStreamCopy:  true,
			ConcatMode:             "stream_copy",
			TempBytesWritten:       524288000,
			DuplicateDownloadBytes: 262144,
			MediaDurationSeconds:   120.5,
			WallClockSeconds:       51.5,
			PipelineResolveMs:      120,
			PipelineValidateMs:     85,
			PipelineCompileMs:      340,
			PipelineRenderMs:       18200,
			PipelineTotalMs:        18745,
			NativeTotalMs:          19150,
			NativeProcessWaitMs:    405,
			EngineAssetDownloadMs:  2500,
			EngineSegmentBuildMs:   8200,
			EngineConcatMs:         2100,
			EngineAudioDownloadMs:  1500,
			EngineMuxAudioMs:       900,
			EngineCopyFinalMs:      350,
			FFprobeValid:           1,
			DurationDiffSec:        0.03,
			HasVideoStream:         true,
			HasAudioStream:         true,
			OutputFileSize:         52428800,
		},
		CacheStats: taskattempts.AttemptCacheStats{
			AttemptID:        attemptID,
			CacheHits:        60,
			CacheMisses:      4,
			CacheEvictions:   1,
			CacheCorruptions: 0,
			CacheBytesUsed:   1048576,
			CacheEntries:     12,
		},
		CostBasis: taskattempts.AttemptCostBasis{
			AttemptID:           attemptID,
			CPUPricePerSecond:   0.0001,
			StoragePricePerGB:   0.0002,
			NetworkPricePerGB:   0.0003,
			CPUTimeSecondsTotal: 18.432,
			StorageGBWritten:    0.5,
			NetworkGBEgressed:   0.001,
			OutputMinutesTotal:  2.008333,
		},
		OutputArtifacts: []ingest.DeclaredArtifact{
			{ArtifactID: "art-1", ArtifactType: "video", Path: "/out/video.mp4", Size: 1048576, SHA256: "deadbeef"},
		},
	}

	// ── 3. First ingestion: worker sends the PerformanceReport ────────────
	res, err := ingestSvc.IngestTaskResult(ctx, cmd)
	if err != nil {
		t.Fatalf("IngestTaskResult first call: %v", err)
	}
	if !res.AttemptClosed {
		t.Errorf("AttemptClosed=false; want true")
	}
	if res.ArtifactsNew != 1 {
		t.Errorf("ArtifactsNew=%d; want 1", res.ArtifactsNew)
	}

	// ── 4. Verify tables after first ingestion ───────────────────────────
	// Phase 2.6 commit protocol: ingesting a SUCCEEDED result leaves the
	// task in RUNNING and sets winning_attempt_terminal_pending=1. The
	// Coordinator.CommitAttempt ratifies SUCCEEDED later, after artifact
	// verification. The attempt itself is terminal (SUCCEEDED).
	var taskStatus string
	var winningPending int
	if err := store.db.QueryRowContext(ctx,
		`SELECT status, COALESCE(winning_attempt_terminal_pending, 0) FROM tasks WHERE task_id = ?`,
		taskID,
	).Scan(&taskStatus, &winningPending); err != nil {
		t.Fatalf("SELECT tasks.status: %v", err)
	}
	if taskStatus != "RUNNING" {
		t.Errorf("tasks.status = %q; want RUNNING (commit protocol)", taskStatus)
	}
	if winningPending != 1 {
		t.Errorf("winning_attempt_terminal_pending = %d; want 1", winningPending)
	}

	var attemptStatus string
	if err := store.db.QueryRowContext(ctx,
		`SELECT status FROM task_attempts WHERE id = ?`, attemptID,
	).Scan(&attemptStatus); err != nil {
		t.Fatalf("SELECT task_attempts.status: %v", err)
	}
	if attemptStatus != "SUCCEEDED" {
		t.Errorf("task_attempts.status = %q; want SUCCEEDED", attemptStatus)
	}

	// Metrics persisted.
	storedMetrics, err := attemptRepo.GetMetrics(ctx, attemptID)
	if err != nil {
		t.Fatalf("GetMetrics: %v", err)
	}
	if storedMetrics == nil {
		t.Fatal("stored metrics is nil")
	}
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

	// Cache stats persisted.
	storedCache, err := attemptRepo.GetCacheStats(ctx, attemptID)
	if err != nil {
		t.Fatalf("GetCacheStats: %v", err)
	}
	if storedCache == nil {
		t.Fatal("stored cache stats is nil")
	}
	if storedCache.CacheHits != 60 {
		t.Errorf("CacheHits = %d; want 60", storedCache.CacheHits)
	}
	if storedCache.CacheMisses != 4 {
		t.Errorf("CacheMisses = %d; want 4", storedCache.CacheMisses)
	}

	// Cost basis persisted.
	storedCost, err := attemptRepo.GetCostBasis(ctx, attemptID)
	if err != nil {
		t.Fatalf("GetCostBasis: %v", err)
	}
	if storedCost == nil {
		t.Fatal("stored cost basis is nil")
	}
	if storedCost.CostPerOutputMinute == 0 {
		t.Errorf("CostPerOutputMinute should be > 0")
	}

	// Output artifact persisted.
	artifacts, err := outputArtRepo.ListByTask(ctx, taskID)
	if err != nil {
		t.Fatalf("ListByTask: %v", err)
	}
	if len(artifacts) != 1 {
		t.Errorf("artifacts count = %d; want 1", len(artifacts))
	}

	// Raw report persisted.
	var storedHash, storedJSON string
	if err := store.db.QueryRowContext(ctx,
		`SELECT report_hash, raw_report_json FROM task_attempt_reports WHERE attempt_id = ?`,
		attemptID,
	).Scan(&storedHash, &storedJSON); err != nil {
		t.Fatalf("query task_attempt_reports: %v", err)
	}
	if storedJSON != rawReportJSON {
		t.Errorf("raw_report_json = %q; want %q", storedJSON, rawReportJSON)
	}
	if storedHash != reportHash {
		t.Errorf("report_hash = %q; want %q", storedHash, reportHash)
	}

	// ── 5. Idempotent retry: same report again ──────────────────────────
	// The ingestion service allows the retry (ValidateIdentityTuple now
	// accepts terminal attempts for the same canonical tuple). The atomic
	// path is idempotent at the DB level: no duplicate rows are written.
	res2, err := ingestSvc.IngestTaskResult(ctx, cmd)
	if err != nil {
		t.Fatalf("IngestTaskResult idempotent retry: %v", err)
	}
	if !res2.AttemptClosed {
		t.Errorf("AttemptClosed=false on idempotent retry; want true (attempt remains closed)")
	}
	if res2.ArtifactsNew != 1 {
		t.Errorf("ArtifactsNew=%d on idempotent retry; want 1 (artifact count from command)", res2.ArtifactsNew)
	}

	// Raw report hash unchanged.
	var idempotentHash string
	if err := store.db.QueryRowContext(ctx,
		`SELECT report_hash FROM task_attempt_reports WHERE attempt_id = ?`,
		attemptID,
	).Scan(&idempotentHash); err != nil {
		t.Fatalf("query task_attempt_reports after idempotent retry: %v", err)
	}
	if idempotentHash != reportHash {
		t.Errorf("report_hash changed after idempotent retry: %s -> %s", reportHash, idempotentHash)
	}

	// ── 6. Conflict: different raw report for the same attempt ──────────
	cmd.RawReportJSON = `{"task_id":"` + taskID + `","attempt_id":"` + attemptID + `","status":"succeeded","extra":true}`
	cmd.ReportHash = fmt.Sprintf("%x", sha256.Sum256([]byte(cmd.RawReportJSON)))
	_, err = ingestSvc.IngestTaskResult(ctx, cmd)
	if err == nil {
		t.Fatal("expected conflict error when re-ingesting different raw report for same attempt")
	}

	t.Log("E2E PerformanceReport through ingestion service: PASS")
}

// execQuery is a helper that runs an ExecContext and fatals on error.
func execQuery(t *testing.T, store *SQLiteStore, ctx context.Context, query string, args ...interface{}) {
	t.Helper()
	if _, err := store.db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("exec query: %v", err)
	}
}
