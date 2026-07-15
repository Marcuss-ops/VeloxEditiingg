package store

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"velox-server/internal/ingest"
	"velox-server/internal/taskattempts"
)

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
