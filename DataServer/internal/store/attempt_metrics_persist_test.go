// attempt_metrics_persist_test.go — verifies that PersistMetrics (the
// standalone non-atomic path) correctly inserts metrics after the
// column/placeholder count fix.

package store

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/taskattempts"
)

func TestPersistMetrics_ColumnPlaceholderAlignment(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()
	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339)

	const (
		jobID     = "persist-job-001"
		taskID    = "persist-task-001"
		workerID  = "persist-worker-01"
		leaseID   = "persist-lease-001"
		attemptID = "persist-attempt-001"
	)

	// Seed job + task + attempt.
	seedDestinations(t, s, map[string]bool{"drive-main": true})
	execQuery(t, s, ctx,
		`INSERT INTO jobs (job_id, status, max_retries, revision, created_at, updated_at, migrated_at)
		 VALUES (?, 'RUNNING', 3, 0, ?, ?, ?)`, jobID, now, now, now)
	execQuery(t, s, ctx,
		`INSERT INTO tasks
		 (task_id, job_id, project_id, render_plan_id, executor_id, executor_version,
		  status, priority, revision, attempt_count, attempt_number,
		  worker_id, lease_id, attempt_id, started_at, created_at, updated_at)
		 VALUES (?, ?, '', '', 'render_batch', 3,
		         'RUNNING', 0, 0, 1, 1,
		         ?, ?, ?, ?, ?, ?)`,
		taskID, jobID, workerID, leaseID, attemptID, now, now, now)
	execQuery(t, s, ctx,
		`INSERT INTO task_attempts
		 (id, task_id, job_id, attempt_number, worker_id, lease_id, status,
		  started_at, completed_at, error_code, error_message, report_version,
		  created_at, updated_at)
		 VALUES (?, ?, ?, 1, ?, ?, 'RUNNING',
		         ?, ?, '', '', 0,
		         ?, ?)`,
		attemptID, taskID, jobID, workerID, leaseID, now, now, now, now)

	attemptRepo := NewSQLiteTaskAttemptRepository(s)

	// This previously failed with "88 values for 97 columns".
	metrics := taskattempts.AttemptMetrics{
		AttemptID:             attemptID,
		InputBytes:            2048,
		OutputBytes:           1024,
		CPUTimeMS:             5000,
		GPUTimeMS:             2000,
		PeakRSSBytes:          500000000,
		WallClockSeconds:      20.5,
		PipelineRenderMs:      15000,
		PipelineTotalMs:       18000,
		EngineAssetDownloadMs: 3000,
		EngineSegmentBuildMs:  8000,
		EngineConcatMs:        2000,
		EngineMuxAudioMs:      1000,
		EngineCopyFinalMs:     500,
		FramesDecoded:         3600,
		FramesComposited:      3600,
		FramesEncoded:         3600,
		FFmpegSpeedRatio:      3.2,
		EncodePasses:          1,
		OutputFileSize:        104857600,
		SceneCount:            36,
		SegmentCount:          12,
		OutputSHA256:          "abc123def456",
		// Capacity columns.
		JobPublishBytes:      2097152,
		JobPageFaults:        84,
		JobScratchPeakBytes:  4294967296, // 4 GB
		ProgressiveOverlapMs: 1200,
		TrailerToOpenMs:      350,
		MuxToOpenUS:          850,
	}

	if err := attemptRepo.PersistMetrics(ctx, metrics); err != nil {
		t.Fatalf("PersistMetrics: %v", err)
	}

	// Verify the data was persisted correctly via GetMetrics.
	got, err := attemptRepo.GetMetrics(ctx, attemptID)
	if err != nil {
		t.Fatalf("GetMetrics: %v", err)
	}
	if got == nil {
		t.Fatal("GetMetrics returned nil")
	}

	// Core fields.
	if got.InputBytes != 2048 {
		t.Errorf("InputBytes = %d; want 2048", got.InputBytes)
	}
	if got.CPUTimeMS != 5000 {
		t.Errorf("CPUTimeMS = %d; want 5000", got.CPUTimeMS)
	}
	if got.PipelineRenderMs != 15000 {
		t.Errorf("PipelineRenderMs = %d; want 15000", got.PipelineRenderMs)
	}
	if got.SceneCount != 36 {
		t.Errorf("SceneCount = %d; want 36", got.SceneCount)
	}

	// Capacity columns.
	if got.JobPublishBytes != 2097152 {
		t.Errorf("JobPublishBytes = %d; want 2097152", got.JobPublishBytes)
	}
	if got.JobPageFaults != 84 {
		t.Errorf("JobPageFaults = %d; want 84", got.JobPageFaults)
	}
	if got.JobScratchPeakBytes != 4294967296 {
		t.Errorf("JobScratchPeakBytes = %d; want 4294967296", got.JobScratchPeakBytes)
	}

	// Progressive overlap / trailer / mux columns.
	if got.ProgressiveOverlapMs != 1200 {
		t.Errorf("ProgressiveOverlapMs = %d; want 1200", got.ProgressiveOverlapMs)
	}
	if got.TrailerToOpenMs != 350 {
		t.Errorf("TrailerToOpenMs = %d; want 350", got.TrailerToOpenMs)
	}
	if got.MuxToOpenUS != 850 {
		t.Errorf("MuxToOpenUS = %d; want 850", got.MuxToOpenUS)
	}
}
