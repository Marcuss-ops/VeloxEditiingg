// attempt_metrics_capacity_test.go — verifies that the capacity columns
// added in migration 166 (job_publish_bytes, job_page_faults,
// job_scratch_peak_bytes) survive the PersistMetrics → GetMetrics round-trip.

package store

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/taskattempts"
)

func TestAttemptMetrics_CapacityColumnsRoundTrip(t *testing.T) {
	s := openTestDB(t)
	defer s.Close()
	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339)

	const (
		jobID     = "cap-metrics-job-001"
		taskID    = "cap-metrics-task-001"
		workerID  = "cap-metrics-worker-01"
		leaseID   = "cap-metrics-lease-001"
		attemptID = "cap-metrics-attempt-001"
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

	// Persist metrics with non-zero capacity columns.
	metrics := taskattempts.AttemptMetrics{
		AttemptID:         attemptID,
		InputBytes:        1024,
		OutputBytes:       512,
		CPUTimeMS:         1000,
		GPUTimeMS:         500,
		PeakRSSBytes:      200000000,
		WallClockSeconds:  10.5,
		// Capacity columns (migration 166).
		JobPublishBytes:     1048576,
		JobPageFaults:       42,
		JobScratchPeakBytes: 2147483648, // 2 GB
	}

	if err := attemptRepo.PersistMetrics(ctx, metrics); err != nil {
		t.Fatalf("PersistMetrics: %v", err)
	}

	// GetMetrics must return the same capacity values.
	got, err := attemptRepo.GetMetrics(ctx, attemptID)
	if err != nil {
		t.Fatalf("GetMetrics: %v", err)
	}
	if got == nil {
		t.Fatal("GetMetrics returned nil")
	}

	if got.JobPublishBytes != 1048576 {
		t.Errorf("JobPublishBytes = %d; want 1048576", got.JobPublishBytes)
	}
	if got.JobPageFaults != 42 {
		t.Errorf("JobPageFaults = %d; want 42", got.JobPageFaults)
	}
	if got.JobScratchPeakBytes != 2147483648 {
		t.Errorf("JobScratchPeakBytes = %d; want 2147483648", got.JobScratchPeakBytes)
	}

	// Verify other fields are not corrupted by the new columns.
	if got.InputBytes != 1024 {
		t.Errorf("InputBytes = %d; want 1024", got.InputBytes)
	}
	if got.CPUTimeMS != 1000 {
		t.Errorf("CPUTimeMS = %d; want 1000", got.CPUTimeMS)
	}
}
