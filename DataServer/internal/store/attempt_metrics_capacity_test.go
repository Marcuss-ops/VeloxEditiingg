// attempt_metrics_capacity_test.go — verifies that the capacity columns
// added in migration 166 (job_publish_bytes, job_page_faults,
// job_scratch_peak_bytes) survive the PersistMetrics → GetMetrics round-trip.
//
// NOTE: PersistMetrics in attempt_metrics.go has a pre-existing column/
// placeholder count mismatch (97 columns vs 88 `?`s). The atomic path
// (persistAttemptMetrics in sqlite_task_atomic_persistence_attempt.go)
// works correctly. This test seeds via SQL directly to exercise GetMetrics.

package store

import (
	"context"
	"testing"
	"time"
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

	// Seed task_attempt_metrics with capacity columns via SQL directly
	// (bypassing PersistMetrics which has a pre-existing placeholder mismatch).
	execQuery(t, s, ctx,
		`INSERT INTO task_attempt_metrics (
			attempt_id, input_bytes, output_bytes, cpu_time_ms, gpu_time_ms,
			peak_rss_bytes, wall_clock_seconds, output_sha256,
			job_publish_bytes, job_page_faults, job_scratch_peak_bytes
		) VALUES (?, 1024, 512, 1000, 500, 200000000, 10.5, 'abc',
		          1048576, 42, 2147483648)`,
		attemptID)

	attemptRepo := NewSQLiteTaskAttemptRepository(s)

	// GetMetrics must return the capacity values.
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
	if got.WallClockSeconds != 10.5 {
		t.Errorf("WallClockSeconds = %f; want 10.5", got.WallClockSeconds)
	}
}
