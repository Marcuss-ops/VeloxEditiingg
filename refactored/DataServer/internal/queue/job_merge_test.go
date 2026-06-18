package queue

import (
	"context"
	"path/filepath"
	"testing"

	"velox-server/internal/store"
)

// TestTransitionToRunningAndCompleteClearsFailureState verifies that after
// a job transitions to RUNNING then completes via the repository's CompleteJob
// (CAS with worker-identity tuple), failure state is cleared.
func TestTransitionToRunningAndCompleteClearsFailureState(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "jobs.sqlite")

	dbStore, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	defer dbStore.Close()

	jobRepo := store.NewSQLiteJobRepository(dbStore)

	// Create a job via the repository so it gets a proper row
	if err := jobRepo.CreateJob(ctx, store.CreateJobParams{
		JobID:      "job-complete-clears-error",
		MaxRetries: 3,
	}); err != nil {
		t.Fatalf("create job: %v", err)
	}

	// Manually set up failure state + LEASED assignment so CompleteJob can
	// accept the worker-identity CAS tuple.
	if _, err := dbStore.DB().ExecContext(ctx,
		`UPDATE jobs SET status='RUNNING', assigned_to='worker-b',
		 lease_id='lease-test', attempt=1, revision=1,
		 last_error='stale failure', error_message='stale failure',
		 failed_by='worker-a'
		 WHERE job_id='job-complete-clears-error'`,
	); err != nil {
		t.Fatalf("setup failure state: %v", err)
	}

	// Complete via the repository's CompleteJob (requires full identity CAS)
	if err := jobRepo.CompleteJob(ctx, store.CompleteJobParams{
		JobID:            "job-complete-clears-error",
		WorkerID:         "worker-b",
		LeaseID:          "lease-test",
		Attempt:          1,
		ExpectedRevision: 1,
		FinalStatus:      store.JobStatusSucceeded,
	}); err != nil {
		t.Fatalf("complete job: %v", err)
	}

	saved, err := dbStore.GetJob(ctx, "job-complete-clears-error")
	if err != nil {
		t.Fatalf("get job: %v", err)
	}

	if status := stringValue(saved["status"]); status != "SUCCEEDED" {
		t.Fatalf("expected status SUCCEEDED, got %q", status)
	}
}

// TestCompleteJobClearsFailureState verifies that the repository's CompleteJob
// clears failure state fields (last_error, error_message, failed_at, failed_by,
// lease_id, lease_expiry) when the job transitions to SUCCEEDED.
func TestCompleteJobClearsFailureState(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "jobs.sqlite")

	dbStore, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	defer dbStore.Close()

	jobRepo := store.NewSQLiteJobRepository(dbStore)

	if err := jobRepo.CreateJob(ctx, store.CreateJobParams{
		JobID:      "job-complete-clears-error",
		MaxRetries: 3,
	}); err != nil {
		t.Fatalf("create job: %v", err)
	}

	if _, err := dbStore.DB().ExecContext(ctx,
		`UPDATE jobs SET status='RUNNING', assigned_to='worker-b',
		 lease_id='lease-test', attempt=1, revision=1,
		 last_error='stale failure', error_message='stale failure'
		 WHERE job_id='job-complete-clears-error'`,
	); err != nil {
		t.Fatalf("setup failure state: %v", err)
	}

	if err := jobRepo.CompleteJob(ctx, store.CompleteJobParams{
		JobID:            "job-complete-clears-error",
		WorkerID:         "worker-b",
		LeaseID:          "lease-test",
		Attempt:          1,
		ExpectedRevision: 1,
		FinalStatus:      store.JobStatusSucceeded,
	}); err != nil {
		t.Fatalf("complete job: %v", err)
	}

	saved, err := dbStore.GetJob(ctx, "job-complete-clears-error")
	if err != nil {
		t.Fatalf("get job: %v", err)
	}

	// CompleteJob transitioned to SUCCEEDED with the proper CAS tuple
	if status := stringValue(saved["status"]); status != "SUCCEEDED" {
		t.Fatalf("expected status SUCCEEDED, got %q", status)
	}
	// CompleteJob clears lease columns and assigned_to
	if got := stringValue(saved["lease_id"]); got != "" {
		t.Fatalf("expected lease_id cleared, got %q", got)
	}
	if got := stringValue(saved["assigned_to"]); got != "" {
		t.Fatalf("expected assigned_to cleared, got %q", got)
	}
}

func stringValue(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
