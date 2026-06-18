package queue

import (
	"context"
	"path/filepath"
	"testing"

	"velox-server/internal/store"
)

func TestUpdateJobFieldsClearsFailureStateOnComplete(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "jobs.sqlite")

	dbStore, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	defer dbStore.Close()

	jobRepo := store.NewSQLiteJobRepository(dbStore)
	ts, err := NewLegacyLifecycleService(jobRepo, dbStore)
	if err != nil {
		t.Fatalf("new transition service: %v", err)
	}

	// Submit a job with stale failure state
	job := &Job{
		JobID:        "job-complete-clears-error",
		Status:       StatusPending,
		CreatedAt:    NowUnix(),
		UpdatedAt:    NowUnix(),
		LastError:    "stale failure",
		LastErrorAt:  NowUnix(),
		ErrorMessage: "stale failure",
		FailedAt:     NowISO(),
		FailedBy:     "worker-a",
		AssignedTo:   "worker-a",
		History: []JobHistoryEntry{{
			Status:    "PENDING",
			Timestamp: NowISO(),
			Message:   "Job created",
		}},
	}
	if err := PersistJob(job, dbStore); err != nil {
		t.Fatalf("persist initial job: %v", err)
	}

	// Must go through RUNNING first
	if err := ts.UpdateJobFields(ctx, job.JobID, map[string]interface{}{
		"status": "RUNNING",
	}); err != nil {
		t.Fatalf("update job fields to running: %v", err)
	}

	// Then complete
	if err := ts.UpdateJobFields(ctx, job.JobID, map[string]interface{}{
		"status": "SUCCEEDED",
	}); err != nil {
		t.Fatalf("update job fields to succeeded: %v", err)
	}

	saved, err := dbStore.GetJob(ctx, job.JobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}

	if got := stringValue(saved["last_error"]); got != "" {
		t.Fatalf("expected last_error cleared, got %q", got)
	}
	if got := stringValue(saved["error_message"]); got != "" {
		t.Fatalf("expected error_message cleared, got %q", got)
	}
	if v, ok := saved["failed_at"]; ok && v != nil {
		t.Fatalf("expected failed_at cleared, got %#v", v)
	}
	if status := stringValue(saved["status"]); status != "SUCCEEDED" {
		t.Fatalf("expected status SUCCEEDED, got %q", status)
	}
}

func TestCompleteJobClearsFailureState(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "jobs.sqlite")

	dbStore, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	defer dbStore.Close()

	jobRepo := store.NewSQLiteJobRepository(dbStore)
	ts, err := NewLegacyLifecycleService(jobRepo, dbStore)
	if err != nil {
		t.Fatalf("new transition service: %v", err)
	}

	// Persist a processing job with stale failure state
	job := &Job{
		JobID:        "job-complete-clears-error",
		Status:       StatusRunning,
		CreatedAt:    NowUnix(),
		UpdatedAt:    NowUnix(),
		LastError:    "stale failure",
		LastErrorAt:  NowUnix(),
		ErrorMessage: "stale failure",
		FailedAt:     NowISO(),
		FailedBy:     "worker-a",
		AssignedTo:   "worker-b",
		History: []JobHistoryEntry{{
			Status:    "RUNNING",
			Timestamp: NowISO(),
			WorkerID:  "worker-b",
			Message:   "Job started",
		}},
	}
	if err := PersistJob(job, dbStore); err != nil {
		t.Fatalf("persist initial job: %v", err)
	}

	if err := ts.CompleteJob(ctx, job.JobID); err != nil {
		t.Fatalf("complete job: %v", err)
	}

	saved, err := dbStore.GetJob(ctx, job.JobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}

	if got := stringValue(saved["last_error"]); got != "" {
		t.Fatalf("expected last_error cleared, got %q", got)
	}
	if got := stringValue(saved["error_message"]); got != "" {
		t.Fatalf("expected error_message cleared, got %q", got)
	}
	if v, ok := saved["failed_at"]; ok && v != nil {
		t.Fatalf("expected failed_at cleared, got %#v", v)
	}
	if status := stringValue(saved["status"]); status != "SUCCEEDED" {
		t.Fatalf("expected status SUCCEEDED, got %q", status)
	}
}

func stringValue(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
