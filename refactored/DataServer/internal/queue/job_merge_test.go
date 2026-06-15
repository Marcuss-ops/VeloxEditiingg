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

	q := &FileQueue{
		dbStore:    dbStore,
		activeJobs: map[string]*Job{},
		maxRetries: 3,
	}

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
	q.activeJobs[job.JobID] = job

	if err := PersistJob(job, dbStore); err != nil {
		t.Fatalf("persist initial job: %v", err)
	}

	// Must go through PROCESSING first (PENDING→COMPLETED is not a valid transition)
	if err := UpdateJobFields(ctx, job.JobID, map[string]interface{}{
		"status": "PROCESSING",
	}, dbStore, q.activeJobs); err != nil {
		t.Fatalf("update job fields to processing: %v", err)
	}

	if err := UpdateJobFields(ctx, job.JobID, map[string]interface{}{
		"status": "COMPLETED",
	}, dbStore, q.activeJobs); err != nil {
		t.Fatalf("update job fields to completed: %v", err)
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
	if status := stringValue(saved["status"]); status != "COMPLETED" {
		t.Fatalf("expected status COMPLETED, got %q", status)
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

	q := &FileQueue{
		dbStore:    dbStore,
		activeJobs: map[string]*Job{},
		maxRetries: 3,
	}

	job := &Job{
		JobID:        "job-complete-clears-error-2",
		Status:       StatusProcessing,
		CreatedAt:    NowUnix(),
		UpdatedAt:    NowUnix(),
		LastError:    "stale failure",
		LastErrorAt:  NowUnix(),
		ErrorMessage: "stale failure",
		FailedAt:     NowISO(),
		FailedBy:     "worker-a",
		AssignedTo:   "worker-b",
		History: []JobHistoryEntry{{
			Status:    "PROCESSING",
			Timestamp: NowISO(),
			WorkerID:  "worker-b",
			Message:   "Job started",
		}},
	}
	q.activeJobs[job.JobID] = job

	if err := PersistJob(job, dbStore); err != nil {
		t.Fatalf("persist initial job: %v", err)
	}

	if err := q.CompleteJob(ctx, job.JobID); err != nil {
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
	if status := stringValue(saved["status"]); status != "COMPLETED" {
		t.Fatalf("expected status COMPLETED, got %q", status)
	}
}

func stringValue(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
