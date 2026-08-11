package store

import (
	"context"
	"database/sql"
	"testing"
)

func openJobProgressTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:?_busy_timeout=5000")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`
CREATE TABLE job_progress (
	job_id TEXT PRIMARY KEY,
	attempt_number INTEGER NOT NULL DEFAULT 1,
	percent REAL NOT NULL DEFAULT 0,
	stage TEXT NOT NULL DEFAULT '',
	current_item INTEGER NOT NULL DEFAULT 0,
	total_items INTEGER NOT NULL DEFAULT 0,
	message TEXT NOT NULL DEFAULT '',
	updated_at TEXT NOT NULL
)`); err != nil {
		t.Fatalf("create job_progress: %v", err)
	}
	return &SQLiteStore{db: db}
}

func TestUpsertJobProgress_IgnoresStaleAttemptAndTimestamp(t *testing.T) {
	store := openJobProgressTestStore(t)
	ctx := context.Background()

	if err := store.UpsertJobProgress(ctx, "job-progress", 2, 80, "render", 8, 10, "current", "2026-08-11T10:00:00Z"); err != nil {
		t.Fatalf("initial progress: %v", err)
	}
	if err := store.UpsertJobProgress(ctx, "job-progress", 1, 99, "done", 10, 10, "stale attempt", "2026-08-11T11:00:00Z"); err != nil {
		t.Fatalf("stale attempt progress: %v", err)
	}
	if err := store.UpsertJobProgress(ctx, "job-progress", 2, 10, "started", 1, 10, "stale timestamp", "2026-08-11T09:00:00Z"); err != nil {
		t.Fatalf("stale timestamp progress: %v", err)
	}

	got, err := store.GetJobProgress(ctx, "job-progress")
	if err != nil {
		t.Fatalf("GetJobProgress: %v", err)
	}
	if got == nil {
		t.Fatal("GetJobProgress returned nil")
	}
	if got.AttemptNumber != 2 || got.Percent != 80 || got.Stage != "render" || got.Message != "current" {
		t.Fatalf("stale progress overwrote current snapshot: %+v", got)
	}

	if err := store.UpsertJobProgress(ctx, "job-progress", 2, 100, "done", 10, 10, "new", "2026-08-11T12:00:00Z"); err != nil {
		t.Fatalf("new progress: %v", err)
	}
	got, err = store.GetJobProgress(ctx, "job-progress")
	if err != nil {
		t.Fatalf("GetJobProgress after new snapshot: %v", err)
	}
	if got.Percent != 100 || got.Stage != "done" || got.Message != "new" {
		t.Fatalf("new progress was not applied: %+v", got)
	}
}
