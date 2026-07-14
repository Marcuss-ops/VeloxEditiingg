package store

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func openTaskAttemptTestDB(t *testing.T) *SQLiteTaskAttemptRepository {
	t.Helper()

	// Append `_busy_timeout=5000` so concurrent readers/writers don't
	// immediately trip SQLITE_BUSY when the test races on the private
	// in-memory connection pool. Matches the canonical pattern used
	// across DataServer/internal/store/*_test.go.
	db, err := sql.Open("sqlite3", ":memory:?_busy_timeout=5000")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const schema = `
CREATE TABLE task_attempts (
	id              TEXT PRIMARY KEY,
	task_id         TEXT NOT NULL,
	job_id          TEXT NOT NULL,
	attempt_number  INTEGER NOT NULL,
	worker_id       TEXT NOT NULL,
	lease_id        TEXT NOT NULL,
	status          TEXT NOT NULL,
	started_at      TEXT,
	completed_at    TEXT,
	error_code      TEXT NOT NULL DEFAULT '',
	error_message   TEXT NOT NULL DEFAULT '',
	report_version  INTEGER NOT NULL DEFAULT 0,
	created_at      TEXT NOT NULL,
	updated_at      TEXT NOT NULL,
	git_sha              TEXT NOT NULL DEFAULT '',
	worker_version       TEXT NOT NULL DEFAULT '',
	engine_version       TEXT NOT NULL DEFAULT '',
	ffmpeg_version       TEXT NOT NULL DEFAULT '',
	config_hash          TEXT NOT NULL DEFAULT '',
	docker_image_digest  TEXT NOT NULL DEFAULT '',
	trace_id             TEXT NOT NULL DEFAULT '',
	span_id              TEXT NOT NULL DEFAULT ''
);
`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	return NewSQLiteTaskAttemptRepository(&SQLiteStore{db: db})
}

func TestGetByTaskIDAndWorkerAndLease_ScansTextTimestamps(t *testing.T) {
	repo := openTaskAttemptTestDB(t)
	ctx := context.Background()

	createdAt := time.Date(2026, 7, 1, 15, 22, 16, 0, time.UTC).Format(time.RFC3339)
	updatedAt := time.Date(2026, 7, 1, 15, 22, 36, 0, time.UTC).Format(time.RFC3339)
	startedAt := time.Date(2026, 7, 1, 15, 22, 17, 0, time.UTC).Format(time.RFC3339)

	if _, err := repo.store.db.ExecContext(ctx,
		`INSERT INTO task_attempts
		 (id, task_id, job_id, attempt_number, worker_id, lease_id, status,
		  started_at, completed_at, error_code, error_message, report_version, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, '', '', 0, ?, ?)`,
		"attempt-1", "task-1", "job-1", 4, "worker-1", "lease-1", "RUNNING",
		startedAt, createdAt, updatedAt,
	); err != nil {
		t.Fatalf("insert attempt: %v", err)
	}

	attempt, err := repo.GetByTaskIDAndWorkerAndLease(ctx, "task-1", "worker-1", "lease-1")
	if err != nil {
		t.Fatalf("GetByTaskIDAndWorkerAndLease: %v", err)
	}
	if attempt == nil {
		t.Fatal("attempt = nil; want populated attempt")
	}
	if attempt.CreatedAt.Format(time.RFC3339) != createdAt {
		t.Fatalf("CreatedAt = %s; want %s", attempt.CreatedAt.Format(time.RFC3339), createdAt)
	}
	if attempt.UpdatedAt.Format(time.RFC3339) != updatedAt {
		t.Fatalf("UpdatedAt = %s; want %s", attempt.UpdatedAt.Format(time.RFC3339), updatedAt)
	}
	if attempt.StartedAt == nil || attempt.StartedAt.Format(time.RFC3339) != startedAt {
		t.Fatalf("StartedAt = %v; want %s", attempt.StartedAt, startedAt)
	}
}
