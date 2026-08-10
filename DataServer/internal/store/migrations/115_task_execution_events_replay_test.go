package migrations

import (
	"database/sql"
	_ "embed"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed sqlite/114_task_execution_events_timing.sql
var sqliteSQL114TaskExecutionEventsTiming string

//go:embed sqlite/115_task_execution_events_replay.sql
var sqliteSQL115TaskExecutionEventsReplay string

func applyTaskExecutionEventsMigrationsThrough115(t *testing.T, db *sql.DB) {
	t.Helper()
	applyMigrationSQL(t, db, sqliteSQL110TaskExecutionEvents)
	applyMigrationSQL(t, db, sqliteSQL113TaskExecutionEventsAppendOnly)
	applyMigrationSQL(t, db, sqliteSQL114TaskExecutionEventsTiming)
	applyMigrationSQL(t, db, sqliteSQL115TaskExecutionEventsReplay)
}

func TestMigration115_ReplayIgnoresIngestTimestampButRejectsPayloadConflict(t *testing.T) {
	db := openTestDB(t)
	applyTaskExecutionEventsMigrationsThrough115(t, db)

	insert := `INSERT INTO task_execution_events (
		event_id, attempt_id, job_id, task_id, worker_id,
		worker_session_id, worker_snapshot_id, lease_id,
		executor_id, executor_version, event_index, origin, scope,
		component, action, phase, status, metadata_json, created_at,
		segment_index, started_offset_ms, finished_offset_ms, cpu_ms, queue_wait_ms,
		frames_in, frames_out
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	args := []any{
		"replay-1", "attempt-1", "job-1", "task-1", "worker-1", "session-1", "snapshot-1", "lease-1",
		"executor-1", 1, 0, "engine", "segment", "engine.encode", "setup", "encode", "ok", "{}",
		"2026-07-31T00:00:00Z", 0, 0.0, 10.0, 1.0, 0.5, 10, 10,
	}
	if _, err := db.Exec(insert, args...); err != nil {
		t.Fatalf("insert original event: %v", err)
	}

	replay := append([]any(nil), args...)
	replay[18] = "2026-07-31T00:01:00Z"
	if _, err := db.Exec(insert, replay...); err != nil {
		t.Fatalf("same event with a new created_at should replay idempotently: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_execution_events WHERE event_id='replay-1'`).Scan(&count); err != nil {
		t.Fatalf("count replayed event: %v", err)
	}
	if count != 1 {
		t.Fatalf("replayed event count=%d, want 1", count)
	}

	conflict := append([]any(nil), args...)
	conflict[14] = "different-action"
	if _, err := db.Exec(insert, conflict...); err == nil || !strings.Contains(strings.ToLower(err.Error()), "conflict") {
		t.Fatalf("changed event payload should conflict, got %v", err)
	}
}
