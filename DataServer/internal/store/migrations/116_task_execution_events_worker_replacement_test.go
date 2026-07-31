package migrations

import (
	"database/sql"
	"strings"
	"testing"

	_ "embed"
	_ "github.com/mattn/go-sqlite3"
)

//go:embed sqlite/114_task_execution_events_timing.sql
var sqliteSQL114TaskExecutionEventsWorkerReplacement string

//go:embed sqlite/115_task_execution_events_replay.sql
var sqliteSQL115TaskExecutionEventsWorkerReplacement string

//go:embed sqlite/116_task_execution_events_worker_replacement.sql
var sqliteSQL116TaskExecutionEventsWorkerReplacement string

func applyTaskExecutionEventsMigrationsThrough116(t *testing.T, db *sql.DB) {
	t.Helper()
	applyMigrationSQL(t, db, sqliteSQL110TaskExecutionEvents)
	applyMigrationSQL(t, db, sqliteSQL113TaskExecutionEventsAppendOnly)
	applyMigrationSQL(t, db, sqliteSQL114TaskExecutionEventsWorkerReplacement)
	applyMigrationSQL(t, db, sqliteSQL115TaskExecutionEventsWorkerReplacement)
	applyMigrationSQL(t, db, sqliteSQL116TaskExecutionEventsWorkerReplacement)
}

func insertReplacementTestEvent(t *testing.T, db *sql.DB, eventID, origin string) {
	t.Helper()
	_, err := db.Exec(`
		INSERT INTO task_execution_events (
			event_id, attempt_id, job_id, task_id, worker_id, event_index,
			origin, scope, component, action, phase, status, created_at,
			segment_index
		) VALUES (?, 'attempt-replace', 'job-replace', 'task-replace', 'worker-replace',
			0, ?, 'segment', 'engine.encode', 'setup', 'encode', 'ok',
			'2026-07-31T00:00:00Z', 0)`, eventID, origin)
	if err != nil {
		t.Fatalf("insert %s event: %v", origin, err)
	}
}

func TestMigration116_WorkerReplacementPreservesMasterEvents(t *testing.T) {
	db := openTestDB(t)
	applyTaskExecutionEventsMigrationsThrough116(t, db)

	insertReplacementTestEvent(t, db, "master-event", "master")
	insertReplacementTestEvent(t, db, "stale-worker-event", "engine")

	if _, err := db.Exec(`DELETE FROM task_execution_events WHERE attempt_id = 'attempt-replace' AND origin <> 'master'`); err == nil || !strings.Contains(strings.ToLower(err.Error()), "authorization") {
		t.Fatalf("unauthorized worker event deletion should be rejected, got %v", err)
	}
	if _, err := db.Exec(`INSERT INTO task_execution_event_replacement_authorizations (attempt_id, authorization, created_at) VALUES ('attempt-replace', 'atomic_ingest', '2026-07-31T00:00:00Z')`); err != nil {
		t.Fatalf("authorize worker event replacement: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM task_execution_events WHERE attempt_id = 'attempt-replace' AND origin <> 'master'`); err != nil {
		t.Fatalf("authorized worker event replacement should be allowed: %v", err)
	}

	var masterCount, workerCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_execution_events WHERE origin = 'master'`).Scan(&masterCount); err != nil {
		t.Fatalf("count master events: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_execution_events WHERE origin <> 'master'`).Scan(&workerCount); err != nil {
		t.Fatalf("count worker events: %v", err)
	}
	if masterCount != 1 || workerCount != 0 {
		t.Fatalf("after replacement master=%d worker=%d; want master=1 worker=0", masterCount, workerCount)
	}

	if _, err := db.Exec(`DELETE FROM task_execution_events WHERE event_id = 'master-event'`); err == nil || !strings.Contains(strings.ToLower(err.Error()), "master") {
		t.Fatalf("master event deletion should be rejected, got %v", err)
	}
}
