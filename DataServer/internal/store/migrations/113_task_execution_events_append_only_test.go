package migrations

import (
	"database/sql"
	"strings"
	"testing"

	_ "embed"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed sqlite/110_task_execution_events.sql
var sqliteSQL110TaskExecutionEvents string

//go:embed sqlite/113_task_execution_events_append_only.sql
var sqliteSQL113TaskExecutionEventsAppendOnly string

func applyTaskExecutionEventsMigrations(t *testing.T, db *sql.DB) {
	t.Helper()
	applyMigrationSQL(t, db, sqliteSQL110TaskExecutionEvents)
	applyMigrationSQL(t, db, sqliteSQL113TaskExecutionEventsAppendOnly)
}

func TestMigration113_TaskExecutionEventsSchemaAndIndexes(t *testing.T) {
	db := openTestDB(t)
	applyTaskExecutionEventsMigrations(t, db)

	for _, column := range []string{
		"event_id", "attempt_id", "origin", "scope", "event_index",
		"segment_index", "track_kind", "track_index", "artifact_id",
	} {
		if !columnExists(t, db, "task_execution_events", column) {
			t.Errorf("task_execution_events column %q is missing", column)
		}
	}

	for _, index := range []string{
		"uq_task_execution_events_event_id",
		"idx_task_execution_events_attempt_origin_index",
		"idx_task_execution_events_attempt_scope",
		"idx_task_execution_events_artifact",
		"idx_task_execution_events_track",
	} {
		if !indexExists(t, db, index) {
			t.Errorf("expected task execution event index %q", index)
		}
	}

	for _, trigger := range []string{
		"trg_task_execution_events_validate_event_id",
		"trg_task_execution_events_handle_duplicate_event_id",
		"trg_task_execution_events_validate_event_index",
		"trg_task_execution_events_validate_segment",
		"trg_task_execution_events_validate_artifact",
		"trg_task_execution_events_validate_track",
		"trg_task_execution_events_append_only_update",
		"trg_task_execution_events_append_only_delete",
	} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name=?`, trigger).Scan(&count); err != nil {
			t.Fatalf("check trigger %q: %v", trigger, err)
		}
		if count != 1 {
			t.Errorf("trigger %q count=%d, want 1", trigger, count)
		}
	}
}

func TestMigration113_RepeatedScopedEventsAndEventIdempotency(t *testing.T) {
	db := openTestDB(t)
	applyTaskExecutionEventsMigrations(t, db)

	const insert = `
		INSERT INTO task_execution_events (
			event_id, attempt_id, job_id, task_id, origin, scope,
			event_index, component, action, segment_index,
			track_kind, track_index, artifact_id, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	// Same attempt/component/action and even the same event_index are
	// valid when the event IDs and scopes differ.
	events := []struct {
		id, scope, action, trackKind, artifactID string
		segmentIndex, trackIndex                 any
	}{
		{"encode-segment-0", "segment", "encode", "", "", 0, nil},
		{"encode-segment-1", "segment", "encode", "", "", 1, nil},
		{"mix-audio-0", "audio_track", "mix", "music", "", nil, 0},
		{"mix-audio-1", "audio_track", "mix", "music", "", nil, 1},
		{"upload-artifact-0", "artifact", "transfer", "", "artifact-0", nil, nil},
	}
	for _, event := range events {
		if _, err := db.Exec(insert,
			event.id, "attempt-1", "job-1", "task-1", "engine", event.scope,
			0, "engine", event.action, event.segmentIndex,
			event.trackKind, event.trackIndex, event.artifactID, "2026-07-31T00:00:00Z",
		); err != nil {
			t.Fatalf("insert %s: %v", event.id, err)
		}
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_execution_events WHERE attempt_id='attempt-1'`).Scan(&count); err != nil {
		t.Fatalf("count repeated events: %v", err)
	}
	if count != len(events) {
		t.Fatalf("repeated event count=%d, want %d", count, len(events))
	}
	var eventIDIndexUnique int
	if err := db.QueryRow(`SELECT "unique" FROM pragma_index_list('task_execution_events') WHERE name='uq_task_execution_events_event_id'`).Scan(&eventIDIndexUnique); err != nil {
		t.Fatalf("inspect event_id uniqueness: %v", err)
	}
	if eventIDIndexUnique != 1 {
		t.Fatalf("event_id index unique=%d, want 1", eventIDIndexUnique)
	}

	// A replay of the exact event_id is idempotent with INSERT OR IGNORE,
	// while another event with the same action remains insertable.
	if _, err := db.Exec(strings.Replace(strings.TrimSpace(insert), "INSERT INTO", "INSERT OR IGNORE INTO", 1),
		"encode-segment-0", "attempt-1", "job-1", "task-1", "engine", "segment",
		0, "engine", "encode", 0, "", nil, "", "2026-07-31T00:00:00Z"); err != nil {
		t.Fatalf("duplicate event_id replay should be ignored: %v", err)
	}
	if _, err := db.Exec(insert,
		"encode-segment-0", "attempt-1", "job-1", "task-1", "engine", "segment",
		0, "engine", "different-action", 0, "", nil, "", "2026-07-31T00:03:00Z"); err == nil || !strings.Contains(strings.ToLower(err.Error()), "conflict") {
		t.Fatalf("different payload with reused event_id should conflict, got %v", err)
	}
	if _, err := db.Exec(strings.Replace(strings.TrimSpace(insert), "INSERT INTO", "INSERT OR IGNORE INTO", 1),
		"encode-segment-0", "attempt-1", "job-1", "task-1", "engine", "segment",
		0, "engine", "different-action", 0, "", nil, "", "2026-07-31T00:03:00Z"); err == nil || !strings.Contains(strings.ToLower(err.Error()), "conflict") {
		t.Fatalf("INSERT OR IGNORE with a different payload should conflict, got %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_execution_events WHERE attempt_id='attempt-1'`).Scan(&count); err != nil {
		t.Fatalf("count events after replay: %v", err)
	}
	if count != len(events) {
		t.Fatalf("replay changed event count=%d, want %d", count, len(events))
	}
	var originalAttempt, originalCreatedAt string
	if err := db.QueryRow(`SELECT attempt_id, created_at FROM task_execution_events WHERE event_id='encode-segment-0'`).Scan(&originalAttempt, &originalCreatedAt); err != nil {
		t.Fatalf("read replayed event: %v", err)
	}
	if originalAttempt != "attempt-1" || originalCreatedAt != "2026-07-31T00:00:00Z" {
		t.Fatalf("replay changed original event: attempt=%q created_at=%q", originalAttempt, originalCreatedAt)
	}
	if _, err := db.Exec(insert,
		"encode-segment-2", "attempt-1", "job-1", "task-1", "engine", "segment",
		0, "engine", "encode", 2, "", nil, "", "2026-07-31T00:02:00Z"); err != nil {
		t.Fatalf("same action with a new event_id should be insertable: %v", err)
	}
}

func TestMigration113_ClosedOriginAndScopeChecks(t *testing.T) {
	db := openTestDB(t)
	applyTaskExecutionEventsMigrations(t, db)

	for _, test := range []struct {
		name   string
		origin string
		scope  string
	}{
		{name: "origin", origin: "custom", scope: "task"},
		{name: "scope", origin: "worker", scope: "custom"},
	} {
		_, err := db.Exec(`
			INSERT INTO task_execution_events (
				event_id, attempt_id, origin, scope, event_index
			) VALUES (?, 'attempt-closed-enums', ?, ?, 0)`,
			test.name, test.origin, test.scope)
		if err == nil {
			t.Fatalf("invalid %s should be rejected by SQL CHECK constraint", test.name)
		}
		if !strings.Contains(strings.ToLower(err.Error()), "check constraint") {
			t.Fatalf("invalid %s error=%v, want CHECK constraint failure", test.name, err)
		}
	}
}

func TestMigration113_AppendOnlyAndScopedValidation(t *testing.T) {
	db := openTestDB(t)
	applyTaskExecutionEventsMigrations(t, db)

	insert := func(id, scope string, segmentIndex, trackIndex any, artifactID string) error {
		_, err := db.Exec(`
			INSERT INTO task_execution_events (
				event_id, attempt_id, origin, scope, event_index,
				segment_index, track_index, artifact_id
			) VALUES (?, 'attempt-2', 'worker', ?, 1, ?, ?, ?)`,
			id, scope, segmentIndex, trackIndex, artifactID)
		return err
	}

	if err := insert("bad-segment", "segment", nil, nil, ""); err == nil {
		t.Fatal("segment event without segment_index should be rejected")
	}
	if err := insert("bad-track", "audio_track", nil, nil, ""); err == nil {
		t.Fatal("track event without track_index should be rejected")
	}
	if err := insert("bad-artifact", "artifact", nil, nil, ""); err == nil {
		t.Fatal("artifact event without artifact_id should be rejected")
	}

	if err := insert("append-only-1", "task", nil, nil, ""); err != nil {
		t.Fatalf("insert valid event: %v", err)
	}
	if _, err := db.Exec(`UPDATE task_execution_events SET action='changed' WHERE event_id='append-only-1'`); err == nil {
		t.Fatal("UPDATE should be rejected for append-only events")
	}
	if _, err := db.Exec(`DELETE FROM task_execution_events WHERE event_id='append-only-1'`); err == nil {
		t.Fatal("DELETE should be rejected for append-only events")
	}
	if _, err := db.Exec(`INSERT INTO task_execution_events (event_id, attempt_id, origin, scope, event_index) VALUES ('append-only-1', 'attempt-2', 'worker', 'task', 1)`); err != nil {
		t.Fatalf("identical INSERT replay should be ignored: %v", err)
	}
	if _, err := db.Exec(`INSERT OR REPLACE INTO task_execution_events (event_id, attempt_id, origin, scope, event_index) VALUES ('append-only-1', 'attempt-2', 'worker', 'task', 1)`); err != nil {
		t.Fatalf("identical INSERT OR REPLACE replay should be ignored: %v", err)
	}
	if _, err := db.Exec(`INSERT OR REPLACE INTO task_execution_events (event_id, attempt_id, origin, scope, event_index) VALUES ('append-only-1', 'attempt-3', 'worker', 'task', 2)`); err == nil || !strings.Contains(strings.ToLower(err.Error()), "conflict") {
		t.Fatalf("INSERT OR REPLACE with a different payload should conflict, got %v", err)
	}
	var attemptID string
	if err := db.QueryRow(`SELECT attempt_id FROM task_execution_events WHERE event_id='append-only-1'`).Scan(&attemptID); err != nil {
		t.Fatalf("read append-only event after replacement attempt: %v", err)
	}
	if attemptID != "attempt-2" {
		t.Fatalf("INSERT OR REPLACE changed event unexpectedly to attempt %q", attemptID)
	}
}

func TestMigration113_BackfillsLegacyEventIDs(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sqliteSQL110TaskExecutionEvents)
	if _, err := db.Exec(`
		INSERT INTO task_execution_events (attempt_id, origin, scope, event_index)
		VALUES ('legacy-attempt', 'engine', 'attempt', 0)`); err != nil {
		t.Fatalf("insert legacy event: %v", err)
	}
	applyMigrationSQL(t, db, sqliteSQL113TaskExecutionEventsAppendOnly)

	var eventID string
	if err := db.QueryRow(`SELECT event_id FROM task_execution_events WHERE attempt_id='legacy-attempt'`).Scan(&eventID); err != nil {
		t.Fatalf("read backfilled event_id: %v", err)
	}
	if eventID == "" {
		t.Fatal("legacy event_id should be backfilled")
	}
}
