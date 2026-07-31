package store

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

const executionEventPersistenceTestSchema = `
CREATE TABLE tasks (
    task_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    executor_id TEXT NOT NULL DEFAULT '',
    executor_version INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE task_attempts (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    worker_session_id TEXT NOT NULL DEFAULT '',
    worker_snapshot_id TEXT NOT NULL DEFAULT '',
    lease_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'RUNNING'
);
CREATE TABLE task_phase_timings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt_id TEXT NOT NULL,
    phase TEXT NOT NULL,
    duration_ms INTEGER NOT NULL DEFAULT 0,
    wall_start TEXT NOT NULL DEFAULT '',
    wall_end TEXT NOT NULL DEFAULT '',
    phase_order INTEGER NOT NULL DEFAULT 0,
    component TEXT NOT NULL DEFAULT '',
    action TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'ok',
    error_code TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    bytes_in INTEGER NOT NULL DEFAULT 0,
    bytes_out INTEGER NOT NULL DEFAULT 0,
    frames INTEGER NOT NULL DEFAULT 0,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    job_id TEXT NOT NULL DEFAULT '',
    task_id TEXT NOT NULL DEFAULT '',
    worker_id TEXT NOT NULL DEFAULT '',
    worker_snapshot_id TEXT NOT NULL DEFAULT '',
    executor_id TEXT NOT NULL DEFAULT '',
    executor_version INTEGER NOT NULL DEFAULT 0,
    UNIQUE (attempt_id, component, action)
);
CREATE TABLE task_execution_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    attempt_id TEXT NOT NULL,
    job_id TEXT NOT NULL DEFAULT '',
    task_id TEXT NOT NULL DEFAULT '',
    worker_id TEXT NOT NULL DEFAULT '',
    worker_session_id TEXT NOT NULL DEFAULT '',
    worker_snapshot_id TEXT NOT NULL DEFAULT '',
    lease_id TEXT NOT NULL DEFAULT '',
    executor_id TEXT NOT NULL DEFAULT '',
    executor_version INTEGER NOT NULL DEFAULT 0,
    event_index INTEGER NOT NULL DEFAULT 0,
    origin TEXT NOT NULL,
    scope TEXT NOT NULL DEFAULT 'task',
    event_type TEXT NOT NULL DEFAULT '',
    event_name TEXT NOT NULL DEFAULT '',
    component TEXT NOT NULL DEFAULT '',
    action TEXT NOT NULL DEFAULT '',
    phase TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'ok',
    error_code TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    started_at TEXT NOT NULL DEFAULT '',
    completed_at TEXT NOT NULL DEFAULT '',
    duration_ms INTEGER NOT NULL DEFAULT 0,
    bytes_in INTEGER NOT NULL DEFAULT 0,
    bytes_out INTEGER NOT NULL DEFAULT 0,
    frames INTEGER NOT NULL DEFAULT 0,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT '',
    segment_index INTEGER,
    track_kind TEXT NOT NULL DEFAULT '',
    track_index INTEGER,
    artifact_id TEXT NOT NULL DEFAULT '',
    started_offset_ms REAL NOT NULL DEFAULT 0,
    finished_offset_ms REAL NOT NULL DEFAULT 0,
    cpu_ms REAL NOT NULL DEFAULT 0,
    queue_wait_ms REAL NOT NULL DEFAULT 0,
    frames_in INTEGER NOT NULL DEFAULT 0,
    frames_out INTEGER NOT NULL DEFAULT 0
);
`

func openExecutionEventPersistenceTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if _, err := db.Exec(executionEventPersistenceTestSchema); err != nil {
		db.Close()
		t.Fatalf("create execution event schema: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func seedExecutionEventIdentity(t *testing.T, db *sql.DB) {
	t.Helper()
	const now = "2026-07-31T00:00:00Z"
	for _, stmt := range []string{
		`INSERT INTO tasks (task_id, job_id, executor_id, executor_version) VALUES ('task-1', 'job-1', 'executor.canonical', 7)`,
		`INSERT INTO task_attempts (id, task_id, job_id, worker_id, worker_session_id, worker_snapshot_id, lease_id) VALUES ('attempt-1', 'task-1', 'job-1', 'worker-1', 'session-1', 'snapshot-1', 'lease-1')`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("seed identity: %v", err)
		}
	}
	_ = now
}

func modernExecutionTimings() []taskattempts.PhaseTimingDetailed {
	base := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)
	return []taskattempts.PhaseTimingDetailed{
		{
			AttemptID: "attempt-1", Origin: "engine", Scope: "segment",
			EventID: "encode-segment-0", EventIndex: 0, Component: "engine.encode", Action: "setup",
			Phase: "encode", SegmentIndex: 0, PhaseOrder: 1, Status: "ok",
			StartedAt: base, CompletedAt: base.Add(10 * time.Millisecond), DurationMS: 10,
			StartedOffsetMS: 0, FinishedOffsetMS: 10, CPUMS: 12, QueueWaitMS: 1,
			Frames: 100, FramesIn: 100, FramesOut: 100, BytesIn: 1000, BytesOut: 500,
		},
		{
			AttemptID: "attempt-1", Origin: "engine", Scope: "segment",
			EventID: "encode-segment-1", EventIndex: 1, Component: "engine.encode", Action: "setup",
			Phase: "encode", SegmentIndex: 1, PhaseOrder: 2, Status: "ok",
			StartedAt: base.Add(20 * time.Millisecond), CompletedAt: base.Add(35 * time.Millisecond), DurationMS: 15,
			StartedOffsetMS: 20, FinishedOffsetMS: 35, CPUMS: 18, QueueWaitMS: 2,
			Frames: 120, FramesIn: 120, FramesOut: 120, BytesIn: 1200, BytesOut: 600,
		},
		{
			AttemptID: "attempt-1", Origin: "engine", Scope: "segment",
			EventID: "encode-segment-2", EventIndex: 2, Component: "engine.encode", Action: "setup",
			Phase: "encode", SegmentIndex: 2, PhaseOrder: 3, Status: "ok",
			StartedAt: base.Add(40 * time.Millisecond), CompletedAt: base.Add(60 * time.Millisecond), DurationMS: 20,
			StartedOffsetMS: 40, FinishedOffsetMS: 60, CPUMS: 22, QueueWaitMS: 3,
			Frames: 140, FramesIn: 140, FramesOut: 140, BytesIn: 1400, BytesOut: 700,
		},
	}
}

func persistExecutionEventTestBatch(t *testing.T, db *sql.DB, timings []taskattempts.PhaseTimingDetailed) error {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	cmd := taskgraph.IngestResultCommand{
		AttemptID:    "attempt-1",
		TaskID:       "task-1",
		WorkerID:     "worker-1",
		LeaseID:      "lease-1",
		PhaseTimings: timings,
	}
	if err := persistPhaseTimingsAndExecutionEvents(context.Background(), tx, cmd); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func TestPersistExecutionEvents_RepeatedSegmentsAndCanonicalIdentity(t *testing.T) {
	db := openExecutionEventPersistenceTestDB(t)
	seedExecutionEventIdentity(t, db)

	if err := persistExecutionEventTestBatch(t, db, modernExecutionTimings()); err != nil {
		t.Fatalf("persist modern event batch: %v", err)
	}

	var events int
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_execution_events WHERE attempt_id = 'attempt-1'`).Scan(&events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if events != 3 {
		t.Fatalf("event count=%d, want 3 repeated segment events", events)
	}

	var summaryDuration, summaryBytes, summaryFrames int64
	if err := db.QueryRow(`
		SELECT duration_ms, bytes_in, frames
		FROM task_phase_timings WHERE attempt_id = 'attempt-1' AND component = 'engine.encode' AND action = 'setup'`).Scan(
		&summaryDuration, &summaryBytes, &summaryFrames); err != nil {
		t.Fatalf("read compact summary: %v", err)
	}
	if summaryDuration != 45 || summaryBytes != 3600 || summaryFrames != 360 {
		t.Fatalf("summary=(duration=%d bytes=%d frames=%d), want (45,3600,360)", summaryDuration, summaryBytes, summaryFrames)
	}

	var jobID, taskID, workerID, sessionID, snapshotID, executorID string
	var executorVersion int
	if err := db.QueryRow(`
		SELECT job_id, task_id, worker_id, worker_session_id, worker_snapshot_id, executor_id, executor_version
		FROM task_execution_events WHERE event_id = 'encode-segment-1'`).Scan(
		&jobID, &taskID, &workerID, &sessionID, &snapshotID, &executorID, &executorVersion); err != nil {
		t.Fatalf("read canonical event identity: %v", err)
	}
	if jobID != "job-1" || taskID != "task-1" || workerID != "worker-1" || sessionID != "session-1" ||
		snapshotID != "snapshot-1" || executorID != "executor.canonical" || executorVersion != 7 {
		t.Fatalf("event identity=%q/%q/%q/%q/%q/%q/%d; want canonical master identity", jobID, taskID, workerID, sessionID, snapshotID, executorID, executorVersion)
	}
}

func TestPersistExecutionEvents_ReplayIsIdempotentAndLegacySummarySurvives(t *testing.T) {
	db := openExecutionEventPersistenceTestDB(t)
	seedExecutionEventIdentity(t, db)

	legacy := taskattempts.PhaseTimingDetailed{
		AttemptID: "attempt-1", Component: "download", Action: "asset_fetch",
		PhaseOrder: 1, DurationMS: 25, Status: "ok", BytesIn: 99,
	}
	batch := append(modernExecutionTimings(), legacy)
	if err := persistExecutionEventTestBatch(t, db, batch); err != nil {
		t.Fatalf("persist mixed legacy/modern batch: %v", err)
	}
	if err := persistExecutionEventTestBatch(t, db, batch); err != nil {
		t.Fatalf("replay mixed batch: %v", err)
	}

	var events int
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_execution_events`).Scan(&events); err != nil {
		t.Fatalf("count replayed events: %v", err)
	}
	if events != 3 {
		t.Fatalf("replayed event count=%d, want 3", events)
	}
	var legacyRows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_phase_timings WHERE component='download' AND action='asset_fetch'`).Scan(&legacyRows); err != nil {
		t.Fatalf("count legacy summary: %v", err)
	}
	if legacyRows != 1 {
		t.Fatalf("legacy summary rows=%d, want 1", legacyRows)
	}
}

func TestPersistExecutionEvents_RejectsUnregisteredModernEvent(t *testing.T) {
	db := openExecutionEventPersistenceTestDB(t)
	seedExecutionEventIdentity(t, db)

	bad := modernExecutionTimings()[:1]
	bad[0].EventID = "bad-event"
	bad[0].Component = "engine.unknown"
	bad[0].Action = "invented"
	if err := persistExecutionEventTestBatch(t, db, bad); err == nil || !strings.Contains(err.Error(), "unregistered component/action") {
		t.Fatalf("error=%v, want unregistered component/action rejection", err)
	}

	var events, summaries int
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_execution_events`).Scan(&events); err != nil {
		t.Fatalf("count events after rejected batch: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_phase_timings`).Scan(&summaries); err != nil {
		t.Fatalf("count summaries after rejected batch: %v", err)
	}
	if events != 0 || summaries != 0 {
		t.Fatalf("rejected transaction persisted events=%d summaries=%d", events, summaries)
	}
}

func TestExecutionEventRegistry_Closed(t *testing.T) {
	if !isRegisteredExecutionEvent("engine.encode", "setup") {
		t.Fatal("engine.encode.setup should be registered")
	}
	if !isRegisteredExecutionEvent("worker.cache", "lookup") {
		t.Fatal("worker.cache.lookup should be registered")
	}
	if isRegisteredExecutionEvent("engine", "free_form") {
		t.Fatal("free-form component/action must not be registered")
	}

	registered, ok := canonicalExecutionEventRegistration("engine.encode", "setup")
	if !ok || registered.Origin != "engine" || registered.Scope != "segment" {
		t.Fatalf("canonical engine.encode.setup registration=%+v/%v; want engine/segment", registered, ok)
	}
}

func TestPersistExecutionEvents_RejectsCanonicalOriginScopeMismatch(t *testing.T) {
	db := openExecutionEventPersistenceTestDB(t)
	seedExecutionEventIdentity(t, db)

	bad := modernExecutionTimings()[:1]
	bad[0].EventID = "wrong-tuple"
	bad[0].Origin = "worker"
	bad[0].Scope = "attempt"
	if err := persistExecutionEventTestBatch(t, db, bad); err == nil || !strings.Contains(err.Error(), "origin/scope mismatch") {
		t.Fatalf("error=%v, want canonical origin/scope mismatch rejection", err)
	}
}
