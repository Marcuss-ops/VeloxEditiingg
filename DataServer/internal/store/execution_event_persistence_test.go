package store

import (
	"context"
	"database/sql"
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
CREATE TABLE task_execution_event_replacement_authorizations (
    attempt_id TEXT PRIMARY KEY,
    authorization TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE telemetry_event_quarantine (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    origin TEXT NOT NULL DEFAULT '',
    scope TEXT NOT NULL DEFAULT '',
    component TEXT NOT NULL DEFAULT '',
    action TEXT NOT NULL DEFAULT '',
    schema_version INTEGER NOT NULL DEFAULT 0,
    reason TEXT NOT NULL,
    event_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    UNIQUE (attempt_id, event_id)
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
    telemetry_schema_version INTEGER NOT NULL DEFAULT 0,
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
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared&_busy_timeout=5000")
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
			TelemetrySchemaVersion: 1,
		},
		{
			AttemptID: "attempt-1", Origin: "engine", Scope: "segment",
			EventID: "encode-segment-1", EventIndex: 1, Component: "engine.encode", Action: "setup",
			Phase: "encode", SegmentIndex: 1, PhaseOrder: 2, Status: "ok",
			StartedAt: base.Add(20 * time.Millisecond), CompletedAt: base.Add(35 * time.Millisecond), DurationMS: 15,
			StartedOffsetMS: 20, FinishedOffsetMS: 35, CPUMS: 18, QueueWaitMS: 2,
			Frames: 120, FramesIn: 120, FramesOut: 120, BytesIn: 1200, BytesOut: 600,
			TelemetrySchemaVersion: 1,
		},
		{
			AttemptID: "attempt-1", Origin: "engine", Scope: "segment",
			EventID: "encode-segment-2", EventIndex: 2, Component: "engine.encode", Action: "setup",
			Phase: "encode", SegmentIndex: 2, PhaseOrder: 3, Status: "ok",
			StartedAt: base.Add(40 * time.Millisecond), CompletedAt: base.Add(60 * time.Millisecond), DurationMS: 20,
			StartedOffsetMS: 40, FinishedOffsetMS: 60, CPUMS: 22, QueueWaitMS: 3,
			Frames: 140, FramesIn: 140, FramesOut: 140, BytesIn: 1400, BytesOut: 700,
			TelemetrySchemaVersion: 1,
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
		AttemptID: "attempt-1",
		TaskID:    "task-1",
		WorkerID:  "worker-1", LeaseID: "lease-1",
		TelemetrySchemaVersion: 1,
		PhaseTimings:           timings,
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
	var executorVersion, telemetrySchemaVersion int
	if err := db.QueryRow(`
		SELECT job_id, task_id, worker_id, worker_session_id, worker_snapshot_id, executor_id, executor_version, telemetry_schema_version
		FROM task_execution_events WHERE event_id = 'encode-segment-1'`).Scan(
		&jobID, &taskID, &workerID, &sessionID, &snapshotID, &executorID, &executorVersion, &telemetrySchemaVersion); err != nil {
		t.Fatalf("read canonical event identity: %v", err)
	}
	if jobID != "job-1" || taskID != "task-1" || workerID != "worker-1" || sessionID != "session-1" ||
		snapshotID != "snapshot-1" || executorID != "executor.canonical" || executorVersion != 7 || telemetrySchemaVersion != 1 {
		t.Fatalf("event identity=%q/%q/%q/%q/%q/%q/%d schema=%d; want canonical master identity with schema=1", jobID, taskID, workerID, sessionID, snapshotID, executorID, executorVersion, telemetrySchemaVersion)
	}

	var segmentIndex, trackIndex sql.NullInt64
	var trackKind, status, errorCode string
	var startedOffset, finishedOffset, cpuMS, queueWait float64
	var bytesIn, bytesOut, frames, framesIn, framesOut int64
	if err := db.QueryRow(`
		SELECT segment_index, track_kind, track_index, started_offset_ms,
		       finished_offset_ms, cpu_ms, queue_wait_ms, bytes_in, bytes_out,
		       frames, frames_in, frames_out, status, error_code
		FROM task_execution_events WHERE event_id = 'encode-segment-1'`).Scan(
		&segmentIndex, &trackKind, &trackIndex, &startedOffset, &finishedOffset,
		&cpuMS, &queueWait, &bytesIn, &bytesOut, &frames, &framesIn, &framesOut,
		&status, &errorCode); err != nil {
		t.Fatalf("read mapped event telemetry: %v", err)
	}
	if !segmentIndex.Valid || segmentIndex.Int64 != 1 || trackKind != "" || trackIndex.Valid ||
		startedOffset != 20 || finishedOffset != 35 || cpuMS != 18 || queueWait != 2 ||
		bytesIn != 1200 || bytesOut != 600 || frames != 120 || framesIn != 120 || framesOut != 120 ||
		status != "ok" || errorCode != "" {
		t.Fatalf("mapped event telemetry=segment:%v track:%q/%v offsets:%v/%v cpu:%v queue:%v bytes:%d/%d frames:%d/%d/%d status:%q code:%q",
			segmentIndex, trackKind, trackIndex, startedOffset, finishedOffset, cpuMS, queueWait,
			bytesIn, bytesOut, frames, framesIn, framesOut, status, errorCode)
	}
}

func TestPersistExecutionEvents_MapsAudioTrackIdentityAndTelemetry(t *testing.T) {
	db := openExecutionEventPersistenceTestDB(t)
	seedExecutionEventIdentity(t, db)

	base := time.Date(2026, 7, 31, 0, 1, 0, 0, time.UTC)
	track := taskattempts.PhaseTimingDetailed{
		AttemptID: "attempt-1", Origin: "engine", Scope: "audio_track",
		EventID: "voiceover-track-0", EventIndex: 9,
		Component: "engine.audio", Action: "voiceover_decode", Phase: "audio",
		TrackKind: "voiceover", TrackIndex: 0, PhaseOrder: 4, Status: "ok",
		StartedAt: base, CompletedAt: base.Add(25 * time.Millisecond), DurationMS: 25,
		StartedOffsetMS: 61, FinishedOffsetMS: 86, CPUMS: 31.5, QueueWaitMS: 2.25,
		BytesIn: 4096, BytesOut: 2048, Frames: 100, FramesIn: 100, FramesOut: 100,
		TelemetrySchemaVersion: 1,
	}
	if err := persistExecutionEventTestBatch(t, db, []taskattempts.PhaseTimingDetailed{track}); err != nil {
		t.Fatalf("persist audio track event: %v", err)
	}

	var trackKind, status string
	var trackIndex int
	var startedOffset, finishedOffset, cpuMS, queueWait float64
	var bytesIn, bytesOut, frames, framesIn, framesOut int64
	if err := db.QueryRow(`
		SELECT track_kind, track_index, started_offset_ms, finished_offset_ms,
		       cpu_ms, queue_wait_ms, bytes_in, bytes_out, frames, frames_in,
		       frames_out, status
		FROM task_execution_events WHERE event_id = 'voiceover-track-0'`).Scan(
		&trackKind, &trackIndex, &startedOffset, &finishedOffset, &cpuMS, &queueWait,
		&bytesIn, &bytesOut, &frames, &framesIn, &framesOut, &status); err != nil {
		t.Fatalf("read audio track event: %v", err)
	}
	if trackKind != "voiceover" || trackIndex != 0 || startedOffset != 61 || finishedOffset != 86 ||
		cpuMS != 31.5 || queueWait != 2.25 || bytesIn != 4096 || bytesOut != 2048 ||
		frames != 100 || framesIn != 100 || framesOut != 100 || status != "ok" {
		t.Fatalf("audio track mapping=%q/%d offsets=%v/%v cpu=%v queue=%v bytes=%d/%d frames=%d/%d/%d status=%q",
			trackKind, trackIndex, startedOffset, finishedOffset, cpuMS, queueWait,
			bytesIn, bytesOut, frames, framesIn, framesOut, status)
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

func TestPersistExecutionEvents_ReplacementRemovesStaleRowsAndRollsBack(t *testing.T) {
	db := openExecutionEventPersistenceTestDB(t)
	seedExecutionEventIdentity(t, db)

	if err := persistExecutionEventTestBatch(t, db, modernExecutionTimings()); err != nil {
		t.Fatalf("persist initial event batch: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO task_phase_timings (attempt_id, phase, component, action, duration_ms)
		VALUES ('attempt-1', 'engine.decode.probe', 'engine.decode', 'probe', 999)`); err != nil {
		t.Fatalf("insert stale phase summary: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO task_execution_events
			(event_id, attempt_id, origin, scope, component, action, phase, created_at)
		VALUES ('master-preserved', 'attempt-1', 'master', 'task', 'master.queue', 'ready_wait', 'queue', '2026-07-31T00:00:00Z')`); err != nil {
		t.Fatalf("insert master event: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO task_execution_events
			(event_id, attempt_id, origin, scope, component, action, phase, created_at)
		VALUES ('stale-worker', 'attempt-1', 'engine', 'segment', 'engine.decode', 'probe', 'decode', '2026-07-31T00:00:00Z')`); err != nil {
		t.Fatalf("insert stale worker event: %v", err)
	}

	// A replacement report contains only segment zero. It must remove the
	// omitted worker event and phase summary, while retaining master history.
	if err := persistExecutionEventTestBatch(t, db, modernExecutionTimings()[:1]); err != nil {
		t.Fatalf("persist replacement event batch: %v", err)
	}

	var phaseCount, eventCount, masterCount, staleWorkerCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_phase_timings WHERE attempt_id = 'attempt-1'`).Scan(&phaseCount); err != nil {
		t.Fatalf("count replacement phase summaries: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_execution_events WHERE attempt_id = 'attempt-1'`).Scan(&eventCount); err != nil {
		t.Fatalf("count replacement events: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_execution_events WHERE event_id = 'master-preserved'`).Scan(&masterCount); err != nil {
		t.Fatalf("count preserved master event: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_execution_events WHERE event_id = 'stale-worker'`).Scan(&staleWorkerCount); err != nil {
		t.Fatalf("count removed stale worker event: %v", err)
	}
	if phaseCount != 1 || eventCount != 2 || masterCount != 1 || staleWorkerCount != 0 {
		t.Fatalf("replacement state phases=%d events=%d master=%d stale_worker=%d; want 1/2/1/0", phaseCount, eventCount, masterCount, staleWorkerCount)
	}

	// A later invalid replacement must roll back both deletion and inserts,
	// leaving the previously committed worker event and phase summary intact.
	if _, err := db.Exec(`
		INSERT INTO task_phase_timings (attempt_id, phase, component, action, duration_ms)
		VALUES ('attempt-1', 'engine.decode.probe', 'engine.decode', 'probe', 777)`); err != nil {
		t.Fatalf("insert rollback sentinel phase summary: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO task_execution_events
			(event_id, attempt_id, origin, scope, component, action, phase, created_at)
		VALUES ('rollback-sentinel', 'attempt-1', 'engine', 'segment', 'engine.decode', 'probe', 'decode', '2026-07-31T00:00:00Z')`); err != nil {
		t.Fatalf("insert rollback sentinel event: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin rollback transaction: %v", err)
	}
	invalid := append([]taskattempts.PhaseTimingDetailed(nil), modernExecutionTimings()[:1]...)
	invalid = append(invalid, taskattempts.PhaseTimingDetailed{
		AttemptID: "attempt-1", Origin: "engine", Scope: "segment", EventID: "invalid-replacement",
		EventIndex: 99, Component: "engine.unknown", Action: "invented", Status: "ok",
	})
	err = persistPhaseTimingsAndExecutionEvents(context.Background(), tx, taskgraph.IngestResultCommand{
		AttemptID: "attempt-1", TaskID: "task-1", WorkerID: "worker-1", LeaseID: "lease-1",
		TelemetrySchemaVersion: 1, PhaseTimings: invalid,
	})
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("invalid telemetry replacement must not fail: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit quarantined replacement: %v", err)
	}

	var sentinelPhaseCount, sentinelEventCount, currentEventCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_phase_timings WHERE component = 'engine.decode' AND action = 'probe'`).Scan(&sentinelPhaseCount); err != nil {
		t.Fatalf("count rollback phase sentinel: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_execution_events WHERE event_id = 'rollback-sentinel'`).Scan(&sentinelEventCount); err != nil {
		t.Fatalf("count rollback event sentinel: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_execution_events WHERE event_id = 'encode-segment-0'`).Scan(&currentEventCount); err != nil {
		t.Fatalf("count committed event after rollback: %v", err)
	}
	if sentinelPhaseCount != 0 || sentinelEventCount != 0 || currentEventCount != 1 {
		t.Fatalf("replacement state phase_sentinel=%d event_sentinel=%d current=%d; want 0/0/1", sentinelPhaseCount, sentinelEventCount, currentEventCount)
	}
	var quarantined int
	if err := db.QueryRow(`SELECT COUNT(*) FROM telemetry_event_quarantine WHERE event_id = 'invalid-replacement'`).Scan(&quarantined); err != nil {
		t.Fatalf("count quarantined replacement event: %v", err)
	}
	if quarantined != 1 {
		t.Fatalf("quarantined replacement rows=%d, want 1", quarantined)
	}
}

func TestPersistExecutionEvents_QuarantinesUnregisteredModernEvent(t *testing.T) {
	db := openExecutionEventPersistenceTestDB(t)
	seedExecutionEventIdentity(t, db)

	bad := modernExecutionTimings()[:1]
	bad[0].EventID = "bad-event"
	bad[0].Component = "engine.unknown"
	bad[0].Action = "invented"
	if err := persistExecutionEventTestBatch(t, db, bad); err != nil {
		t.Fatalf("invalid telemetry must not block TaskResult transaction: %v", err)
	}

	var events, summaries int
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_execution_events`).Scan(&events); err != nil {
		t.Fatalf("count events after rejected batch: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_phase_timings`).Scan(&summaries); err != nil {
		t.Fatalf("count summaries after rejected batch: %v", err)
	}
	if events != 0 || summaries != 1 {
		t.Fatalf("quarantined event should preserve compact summary: events=%d summaries=%d", events, summaries)
	}
	var quarantined int
	if err := db.QueryRow(`SELECT COUNT(*) FROM telemetry_event_quarantine WHERE event_id = 'bad-event'`).Scan(&quarantined); err != nil {
		t.Fatalf("count quarantined event: %v", err)
	}
	if quarantined != 1 {
		t.Fatalf("quarantined rows=%d, want 1", quarantined)
	}
}

func TestPersistExecutionEvents_ValidPlusInvalidInvariant(t *testing.T) {
	db := openExecutionEventPersistenceTestDB(t)
	seedExecutionEventIdentity(t, db)

	valid := modernExecutionTimings()[:1]
	invalid := valid[0]
	invalid.EventID = "quarantined-invariant"
	invalid.Component = "engine.unknown"
	invalid.Action = "invented"
	if err := persistExecutionEventTestBatch(t, db, append(valid, invalid)); err != nil {
		t.Fatalf("mixed valid/invalid telemetry must not block TaskResult transaction: %v", err)
	}

	var persisted, quarantined int
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_execution_events`).Scan(&persisted); err != nil {
		t.Fatalf("count persisted events: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM telemetry_event_quarantine`).Scan(&quarantined); err != nil {
		t.Fatalf("count quarantined events: %v", err)
	}
	if persisted != 1 || quarantined != 1 || persisted+quarantined != 2 {
		t.Fatalf("event accounting persisted=%d quarantined=%d; want 1+1=2", persisted, quarantined)
	}
}

func TestPersistExecutionEvents_QuarantineWriteIsBestEffort(t *testing.T) {
	db := openExecutionEventPersistenceTestDB(t)
	seedExecutionEventIdentity(t, db)
	if _, err := db.Exec(`DROP TABLE telemetry_event_quarantine`); err != nil {
		t.Fatalf("drop optional quarantine table: %v", err)
	}

	valid := modernExecutionTimings()[:1]
	invalid := valid[0]
	invalid.EventID = "quarantine-table-unavailable"
	invalid.Component = "engine.unknown"
	invalid.Action = "invented"
	if err := persistExecutionEventTestBatch(t, db, append(valid, invalid)); err != nil {
		t.Fatalf("missing quarantine table must not block TaskResult transaction: %v", err)
	}

	var persisted int
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_execution_events`).Scan(&persisted); err != nil {
		t.Fatalf("count persisted event: %v", err)
	}
	if persisted != 1 {
		t.Fatalf("valid event count=%d, want 1 despite unavailable quarantine table", persisted)
	}
}

func TestPersistExecutionEvents_RejectsUnsupportedSchemaWithoutBlockingTaskResult(t *testing.T) {
	db := openExecutionEventPersistenceTestDB(t)
	seedExecutionEventIdentity(t, db)

	bad := modernExecutionTimings()[:1]
	bad[0].EventID = "unsupported-schema"
	bad[0].TelemetrySchemaVersion = 99
	if err := persistExecutionEventTestBatch(t, db, bad); err != nil {
		t.Fatalf("unsupported schema must not block TaskResult transaction: %v", err)
	}

	var events, quarantined int
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_execution_events`).Scan(&events); err != nil {
		t.Fatalf("count persisted events: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM telemetry_event_quarantine WHERE event_id = 'unsupported-schema'`).Scan(&quarantined); err != nil {
		t.Fatalf("count quarantined event: %v", err)
	}
	if events != 0 || quarantined != 1 {
		t.Fatalf("unsupported schema accounting events=%d quarantined=%d; want 0/1", events, quarantined)
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

func TestPersistExecutionEvents_QuarantinesCanonicalOriginScopeMismatch(t *testing.T) {
	db := openExecutionEventPersistenceTestDB(t)
	seedExecutionEventIdentity(t, db)

	before := TelemetryQuarantineCount()
	bad := modernExecutionTimings()[:1]
	bad[0].EventID = "wrong-tuple"
	bad[0].Origin = "worker"
	bad[0].Scope = "attempt"
	if err := persistExecutionEventTestBatch(t, db, bad); err != nil {
		t.Fatalf("origin/scope mismatch must not block TaskResult transaction: %v", err)
	}
	if TelemetryQuarantineCount() <= before {
		t.Fatalf("quarantine counter did not increase: before=%d after=%d", before, TelemetryQuarantineCount())
	}
}

func TestPersistExecutionEvents_QuarantinesMissingComponentAction(t *testing.T) {
	db := openExecutionEventPersistenceTestDB(t)
	seedExecutionEventIdentity(t, db)

	bad := modernExecutionTimings()[:1]
	bad[0].EventID = "missing-component-action"
	bad[0].Component = ""
	bad[0].Action = ""
	if err := persistExecutionEventTestBatch(t, db, bad); err != nil {
		t.Fatalf("missing component/action must not block TaskResult transaction: %v", err)
	}

	var quarantined int
	if err := db.QueryRow(`SELECT COUNT(*) FROM telemetry_event_quarantine WHERE event_id = 'missing-component-action'`).Scan(&quarantined); err != nil {
		t.Fatalf("count quarantined malformed event: %v", err)
	}
	if quarantined != 1 {
		t.Fatalf("quarantined malformed rows=%d, want 1", quarantined)
	}
}

func TestPersistMasterExecutionEvent_PersistsCatalogSchemaVersion(t *testing.T) {
	db := openExecutionEventPersistenceTestDB(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	if err := persistMasterExecutionEventTx(context.Background(), tx, masterExecutionEvent{
		EventID: "master-valid-schema", AttemptID: "attempt-1", TaskID: "task-1",
		JobID: "job-1", WorkerID: "worker-1", ExecutorID: "executor.canonical",
		Scope: "task", Component: "master.placement", Action: "match", Phase: "queue",
	}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("persist valid master telemetry: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit valid master event: %v", err)
	}
	var origin, scope string
	var schemaVersion int
	if err := db.QueryRow(`SELECT origin, scope, telemetry_schema_version FROM task_execution_events WHERE event_id = 'master-valid-schema'`).Scan(&origin, &scope, &schemaVersion); err != nil {
		t.Fatalf("read valid master event: %v", err)
	}
	if origin != "master" || scope != "task" || schemaVersion != 1 {
		t.Fatalf("master event identity=%q/%q schema=%d; want master/task/1", origin, scope, schemaVersion)
	}
}

func TestPersistMasterExecutionEvent_QuarantinesInvalidSchemaWithoutBlockingState(t *testing.T) {
	db := openExecutionEventPersistenceTestDB(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	before := TelemetryQuarantineCount()
	err = persistMasterExecutionEventTx(context.Background(), tx, masterExecutionEvent{
		EventID: "master-invalid-schema", AttemptID: "attempt-1", TaskID: "task-1",
		Scope: "task", Component: "master.placement", Action: "match", Phase: "queue",
		TelemetrySchemaVersion: 99,
	})
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("invalid master telemetry must not fail state transaction: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit quarantined master event: %v", err)
	}
	var events, quarantined int
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_execution_events`).Scan(&events); err != nil {
		t.Fatalf("count master events: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM telemetry_event_quarantine WHERE event_id = 'master-invalid-schema'`).Scan(&quarantined); err != nil {
		t.Fatalf("count master quarantine: %v", err)
	}
	if events != 0 || quarantined != 1 || TelemetryQuarantineCount() <= before {
		t.Fatalf("master invalid accounting events=%d quarantined=%d counter_before=%d counter_after=%d; want 0/1/increment", events, quarantined, before, TelemetryQuarantineCount())
	}
}
