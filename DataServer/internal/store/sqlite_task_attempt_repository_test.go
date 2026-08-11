package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"velox-server/internal/taskattempts"
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
CREATE TABLE tasks (
	task_id          TEXT PRIMARY KEY,
	job_id           TEXT NOT NULL,
	executor_id      TEXT NOT NULL DEFAULT '',
	executor_version INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE task_phase_timings (
	id                 INTEGER PRIMARY KEY AUTOINCREMENT,
	attempt_id         TEXT NOT NULL,
	phase              TEXT NOT NULL,
	duration_ms        INTEGER NOT NULL DEFAULT 0,
	wall_start         TEXT NOT NULL DEFAULT '',
	wall_end           TEXT NOT NULL DEFAULT '',
	phase_order        INTEGER NOT NULL DEFAULT 0,
	component          TEXT NOT NULL DEFAULT '',
	action             TEXT NOT NULL DEFAULT '',
	status             TEXT NOT NULL DEFAULT 'ok',
	error_code         TEXT NOT NULL DEFAULT '',
	error_message      TEXT NOT NULL DEFAULT '',
	bytes_in           INTEGER NOT NULL DEFAULT 0,
	bytes_out          INTEGER NOT NULL DEFAULT 0,
	frames             INTEGER NOT NULL DEFAULT 0,
	metadata_json      TEXT NOT NULL DEFAULT '{}',
	job_id             TEXT NOT NULL DEFAULT '',
	task_id            TEXT NOT NULL DEFAULT '',
	worker_id          TEXT NOT NULL DEFAULT '',
	worker_snapshot_id TEXT NOT NULL DEFAULT '',
	executor_id        TEXT NOT NULL DEFAULT '',
	executor_version   INTEGER NOT NULL DEFAULT 0,
	UNIQUE (attempt_id, component, action)
);
CREATE TABLE task_attempts (
	id              TEXT PRIMARY KEY,
	task_id         TEXT NOT NULL,
	job_id          TEXT NOT NULL,
	attempt_number  INTEGER NOT NULL,
	worker_id       TEXT NOT NULL,
	worker_session_id  TEXT NOT NULL DEFAULT '',
	worker_snapshot_id TEXT NOT NULL DEFAULT '',
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
	span_id              TEXT NOT NULL DEFAULT '',
	renderer_version     TEXT NOT NULL DEFAULT '', -- migration 148
	artifact_sha256      TEXT NOT NULL DEFAULT '', -- migration 148
	worker_sha256        TEXT NOT NULL DEFAULT '', -- migration 149
	artifact_sha256_mismatch INTEGER NOT NULL DEFAULT 0 -- migration 149
);
`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	return NewSQLiteTaskAttemptRepository(&SQLiteStore{db: db})
}

func TestPersistPhaseTimingsDetailed_UsesCanonicalIdentity(t *testing.T) {
	repo := openTaskAttemptTestDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339)

	if _, err := repo.store.db.ExecContext(ctx, `
		INSERT INTO tasks (task_id, job_id, executor_id, executor_version)
		VALUES ('task-canonical', 'job-canonical', 'executor.canonical', 7)`); err != nil {
		t.Fatalf("insert canonical task: %v", err)
	}
	if _, err := repo.store.db.ExecContext(ctx, `
		INSERT INTO task_attempts (
			id, task_id, job_id, attempt_number, worker_id,
			worker_session_id, worker_snapshot_id, lease_id, status,
			created_at, updated_at
		) VALUES ('attempt-canonical', 'task-canonical', 'job-canonical', 1,
			'worker-canonical', 'session-canonical', 'snapshot-canonical',
			'lease-canonical', 'FAILED', ?, ?)`, now, now); err != nil {
		t.Fatalf("insert canonical attempt: %v", err)
	}

	err := repo.PersistPhaseTimingsDetailed(ctx, "attempt-canonical", []taskattempts.PhaseTimingDetailed{{
		AttemptID:        "attempt-canonical",
		JobID:            "spoofed-job",
		TaskID:           "spoofed-task",
		WorkerID:         "spoofed-worker",
		WorkerSnapshotID: "spoofed-snapshot",
		ExecutorID:       "spoofed.executor",
		ExecutorVersion:  999,
		Component:        "engine",
		Action:           "encode",
		Status:           "ok",
		DurationMS:       42,
	}})
	if err != nil {
		t.Fatalf("PersistPhaseTimingsDetailed: %v", err)
	}

	var jobID, taskID, workerID, snapshotID, executorID string
	var executorVersion int
	if err := repo.store.db.QueryRowContext(ctx, `
		SELECT job_id, task_id, worker_id, worker_snapshot_id, executor_id, executor_version
		FROM task_phase_timings WHERE attempt_id = 'attempt-canonical'`).Scan(
		&jobID, &taskID, &workerID, &snapshotID, &executorID, &executorVersion); err != nil {
		t.Fatalf("read persisted phase identity: %v", err)
	}
	if jobID != "job-canonical" || taskID != "task-canonical" || workerID != "worker-canonical" ||
		snapshotID != "snapshot-canonical" || executorID != "executor.canonical" || executorVersion != 7 {
		t.Fatalf("persisted phase identity = %q/%q/%q/%q/%q/%d; want canonical values",
			jobID, taskID, workerID, snapshotID, executorID, executorVersion)
	}
}

func TestCreateRejectsNonCanonicalWorkerSnapshot(t *testing.T) {
	repo := openTaskAttemptTestDB(t)
	ctx := context.Background()

	if _, err := repo.store.db.ExecContext(ctx, `
		CREATE TABLE worker_sessions (
			session_id TEXT PRIMARY KEY,
			worker_id TEXT NOT NULL,
			session_type TEXT NOT NULL,
			status TEXT NOT NULL,
			revoked INTEGER NOT NULL DEFAULT 0
		);
		CREATE TABLE worker_runtime_snapshots (
			snapshot_id TEXT PRIMARY KEY,
			worker_id TEXT NOT NULL,
			session_id TEXT NOT NULL
		);`); err != nil {
		t.Fatalf("create runtime identity schema: %v", err)
	}

	if _, err := repo.store.db.ExecContext(ctx, `
		INSERT INTO worker_sessions(session_id, worker_id, session_type, status, revoked)
		VALUES ('session-create-a', 'worker-create', 'control', 'ACTIVE', 0),
		       ('session-create-b', 'worker-create', 'control', 'ACTIVE', 0)`); err != nil {
		t.Fatalf("insert runtime sessions: %v", err)
	}
	if _, err := repo.store.db.ExecContext(ctx, `
		INSERT INTO worker_runtime_snapshots(snapshot_id, worker_id, session_id)
		VALUES ('snapshot-create-a', 'worker-create', 'session-create-a'),
		       ('snapshot-create-b', 'worker-create', 'session-create-b')`); err != nil {
		t.Fatalf("insert runtime snapshots: %v", err)
	}

	attempt := &taskattempts.TaskAttempt{
		ID:               "attempt-create-spoof",
		TaskID:           "task-create-spoof",
		JobID:            "job-create-spoof",
		AttemptNumber:    1,
		WorkerID:         "worker-create",
		WorkerSessionID:  "session-create-a",
		WorkerSnapshotID: "snapshot-create-b", // belongs to session B
		LeaseID:          "lease-create-spoof",
		Status:           taskattempts.AttemptStatusPending,
	}
	if err := repo.Create(ctx, attempt); err == nil {
		t.Fatal("Create should reject a snapshot belonging to a different session")
	}

	var rows int
	if err := repo.store.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_attempts WHERE id = ?`, attempt.ID).Scan(&rows); err != nil {
		t.Fatalf("count rejected attempt rows: %v", err)
	}
	if rows != 0 {
		t.Fatalf("rejected attempt rows = %d, want 0", rows)
	}
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
