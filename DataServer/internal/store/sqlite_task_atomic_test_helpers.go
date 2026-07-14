package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/taskattempts"
)

// =====================================================================
// §9.5 invariant: Task RUNNING ⇒ active Attempt RUNNING.
//
// handleTaskAccepted and handleTaskResult previously committed two
// independent statements (Task CAS + Attempt INSERT/UPDATE). A
// crash between them could leave either of these observable states:
//
//	 A. Task = RUNNING, no active Attempt   (§9.5 breach: stale RUNNING)
//	 B. Task = terminal, Attempt = RUNNING (§9.5 breach: zombie Attempt)
//
// AcceptTaskAtomic and TransitionTaskToTerminalAtomic on
// SQLiteTaskRepository commit both rows in ONE transaction. The tests
// below assert both the atomicity itself and the §9.5 invariant after
// the call returns — including a defensive rollback case for missing
// attempts that hand-rolls an out-of-band Task RUNNING row to verify
// the guard refuses to deepen the breach.
// =====================================================================

// taskAtomicTestSchema mirrors the columns AcceptTaskAtomic and
// TransitionTaskToTerminalAtomic actually touch. Foreign keys are
// enforced so the missing-attempt guard can rely on the FK constraint.
//
// PR-2 (canonical-attempt-identity) also added attempt_id + attempt_number
// so ClaimNextWithAttemptAtomic can stamp the canonical identity on the
// row inside its single tx. Both columns are nullable / default-zero so
// pre-PR-2 seeders (seedLeasedTask / seedRunningTask) continue to work
// unchanged — they simply leave the identity columns NULL/0, which the
// existing test assertions ignore.
//
// migration 052 also added lease_expires_at (master-side lease TTL,
// written by ClaimNextWithAttemptAtomic on the LEASED CAS) and
// ExpireTaskLeaseAtomic reads it for the CAS gate. Mirroring
// lease_expires_at as nullable TEXT here so ClaimNextWithAttemptAtomic's
// UPDATE can write it without blowing up under -race.
//
// cache=shared on the DSN (below) is required so concurrent goroutine
// tests land on the same logical in-memory store — mattn/go-sqlite3
// makes plain ":memory:" private to each pooled connection.
const taskAtomicTestSchema = `
CREATE TABLE tasks (
	task_id            TEXT PRIMARY KEY,
	job_id             TEXT,
	project_id         TEXT,
	render_plan_id     TEXT,
	executor_id        TEXT,
	executor_version   TEXT,
	status             TEXT,
	priority           INTEGER,
	revision           INTEGER NOT NULL DEFAULT 0,
	attempt_count      INTEGER NOT NULL DEFAULT 0,
	worker_id          TEXT,
	lease_id           TEXT,
	attempt_id         TEXT,        -- PR-2 canonical attempt_id
	attempt_number     INTEGER,     -- PR-2 canonical attempt_number
	lease_expires_at   TEXT,        -- §9.5 reaper / TTL gate
	ready_at           TEXT,
	started_at         TEXT,
	completed_at       TEXT,
	created_at         TEXT,
	updated_at         TEXT
);
CREATE TABLE task_attempts (
	id                 TEXT PRIMARY KEY,
	task_id            TEXT NOT NULL,
	job_id             TEXT,
	attempt_number     INTEGER NOT NULL,
	worker_id          TEXT,
	lease_id           TEXT,
	status             TEXT,
	started_at         TEXT,
	completed_at       TEXT,
	error_code         TEXT,
	error_message      TEXT,
	report_version     INTEGER NOT NULL DEFAULT 0,
	created_at         TEXT,
	updated_at         TEXT,
	UNIQUE (task_id, attempt_number),
	FOREIGN KEY (task_id) REFERENCES tasks(task_id) ON DELETE CASCADE
);
CREATE TABLE task_specs (
	task_id        TEXT NOT NULL PRIMARY KEY,
	spec_version   INTEGER NOT NULL DEFAULT 1,
	spec_hash      TEXT NOT NULL DEFAULT '',
	executor_id    TEXT NOT NULL DEFAULT '',
	payload_json   TEXT NOT NULL DEFAULT '{}',
	created_at     TEXT NOT NULL
);
CREATE TABLE jobs (
	job_id             TEXT PRIMARY KEY,
	status             TEXT NOT NULL,
	revision           INTEGER NOT NULL DEFAULT 0,
	max_retries        INTEGER NOT NULL DEFAULT 0,
	started_at         TEXT,
	updated_at         TEXT,
	created_at         TEXT,
	completed_at       TEXT
);
`

// openTaskAtomicTestDB returns *SQLiteStore + *SQLiteTaskRepository with the
// minimal schema for atomic-tx tests in a shared in-memory SQLite.
// Root cause of _busy_timeout=5000: mattn default=0 → flaky `database table
// is locked` under concurrent CAS. Cross-ref openInMemoryTestDB (artifacts).
func openTaskAtomicTestDB(t *testing.T) (*SQLiteStore, *SQLiteTaskRepository) {
	t.Helper()
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open sqlite (task atomic): %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(taskAtomicTestSchema); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	s := &SQLiteStore{db: db}
	return s, NewSQLiteTaskRepository(s)
}

// seedLeasedTask inserts a Task row in LEASED status with supplied
// (worker, lease, attemptID, attemptNumber, revision) AND a matching
// PENDING task_attempts row — mimicking what ClaimNextWithAttemptAtomic
// produces. AcceptTaskAtomic's CAS gate checks all four identity fields
// (worker + lease + attempt_id + attempt_number) and UPDATEs the
// attempt from PENDING → RUNNING, so both rows must be pre-seeded.
func seedLeasedTask(t *testing.T, db *sql.DB,
	taskID, workerID, leaseID, attemptID string, attemptNumber, revision int,
) int {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO tasks
		 (task_id, job_id, status, priority, revision, worker_id, lease_id,
		  attempt_id, attempt_number, created_at, updated_at)
		 VALUES (?, ?, 'LEASED', 0, ?, ?, ?, ?, ?, ?, ?)`,
		taskID, "job-"+taskID, revision, workerID, leaseID,
		attemptID, attemptNumber, now, now,
	); err != nil {
		t.Fatalf("seed LEASED task: %v", err)
	}
	// Pre-seed PENDING attempt so AcceptTaskAtomic's UPDATE
	// (PENDING→RUNNING) has a row to match.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO task_attempts
		 (id, task_id, job_id, attempt_number, worker_id, lease_id, status,
		  report_version, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, 'PENDING', 0, ?, ?)`,
		attemptID, taskID, "job-"+taskID, attemptNumber,
		workerID, leaseID, now, now,
	); err != nil {
		t.Fatalf("seed PENDING attempt: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO jobs
		 (job_id, status, revision, max_retries, created_at, updated_at)
		 VALUES (?, 'PENDING', 0, 3, ?, ?)`,
		"job-"+taskID, now, now,
	); err != nil {
		t.Fatalf("seed PENDING job: %v", err)
	}
	return revision
}

// seedRunningTask inserts a Task directly in RUNNING status with
// supplied (worker, lease) but no matching attempt. Used by the
// §9.5-rollback-guard tests to hand-roll an out-of-band desync.
func seedRunningTask(t *testing.T, db *sql.DB,
	taskID, workerID, leaseID string,
) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO tasks
		 (task_id, job_id, status, priority, revision, attempt_count, worker_id, lease_id,
		  started_at, created_at, updated_at)
		 VALUES (?, ?, 'RUNNING', 0, 1, 1, ?, ?, ?, ?, ?)`,
		taskID, "job-"+taskID, workerID, leaseID, now, now, now,
	); err != nil {
		t.Fatalf("seed RUNNING task: %v", err)
	}
}

// seedReadyTask inserts a Task row in READY status with empty worker/lease
// and the supplied revision. Used by ClaimNextWithAttemptAtomic test
// (the dispatcher selector picks WHERE status='READY' AND worker_id=”).
func seedReadyTask(t *testing.T, db *sql.DB,
	taskID string, revision int,
) int {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO tasks
		 (task_id, job_id, project_id, render_plan_id, executor_id, executor_version,
		  status, priority, revision, attempt_number, worker_id, lease_id, created_at, updated_at)
		 VALUES (?, ?, '', '', '', 0, 'READY', 0, ?, 0, '', '', ?, ?)`,
		taskID, "job-"+taskID, revision, now, now,
	); err != nil {
		t.Fatalf("seed READY task: %v", err)
	}
	return revision
}

// seedReadyTaskWithExecutor inserts a READY task with specific
// executor_id + executor_version AND a matching task_specs row so
// ClaimTaskForWorkerAtomic can read the spec payload after claiming.
func seedReadyTaskWithExecutor(t *testing.T, db *sql.DB,
	taskID, executorID string, executorVersion, revision int,
) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO tasks
		 (task_id, job_id, project_id, render_plan_id, executor_id, executor_version,
		  status, priority, revision, attempt_count, attempt_number,
		  worker_id, lease_id, created_at, updated_at)
		 VALUES (?, ?, '', '', ?, ?, 'READY', 0, ?, 0, 0, '', '', ?, ?)`,
		taskID, "job-"+taskID, executorID, executorVersion, revision, now, now,
	); err != nil {
		t.Fatalf("seed READY task with executor: %v", err)
	}
	// Seed a task_specs row so the spec read in ClaimTaskForWorkerAtomic
	// succeeds (the last tx step reads payload_json FROM task_specs).
	if _, err := db.ExecContext(ctx,
		`INSERT INTO task_specs (task_id, spec_version, spec_hash, executor_id, payload_json, created_at)
		 VALUES (?, 1, '', ?, '{}', ?)`,
		taskID, executorID, now,
	); err != nil {
		t.Fatalf("seed task_specs for ready task: %v", err)
	}
}

// attemptForTask returns the matching active-or-terminal attempt for
// (task_id, worker_id, lease_id), or nil if no row.
//
// NULL columns on active (non-terminal) attempts — started_at,
// completed_at, error_code, error_message — AND the always-populated
// created_at / updated_at TEXT timestamps are all scanned via
// sql.NullString intermediaries and parsed/assigned conditionally.
// This avoids both direct-pointer Scan failures on NULL columns and
// driver-version drift on TEXT→time.Time conversion in the
// connection-shared in-memory SQLite used by these tests.
func attemptForTask(t *testing.T, db *sql.DB,
	taskID, workerID, leaseID string,
) *taskattempts.TaskAttempt {
	t.Helper()
	var (
		a            taskattempts.TaskAttempt
		startedAt    sql.NullString
		completedAt  sql.NullString
		errorCode    sql.NullString
		errorMessage sql.NullString
		createdAt    sql.NullString
		updatedAt    sql.NullString
	)
	row := db.QueryRowContext(context.Background(),
		`SELECT id, task_id, job_id, attempt_number, worker_id, lease_id,
		        status, started_at, completed_at, error_code, error_message,
		        report_version, created_at, updated_at
		 FROM task_attempts
		 WHERE task_id = ? AND worker_id = ? AND lease_id = ?
		 ORDER BY attempt_number DESC LIMIT 1`,
		taskID, workerID, leaseID)
	if err := row.Scan(&a.ID, &a.TaskID, &a.JobID, &a.AttemptNumber,
		&a.WorkerID, &a.LeaseID, &a.Status,
		&startedAt, &completedAt, &errorCode, &errorMessage,
		&a.ReportVersion, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		t.Fatalf("attemptForTask scan: %v", err)
	}
	if startedAt.Valid && startedAt.String != "" {
		if pt, e := time.Parse(time.RFC3339, startedAt.String); e == nil {
			a.StartedAt = &pt
		}
	}
	if completedAt.Valid && completedAt.String != "" {
		if pt, e := time.Parse(time.RFC3339, completedAt.String); e == nil {
			a.CompletedAt = &pt
		}
	}
	if errorCode.Valid {
		a.ErrorCode = errorCode.String
	}
	if errorMessage.Valid {
		a.ErrorMessage = errorMessage.String
	}
	if createdAt.Valid && createdAt.String != "" {
		if pt, e := time.Parse(time.RFC3339, createdAt.String); e == nil {
			a.CreatedAt = pt
		}
	}
	if updatedAt.Valid && updatedAt.String != "" {
		if pt, e := time.Parse(time.RFC3339, updatedAt.String); e == nil {
			a.UpdatedAt = pt
		}
	}
	return &a
}
