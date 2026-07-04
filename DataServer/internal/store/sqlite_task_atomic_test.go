package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
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

// =====================================================================
// AcceptTaskAtomic tests
// =====================================================================

// TestAcceptTaskAtomic_HappyPath: LEASED + matching worker/lease/revision
// ⇒ Task RUNNING AND attempt INSERT both committed atomically.
func TestAcceptTaskAtomic_HappyPath(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()
	seedLeasedTask(t, s.db, "T-accept-1", "w-1", "L-1", "A-accept-1", 1, 0)

	attempt := &taskattempts.TaskAttempt{
		ID:            "A-accept-1",
		TaskID:        "T-accept-1",
		JobID:         "job-T-accept-1",
		WorkerID:      "w-1",
		LeaseID:       "L-1",
		AttemptNumber: 1,
		Status:        taskattempts.AttemptStatusRunning,
	}
	if err := r.AcceptTaskAtomic(ctx, attempt, 0); err != nil {
		t.Fatalf("AcceptTaskAtomic happy path: %v", err)
	}

	var taskStatus, workerID, leaseID string
	var revision int
	if err := s.db.QueryRowContext(ctx,
		`SELECT status, worker_id, lease_id, revision FROM tasks WHERE task_id = ?`,
		"T-accept-1").Scan(&taskStatus, &workerID, &leaseID, &revision); err != nil {
		t.Fatalf("post-accept SELECT tasks: %v", err)
	}
	if taskStatus != "RUNNING" {
		t.Errorf("tasks.status = %s; want RUNNING", taskStatus)
	}
	if revision != 1 {
		t.Errorf("tasks.revision = %d; want 1 (CAS increment)", revision)
	}
	if workerID != "w-1" || leaseID != "L-1" {
		t.Errorf("worker/lease drifted: w=%s L=%s", workerID, leaseID)
	}

	// §9.5 invariant (positive case): Task RUNNING ⇒ attempt RUNNING exists.
	att := attemptForTask(t, s.db, "T-accept-1", "w-1", "L-1")
	if att == nil {
		t.Fatal("active attempt missing after AcceptTaskAtomic")
	}
	if att.Status != taskattempts.AttemptStatusRunning {
		t.Errorf("task_attempts.status = %s; want RUNNING", att.Status)
	}

	var jobStatus, jobStartedAt string
	var jobRevision int
	if err := s.db.QueryRowContext(ctx,
		`SELECT status, COALESCE(started_at, ''), revision FROM jobs WHERE job_id = ?`,
		"job-T-accept-1").Scan(&jobStatus, &jobStartedAt, &jobRevision); err != nil {
		t.Fatalf("post-accept SELECT jobs: %v", err)
	}
	if jobStatus != "RUNNING" {
		t.Errorf("jobs.status = %s; want RUNNING", jobStatus)
	}
	if jobStartedAt == "" {
		t.Errorf("jobs.started_at empty; want RFC3339 timestamp")
	}
	if jobRevision != 1 {
		t.Errorf("jobs.revision = %d; want 1", jobRevision)
	}
}

// TestAcceptTaskAtomic_StaleRevision: wrong revision ⇒ ErrTransitionConflict
// AND no attempt row inserted (rolled back).
func TestAcceptTaskAtomic_StaleRevision(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()
	seedLeasedTask(t, s.db, "T-accept-2", "w-2", "L-2", "A-accept-2", 1, 0)

	attempt := &taskattempts.TaskAttempt{
		ID:            "A-accept-2",
		TaskID:        "T-accept-2",
		JobID:         "job-T-accept-2",
		WorkerID:      "w-2",
		LeaseID:       "L-2",
		AttemptNumber: 1,
		Status:        taskattempts.AttemptStatusRunning,
	}
	err := r.AcceptTaskAtomic(ctx, attempt, 99) // stale revision
	if err == nil {
		t.Fatal("expected ErrTransitionConflict on stale revision, got nil")
	}
	if !errors.Is(err, taskgraph.ErrTransitionConflict) {
		t.Errorf("expected taskgraph.ErrTransitionConflict; got %v", err)
	}

	// Verify rollback: task stayed LEASED, PENDING attempt row remains
	// unchanged (rollback preserved the pre-seeded state).
	var taskStatus string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status FROM tasks WHERE task_id = ?`,
		"T-accept-2").Scan(&taskStatus); err != nil {
		t.Fatalf("post-reject SELECT tasks: %v", err)
	}
	if taskStatus != "LEASED" {
		t.Errorf("tasks.status = %s; want LEASED (rollback)", taskStatus)
	}
	var n int
	var attemptStatus string
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(MIN(status), '') FROM task_attempts WHERE task_id = ? GROUP BY task_id`,
		"T-accept-2").Scan(&n, &attemptStatus); err != nil {
		// No rows at all (unexpected — seedLeasedTask pre-inserts one)
		n = 0
	}
	if n != 1 {
		t.Errorf("task_attempts rows = %d; want 1 (pre-seeded PENDING row, rollback preserved it)", n)
	}
	if attemptStatus != "PENDING" {
		t.Errorf("task_attempts status = %s; want PENDING (rollback did NOT promote it)", attemptStatus)
	}

	var jobStatus string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status FROM jobs WHERE job_id = ?`,
		"job-T-accept-2").Scan(&jobStatus); err != nil {
		t.Fatalf("post-reject SELECT jobs: %v", err)
	}
	if jobStatus != "PENDING" {
		t.Errorf("jobs.status = %s; want PENDING (rollback)", jobStatus)
	}
}

func TestAcceptTaskAtomic_PromotesRetryWaitJob(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()
	seedLeasedTask(t, s.db, "T-accept-retry", "w-r", "L-r", "A-accept-r", 1, 0)

	if _, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status = 'RETRY_WAIT', revision = 4 WHERE job_id = ?`,
		"job-T-accept-retry"); err != nil {
		t.Fatalf("seed RETRY_WAIT job: %v", err)
	}

	attempt := &taskattempts.TaskAttempt{
		ID:            "A-accept-r",
		TaskID:        "T-accept-retry",
		JobID:         "job-T-accept-retry",
		WorkerID:      "w-r",
		LeaseID:       "L-r",
		AttemptNumber: 1,
		Status:        taskattempts.AttemptStatusRunning,
	}
	if err := r.AcceptTaskAtomic(ctx, attempt, 0); err != nil {
		t.Fatalf("AcceptTaskAtomic retry_wait job: %v", err)
	}

	var jobStatus string
	var jobRevision int
	if err := s.db.QueryRowContext(ctx,
		`SELECT status, revision FROM jobs WHERE job_id = ?`,
		"job-T-accept-retry").Scan(&jobStatus, &jobRevision); err != nil {
		t.Fatalf("post-accept retry_wait SELECT jobs: %v", err)
	}
	if jobStatus != "RUNNING" {
		t.Errorf("jobs.status = %s; want RUNNING", jobStatus)
	}
	if jobRevision != 5 {
		t.Errorf("jobs.revision = %d; want 5", jobRevision)
	}
}

func TestAcceptTaskAtomic_RejectsTerminalJobState(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()
	seedLeasedTask(t, s.db, "T-accept-terminal", "w-t", "L-t", "A-accept-t", 1, 0)

	if _, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status = 'FAILED', revision = 2 WHERE job_id = ?`,
		"job-T-accept-terminal"); err != nil {
		t.Fatalf("seed FAILED job: %v", err)
	}

	attempt := &taskattempts.TaskAttempt{
		ID:            "A-accept-t",
		TaskID:        "T-accept-terminal",
		JobID:         "job-T-accept-terminal",
		WorkerID:      "w-t",
		LeaseID:       "L-t",
		AttemptNumber: 1,
		Status:        taskattempts.AttemptStatusRunning,
	}
	err := r.AcceptTaskAtomic(ctx, attempt, 0)
	if err == nil {
		t.Fatal("expected ErrTransitionConflict on terminal job state, got nil")
	}
	if !errors.Is(err, taskgraph.ErrTransitionConflict) {
		t.Errorf("expected taskgraph.ErrTransitionConflict; got %v", err)
	}

	var taskStatus string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status FROM tasks WHERE task_id = ?`,
		"T-accept-terminal").Scan(&taskStatus); err != nil {
		t.Fatalf("post-terminal-conflict SELECT tasks: %v", err)
	}
	if taskStatus != "LEASED" {
		t.Errorf("tasks.status = %s; want LEASED (rollback)", taskStatus)
	}

	var attemptStatusAfter string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status FROM task_attempts WHERE id = ?`,
		"A-accept-t").Scan(&attemptStatusAfter); err != nil {
		t.Fatalf("post-terminal-conflict SELECT task_attempts: %v", err)
	}
	if attemptStatusAfter != "PENDING" {
		t.Errorf("task_attempts.status = %s; want PENDING (rollback)", attemptStatusAfter)
	}
}

// =====================================================================
// TransitionTaskToTerminalAtomic tests
// =====================================================================

// TestTransitionTaskToTerminalAtomic_HappyPath: Task RUNNING + active
// attempt RUNNING ⇒ both terminal in one commit.
func TestTransitionTaskToTerminalAtomic_HappyPath(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	seedLeasedTask(t, s.db, "T-term-1", "w-1", "L-1", "A-term-1", 1, 0)
	attempt := &taskattempts.TaskAttempt{
		ID:            "A-term-1",
		TaskID:        "T-term-1",
		JobID:         "job-T-term-1",
		WorkerID:      "w-1",
		LeaseID:       "L-1",
		AttemptNumber: 1,
		Status:        taskattempts.AttemptStatusRunning,
	}
	if err := r.AcceptTaskAtomic(ctx, attempt, 0); err != nil {
		t.Fatalf("seed AcceptTaskAtomic: %v", err)
	}

	if err := r.TransitionTaskToTerminalAtomic(ctx, "T-term-1", "w-1", "L-1",
		taskgraph.StatusSucceeded, taskattempts.AttemptStatusSucceeded, "", ""); err != nil {
		t.Fatalf("TransitionTaskToTerminalAtomic happy path: %v", err)
	}

	var taskStatus, taskCompletedAt string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status, COALESCE(completed_at, '') FROM tasks WHERE task_id = ?`,
		"T-term-1").Scan(&taskStatus, &taskCompletedAt); err != nil {
		t.Fatalf("post-terminal SELECT tasks: %v", err)
	}
	if taskStatus != "SUCCEEDED" {
		t.Errorf("tasks.status = %s; want SUCCEEDED", taskStatus)
	}
	if taskCompletedAt == "" {
		t.Errorf("tasks.completed_at empty; want RFC3339 timestamp")
	}

	// §9.5 invariant (positive case): Task SUCCEEDED AND attempt SUCCEEDED.
	att := attemptForTask(t, s.db, "T-term-1", "w-1", "L-1")
	if att == nil {
		t.Fatal("attempt missing after TransitionTaskToTerminalAtomic")
	}
	if att.Status != taskattempts.AttemptStatusSucceeded {
		t.Errorf("task_attempts.status = %s; want SUCCEEDED", att.Status)
	}
}

// TestTransitionTaskToTerminalAtomic_IdempotentRetry: a second call
// after the first succeeds (Task terminal) must return
// ErrTransitionConflict (Task CAS did not match), not silently
// double-promote. The replay-safe short-circuit on attempt-already-
// terminal only fires if the Task CAS still succeeded; here Task is
// already SUCCEEDED so the second call is a clean refusal.
func TestTransitionTaskToTerminalAtomic_IdempotentRetry(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	seedLeasedTask(t, s.db, "T-term-2", "w-2", "L-2", "A-term-2", 1, 0)
	attempt := &taskattempts.TaskAttempt{
		ID: "A-term-2", TaskID: "T-term-2", JobID: "job-T-term-2",
		WorkerID: "w-2", LeaseID: "L-2",
		AttemptNumber: 1, Status: taskattempts.AttemptStatusRunning,
	}
	if err := r.AcceptTaskAtomic(ctx, attempt, 0); err != nil {
		t.Fatalf("seed AcceptTaskAtomic: %v", err)
	}

	// First terminal transition.
	if err := r.TransitionTaskToTerminalAtomic(ctx, "T-term-2", "w-2", "L-2",
		taskgraph.StatusSucceeded, taskattempts.AttemptStatusSucceeded, "", ""); err != nil {
		t.Fatalf("first TransitionTaskToTerminalAtomic: %v", err)
	}
	// Second transition (worker retry): Task CAS fails (already SUCCEEDED)
	// ⇒ ErrTransitionConflict. This is the correct semantic: idempotency
	// at the Task-CAS layer, not at the row layer.
	err := r.TransitionTaskToTerminalAtomic(ctx, "T-term-2", "w-2", "L-2",
		taskgraph.StatusSucceeded, taskattempts.AttemptStatusSucceeded, "", "")
	if err == nil {
		t.Fatal("second call expected ErrTransitionConflict (Task already terminal)")
	}
	if !errors.Is(err, taskgraph.ErrTransitionConflict) {
		t.Errorf("second call expected ErrTransitionConflict; got %v", err)
	}

	// Verify state unchanged.
	var taskStatus string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status FROM tasks WHERE task_id = ?`,
		"T-term-2").Scan(&taskStatus); err != nil {
		t.Fatal(err)
	}
	if taskStatus != "SUCCEEDED" {
		t.Errorf("tasks.status = %s; want SUCCEEDED (no drift)", taskStatus)
	}
}

// TestTransitionTaskToTerminalAtomic_RollsBackOnMissingAttempt is THE
// §9.5 invariant guard: a Task RUNNING row that has no matching attempt
// (out-of-band desync, e.g. handleTaskAccepted attempted Create but
// crashed) MUST NOT be allowed to deepen the breach by also transitioning
// the Task to terminal. The method rolls back with ErrStaleReport and
// leaves the Task row at RUNNING so an operator can inspect it.
func TestTransitionTaskToTerminalAtomic_RollsBackOnMissingAttempt(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	seedRunningTask(t, s.db, "T-orphan-no-attempt", "w-orph", "L-orph")

	err := r.TransitionTaskToTerminalAtomic(ctx, "T-orphan-no-attempt", "w-orph", "L-orph",
		taskgraph.StatusSucceeded, taskattempts.AttemptStatusSucceeded, "", "")
	if err == nil {
		t.Fatal("expected ErrStaleReport when attempt row missing; got nil")
	}
	if !errors.Is(err, taskattempts.ErrStaleReport) {
		t.Errorf("expected taskattempts.ErrStaleReport; got %v", err)
	}
	if !strings.Contains(err.Error(), "§9.5 invariant") {
		t.Errorf("error message should reference §9.5 invariant guard; got %q", err.Error())
	}

	// §9.5 preservation: Task stayed at RUNNING even though caller asked
	// for SUCCEEDED. The desync was already present, but the guard
	// refuses to deepen it.
	var taskStatus string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status FROM tasks WHERE task_id = ?`,
		"T-orphan-no-attempt").Scan(&taskStatus); err != nil {
		t.Fatal(err)
	}
	if taskStatus != "RUNNING" {
		t.Errorf("tasks.status = %s; want RUNNING (rollback preserved §9.5 state)", taskStatus)
	}
}

// TestTransitionTaskToTerminalAtomic_RejectsTerminalStatusInput guards
// the precondition that callers must supply a terminal target status.
func TestTransitionTaskToTerminalAtomic_RejectsTerminalStatusInput(t *testing.T) {
	_, r := openTaskAtomicTestDB(t)
	ctx := context.Background()
	err := r.TransitionTaskToTerminalAtomic(ctx, "T-bad-input", "w", "L",
		taskgraph.StatusRunning, taskattempts.AttemptStatusSucceeded, "", "")
	if err == nil {
		t.Fatal("expected validation error for non-terminal task status; got nil")
	}
	if !strings.Contains(err.Error(), "terminal task status") {
		t.Errorf("error should mention terminal task status; got %q", err.Error())
	}
}

// =====================================================================
// ClaimNextWithAttemptAtomic — atomicity test (PR-2 / fix/canonical-attempt-identity).
//
// The 4-sub-op claim tx (SELECT READY → UPDATE CAS stamp
//   attempt_id + attempt_number + revision + lease_expires_at
// → INSERT task_attempts → SELECT task_specs.payload_json → COMMIT)
// MUST commit-or-rollback atomically. A middle-step failure (e.g.
// UNIQUE collision on task_attempts(task_id, attempt_number)) must NOT
// leave the Task in a half-claimed state with attempt_id stamped but
// no Attempt row minted — that would violate the §9.5 invariant.
//
// Failure-injection mechanism: drop task_specs so the final
// `SELECT payload_json FROM task_specs ...` fails AFTER the tx has
// already done the READY→LEASED CAS and inserted the PENDING attempt.
// The tx must roll back fully. The test then asserts:
//   1. tasks.status remained 'READY' (step-3 UPDATE rolled back)
//   2. tasks.attempt_id is NULL/'' (NOT stamped with uuid from step-2)
//   3. tasks.attempt_number is 0 (NOT stamped with computed value)
//   4. tasks.revision is unchanged (NOT bumped)
//   5. tasks.lease_expires_at is empty (NOT stamped with TTL date)
//   6. task_attempts has zero rows (the inserted pending attempt rolled back)
//   7. The function returned (nil, nil, err) — no phantom claim contract
//
// migration 052 added lease_expires_at to the production schema (mirrored
// at the top of this file) so ClaimNextWithAttemptAtomic's UPDATE can
// write it under -race; the new column is purely additive and unused by
// pre-existing test seeds.
// =====================================================================

func TestClaimNextWithAttemptAtomic_Atomicity(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	const (
		taskID      = "T-claim-atomicity-1"
		newWorkerID = "w-1"
		newLeaseID  = "L-1"
	)
	seedReadyTask(t, s.db, taskID, 0)

	if _, err := s.db.ExecContext(ctx, `DROP TABLE task_specs`); err != nil {
		t.Fatalf("drop task_specs for rollback injection: %v", err)
	}

	// Claim — the final task_specs read must fail, forcing full rollback.
	tws, att, err := r.ClaimNextWithAttemptAtomic(ctx, newWorkerID, newLeaseID)
	if err == nil {
		t.Fatalf("ClaimNextWithAttemptAtomic: expected spec-read failure, got nil (tws=%v att=%v)", tws, att)
	}
	if tws != nil {
		t.Errorf("tws=%v; want nil (rollback prevented claim contract)", tws)
	}
	if att != nil {
		t.Errorf("att=%v; want nil (rollback prevented attempt mint)", att)
	}

	// Atomicity assertion 1–4: tasks row preserved at READY + pre-state.
	var (
		taskStatus                                    string
		workerCol, leaseCol                           string
		attemptIDCol                                  sql.NullString
		attemptNumCol                                 sql.NullInt64
		revision                                      int
		leaseExpiresCol, startedAtCol, completedAtCol sql.NullString
	)
	err = s.db.QueryRowContext(ctx,
		`SELECT status, COALESCE(worker_id, ''), COALESCE(lease_id, ''),
		        attempt_id, attempt_number, revision,
		        COALESCE(lease_expires_at, ''), COALESCE(started_at, ''), COALESCE(completed_at, '')
		 FROM tasks WHERE task_id = ?`,
		taskID,
	).Scan(&taskStatus, &workerCol, &leaseCol, &attemptIDCol, &attemptNumCol, &revision,
		&leaseExpiresCol, &startedAtCol, &completedAtCol)
	if err != nil {
		t.Fatalf("post-rollback SELECT tasks: %v", err)
	}
	if taskStatus != "READY" {
		t.Errorf("tasks.status = %q; want READY (rollback preserved pre-state)", taskStatus)
	}
	if workerCol != "" || leaseCol != "" {
		t.Errorf("tasks worker_id=%q lease_id=%q want both empty (rollback preserved pre-state)", workerCol, leaseCol)
	}
	if attemptIDCol.Valid && attemptIDCol.String != "" {
		t.Errorf("tasks.attempt_id = %q; want NULL/empty (rollback did NOT clear canonical stamp)", attemptIDCol.String)
	}
	if attemptNumCol.Valid && attemptNumCol.Int64 != 0 {
		t.Errorf("tasks.attempt_number = %d; want 0 (rollback did NOT clear canonical stamp)", attemptNumCol.Int64)
	}
	if revision != 0 {
		t.Errorf("tasks.revision = %d; want 0 (rollback did NOT bump CAS counter)", revision)
	}
	if leaseExpiresCol.Valid && leaseExpiresCol.String != "" {
		t.Errorf("tasks.lease_expires_at = %q; want empty (rollback did NOT write lease_expires_at)", leaseExpiresCol.String)
	}
	if startedAtCol.Valid && startedAtCol.String != "" {
		t.Errorf("tasks.started_at = %q; want empty", startedAtCol.String)
	}
	if completedAtCol.Valid && completedAtCol.String != "" {
		t.Errorf("tasks.completed_at = %q; want empty", completedAtCol.String)
	}

	// Atomicity assertion 5: the inserted pending attempt rolled back.
	var count int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_attempts WHERE task_id = ?`,
		taskID,
	).Scan(&count); err != nil {
		t.Fatalf("COUNT task_attempts: %v", err)
	}
	if count != 0 {
		t.Errorf("task_attempts count = %d; want 0 after rollback", count)
	}
}

func TestReleaseLease_DeletesPendingAttemptAndClearsClaimIdentity(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	const (
		taskID      = "T-release-1"
		workerID    = "w-release-1"
		leaseID     = "L-release-1"
		attemptID   = "A-release-1"
		attemptNum  = 1
		taskRevSeed = 0
	)
	seedLeasedTask(t, s.db, taskID, workerID, leaseID, attemptID, attemptNum, taskRevSeed)

	if err := r.ReleaseLease(ctx, taskID, workerID, leaseID); err != nil {
		t.Fatalf("ReleaseLease: %v", err)
	}

	var (
		taskStatus      string
		clearedWorkerID string
		clearedLeaseID  string
		revision        int
		attemptIDCol    sql.NullString
		attemptNumCol   int
		leaseExpCol     sql.NullString
	)
	if err := s.db.QueryRowContext(ctx,
		`SELECT status, COALESCE(worker_id, ''), COALESCE(lease_id, ''),
		        revision, attempt_id, attempt_number, lease_expires_at
		 FROM tasks WHERE task_id = ?`,
		taskID,
	).Scan(&taskStatus, &clearedWorkerID, &clearedLeaseID,
		&revision, &attemptIDCol, &attemptNumCol, &leaseExpCol); err != nil {
		t.Fatalf("post-release SELECT tasks: %v", err)
	}
	if taskStatus != "READY" {
		t.Errorf("tasks.status = %q; want READY", taskStatus)
	}
	if clearedWorkerID != "" || clearedLeaseID != "" {
		t.Errorf("tasks worker_id=%q lease_id=%q; want both empty", clearedWorkerID, clearedLeaseID)
	}
	if attemptIDCol.Valid && attemptIDCol.String != "" {
		t.Errorf("tasks.attempt_id = %q; want NULL/empty after release", attemptIDCol.String)
	}
	if attemptNumCol != 0 {
		t.Errorf("tasks.attempt_number = %d; want 0 after release", attemptNumCol)
	}
	if leaseExpCol.Valid && leaseExpCol.String != "" {
		t.Errorf("tasks.lease_expires_at = %q; want NULL/empty after release", leaseExpCol.String)
	}
	if revision != 1 {
		t.Errorf("tasks.revision = %d; want 1 after release CAS", revision)
	}

	var attempts int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_attempts WHERE task_id = ?`,
		taskID,
	).Scan(&attempts); err != nil {
		t.Fatalf("count task_attempts after release: %v", err)
	}
	if attempts != 0 {
		t.Errorf("task_attempts rows = %d; want 0 after release deletes pending attempt", attempts)
	}
}

func TestClaimNextWithAttemptAtomic_UsesHistoricalMaxAttemptNumber(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	const (
		taskID          = "T-claim-historical-max"
		workerID        = "w-history-1"
		leaseID         = "L-history-1"
		previousAttempt = "A-history-1"
	)

	seedReadyTask(t, s.db, taskID, 0)

	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO task_attempts
		 (id, task_id, job_id, attempt_number, worker_id, lease_id, status,
		  report_version, created_at, updated_at)
		 VALUES (?, ?, ?, 1, ?, ?, 'TIMED_OUT', 0, ?, ?)`,
		previousAttempt, taskID, "job-"+taskID, workerID, leaseID, now, now,
	); err != nil {
		t.Fatalf("seed historical attempt: %v", err)
	}

	tws, att, err := r.ClaimNextWithAttemptAtomic(ctx, workerID, "L-history-2")
	if err != nil {
		t.Fatalf("ClaimNextWithAttemptAtomic drift-repair: %v", err)
	}
	if tws == nil || att == nil {
		t.Fatalf("claim returned nil tws/att: tws=%v att=%v", tws, att)
	}
	if att.AttemptNumber != 2 {
		t.Fatalf("attempt.AttemptNumber = %d; want 2", att.AttemptNumber)
	}

	var attemptCount, attemptNumber int
	if err := s.db.QueryRowContext(ctx,
		`SELECT attempt_count, attempt_number FROM tasks WHERE task_id = ?`,
		taskID,
	).Scan(&attemptCount, &attemptNumber); err != nil {
		t.Fatalf("SELECT repaired task: %v", err)
	}
	if attemptCount != 2 {
		t.Errorf("tasks.attempt_count = %d; want newly-minted attempt ordinal 2", attemptCount)
	}
	if attemptNumber != 2 {
		t.Errorf("tasks.attempt_number = %d; want claimed attempt 2", attemptNumber)
	}

	var invariantBreaches int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*)
		   FROM (
		     SELECT t.task_id
		     FROM tasks t
		     JOIN task_attempts ta ON ta.task_id = t.task_id
		     GROUP BY t.task_id
		     HAVING t.attempt_count < MAX(ta.attempt_number)
		   )`,
	).Scan(&invariantBreaches); err != nil {
		t.Fatalf("invariant query: %v", err)
	}
	if invariantBreaches != 0 {
		t.Errorf("attempt-count invariant breaches = %d; want 0", invariantBreaches)
	}
}

// =====================================================================
// §9.5 invariant property-style scan for the full happy path.
// =====================================================================

// TestSqliteTaskAtomic_Invariant_TaskRunningInvActiveAttempt sweeps
// 8 (worker, lease) pairs through the canonical accept → terminate
// lifecycle and asserts §9.5 holds at every step: at no point may
// tasks have status RUNNING without a matching active attempt.
func TestSqliteTaskAtomic_Invariant_TaskRunningInvActiveAttempt(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	for i := 0; i < 8; i++ {
		taskID := fmt.Sprintf("T-sweep-%d", i)
		workerID := fmt.Sprintf("w-sweep-%d", i)
		leaseID := fmt.Sprintf("L-sweep-%d", i)

		seedLeasedTask(t, s.db, taskID, workerID, leaseID, "A-sweep-"+fmt.Sprint(i), 1, 0)
		attempt := &taskattempts.TaskAttempt{
			ID:     "A-sweep-" + fmt.Sprint(i),
			TaskID: taskID, JobID: "job-" + taskID,
			WorkerID: workerID, LeaseID: leaseID,
			AttemptNumber: 1, Status: taskattempts.AttemptStatusRunning,
		}
		if err := r.AcceptTaskAtomic(ctx, attempt, 0); err != nil {
			t.Fatalf("iteration %d: AcceptTaskAtomic: %v", i, err)
		}

		// Invariant check mid-flight: Task RUNNING ⇒ Attempt RUNNING.
		var taskStatus string
		if err := s.db.QueryRowContext(ctx,
			`SELECT status FROM tasks WHERE task_id = ?`,
			taskID).Scan(&taskStatus); err != nil {
			t.Fatalf("iteration %d: SELECT tasks.status: %v", i, err)
		}
		if taskStatus != "RUNNING" {
			t.Fatalf("iteration %d: tasks.status = %s; want RUNNING", i, taskStatus)
		}
		att := attemptForTask(t, s.db, taskID, workerID, leaseID)
		if att == nil {
			t.Fatalf("iteration %d: §9.5 BREACH — Task RUNNING but no active Attempt", i)
		}
		if att.Status != taskattempts.AttemptStatusRunning {
			t.Fatalf("iteration %d: §9.5 BREACH — Task RUNNING but Attempt status=%s", i, att.Status)
		}

		// Now terminate atomically.
		if err := r.TransitionTaskToTerminalAtomic(ctx, taskID, workerID, leaseID,
			taskgraph.StatusSucceeded, taskattempts.AttemptStatusSucceeded, "", ""); err != nil {
			t.Fatalf("iteration %d: TransitionTaskToTerminalAtomic: %v", i, err)
		}

		// Final invariant check: Task SUCCEEDED AND Attempt SUCCEEDED.
		if err := s.db.QueryRowContext(ctx,
			`SELECT status FROM tasks WHERE task_id = ?`,
			taskID).Scan(&taskStatus); err != nil {
			t.Fatalf("iteration %d: final SELECT tasks.status: %v", i, err)
		}
		if taskStatus != "SUCCEEDED" {
			t.Errorf("iteration %d: tasks.status = %s; want SUCCEEDED", i, taskStatus)
		}
		att = attemptForTask(t, s.db, taskID, workerID, leaseID)
		if att == nil || att.Status != taskattempts.AttemptStatusSucceeded {
			t.Errorf("iteration %d: final attempt wrong: %+v", i, att)
		}
	}
}

// TestSqliteTaskAtomic_Invariant_ConcurrentTerminalTransitionsIsSafe:
// 2 goroutines racing TransitionTaskToTerminalAtomic on the same task
// produce EXACTLY one SUCCEEDED Task, EXACTLY one SUCCEEDED attempt.
// The loser must NOT leave the attempt stranded in RUNNING.
func TestSqliteTaskAtomic_Invariant_ConcurrentTerminalTransitionsIsSafe(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	seedLeasedTask(t, s.db, "T-race", "w-race", "L-race", "A-race", 1, 0)
	attempt := &taskattempts.TaskAttempt{
		ID: "A-race", TaskID: "T-race", JobID: "job-T-race",
		WorkerID: "w-race", LeaseID: "L-race",
		AttemptNumber: 1, Status: taskattempts.AttemptStatusRunning,
	}
	if err := r.AcceptTaskAtomic(ctx, attempt, 0); err != nil {
		t.Fatalf("seed AcceptTaskAtomic: %v", err)
	}

	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := r.TransitionTaskToTerminalAtomic(ctx, "T-race", "w-race", "L-race",
				taskgraph.StatusSucceeded, taskattempts.AttemptStatusSucceeded, "", "")
			results <- err
		}()
	}
	wg.Wait()
	close(results)

	successes := 0
	var firstErr error
	for err := range results {
		if err == nil {
			successes++
		} else if firstErr == nil {
			firstErr = err
		}
	}
	if successes != 1 {
		t.Errorf("concurrent terminal transitions: successes=%d; want exactly 1 (first err=%v)",
			successes, firstErr)
	}
	// §9.5 invariant under contention: exactly one SUCCEEDED on tasks and
	// exactly one SUCCEEDED on attempts. Anything else is a §9.5 breach.
	var taskSucc int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM tasks WHERE task_id = ? AND status = 'SUCCEEDED'`,
		"T-race").Scan(&taskSucc); err != nil {
		t.Fatal(err)
	}
	if taskSucc != 1 {
		t.Errorf("SUCCEEDED tasks = %d; want 1 (§9.5)", taskSucc)
	}
	var attSucc int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_attempts
		 WHERE task_id = ? AND status = 'SUCCEEDED'`,
		"T-race").Scan(&attSucc); err != nil {
		t.Fatal(err)
	}
	if attSucc != 1 {
		t.Errorf("SUCCEEDED attempts = %d; want 1 (§9.5)", attSucc)
	}
}

// =====================================================================
// ClaimTaskForWorkerAtomic — placement-driven exact-task claim tests.
//
// The placement matcher produces a (task_id, executor_id, executor_version)
// tuple. ClaimTaskForWorkerAtomic CAS-gates on all three plus revision
// so a concurrent dispatcher cannot claim the same task between
// ListReadyCandidates and Claim. The tests below assert:
//   - Happy path: matching revision + executor → LEASED + PENDING attempt
//   - Stale revision: wrong revision → ErrTransitionConflict
//   - Executor mismatch: wrong executor_id or executor_version → conflict
//   - Status mismatch: task not READY → conflict
//   - Concurrent claim: task already claimed by another dispatcher → conflict
//   - Atomicity: mid-tx failure (dropped task_specs) → full rollback
// =====================================================================

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

// TestClaimTaskForWorkerAtomic_HappyPath: READY task with matching
// revision + executor_id + executor_version → successful claim.
func TestClaimTaskForWorkerAtomic_HappyPath(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	const (
		taskID          = "T-claim-1"
		executorID      = "blender"
		executorVersion = 4
		taskRevision    = 0
		workerID        = "w-claim-1"
		leaseID         = "L-claim-1"
	)
	seedReadyTaskWithExecutor(t, s.db, taskID, executorID, executorVersion, taskRevision)

	cmd := taskgraph.ClaimTaskForWorkerCommand{
		TaskID:               taskID,
		ExpectedTaskRevision: taskRevision,
		WorkerID:             workerID,
		SessionID:            "sess-1",
		LeaseID:              leaseID,
		ExecutorID:           executorID,
		ExecutorVersion:      executorVersion,
		CapabilityRevision:   1,
	}
	tws, att, err := r.ClaimTaskForWorkerAtomic(ctx, cmd)
	if err != nil {
		t.Fatalf("ClaimTaskForWorkerAtomic happy path: %v", err)
	}
	if tws == nil || att == nil {
		t.Fatalf("expected non-nil TaskWithSpec + TaskAttempt; got tws=%v att=%v", tws, att)
	}

	// Verify Task row: LEASED + worker/lease stamped + revision bumped.
	var status, storedWorker, storedLease string
	var revision int
	if err := s.db.QueryRowContext(ctx,
		`SELECT status, worker_id, lease_id, revision FROM tasks WHERE task_id = ?`,
		taskID,
	).Scan(&status, &storedWorker, &storedLease, &revision); err != nil {
		t.Fatalf("post-claim SELECT tasks: %v", err)
	}
	if status != "LEASED" {
		t.Errorf("tasks.status = %q; want LEASED", status)
	}
	if storedWorker != workerID {
		t.Errorf("tasks.worker_id = %q; want %q", storedWorker, workerID)
	}
	if storedLease != leaseID {
		t.Errorf("tasks.lease_id = %q; want %q", storedLease, leaseID)
	}
	if revision != taskRevision+1 {
		t.Errorf("tasks.revision = %d; want %d", revision, taskRevision+1)
	}

	// Verify PENDING attempt exists.
	attFromDB := attemptForTask(t, s.db, taskID, workerID, leaseID)
	if attFromDB == nil {
		t.Fatal("PENDING attempt missing after claim")
	}
	if attFromDB.Status != taskattempts.AttemptStatusPending {
		t.Errorf("attempt.status = %s; want PENDING", attFromDB.Status)
	}
	if attFromDB.AttemptNumber != 1 {
		t.Errorf("attempt.attempt_number = %d; want 1", attFromDB.AttemptNumber)
	}

	// Verify returned TaskWithSpec carries the spec payload.
	if tws.Task.ID != taskID {
		t.Errorf("tws.Task.ID = %q; want %q", tws.Task.ID, taskID)
	}
	if tws.Task.WorkerID != workerID {
		t.Errorf("tws.Task.WorkerID = %q; want %q", tws.Task.WorkerID, workerID)
	}

	// Verify returned attempt matches the DB.
	if att.ID != attFromDB.ID {
		t.Errorf("returned attempt.ID = %q; DB attempt.ID = %q", att.ID, attFromDB.ID)
	}
}

// TestClaimTaskForWorkerAtomic_RevisionMismatch: task exists READY but
// the expected revision doesn't match → ErrTransitionConflict.
func TestClaimTaskForWorkerAtomic_RevisionMismatch(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	const (
		taskID          = "T-claim-rev"
		executorID      = "blender"
		executorVersion = 4
		taskRevision    = 3
	)
	seedReadyTaskWithExecutor(t, s.db, taskID, executorID, executorVersion, taskRevision)

	cmd := taskgraph.ClaimTaskForWorkerCommand{
		TaskID:               taskID,
		ExpectedTaskRevision: 99, // stale revision
		WorkerID:             "w-rev",
		SessionID:            "sess-rev",
		LeaseID:              "L-rev",
		ExecutorID:           executorID,
		ExecutorVersion:      executorVersion,
		CapabilityRevision:   1,
	}
	tws, att, err := r.ClaimTaskForWorkerAtomic(ctx, cmd)
	if err == nil {
		t.Fatalf("expected ErrTransitionConflict on stale revision; got nil (tws=%v att=%v)", tws, att)
	}
	if !errors.Is(err, taskgraph.ErrTransitionConflict) {
		t.Errorf("expected taskgraph.ErrTransitionConflict; got %v", err)
	}
	if tws != nil || att != nil {
		t.Errorf("expected nil return values on conflict; got tws=%v att=%v", tws, att)
	}

	// Verify task stayed READY (rollback preserved pre-claim state).
	var status string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status FROM tasks WHERE task_id = ?`, taskID,
	).Scan(&status); err != nil {
		t.Fatalf("post-conflict SELECT: %v", err)
	}
	if status != "READY" {
		t.Errorf("tasks.status = %q; want READY (rollback)", status)
	}

	// Verify no attempt row was created.
	var count int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_attempts WHERE task_id = ?`, taskID,
	).Scan(&count); err != nil {
		t.Fatalf("count attempts: %v", err)
	}
	if count != 0 {
		t.Errorf("task_attempts rows = %d; want 0", count)
	}
}

// TestClaimTaskForWorkerAtomic_ExecutorIDMismatch: task READY but
// executor_id doesn't match → ErrTransitionConflict.
func TestClaimTaskForWorkerAtomic_ExecutorIDMismatch(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	seedReadyTaskWithExecutor(t, s.db, "T-claim-eid", "blender", 4, 0)

	cmd := taskgraph.ClaimTaskForWorkerCommand{
		TaskID:               "T-claim-eid",
		ExpectedTaskRevision: 0,
		WorkerID:             "w-eid",
		SessionID:            "sess-eid",
		LeaseID:              "L-eid",
		ExecutorID:           "nuke", // wrong executor
		ExecutorVersion:      4,
		CapabilityRevision:   1,
	}
	_, _, err := r.ClaimTaskForWorkerAtomic(ctx, cmd)
	if err == nil {
		t.Fatal("expected ErrTransitionConflict on executor_id mismatch")
	}
	if !errors.Is(err, taskgraph.ErrTransitionConflict) {
		t.Errorf("expected taskgraph.ErrTransitionConflict; got %v", err)
	}

	// Verify task stayed READY.
	var status string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status FROM tasks WHERE task_id = 'T-claim-eid'`,
	).Scan(&status); err != nil {
		t.Fatalf("post-conflict SELECT: %v", err)
	}
	if status != "READY" {
		t.Errorf("tasks.status = %q; want READY", status)
	}
}

// TestClaimTaskForWorkerAtomic_ExecutorVersionMismatch: task READY but
// executor_version doesn't match → ErrTransitionConflict.
func TestClaimTaskForWorkerAtomic_ExecutorVersionMismatch(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	seedReadyTaskWithExecutor(t, s.db, "T-claim-ever", "blender", 4, 0)

	cmd := taskgraph.ClaimTaskForWorkerCommand{
		TaskID:               "T-claim-ever",
		ExpectedTaskRevision: 0,
		WorkerID:             "w-ever",
		SessionID:            "sess-ever",
		LeaseID:              "L-ever",
		ExecutorID:           "blender",
		ExecutorVersion:      99, // wrong version
		CapabilityRevision:   1,
	}
	_, _, err := r.ClaimTaskForWorkerAtomic(ctx, cmd)
	if err == nil {
		t.Fatal("expected ErrTransitionConflict on executor_version mismatch")
	}
	if !errors.Is(err, taskgraph.ErrTransitionConflict) {
		t.Errorf("expected taskgraph.ErrTransitionConflict; got %v", err)
	}

	var status string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status FROM tasks WHERE task_id = 'T-claim-ever'`,
	).Scan(&status); err != nil {
		t.Fatalf("post-conflict SELECT: %v", err)
	}
	if status != "READY" {
		t.Errorf("tasks.status = %q; want READY", status)
	}
}

// TestClaimTaskForWorkerAtomic_NotReady: task exists but is not READY
// (e.g. already LEASED by another worker) → ErrTransitionConflict.
func TestClaimTaskForWorkerAtomic_NotReady(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	const taskID = "T-claim-notready"
	// Seed as LEASED (already claimed by someone else).
	seedLeasedTask(t, s.db, taskID, "w-other", "L-other", "A-other", 1, 0)

	cmd := taskgraph.ClaimTaskForWorkerCommand{
		TaskID:               taskID,
		ExpectedTaskRevision: 0,
		WorkerID:             "w-me",
		SessionID:            "sess-me",
		LeaseID:              "L-me",
		ExecutorID:           "blender",
		ExecutorVersion:      4,
		CapabilityRevision:   1,
	}
	_, _, err := r.ClaimTaskForWorkerAtomic(ctx, cmd)
	if err == nil {
		t.Fatal("expected ErrTransitionConflict when task not READY")
	}
	if !errors.Is(err, taskgraph.ErrTransitionConflict) {
		t.Errorf("expected taskgraph.ErrTransitionConflict; got %v", err)
	}

	// Task must still be LEASED with the original worker (no drift).
	var status, storedWorker, storedLease string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status, worker_id, lease_id FROM tasks WHERE task_id = ?`, taskID,
	).Scan(&status, &storedWorker, &storedLease); err != nil {
		t.Fatalf("post-conflict SELECT: %v", err)
	}
	if status != "LEASED" {
		t.Errorf("tasks.status = %q; want LEASED", status)
	}
	if storedWorker != "w-other" || storedLease != "L-other" {
		t.Errorf("tasks worker/lease = %q/%q; want w-other/L-other", storedWorker, storedLease)
	}
}

// TestClaimTaskForWorkerAtomic_AlreadyClaimed: two concurrent claims
// race on the same task → one wins, the other gets ErrTransitionConflict.
func TestClaimTaskForWorkerAtomic_AlreadyClaimed(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	seedReadyTaskWithExecutor(t, s.db, "T-claim-race", "blender", 4, 0)

	claim := func(workerID, leaseID string) error {
		cmd := taskgraph.ClaimTaskForWorkerCommand{
			TaskID:               "T-claim-race",
			ExpectedTaskRevision: 0,
			WorkerID:             workerID,
			SessionID:            "sess-" + workerID,
			LeaseID:              leaseID,
			ExecutorID:           "blender",
			ExecutorVersion:      4,
			CapabilityRevision:   1,
		}
		_, _, err := r.ClaimTaskForWorkerAtomic(ctx, cmd)
		return err
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs <- claim(
				fmt.Sprintf("w-race-%d", idx),
				fmt.Sprintf("L-race-%d", idx),
			)
		}(i)
	}
	wg.Wait()
	close(errs)

	successes := 0
	conflicts := 0
	for err := range errs {
		if err == nil {
			successes++
		} else if errors.Is(err, taskgraph.ErrTransitionConflict) ||
			strings.Contains(err.Error(), "database table is locked") {
			conflicts++
		} else {
			t.Errorf("unexpected error: %v", err)
		}
	}
	if successes != 1 {
		t.Errorf("concurrent claims: successes=%d; want exactly 1", successes)
	}
	if conflicts != 1 {
		t.Errorf("concurrent claims: conflicts=%d; want exactly 1", conflicts)
	}

	// Verify exactly one LEASED row exists.
	var count int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM tasks WHERE task_id = 'T-claim-race' AND status = 'LEASED'`,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("LEASED tasks = %d; want 1", count)
	}

	// Verify exactly one PENDING attempt.
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_attempts WHERE task_id = 'T-claim-race'`,
	).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("task_attempts rows = %d; want 1", count)
	}
}

// TestClaimTaskForWorkerAtomic_Atomicity: inject a mid-tx failure
// (DROP TABLE task_specs) and verify the entire tx rolls back —
// task stays READY, no attempt row is created, no lease stamp leaked.
func TestClaimTaskForWorkerAtomic_Atomicity(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()

	const (
		taskID          = "T-claim-atom"
		executorID      = "blender"
		executorVersion = 4
		taskRevision    = 0
		workerID        = "w-atom"
		leaseID         = "L-atom"
	)
	seedReadyTaskWithExecutor(t, s.db, taskID, executorID, executorVersion, taskRevision)

	// Drop task_specs so the final SELECT payload_json fails mid-tx.
	if _, err := s.db.ExecContext(ctx, `DROP TABLE task_specs`); err != nil {
		t.Fatalf("drop task_specs: %v", err)
	}

	cmd := taskgraph.ClaimTaskForWorkerCommand{
		TaskID:               taskID,
		ExpectedTaskRevision: taskRevision,
		WorkerID:             workerID,
		SessionID:            "sess-atom",
		LeaseID:              leaseID,
		ExecutorID:           executorID,
		ExecutorVersion:      executorVersion,
		CapabilityRevision:   1,
	}
	tws, att, err := r.ClaimTaskForWorkerAtomic(ctx, cmd)
	if err == nil {
		t.Fatalf("expected spec-read failure (dropped task_specs); got nil (tws=%v att=%v)", tws, att)
	}
	if tws != nil {
		t.Errorf("tws=%v; want nil (rollback prevented claim)", tws)
	}
	if att != nil {
		t.Errorf("att=%v; want nil (rollback prevented attempt mint)", att)
	}

	// Atomicity: task row preserved at READY + pre-state.
	var (
		status        string
		storedWorker  string
		storedLease   string
		revision      int
		attemptIDCol  sql.NullString
		attemptNumCol sql.NullInt64
		leaseExpCol   sql.NullString
	)
	err = s.db.QueryRowContext(ctx,
		`SELECT status, COALESCE(worker_id, ''), COALESCE(lease_id, ''),
		        revision, attempt_id, attempt_number,
		        COALESCE(lease_expires_at, '')
		 FROM tasks WHERE task_id = ?`,
		taskID,
	).Scan(&status, &storedWorker, &storedLease, &revision,
		&attemptIDCol, &attemptNumCol, &leaseExpCol)
	if err != nil {
		t.Fatalf("post-rollback SELECT: %v", err)
	}
	if status != "READY" {
		t.Errorf("tasks.status = %q; want READY (rollback)", status)
	}
	if storedWorker != "" || storedLease != "" {
		t.Errorf("tasks worker=%q lease=%q; want both empty", storedWorker, storedLease)
	}
	if attemptIDCol.Valid && attemptIDCol.String != "" {
		t.Errorf("tasks.attempt_id = %q; want NULL/empty", attemptIDCol.String)
	}
	if attemptNumCol.Valid && attemptNumCol.Int64 != 0 {
		t.Errorf("tasks.attempt_number = %d; want 0", attemptNumCol.Int64)
	}
	if revision != taskRevision {
		t.Errorf("tasks.revision = %d; want %d (no CAS bump)", revision, taskRevision)
	}
	if leaseExpCol.Valid && leaseExpCol.String != "" {
		t.Errorf("tasks.lease_expires_at = %q; want empty", leaseExpCol.String)
	}

	// No attempt row created.
	var count int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_attempts WHERE task_id = ?`, taskID,
	).Scan(&count); err != nil {
		t.Fatalf("count attempts: %v", err)
	}
	if count != 0 {
		t.Errorf("task_attempts rows = %d; want 0", count)
	}
}
