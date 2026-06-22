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
// PR-04 / §9.5 invariant: Task RUNNING ⇒ active Attempt RUNNING.
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
`

// openTaskAtomicTestDB returns *SQLiteStore + *SQLiteTaskRepository scoped
// to a connection-shared in-memory SQLite with the minimal schema.
func openTaskAtomicTestDB(t *testing.T) (*SQLiteStore, *SQLiteTaskRepository) {
	t.Helper()
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
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
// (worker, lease, revision). Returns the assigned revision.
func seedLeasedTask(t *testing.T, db *sql.DB,
	taskID, workerID, leaseID string, revision int,
) int {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.ExecContext(ctx,
		`INSERT INTO tasks
		 (task_id, job_id, status, priority, revision, worker_id, lease_id,
		  created_at, updated_at)
		 VALUES (?, ?, 'LEASED', 0, ?, ?, ?, ?, ?)`,
		taskID, "job-"+taskID, revision, workerID, leaseID, now, now,
	); err != nil {
		t.Fatalf("seed LEASED task: %v", err)
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
	seedLeasedTask(t, s.db, "T-accept-1", "w-1", "L-1", 0)

	attempt := &taskattempts.TaskAttempt{
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
}

// TestAcceptTaskAtomic_StaleRevision: wrong revision ⇒ ErrTransitionConflict
// AND no attempt row inserted (rolled back).
func TestAcceptTaskAtomic_StaleRevision(t *testing.T) {
	s, r := openTaskAtomicTestDB(t)
	ctx := context.Background()
	seedLeasedTask(t, s.db, "T-accept-2", "w-2", "L-2", 0)

	attempt := &taskattempts.TaskAttempt{
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

	// Verify rollback: task stayed LEASED, no attempt row inserted.
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
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_attempts WHERE task_id = ?`,
		"T-accept-2").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("task_attempts rows = %d; want 0 (rollback)", n)
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

	seedLeasedTask(t, s.db, "T-term-1", "w-1", "L-1", 0)
	attempt := &taskattempts.TaskAttempt{
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

	seedLeasedTask(t, s.db, "T-term-2", "w-2", "L-2", 0)
	attempt := &taskattempts.TaskAttempt{
		TaskID: "T-term-2", JobID: "job-T-term-2",
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
	// for SUCCEEDED. The desync was already present (PR-04 cannot
	// retroactively heal an out-of-band orphan), but the guard refuses
	// to deepen it.
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
// §9.5 invariant property-style scan for the full PR-04 happy path.
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

		seedLeasedTask(t, s.db, taskID, workerID, leaseID, 0)
		attempt := &taskattempts.TaskAttempt{
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

	seedLeasedTask(t, s.db, "T-race", "w-race", "L-race", 0)
	attempt := &taskattempts.TaskAttempt{
		TaskID: "T-race", JobID: "job-T-race",
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
