package store

import (
	"context"
	"errors"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// =====================================================================
// AcceptTaskAtomic tests
// =====================================================================
//
// AcceptTaskAtomic atomically transitions a Task from LEASED → RUNNING
// AND UPDATES the matching PENDING TaskAttempt to RUNNING AND promotes
// the parent Job from PENDING/RETRY_WAIT to RUNNING in ONE transaction.
// The §9.5 invariant guarantees that every Task RUNNING has a matching
// active Attempt. Each test below pins a guard around that contract.

// TestAcceptTaskAtomic_HappyPath: LEASED + matching worker/lease/revision
// ⇒ Task RUNNING AND attempt UPDATE both committed atomically.
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
// AND no attempt row updated (rolled back).
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
