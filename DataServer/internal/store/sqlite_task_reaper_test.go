package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/taskgraph"
)

// =====================================================================
// PR-04 / audit §P0.4 + §P0.6: master-side task lease reap tests.
//
// PR-04 split the reap into two single-purpose methods:
//
//   - RequeueExpiredLeases (SELECT-only) — returns a slice of
//     taskgraph.RequeueCandidate carrying the OBSERVED lease fields
//     needed by the atomic reap. No row mutation.
//
//   - ExpireTaskLeaseAtomic (reaper) — CAS-gates on
//     (task_id, lease_id, lease_expires_at_observed, worker_id) and
//     closes Task + TaskAttempt in one tx. Does NOT bump attempt_count
//     (audit P0#4: attempt_count is owned by AcceptTaskAtomic) and
//     does NOT bump revision (idempotent on the identity tuple).
//
// The tests below exercise both methods in sequence, mirroring how
// TaskLeaseReaper.Run wires them. NO migration below — purely a
// repository-level verification, same shape as success_path_test.go
// for FinalizeVerified.
// =====================================================================

const taskReaperSchema = `
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
	attempt_id         TEXT,
	attempt_number     INTEGER NOT NULL DEFAULT 0,
	worker_id          TEXT,
	lease_id           TEXT,
	lease_expires_at   TEXT,
	ready_at           TEXT,
	started_at         TEXT,
	completed_at       TEXT,
	created_at         TEXT,
	updated_at         TEXT
);

CREATE TABLE task_attempts (
	id              TEXT PRIMARY KEY,
	task_id         TEXT NOT NULL,
	job_id          TEXT NOT NULL,
	worker_id       TEXT NOT NULL,
	attempt_number  INTEGER NOT NULL,
	lease_id        TEXT NOT NULL,
	status          TEXT NOT NULL,
	revision        INTEGER NOT NULL DEFAULT 0,
	report_version  INTEGER NOT NULL DEFAULT 0,
	error_code      TEXT,
	error_message   TEXT,
	started_at      TEXT,
	completed_at    TEXT,
	created_at      TEXT NOT NULL,
	updated_at      TEXT NOT NULL
);
`

func openTaskReaperTestDB(t *testing.T) (*SQLiteStore, *SQLiteTaskRepository) {
	t.Helper()
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open sqlite (task reaper): %v", err)
	}
	if _, err := db.Exec(taskReaperSchema); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	s := &SQLiteStore{db: db}
	t.Cleanup(func() { _ = db.Close() })
	return s, NewSQLiteTaskRepository(s)
}

func seedLeasedTaskAt(t *testing.T, db *sql.DB,
	taskID, workerID, leaseID, leaseExpiresAt, status string,
	revision, attemptCount int,
) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO tasks
		 (task_id, job_id, status, priority, revision, attempt_count, attempt_number,
		  worker_id, lease_id, lease_expires_at, created_at, updated_at)
		 VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?)`,
		taskID, "job-"+taskID, status, revision, attemptCount, attemptCount,
		workerID, leaseID, leaseExpiresAt, now, now,
	); err != nil {
		t.Fatalf("seed LEASED task %q: %v", taskID, err)
	}
}

func seedRunningAttempt(t *testing.T, db *sql.DB, attemptID, taskID, workerID, leaseID string, attemptNumber int) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO task_attempts (id, task_id, job_id, worker_id, attempt_number, lease_id, status, revision, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, 'RUNNING', 0, ?, ?)`,
		attemptID, taskID, "job-"+taskID, workerID, attemptNumber, leaseID, now, now,
	); err != nil {
		t.Fatalf("seed RUNNING attempt %q: %v", attemptID, err)
	}
}

// reapOne drives RequeueExpiredLeases (SELECT) followed by
// ExpireTaskLeaseAtomic (REAP). Returns whatever reap atomic returns so
// callers can assert on Result.TaskStatus / AttemptsExhausted.
func reapOne(t *testing.T, taskR *SQLiteTaskRepository, nowStr string, maxRetriesPerJob int) ([]taskgraph.RequeueCandidate, []taskgraph.ExpireResult, error) {
	t.Helper()
	candidates, err := taskR.RequeueExpiredLeases(context.Background(), nowStr, 1)
	if err != nil {
		return nil, nil, err
	}
	if len(candidates) == 0 {
		return candidates, nil, nil
	}
	c := candidates[0]
	res, err := taskR.ExpireTaskLeaseAtomic(context.Background(),
		c.ID, c.LeaseID, c.LeaseExpiresAt, maxRetriesPerJob)
	if err != nil {
		return candidates, nil, err
	}
	return candidates, []taskgraph.ExpireResult{res}, nil
}

// TestRequeueExpiredLeases_ReapsLeaseAndClosesAttempt_HappyPath:
// expired LEASED + active RUNNING attempt → atomic reap puts Task
// READY (re-claimable), closes Attempt as TIMED_OUT, clears
// worker/lease. attempt_count and revision are NOT bumped by reaper
// (audit P0#4 single-writer rule).
func TestRequeueExpiredLeases_ReapsLeaseAndClosesAttempt_HappyPath(t *testing.T) {
	s, taskR := openTaskReaperTestDB(t)
	ctx := context.Background()

	past := time.Now().UTC().Add(-5 * time.Minute).Format(time.RFC3339)
	seedLeasedTaskAt(t, s.db, "T-expired-leased", "w-1", "L-1", past, "LEASED", 0, 1)
	seedRunningAttempt(t, s.db, "A-expired", "T-expired-leased", "w-1", "L-1", 1)

	nowStr := time.Now().UTC().Format(time.RFC3339)
	cands, results, err := reapOne(t, taskR, nowStr, 3)
	if err != nil {
		t.Fatalf("reapOne: %v", err)
	}
	if len(cands) != 1 || cands[0].ID != "T-expired-leased" {
		t.Errorf("SELECT candidates = %+v; want one candidate T-expired-leased", cands)
	}
	if len(results) != 1 {
		t.Fatalf("reap results = %d; want 1", len(results))
	}
	if results[0].TaskStatus != taskgraph.StatusReady {
		t.Errorf("Task after reap = %s; want READY", results[0].TaskStatus)
	}
	if results[0].AttemptsExhausted {
		t.Errorf("Task retries-flag should NOT be exhausted at attempt_count=1/maxRetries=3")
	}
	if !results[0].AttemptClosed {
		t.Errorf("Attempt should be closed (TIMED_OUT)")
	}

	// Tasks row post-reap: READY, worker/lease cleared, attempt_count=1,
	// revision=1 (CAS increment from the reaper's UPDATE).
	var status, worker, lease string
	var attempts, rev int
	if err := s.db.QueryRowContext(ctx,
		`SELECT status, COALESCE(worker_id,''), COALESCE(lease_id,''),
		        attempt_count, revision
		 FROM tasks WHERE task_id = ?`,
		"T-expired-leased").Scan(&status, &worker, &lease, &attempts, &rev); err != nil {
		t.Fatalf("post-reap SELECT: %v", err)
	}
	if status != "READY" {
		t.Errorf("status = %s; want READY", status)
	}
	if worker != "" || lease != "" {
		t.Errorf("worker/lease still set post-reap: w=%q L=%q", worker, lease)
	}
	if attempts != 1 {
		t.Errorf("attempt_count = %d; want 1 (reaper does NOT bump)", attempts)
	}
	if rev != 1 {
		t.Errorf("revision = %d; want 1 (reaper CAS UPDATE bumps revision)", rev)
	}

	// Attempt post-reap: TIMED_OUT.
	var aStatus string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status FROM task_attempts WHERE id = ?`,
		"A-expired").Scan(&aStatus); err != nil {
		t.Fatalf("post-reap attempt SELECT: %v", err)
	}
	if aStatus != "TIMED_OUT" {
		t.Errorf("attempt status = %s; want TIMED_OUT", aStatus)
	}
}

// TestRequeueExpiredLeases_FreshLeaseUntouched: a future-dated lease
// is not in the candidate list, and reap does not bump it.
func TestRequeueExpiredLeases_FreshLeaseUntouched(t *testing.T) {
	s, taskR := openTaskReaperTestDB(t)
	ctx := context.Background()

	past := time.Now().UTC().Add(-5 * time.Minute).Format(time.RFC3339)
	seedLeasedTaskAt(t, s.db, "T-expired-leased", "w-1", "L-1", past, "LEASED", 0, 1)

	future := time.Now().UTC().Add(30 * time.Minute).Format(time.RFC3339)
	seedLeasedTaskAt(t, s.db, "T-fresh-leased", "w-2", "L-2", future, "LEASED", 0, 1)

	nowStr := time.Now().UTC().Format(time.RFC3339)
	cands, _, err := reapOne(t, taskR, nowStr, 3)
	if err != nil {
		t.Fatalf("reapOne: %v", err)
	}
	if len(cands) != 1 || cands[0].ID != "T-expired-leased" {
		t.Errorf("candidates = %+v; want only [T-expired-leased]", cands)
	}

	// Fresh LEASED → unchanged.
	var status, worker, lease string
	var attempts, rev int
	if err := s.db.QueryRowContext(ctx,
		`SELECT status, COALESCE(worker_id,''), COALESCE(lease_id,''),
		        attempt_count, revision
		 FROM tasks WHERE task_id = ?`,
		"T-fresh-leased").Scan(&status, &worker, &lease, &attempts, &rev); err != nil {
		t.Fatalf("post-reap SELECT (fresh): %v", err)
	}
	if status != "LEASED" {
		t.Errorf("fresh status = %s; want LEASED", status)
	}
	if worker != "w-2" || lease != "L-2" {
		t.Errorf("fresh worker/lease drifted: w=%q L=%q", worker, lease)
	}
	if attempts != 1 || rev != 0 {
		t.Errorf("fresh attempt/rev drifted: attempts=%d rev=%d", attempts, rev)
	}
}

// TestRequeueExpiredLeases_StaleCASDoesNotReap: a worker that JUST
// renewed (write lease_expires_at to NEW) must NOT be reaped when the
// reaper observed the OLD value (audit P0#6 closure).
func TestRequeueExpiredLeases_StaleCASDoesNotReap(t *testing.T) {
	s, taskR := openTaskReaperTestDB(t)
	ctx := context.Background()

	originalExpiry := time.Now().UTC().Add(-5 * time.Minute).Format(time.RFC3339)
	seedLeasedTaskAt(t, s.db, "T-renewed", "w-1", "L-1", originalExpiry, "LEASED", 0, 1)

	// Worker renews — writes a FUTURE expiry under the same identity tuple.
	renewedExpiry := time.Now().UTC().Add(30 * time.Minute).Format(time.RFC3339)
	if _, err := s.db.ExecContext(ctx,
		`UPDATE tasks SET lease_expires_at=? WHERE task_id=?`,
		renewedExpiry, "T-renewed"); err != nil {
		t.Fatalf("simulate worker renewal: %v", err)
	}

	// Reaper SELECTs using CURRENT lease_expires_at (now FUTURE) → no candidate returned.
	nowStr := time.Now().UTC().Format(time.RFC3339)
	cands, err := taskR.RequeueExpiredLeases(ctx, nowStr, 100)
	if err != nil {
		t.Fatalf("RequeueExpiredLeases: %v", err)
	}
	if len(cands) != 0 {
		t.Errorf("after worker renewal candidates = %+v; want []", cands)
	}
}

// TestRequeueExpiredLeases_NullLeaseNeverReaped: NULL lease_expires_at
// is treated as "never expires" by the COALESCE guard. Pre-049 rows
// are never wrongly reaped.
func TestRequeueExpiredLeases_NullLeaseNeverReaped(t *testing.T) {
	s, taskR := openTaskReaperTestDB(t)
	ctx := context.Background()

	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO tasks
		 (task_id, job_id, status, priority, revision, attempt_count,
		  worker_id, lease_id, created_at, updated_at)
		 VALUES ('T-null', 'job-T-null', 'LEASED', 0, 0, 1, 'w-x', 'L-x', ?, ?)`,
		now, now); err != nil {
		t.Fatalf("seed NULL-lease task: %v", err)
	}
	nowStr := time.Now().UTC().Format(time.RFC3339)
	reaped, err := taskR.RequeueExpiredLeases(ctx, nowStr, 100)
	if err != nil {
		t.Fatalf("RequeueExpiredLeases: %v", err)
	}
	if len(reaped) != 0 {
		t.Errorf("NULL lease should never be reaped; got %+v", reaped)
	}
}

// TestRequeueExpiredLeases_NilGuard: empty nowRFC3339 → error.
func TestRequeueExpiredLeases_NilGuard(t *testing.T) {
	_, taskR := openTaskReaperTestDB(t)
	if _, err := taskR.RequeueExpiredLeases(context.Background(), "", 100); err == nil {
		t.Fatal("expected error for empty nowRFC3339")
	} else if !strings.Contains(err.Error(), "nowRFC3339") {
		t.Errorf("error should mention nowRFC3339; got %v", err)
	}
}

// TestRequeueExpiredLeases_LimitHonored: limit caps how many candidates
// are returned per call.
func TestRequeueExpiredLeases_LimitHonored(t *testing.T) {
	s, taskR := openTaskReaperTestDB(t)
	ctx := context.Background()

	past := time.Now().UTC().Add(-2 * time.Minute).Format(time.RFC3339)
	for _, id := range []string{"T-a", "T-b", "T-c"} {
		seedLeasedTaskAt(t, s.db, id, "w-"+id, "L-"+id, past, "LEASED", 0, 1)
	}

	nowStr := time.Now().UTC().Format(time.RFC3339)
	cands, err := taskR.RequeueExpiredLeases(ctx, nowStr, 2)
	if err != nil {
		t.Fatalf("RequeueExpiredLeases: %v", err)
	}
	if len(cands) != 2 {
		t.Errorf("with limit=2 across 3 expired tasks: candidates=%d; want 2", len(cands))
	}
	// All 3 tasks remain in the table (SELECT does not mutate).
	var stillLeased int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM tasks WHERE status = 'LEASED'`).Scan(&stillLeased); err != nil {
		t.Fatal(err)
	}
	if stillLeased != 3 {
		t.Errorf("expected 3 tasks still LEASED (SELECT does not mutate); got %d", stillLeased)
	}
}

// TestExpireTaskLeaseAtomic_RetriesExhaustedFailsTask: when the
// candidate's observed attempt_count already exceeds maxRetries, the
// atomic transitions Task → FAILED (terminal, reaper's responsibility)
// and clears worker/lease. The wrapper post-commits a Job aggregate
// update (untested here — lives in jobsRepo).
func TestExpireTaskLeaseAtomic_RetriesExhaustedFailsTask(t *testing.T) {
	s, taskR := openTaskReaperTestDB(t)
	ctx := context.Background()

	past := time.Now().UTC().Add(-5 * time.Minute).Format(time.RFC3339)
	// attempt_count=3 with maxRetries=2 ⇒ exhausted.
	seedLeasedTaskAt(t, s.db, "T-exhausted", "w-1", "L-1", past, "LEASED", 0, 3)
	seedRunningAttempt(t, s.db, "A-exhausted", "T-exhausted", "w-1", "L-1", 3)

	res, err := taskR.ExpireTaskLeaseAtomic(ctx,
		"T-exhausted", "L-1", past, 2)
	if err != nil {
		t.Fatalf("ExpireTaskLeaseAtomic: %v", err)
	}
	if !res.AttemptsExhausted {
		t.Errorf("AttemptsExhausted flag = false; want true (attempt_count=3 > maxRetries=2)")
	}
	if res.TaskStatus != taskgraph.StatusFailed {
		t.Errorf("TaskStatus = %s; want FAILED", res.TaskStatus)
	}
	if !res.AttemptClosed {
		t.Errorf("Attempt should be closed (TIMED_OUT)")
	}

	var status, worker, lease string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status, COALESCE(worker_id,''), COALESCE(lease_id,'')
		 FROM tasks WHERE task_id=?`,
		"T-exhausted").Scan(&status, &worker, &lease); err != nil {
		t.Fatalf("post-reap SELECT: %v", err)
	}
	if status != "FAILED" {
		t.Errorf("status = %s; want FAILED", status)
	}
	if worker != "" || lease != "" {
		t.Errorf("worker/lease should be cleared on FAILED: w=%q L=%q", worker, lease)
	}

	var aStatus string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status FROM task_attempts WHERE id=?`,
		"A-exhausted").Scan(&aStatus); err != nil {
		t.Fatalf("post-reap attempt SELECT: %v", err)
	}
	if aStatus != "TIMED_OUT" {
		t.Errorf("attempt status = %s; want TIMED_OUT", aStatus)
	}

	// Sanity: identity tuple mismatch (wrong lease_id) returns an error so
	// the supervisor surfaces a transition conflict rather than a swallowed
	// silent reap.
	if _, err := taskR.ExpireTaskLeaseAtomic(ctx,
		"T-exhausted", "L-WRONG", past, 2); err == nil {
		t.Errorf("stale lease_id should surface an error; got nil")
	}
}

func TestExpireTaskLeaseAtomic_ReconcilesAttemptCountFromActiveAttempt(t *testing.T) {
	s, taskR := openTaskReaperTestDB(t)
	ctx := context.Background()

	past := time.Now().UTC().Add(-5 * time.Minute).Format(time.RFC3339)
	// Reproduce the Jackie-Chan class of drift:
	// tasks.attempt_count = 0 while the active lease/attempt is #1.
	seedLeasedTaskAt(t, s.db, "T-drifted", "w-1", "L-1", past, "LEASED", 0, 0)
	seedRunningAttempt(t, s.db, "A-drifted-1", "T-drifted", "w-1", "L-1", 1)
	if _, err := s.db.ExecContext(ctx,
		`UPDATE tasks SET attempt_number = 1 WHERE task_id = ?`,
		"T-drifted",
	); err != nil {
		t.Fatalf("align tasks.attempt_number to active attempt: %v", err)
	}

	res, err := taskR.ExpireTaskLeaseAtomic(ctx, "T-drifted", "L-1", past, 3)
	if err != nil {
		t.Fatalf("ExpireTaskLeaseAtomic drifted-count: %v", err)
	}
	if res.TaskStatus != taskgraph.StatusReady {
		t.Fatalf("TaskStatus = %s; want READY", res.TaskStatus)
	}

	var attemptCount, attemptNumber int
	var status, worker, lease string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status, attempt_count, attempt_number, COALESCE(worker_id,''), COALESCE(lease_id,'')
		 FROM tasks WHERE task_id = ?`,
		"T-drifted",
	).Scan(&status, &attemptCount, &attemptNumber, &worker, &lease); err != nil {
		t.Fatalf("post-expire SELECT drifted task: %v", err)
	}
	if status != "READY" {
		t.Errorf("status = %s; want READY", status)
	}
	if attemptCount != 1 {
		t.Errorf("attempt_count = %d; want 1 after timing out active attempt #1", attemptCount)
	}
	if attemptNumber != 0 {
		t.Errorf("attempt_number = %d; want 0 after requeue", attemptNumber)
	}
	if worker != "" || lease != "" {
		t.Errorf("worker/lease still set after requeue: worker=%q lease=%q", worker, lease)
	}
}

// TestExpireTaskLeaseAtomic_StaleExpiryReturnsConflict: audit P0#6
// closure at the ATOMIC layer (companion to the SELECT-level test
// TestRequeueExpiredLeases_StaleCASDoesNotReap above). A worker
// that just renewed (lease_expires_at is now FUTURE) but the reaper
// still holds an OLD observed lease_expires_at from its SELECT phase
// must see ErrTransitionConflict — NOT silently reap. The atomic's
// Pre-UPDATE read verifies (lease_id, lease_expires_at, worker_id,
// status) equality against the candidate; any drift surfaces as
// an error so the supervisor logs the transition-conflict rather
// than committing a destructive FAILED-task write.
//
// Post-condition invariants:
//
//  1. ExpireTaskLeaseAtomic returns a non-nil error whose message
//     contains "lease_expires_at mismatch" (the audit's specific
//     audit-log surface for P0#6).
//  2. The error is wrapped with ErrTransitionConflict so the
//     supervisor treats it as a stale-CAS recoverable signal
//     (NOT a hard reap error).
//  3. The Task row is NOT mutated. worker_id, lease_id,
//     lease_expires_at, status, attempt_count, and revision all
//     stay at the worker-renewed values — proves the atomic
//     failed closed on the CAS gate without side effects.
func TestExpireTaskLeaseAtomic_StaleExpiryReturnsConflict(t *testing.T) {
	s, taskR := openTaskReaperTestDB(t)
	ctx := context.Background()

	// Worker-renewal state: lease_expires_at is FUTURE.
	freshExpiry := time.Now().UTC().Add(30 * time.Minute).Format(time.RFC3339)
	seedLeasedTaskAt(t, s.db, "T-renewed-after-select", "w-1", "L-1",
		freshExpiry, "LEASED", 0, 1)
	seedRunningAttempt(t, s.db, "A-renewed-after-select", "T-renewed-after-select",
		"w-1", "L-1", 1)

	// Reaper-pass observed lease_expires_at: PAST (from before the
	// renewable worker's RenewLease fire). The CAS gate must see
	// lease_expires_at mismatch and refuse to write.
	staleObservedExpiry := time.Now().UTC().Add(-5 * time.Minute).Format(time.RFC3339)

	_, err := taskR.ExpireTaskLeaseAtomic(ctx,
		"T-renewed-after-select", "L-1", staleObservedExpiry, 3)
	if err == nil {
		t.Fatal("stale observed lease_expires_at should surface ErrTransitionConflict; got nil")
	}
	if !strings.Contains(err.Error(), "lease_expires_at mismatch") {
		t.Errorf("error should mention lease_expires_at mismatch (P0#6 audit surface); got %q",
			err.Error())
	}
	if !errors.Is(err, taskgraph.ErrTransitionConflict) {
		t.Errorf("error should wrap taskgraph.ErrTransitionConflict; got %v", err)
	}

	// Post-condition: Task row NOT mutated. Status stays LEASED,
	// worker_id + lease_id stay on the worker-renewed tuple,
	// lease_expires_at stays at the FRESH (worker-renewed) value.
	var status, worker, leaseID, leaseExp string
	var rev, attempts int
	if err := s.db.QueryRowContext(ctx,
		`SELECT status, COALESCE(worker_id,''), COALESCE(lease_id,''),
		        COALESCE(lease_expires_at,''), revision, attempt_count
		 FROM tasks WHERE task_id=?`,
		"T-renewed-after-select").Scan(&status, &worker, &leaseID, &leaseExp, &rev, &attempts); err != nil {
		t.Fatalf("post-conflict SELECT: %v", err)
	}
	if status != "LEASED" {
		t.Errorf("status drifted on stale-CAS: got %s; want LEASED", status)
	}
	if worker != "w-1" {
		t.Errorf("worker_id drifted on stale-CAS: got %q; want w-1", worker)
	}
	if leaseID != "L-1" {
		t.Errorf("lease_id drifted on stale-CAS: got %q; want L-1", leaseID)
	}
	if leaseExp != freshExpiry {
		t.Errorf("lease_expires_at drifted on stale-CAS: got %q; want %q (worker-renewed value)",
			leaseExp, freshExpiry)
	}
	if rev != 0 || attempts != 1 {
		t.Errorf("revision/attempt_count drifted on stale-CAS: rev=%d attempts=%d; want 0/1",
			rev, attempts)
	}

	// Attempt row also untouched — the §9.5 invariant protects
	// against a Task reap committing while the matching attempt
	// row stays RUNNING (correction: here the atomic bails on the
	// Task CAS-gate first, so neither row mutates).
	var aStatus string
	if err := s.db.QueryRowContext(ctx,
		`SELECT status FROM task_attempts WHERE id=?`,
		"A-renewed-after-select").Scan(&aStatus); err != nil {
		t.Fatalf("post-conflict attempt SELECT: %v", err)
	}
	if aStatus != "RUNNING" {
		t.Errorf("attempt status drifted on stale-CAS: got %s; want RUNNING", aStatus)
	}
}
