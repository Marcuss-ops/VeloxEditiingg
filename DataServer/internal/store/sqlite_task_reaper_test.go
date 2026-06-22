package store

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// =====================================================================
// PR-05 / audit §P0.4: master-side task lease reaper tests.
//
// ClaimNextReadyTask writes `lease_expires_at = now + 30min` on every
// READY → LEASED transition (see PR-05 in
// `internal/store/sqlite_task_repository.go::ClaimNextReadyTask`). The
// RequeueExpiredLeases here sweeps tasks whose lease has expired
// without a final TaskResult (worker-crash recovery) and requeues them
// per audit §P0.4:
//
//   - LEASED  expired → READY (re-claimable).
//   - RUNNING expired → READY with attempt_count bumped.
//
// The tests below use a minimal schema mirroring the columns
// RequeueExpiredLeases actually touches. NO migration is required —
// we are validating the repository method in isolation, just like
// success_path_test.go does for FinalizeVerified.
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
	worker_id          TEXT,
	lease_id           TEXT,
	lease_expires_at   TEXT,
	ready_at           TEXT,
	started_at         TEXT,
	completed_at       TEXT,
	created_at         TEXT,
	updated_at         TEXT
);
`

func openTaskReaperTestDB(t *testing.T) (*SQLiteStore, *SQLiteTaskRepository) {
	t.Helper()
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("open sqlite (task reaper): %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(taskReaperSchema); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	s := &SQLiteStore{db: db}
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
		 (task_id, job_id, status, priority, revision, attempt_count,
		  worker_id, lease_id, lease_expires_at, created_at, updated_at)
		 VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?)`,
		taskID, "job-"+taskID, status, revision, attemptCount,
		workerID, leaseID, leaseExpiresAt, now, now,
	); err != nil {
		t.Fatalf("seed LEASED task %q: %v", taskID, err)
	}
}

// TestRequeueExpiredLeases_HappyPath: one LEASED task with an expired
// lease is reaped (status flips to READY, worker/lease cleared,
// attempt_count bumped, revision bumped). The freshly-claimed task
// keeps its lease untouched.
func TestRequeueExpiredLeases_HappyPath(t *testing.T) {
	s, r := openTaskReaperTestDB(t)
	ctx := context.Background()

	// Expired LEASED: 5 minutes ago.
	past := time.Now().UTC().Add(-5 * time.Minute).Format(time.RFC3339)
	seedLeasedTaskAt(t, s.db, "T-expired-leased", "w-1", "L-1", past, "LEASED",
		0, 1)

	// Fresh LEASED: 30 minutes in the future (still valid).
	future := time.Now().UTC().Add(30 * time.Minute).Format(time.RFC3339)
	seedLeasedTaskAt(t, s.db, "T-fresh-leased", "w-2", "L-2", future, "LEASED",
		0, 1)

	nowStr := time.Now().UTC().Format(time.RFC3339)
	reaped, err := r.RequeueExpiredLeases(ctx, nowStr, 100)
	if err != nil {
		t.Fatalf("RequeueExpiredLeases: %v", err)
	}
	if len(reaped) != 1 || reaped[0] != "T-expired-leased" {
		t.Errorf("reaped = %v; want [T-expired-leased]", reaped)
	}

	// Expired LEASED → READY: worker, lease cleared, attempt +1, rev +1.
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
	if attempts != 2 {
		t.Errorf("attempt_count = %d; want 2 (1 → 2)", attempts)
	}
	if rev != 1 {
		t.Errorf("revision = %d; want 1 (0 → 1)", rev)
	}

	// Fresh LEASED → unchanged.
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

// TestRequeueExpiredLeases_RunningExpiredIsReapedToReady: a RUNNING
// task (worker is still alive server-side but no result yet, e.g. we
// have reason to suspect client death) is requeued too. attempt_count
// is bumped.
func TestRequeueExpiredLeases_RunningExpiredIsReapedToReady(t *testing.T) {
	s, r := openTaskReaperTestDB(t)
	ctx := context.Background()

	past := time.Now().UTC().Add(-1 * time.Minute).Format(time.RFC3339)
	seedLeasedTaskAt(t, s.db, "T-running", "w-3", "L-3", past, "RUNNING",
		2, 1)

	nowStr := time.Now().UTC().Format(time.RFC3339)
	reaped, err := r.RequeueExpiredLeases(ctx, nowStr, 100)
	if err != nil {
		t.Fatalf("RequeueExpiredLeases: %v", err)
	}
	if len(reaped) != 1 || reaped[0] != "T-running" {
		t.Errorf("reaped = %v; want [T-running]", reaped)
	}

	var status string
	var attempts int
	if err := s.db.QueryRowContext(ctx,
		`SELECT status, attempt_count FROM tasks WHERE task_id='T-running'`,
	).Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "READY" {
		t.Errorf("RUNNING expired should be reaped to READY; got %s", status)
	}
	if attempts != 2 {
		t.Errorf("attempt_count = %d; want 2 (1 → 2)", attempts)
	}
}

// TestRequeueExpiredLeases_NullLeaseNeverReaped: a task with NULL
// `lease_expires_at` is treated as "never expires" by the COALESCE
// guard. Pre-migration-049 rows are not at risk of being wrongly
// reaped by the new reaper.
func TestRequeueExpiredLeases_NullLeaseNeverReaped(t *testing.T) {
	s, r := openTaskReaperTestDB(t)
	ctx := context.Background()

	// Seed LEASED with NULL lease_expires_at (simulate pre-PR-05 row).
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
	reaped, err := r.RequeueExpiredLeases(ctx, nowStr, 100)
	if err != nil {
		t.Fatalf("RequeueExpiredLeases: %v", err)
	}
	if len(reaped) != 0 {
		t.Errorf("NULL lease should never be reaped; got %v", reaped)
	}
}

// TestRequeueExpiredLeases_NilGuard: empty nowRFC3339 → error.
func TestRequeueExpiredLeases_NilGuard(t *testing.T) {
	_, r := openTaskReaperTestDB(t)
	if _, err := r.RequeueExpiredLeases(context.Background(), "", 100); err == nil {
		t.Fatal("expected error for empty nowRFC3339")
	} else if !strings.Contains(err.Error(), "nowRFC3339") {
		t.Errorf("error should mention nowRFC3339; got %v", err)
	}
}

// TestRequeueExpiredLeases_LimitHonored: limit caps how many tasks are
// scanned. With 3 expired tasks and limit=2, only 2 should be reaped
// in this call (the third remains for the next tick).
func TestRequeueExpiredLeases_LimitHonored(t *testing.T) {
	s, r := openTaskReaperTestDB(t)
	ctx := context.Background()

	past := time.Now().UTC().Add(-2 * time.Minute).Format(time.RFC3339)
	for _, id := range []string{"T-a", "T-b", "T-c"} {
		seedLeasedTaskAt(t, s.db, id, "w-"+id, "L-"+id, past, "LEASED",
			0, 1)
	}

	nowStr := time.Now().UTC().Format(time.RFC3339)
	reaped, err := r.RequeueExpiredLeases(ctx, nowStr, 2)
	if err != nil {
		t.Fatalf("RequeueExpiredLeases: %v", err)
	}
	if len(reaped) != 2 {
		t.Errorf("with limit=2 across 3 expired tasks: reaped=%d; want 2", len(reaped))
	}

	// The remaining task still LEASED.
	var stillLeased int
	if err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM tasks WHERE status = 'LEASED'`,
	).Scan(&stillLeased); err != nil {
		t.Fatal(err)
	}
	if stillLeased != 1 {
		t.Errorf("expected 1 task still LEASED; got %d", stillLeased)
	}
}
