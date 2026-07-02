package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// openTransitionTestDB creates a minimal in-memory SQLite with the
// `jobs` table only. The canonical TransitionJobStatus writes the
// single row (status, revision, updated_at); it does NOT touch
// job_history or job_events, so they're intentionally absent here.
//
// PR #9 + #10: assigned_to, lease_id, lease_expiry, retry_count,
// claimed_by, attempt columns were dropped from jobs by migration
// 048. Identity (worker_id, lease_id) lives in job_attempts +
// tasks now; the jobs table only carries status + revision + audit
// timestamps at this layer.
func openTransitionTestDB(t *testing.T) (*SQLiteStore, *SQLiteJobRepository) {
	t.Helper()
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("open in-memory db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec(`
		CREATE TABLE jobs (
			job_id TEXT PRIMARY KEY,
			status TEXT NOT NULL,
			revision INTEGER NOT NULL DEFAULT 0,
			max_retries INTEGER NOT NULL DEFAULT 0,
			started_at TEXT,
			updated_at TEXT,
			video_name TEXT,
			project_id TEXT,
			created_at TEXT,
			completed_at TEXT
		);
	`); err != nil {
		t.Fatalf("create jobs schema: %v", err)
	}
	s := &SQLiteStore{db: db}
	r := NewSQLiteJobRepository(s)
	return s, r
}

// seedJob inserts a job row in the supplied status with the supplied
// revision. Other columns are zeroed. Returns the post-seed db handle
// so tests can issue follow-up SQL.
func seedJob(t *testing.T, db *sql.DB, jobID, status string, revision int) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec(
		`INSERT INTO jobs (job_id, status, revision, max_retries, created_at, updated_at)
		 VALUES (?, ?, ?, 3, ?, ?)`,
		jobID, status, revision, now, now,
	); err != nil {
		t.Fatalf("seed %s/%s/rev=%d: %v", jobID, status, revision, err)
	}
}

// readJobStatus reads {status, revision} after a Transition so the
// test can assert post-state atomically.
func readJobStatus(t *testing.T, db *sql.DB, jobID string) (string, int) {
	t.Helper()
	var (
		status   string
		revision int
	)
	if err := db.QueryRow(
		`SELECT status, revision FROM jobs WHERE job_id = ?`,
		jobID,
	).Scan(&status, &revision); err != nil {
		t.Fatalf("readJobStatus SELECT: %v", err)
	}
	return status, revision
}

// TestTransition_LeasedToRunning_HappyPath locks the canonical
// LEASED→RUNNING promotion: Transition with matching CAS pre-state
// MUST commit atomically and bump revision by 1.
func TestTransition_LeasedToRunning_HappyPath(t *testing.T) {
	_, repo := openTransitionTestDB(t)
	ctx := context.Background()

	seedJob(t, repo.store.db, "job-1", "LEASED", 7)

	if err := repo.Transition(ctx, TransitionParams{
		JobID:          "job-1",
		ExpectedStatus: JobStatusLeased,
		NewStatus:      JobStatusRunning,
		Revision:       7,
	}); err != nil {
		t.Fatalf("Transition happy path: %v", err)
	}

	status, rev := readJobStatus(t, repo.store.db, "job-1")
	if status != "RUNNING" {
		t.Errorf("expected status RUNNING, got %q", status)
	}
	if rev != 8 {
		t.Errorf("expected revision 8 (7+1), got %d", rev)
	}
}

// TestTransition_StaleRevisionConflict pins the canonical optimistic
// locking: Transition with a stale revision MUST return
// ErrTransitionConflict and leave the row unchanged (no half-state).
func TestTransition_StaleRevisionConflict(t *testing.T) {
	_, repo := openTransitionTestDB(t)
	ctx := context.Background()

	seedJob(t, repo.store.db, "job-1", "LEASED", 3)

	err := repo.Transition(ctx, TransitionParams{
		JobID:          "job-1",
		ExpectedStatus: JobStatusLeased,
		NewStatus:      JobStatusRunning,
		Revision:       2, // stale: real rev = 3
	})
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("expected ErrTransitionConflict for stale revision, got %v", err)
	}

	status, rev := readJobStatus(t, repo.store.db, "job-1")
	if status != "LEASED" {
		t.Errorf("post-conflict status: got %q, want LEASED (no half-state)", status)
	}
	if rev != 3 {
		t.Errorf("post-conflict revision: got %d, want 3 (no half-state)", rev)
	}
}

// TestTransition_WrongExpectedStatus pins the ExpectedStatus branch
// of the CAS: a Transition that uses the WRONG expected status
// (RUNNING when the row is LEASED) MUST return ErrTransitionConflict.
// Companion to StaleRevision — covers the second branch of the
// UPDATE WHERE clause.
func TestTransition_WrongExpectedStatus(t *testing.T) {
	_, repo := openTransitionTestDB(t)
	ctx := context.Background()

	seedJob(t, repo.store.db, "job-1", "RUNNING", 0)

	err := repo.Transition(ctx, TransitionParams{
		JobID:          "job-1",
		ExpectedStatus: JobStatusPending, // wrong
		NewStatus:      JobStatusRunning,
		Revision:       0,
	})
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("expected ErrTransitionConflict for wrong ExpectedStatus, got %v", err)
	}
}

// TestTransition_AlreadyRunning locks the idempotency semantic for
// the canonical Transition API: a LEASED→RUNNING Transition against
// a row already in RUNNING MUST return ErrTransitionConflict (because
// the WHERE Status=LEASED doesn't match). This subsumes the
// pre-fix/remove-job-lease-ops PR3Start_AlreadyRunning test.
func TestTransition_AlreadyRunning(t *testing.T) {
	_, repo := openTransitionTestDB(t)
	ctx := context.Background()

	seedJob(t, repo.store.db, "job-1", "RUNNING", 0)

	err := repo.Transition(ctx, TransitionParams{
		JobID:          "job-1",
		ExpectedStatus: JobStatusLeased,
		NewStatus:      JobStatusRunning,
		Revision:       0,
	})
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("expected ErrTransitionConflict for already-running, got %v", err)
	}

	status, _ := readJobStatus(t, repo.store.db, "job-1")
	if status != "RUNNING" {
		t.Errorf("post-conflict status: got %q, want RUNNING (unchanged)", status)
	}
}

// TestTransition_NonexistentJob pins the "no row matches" branch:
// a Transition against a JobID that has no row MUST return
// ErrTransitionConflict (RowsAffected == 0). There's no "not found
// is fine" path in canonical Transition — it always reports
// conflict so callers can retry / reconcile uniformly.
func TestTransition_NonexistentJob(t *testing.T) {
	_, repo := openTransitionTestDB(t)
	ctx := context.Background()

	err := repo.Transition(ctx, TransitionParams{
		JobID:          "nonexistent",
		ExpectedStatus: JobStatusLeased,
		NewStatus:      JobStatusRunning,
		Revision:       0,
	})
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("expected ErrTransitionConflict for missing job, got %v", err)
	}
}

// TestTransition_EmptyJobID pins the explicit empty-string guard in
// repo.Transition (which is checked BEFORE the SQL UPDATE — so it
// returns a non-nil error without round-tripping to SQLite).
func TestTransition_EmptyJobID(t *testing.T) {
	_, repo := openTransitionTestDB(t)
	ctx := context.Background()

	err := repo.Transition(ctx, TransitionParams{
		JobID:          "",
		ExpectedStatus: JobStatusPending,
		NewStatus:      JobStatusRunning,
		Revision:       0,
	})
	if err == nil {
		t.Fatal("expected error for empty JobID, got nil")
	}
	// Not necessarily ErrTransitionConflict — Transition early-exits with
	// a wrapped sentinel before SQL. We only need non-nil.
}
