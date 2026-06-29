package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// openStartJobTestDB creates an in-memory SQLite with the schema SQLiteJobRepository
// needs (jobs table only; we test the LEASED→RUNNING transition in isolation
// from other tables). Returns the raw DB + the wrapped store.
func openStartJobTestDB(t *testing.T) (*SQLiteStore, *SQLiteJobRepository) {
	t.Helper()
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("open in-memory db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// PR #9 + #10: assigned_to, lease_id, lease_expiry, retry_count,
	// claimed_by columns dropped from jobs table (migration 048).
	if _, err := db.Exec(`
		CREATE TABLE jobs (
			job_id TEXT PRIMARY KEY,
			status TEXT NOT NULL,
			revision INTEGER NOT NULL DEFAULT 0,
			max_retries INTEGER NOT NULL DEFAULT 0,
			attempt INTEGER,
			started_at TEXT,
			updated_at TEXT,
			video_name TEXT,
			project_id TEXT,
			created_at TEXT,
			completed_at TEXT
		);
		CREATE TABLE job_history (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			job_id TEXT NOT NULL,
			status TEXT,
			worker_id TEXT,
			message TEXT,
			raw_json TEXT,
			event_ts TEXT NOT NULL
		);
		CREATE TABLE job_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp TEXT NOT NULL,
			job_id TEXT NOT NULL,
			event TEXT NOT NULL,
			raw_json TEXT NOT NULL DEFAULT '{}'
		)
	`); err != nil {
		t.Fatalf("create jobs table: %v", err)
	}
	s := &SQLiteStore{db: db}
	r := NewSQLiteJobRepository(s)
	return s, r
}

// seedLeasedJob inserts a row in LEASED status with the supplied identity.
// PR #9: assigned_to, lease_id columns dropped — identity lives in job_attempts + tasks.
// Returns the revision assigned at insert time.
func seedLeasedJob(t *testing.T, db *sql.DB,
	jobID string, attempt int, revision int,
) int {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := db.Exec(
		`INSERT INTO jobs
		    (job_id, status, revision, attempt, created_at, updated_at)
		 VALUES (?, 'LEASED', ?, ?, ?, ?)`,
		jobID, revision, attempt, now, now,
	)
	if err != nil {
		t.Fatalf("seed LEASED job: %v", err)
	}
	_ = res
	return revision
}

func TestSQLiteJobRepository_PR3Start_HappyPath(t *testing.T) {
	_, r := openStartJobTestDB(t)
	ctx := context.Background()

	sess := r.store.db
	seedLeasedJob(t, sess, "job-1", 1, 7)

	err := r.PR3Start(ctx, StartCommand{
		JobID:            "job-1",
		WorkerID:         "w1",
		LeaseID:          "lease-A",
		Attempt:          1,
		ExpectedRevision: 7,
	})
	if err != nil {
		t.Fatalf("PR3Start happy path: %v", err)
	}

	// Verify state flipped + revision bumped + started_at set.
	var status, startedAt string
	var newRevision int
	if err := sess.QueryRow(
		`SELECT status, started_at, revision FROM jobs WHERE job_id = ?`,
		"job-1",
	).Scan(&status, &startedAt, &newRevision); err != nil {
		t.Fatalf("post-PR3Start SELECT: %v", err)
	}
	if status != "RUNNING" {
		t.Errorf("expected status RUNNING, got %q", status)
	}
	if newRevision != 8 {
		t.Errorf("expected revision 8, got %d", newRevision)
	}
	if startedAt == "" {
		t.Errorf("expected started_at to be set")
	}
}

// PR #9 + #10: WorkerID/LeaseID CAS checks removed from jobs table (identity
// lives in job_attempts + tasks). PR3Start only checks status='LEASED' +
// attempt + revision. WrongWorkerID + WrongLeaseID tests are no longer
// meaningful (they'd succeed because the WHERE no longer filters on them).

func TestSQLiteJobRepository_PR3Start_WrongAttempt(t *testing.T) {
	_, r := openStartJobTestDB(t)
	ctx := context.Background()
	sess := r.store.db
	seedLeasedJob(t, sess, "job-1", 2, 0)

	err := r.PR3Start(ctx, StartCommand{
		JobID:            "job-1",
		WorkerID:         "w1",
		LeaseID:          "lease-A",
		Attempt:          1, // stale guess
		ExpectedRevision: 0,
	})
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("expected ErrTransitionConflict, got %v", err)
	}
}

func TestSQLiteJobRepository_PR3Start_WrongRevision(t *testing.T) {
	_, r := openStartJobTestDB(t)
	ctx := context.Background()
	sess := r.store.db
	seedLeasedJob(t, sess, "job-1", 1, 3)

	err := r.PR3Start(ctx, StartCommand{
		JobID:            "job-1",
		WorkerID:         "w1",
		LeaseID:          "lease-A",
		Attempt:          1,
		ExpectedRevision: 2, // stale
	})
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("expected ErrTransitionConflict, got %v", err)
	}
}

func TestSQLiteJobRepository_PR3Start_AlreadyRunning(t *testing.T) {
	_, r := openStartJobTestDB(t)
	ctx := context.Background()
	sess := r.store.db
	// Seed directly in RUNNING (rare race: reaper already promoted).
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := sess.Exec(
		`INSERT INTO jobs (job_id, status, revision, attempt, created_at, updated_at, started_at)
		 VALUES ('job-1', 'RUNNING', 0, 1, ?, ?, ?)`,
		now, now, now,
	)
	if err != nil {
		t.Fatalf("seed RUNNING: %v", err)
	}

	err = r.PR3Start(ctx, StartCommand{
		JobID:            "job-1",
		WorkerID:         "w1",
		LeaseID:          "lease-A",
		Attempt:          1,
		ExpectedRevision: 0,
	})
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("expected ErrTransitionConflict, got %v", err)
	}
}

func TestSQLiteJobRepository_PR3Start_NullAttemptLegacy(t *testing.T) {
	_, r := openStartJobTestDB(t)
	ctx := context.Background()
	sess := r.store.db
	// Seed a legacy row with attempt IS NULL (older scheme that pre-dates
	// explicit attempt tracking). COALESCE(attempt, 0) = 0 should match.
	_, err := sess.Exec(
		`INSERT INTO jobs (job_id, status, revision, attempt, created_at, updated_at)
		 VALUES ('job-legacy', 'LEASED', 0, NULL, ?, ?)`,
		time.Now().UTC().Format(time.RFC3339),
		time.Now().UTC().Format(time.RFC3339),
	)
	if err != nil {
		t.Fatalf("seed legacy NULL attempt: %v", err)
	}

	err = r.PR3Start(ctx, StartCommand{
		JobID:            "job-legacy",
		WorkerID:         "w1",
		LeaseID:          "lease-A",
		Attempt:          0, // COALESCE makes NULL = 0 work
		ExpectedRevision: 0,
	})
	if err != nil {
		t.Fatalf("PR3Start on NULL-attempt legacy row should succeed with attempt=0, got %v", err)
	}
}
