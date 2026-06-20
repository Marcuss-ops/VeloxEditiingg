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
	if _, err := db.Exec(`
		CREATE TABLE jobs (
			job_id TEXT PRIMARY KEY,
			status TEXT NOT NULL,
			assigned_to TEXT,
			lease_id TEXT,
			lease_expiry TEXT,
			revision INTEGER NOT NULL DEFAULT 0,
			retry_count INTEGER NOT NULL DEFAULT 0,
			max_retries INTEGER NOT NULL DEFAULT 0,
			attempt INTEGER,
			started_at TEXT,
			updated_at TEXT,
			video_name TEXT,
			project_id TEXT,
			created_at TEXT,
			completed_at TEXT
		)
	`); err != nil {
		t.Fatalf("create jobs table: %v", err)
	}
	s := &SQLiteStore{db: db}
	r := NewSQLiteJobRepository(s)
	return s, r
}

// seedLeasedJob inserts a row in LEASED status with the supplied identity.
// Returns the revision assigned at insert time (always 0 for fresh rows).
func seedLeasedJob(t *testing.T, db *sql.DB,
	jobID, workerID, leaseID string, attempt int, revision int,
) int {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	// Set revision via ON CONFLICT semantics so tests can pick the starting value.
	res, err := db.Exec(
		`INSERT INTO jobs
		    (job_id, status, assigned_to, lease_id, lease_expiry, revision, attempt, created_at, updated_at)
		 VALUES (?, 'LEASED', ?, ?, ?, ?, ?, ?, ?)`,
		jobID, workerID, leaseID,
		time.Now().UTC().Add(30*time.Minute).Format(time.RFC3339),
		revision, attempt, now, now,
	)
	if err != nil {
		t.Fatalf("seed LEASED job: %v", err)
	}
	_ = res
	return revision
}

func TestSQLiteJobRepository_StartJob_HappyPath(t *testing.T) {
	_, r := openStartJobTestDB(t)
	ctx := context.Background()

	sess := r.store.db
	seedLeasedJob(t, sess, "job-1", "w1", "lease-A", 1, 7)

	err := r.StartJob(ctx, StartJobParams{
		JobID:            "job-1",
		WorkerID:         "w1",
		LeaseID:          "lease-A",
		Attempt:          1,
		ExpectedRevision: 7,
	})
	if err != nil {
		t.Fatalf("StartJob happy path: %v", err)
	}

	// Verify state flipped + revision bumped + started_at set.
	var status, startedAt string
	var newRevision int
	if err := sess.QueryRow(
		`SELECT status, started_at, revision FROM jobs WHERE job_id = ?`,
		"job-1",
	).Scan(&status, &startedAt, &newRevision); err != nil {
		t.Fatalf("post-StartJob SELECT: %v", err)
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

func TestSQLiteJobRepository_StartJob_WrongLeaseID(t *testing.T) {
	_, r := openStartJobTestDB(t)
	ctx := context.Background()
	sess := r.store.db
	seedLeasedJob(t, sess, "job-1", "w1", "lease-A", 1, 0)

	err := r.StartJob(ctx, StartJobParams{
		JobID:            "job-1",
		WorkerID:         "w1",
		LeaseID:          "lease-DIFFERENT",
		Attempt:          1,
		ExpectedRevision: 0,
	})
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("expected ErrTransitionConflict, got %v", err)
	}
	// Job must still be in LEASED.
	var status string
	_ = sess.QueryRow(`SELECT status FROM jobs WHERE job_id = 'job-1'`).Scan(&status)
	if status != "LEASED" {
		t.Errorf("expected status unchanged LEASED, got %q", status)
	}
}

func TestSQLiteJobRepository_StartJob_WrongWorkerID(t *testing.T) {
	_, r := openStartJobTestDB(t)
	ctx := context.Background()
	sess := r.store.db
	seedLeasedJob(t, sess, "job-1", "w1", "lease-A", 1, 0)

	err := r.StartJob(ctx, StartJobParams{
		JobID:            "job-1",
		WorkerID:         "w2-impostor",
		LeaseID:          "lease-A",
		Attempt:          1,
		ExpectedRevision: 0,
	})
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("expected ErrTransitionConflict, got %v", err)
	}
}

func TestSQLiteJobRepository_StartJob_WrongAttempt(t *testing.T) {
	_, r := openStartJobTestDB(t)
	ctx := context.Background()
	sess := r.store.db
	seedLeasedJob(t, sess, "job-1", "w1", "lease-A", 2, 0)

	err := r.StartJob(ctx, StartJobParams{
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

func TestSQLiteJobRepository_StartJob_WrongRevision(t *testing.T) {
	_, r := openStartJobTestDB(t)
	ctx := context.Background()
	sess := r.store.db
	seedLeasedJob(t, sess, "job-1", "w1", "lease-A", 1, 3)

	err := r.StartJob(ctx, StartJobParams{
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

func TestSQLiteJobRepository_StartJob_AlreadyRunning(t *testing.T) {
	_, r := openStartJobTestDB(t)
	ctx := context.Background()
	sess := r.store.db
	// Seed directly in RUNNING (rare race: reaper already promoted).
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := sess.Exec(
		`INSERT INTO jobs (job_id, status, assigned_to, lease_id, lease_expiry, revision, attempt, created_at, updated_at, started_at)
		 VALUES ('job-1', 'RUNNING', 'w1', 'lease-A', ?, 0, 1, ?, ?, ?)`,
		time.Now().UTC().Add(30*time.Minute).Format(time.RFC3339),
		now, now, now,
	)
	if err != nil {
		t.Fatalf("seed RUNNING: %v", err)
	}

	err = r.StartJob(ctx, StartJobParams{
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

func TestSQLiteJobRepository_StartJob_NullAttemptLegacy(t *testing.T) {
	_, r := openStartJobTestDB(t)
	ctx := context.Background()
	sess := r.store.db
	// Seed a legacy row with attempt IS NULL (older scheme that pre-dates
	// explicit attempt tracking). COALESCE(attempt, 0) = 0 should match.
	_, err := sess.Exec(
		`INSERT INTO jobs (job_id, status, assigned_to, lease_id, lease_expiry, revision, attempt, created_at, updated_at)
		 VALUES ('job-legacy', 'LEASED', 'w1', 'lease-A', ?, 0, NULL, ?, ?)`,
		time.Now().UTC().Add(30*time.Minute).Format(time.RFC3339),
		time.Now().UTC().Format(time.RFC3339),
		time.Now().UTC().Format(time.RFC3339),
	)
	if err != nil {
		t.Fatalf("seed legacy NULL attempt: %v", err)
	}

	err = r.StartJob(ctx, StartJobParams{
		JobID:            "job-legacy",
		WorkerID:         "w1",
		LeaseID:          "lease-A",
		Attempt:          0, // COALESCE makes NULL = 0 work
		ExpectedRevision: 0,
	})
	if err != nil {
		t.Fatalf("StartJob on NULL-attempt legacy row should succeed with attempt=0, got %v", err)
	}
}
