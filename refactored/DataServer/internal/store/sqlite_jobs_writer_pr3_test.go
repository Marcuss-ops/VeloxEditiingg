package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// ── Test helpers ──────────────────────────────────────────────────────────

// openPR3TestDB creates an in-memory SQLite with all tables needed by
// PR3RecordRenderFinished: jobs, job_history, job_events.
func openPR3TestDB(t *testing.T) (*SQLiteStore, *SQLiteJobRepository, *sql.DB) {
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
			revision INTEGER,
			retry_count INTEGER NOT NULL DEFAULT 0,
			max_retries INTEGER NOT NULL DEFAULT 0,
			attempt INTEGER,
			started_at TEXT,
			updated_at TEXT,
			video_name TEXT,
			project_id TEXT,
			created_at TEXT,
			completed_at TEXT,
			claimed_by TEXT,
			claimed_at TEXT,
			assigned_at TEXT,
			last_error TEXT,
			error_message TEXT,
			failed_at TEXT,
			failed_by TEXT,
			processing_at TEXT,
			last_upload_attempt_at TEXT,
			last_drive_upload_result TEXT,
			remote_status TEXT,
			job_fingerprint TEXT,
			submitted_via TEXT,
			last_activity TEXT,
			run_id TEXT,
			job_run_id TEXT,
			logs_updated_at TEXT,
			slot_data TEXT,
			request_json TEXT,
			result_json TEXT,
			worker_name TEXT
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
		);
		CREATE TABLE job_attempts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			job_id TEXT NOT NULL,
			attempt INTEGER NOT NULL DEFAULT 0,
			status TEXT NOT NULL DEFAULT 'pending',
			started_at TEXT,
			finished_at TEXT,
			error_code TEXT,
			error_message TEXT,
			worker_id TEXT,
			lease_id TEXT,
			created_at TEXT
		);
		CREATE TABLE outbox_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			aggregate_type TEXT NOT NULL,
			aggregate_id TEXT NOT NULL,
			event_type TEXT NOT NULL,
			payload_json TEXT,
			status TEXT NOT NULL DEFAULT 'PENDING',
			available_at TEXT,
			created_at TEXT NOT NULL,
			locked_by TEXT
		);
	`); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	s := &SQLiteStore{db: db}
	r := NewSQLiteJobRepository(s)
	return s, r, db
}

// seedRunningJob inserts a job in RUNNING status with full identity.
func seedRunningJob(t *testing.T, db *sql.DB, jobID, workerID, leaseID string, attempt, revision int) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	// Use NULL for revision when revision < 0 (tests NULL handling).
	var revArg interface{} = revision
	if revision < 0 {
		revArg = nil
	}
	_, err := db.Exec(
		`INSERT INTO jobs
		    (job_id, status, assigned_to, lease_id, lease_expiry, revision, attempt, retry_count,
		     max_retries, started_at, created_at, updated_at)
		 VALUES (?, 'RUNNING', ?, ?, ?, ?, ?, 0, 3, ?, ?, ?)`,
		jobID, workerID, leaseID,
		time.Now().UTC().Add(30*time.Minute).Format(time.RFC3339),
		revArg, attempt,
		now, now, now,
	)
	if err != nil {
		t.Fatalf("seed RUNNING job %s: %v", jobID, err)
	}
}

// assertJobStatus verifies the job is in the expected state.
func assertJobStatus(t *testing.T, db *sql.DB, jobID, wantStatus string) int {
	t.Helper()
	var status string
	var revision int
	if err := db.QueryRow(
		`SELECT UPPER(COALESCE(status,'')), COALESCE(revision, 0) FROM jobs WHERE job_id = ?`,
		jobID,
	).Scan(&status, &revision); err != nil {
		t.Fatalf("assertJobStatus SELECT: %v", err)
	}
	if status != wantStatus {
		t.Errorf("expected status %q, got %q", wantStatus, status)
	}
	return revision
}

// countHistoryAndEvents returns (historyCount, eventsCount) for a job.
func countHistoryAndEvents(t *testing.T, db *sql.DB, jobID string) (int, int) {
	t.Helper()
	var hc, ec int
	if err := db.QueryRow(`SELECT COUNT(*) FROM job_history WHERE job_id = ?`, jobID).Scan(&hc); err != nil {
		t.Fatalf("count history: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM job_events WHERE job_id = ?`, jobID).Scan(&ec); err != nil {
		t.Fatalf("count events: %v", err)
	}
	return hc, ec
}

// makeRenderCmd builds a RecordRenderFinishedCommand with defaults.
func makeRenderCmd(jobID, workerID, leaseID string, attempt int, finishedAt time.Time) RecordRenderFinishedCommand {
	if finishedAt.IsZero() {
		finishedAt = time.Now().UTC()
	}
	return RecordRenderFinishedCommand{
		JobID:         jobID,
		WorkerID:      workerID,
		LeaseID:       leaseID,
		AttemptNumber: attempt,
		FinishedAt:    finishedAt,
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// PR3RecordRenderFinished tests
// ═══════════════════════════════════════════════════════════════════════════

func TestPR3RecordRenderFinished_HappyPath(t *testing.T) {
	_, repo, db := openPR3TestDB(t)
	ctx := context.Background()

	seedRunningJob(t, db, "job-rf-1", "w1", "lease-A", 1, 5)
	cmd := makeRenderCmd("job-rf-1", "w1", "lease-A", 1, time.Time{})

	if err := repo.PR3RecordRenderFinished(ctx, cmd); err != nil {
		t.Fatalf("PR3RecordRenderFinished happy path: %v", err)
	}

	// Verify status transitioned.
	rev := assertJobStatus(t, db, "job-rf-1", "RENDER_FINISHED")
	if rev != 6 {
		t.Errorf("expected revision 6 (5+1), got %d", rev)
	}

	// Verify history + event were inserted.
	hc, ec := countHistoryAndEvents(t, db, "job-rf-1")
	if hc == 0 {
		t.Error("expected at least 1 job_history row")
	}
	if ec == 0 {
		t.Error("expected at least 1 job_events row")
	}
}

func TestPR3RecordRenderFinished_Idempotent_AlreadyRenderFinished(t *testing.T) {
	_, repo, db := openPR3TestDB(t)
	ctx := context.Background()

	// Seed directly in RENDER_FINISHED.
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := db.Exec(
		`INSERT INTO jobs (job_id, status, assigned_to, lease_id, lease_expiry, revision, attempt, started_at, created_at, updated_at)
		 VALUES ('job-idem', 'RENDER_FINISHED', 'w1', 'lease-A', ?, 3, 1, ?, ?, ?)`,
		time.Now().UTC().Add(30*time.Minute).Format(time.RFC3339), now, now, now,
	)
	if err != nil {
		t.Fatalf("seed RENDER_FINISHED: %v", err)
	}

	cmd := makeRenderCmd("job-idem", "w1", "lease-A", 1, time.Time{})
	if err := repo.PR3RecordRenderFinished(ctx, cmd); err != nil {
		t.Fatalf("PR3RecordRenderFinished on already RENDER_FINISHED should be no-op, got: %v", err)
	}

	// Status unchanged.
	assertJobStatus(t, db, "job-idem", "RENDER_FINISHED")

	// No new history or event rows (the no-op committed, but did not INSERT).
	hc, ec := countHistoryAndEvents(t, db, "job-idem")
	if hc > 0 {
		t.Errorf("expected 0 history rows on idempotent call, got %d", hc)
	}
	if ec > 0 {
		t.Errorf("expected 0 event rows on idempotent call, got %d", ec)
	}
}

func TestPR3RecordRenderFinished_Idempotent_AlreadySucceeded(t *testing.T) {
	_, repo, db := openPR3TestDB(t)
	ctx := context.Background()

	now := time.Now().UTC().Format(time.RFC3339)
	_, err := db.Exec(
		`INSERT INTO jobs (job_id, status, assigned_to, lease_id, lease_expiry, revision, attempt, started_at, created_at, updated_at)
		 VALUES ('job-succ', 'SUCCEEDED', 'w1', 'lease-A', ?, 5, 1, ?, ?, ?)`,
		time.Now().UTC().Add(30*time.Minute).Format(time.RFC3339), now, now, now,
	)
	if err != nil {
		t.Fatalf("seed SUCCEEDED: %v", err)
	}

	cmd := makeRenderCmd("job-succ", "w1", "lease-A", 1, time.Time{})
	if err := repo.PR3RecordRenderFinished(ctx, cmd); err != nil {
		t.Fatalf("PR3RecordRenderFinished on SUCCEEDED should be no-op, got: %v", err)
	}

	assertJobStatus(t, db, "job-succ", "SUCCEEDED")
}

func TestPR3RecordRenderFinished_NullRevision_COALESCEHandles(t *testing.T) {
	_, repo, db := openPR3TestDB(t)
	ctx := context.Background()

	// revision < 0 signals NULL in seedRunningJob.
	seedRunningJob(t, db, "job-nullrev", "w1", "lease-A", 1, -1)

	// Verify revision is actually NULL in the DB.
	var revBefore sql.NullInt64
	if err := db.QueryRow(`SELECT revision FROM jobs WHERE job_id = ?`, "job-nullrev").Scan(&revBefore); err != nil {
		t.Fatalf("read revision before: %v", err)
	}
	if revBefore.Valid {
		t.Fatalf("expected NULL revision, got %d", revBefore.Int64)
	}

	cmd := makeRenderCmd("job-nullrev", "w1", "lease-A", 1, time.Time{})
	if err := repo.PR3RecordRenderFinished(ctx, cmd); err != nil {
		t.Fatalf("PR3RecordRenderFinished with NULL revision should succeed via COALESCE(revision,0), got: %v", err)
	}

	rev := assertJobStatus(t, db, "job-nullrev", "RENDER_FINISHED")
	// COALESCE(revision,0)+1 should produce 1 from NULL.
	if rev != 1 {
		t.Errorf("expected revision 1 (COALESCE(NULL,0)+1), got %d", rev)
	}
}

func TestPR3RecordRenderFinished_TOCTOU_ConcurrentRevisionBump(t *testing.T) {
	_, repo, db := openPR3TestDB(t)
	ctx := context.Background()

	seedRunningJob(t, db, "job-toc", "w1", "lease-A", 1, 5)

	// Open a second connection to the same in-memory DB (cache=shared).
	db2, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("open db2: %v", err)
	}
	defer db2.Close()

	// Bump revision from the second connection BEFORE calling PR3RecordRenderFinished.
	// This simulates a concurrent LeaseRenewal happening just before the render
	// finished call. The fix reads revision INSIDE the transaction so it should
	// pick up the new value and succeed.
	res, err := db2.Exec(
		`UPDATE jobs SET revision = COALESCE(revision, 0) + 1, updated_at = ? WHERE job_id = ?`,
		time.Now().UTC().Format(time.RFC3339), "job-toc",
	)
	if err != nil {
		t.Fatalf("db2 concurrent bump: %v", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("db2 rows affected error: %v", err)
	}
	if n != 1 {
		t.Fatalf("concurrent bump affected %d rows, expected 1", n)
	}

	cmd := makeRenderCmd("job-toc", "w1", "lease-A", 1, time.Time{})
	if err := repo.PR3RecordRenderFinished(ctx, cmd); err != nil {
		t.Fatalf("PR3RecordRenderFinished with concurrent revision bump should use latest revision inside tx, got: %v", err)
	}

	// Status should be RENDER_FINISHED.
	rev := assertJobStatus(t, db, "job-toc", "RENDER_FINISHED")
	if rev < 7 {
		t.Errorf("expected revision >= 7 (5 initial + 1 db2 bump + 1 PR3 bump), got %d", rev)
	}
}

func TestPR3RecordRenderFinished_WrongWorkerID(t *testing.T) {
	_, repo, db := openPR3TestDB(t)
	ctx := context.Background()

	seedRunningJob(t, db, "job-wrongw", "w1", "lease-A", 1, 0)

	cmd := makeRenderCmd("job-wrongw", "w2-impostor", "lease-A", 1, time.Time{})
	err := repo.PR3RecordRenderFinished(ctx, cmd)
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("expected ErrTransitionConflict for wrong worker, got %v", err)
	}

	assertJobStatus(t, db, "job-wrongw", "RUNNING")
}

func TestPR3RecordRenderFinished_WrongLeaseID(t *testing.T) {
	_, repo, db := openPR3TestDB(t)
	ctx := context.Background()

	seedRunningJob(t, db, "job-wrongl", "w1", "lease-A", 1, 0)

	cmd := makeRenderCmd("job-wrongl", "w1", "lease-BOGUS", 1, time.Time{})
	err := repo.PR3RecordRenderFinished(ctx, cmd)
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("expected ErrTransitionConflict for wrong lease, got %v", err)
	}

	assertJobStatus(t, db, "job-wrongl", "RUNNING")
}

func TestPR3RecordRenderFinished_WrongAttempt(t *testing.T) {
	_, repo, db := openPR3TestDB(t)
	ctx := context.Background()

	seedRunningJob(t, db, "job-wronga", "w1", "lease-A", 2, 0)

	cmd := makeRenderCmd("job-wronga", "w1", "lease-A", 1, time.Time{})
	err := repo.PR3RecordRenderFinished(ctx, cmd)
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("expected ErrTransitionConflict for wrong attempt, got %v", err)
	}

	assertJobStatus(t, db, "job-wronga", "RUNNING")
}

func TestPR3RecordRenderFinished_WrongStatus_NotRunning(t *testing.T) {
	_, repo, db := openPR3TestDB(t)
	ctx := context.Background()

	now := time.Now().UTC().Format(time.RFC3339)
	_, err := db.Exec(
		`INSERT INTO jobs (job_id, status, assigned_to, lease_id, lease_expiry, revision, attempt, created_at, updated_at)
		 VALUES ('job-pending', 'PENDING', 'w1', 'lease-A', ?, 0, 1, ?, ?)`,
		time.Now().UTC().Add(30*time.Minute).Format(time.RFC3339), now, now,
	)
	if err != nil {
		t.Fatalf("seed PENDING: %v", err)
	}

	cmd := makeRenderCmd("job-pending", "w1", "lease-A", 1, time.Time{})
	err = repo.PR3RecordRenderFinished(ctx, cmd)
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("expected ErrTransitionConflict for PENDING→RENDER_FINISHED, got %v", err)
	}

	assertJobStatus(t, db, "job-pending", "PENDING")
}

func TestPR3RecordRenderFinished_WrongStatus_Leased(t *testing.T) {
	_, repo, db := openPR3TestDB(t)
	ctx := context.Background()

	now := time.Now().UTC().Format(time.RFC3339)
	_, err := db.Exec(
		`INSERT INTO jobs (job_id, status, assigned_to, lease_id, lease_expiry, revision, attempt, created_at, updated_at)
		 VALUES ('job-leased', 'LEASED', 'w1', 'lease-A', ?, 0, 1, ?, ?)`,
		time.Now().UTC().Add(30*time.Minute).Format(time.RFC3339), now, now,
	)
	if err != nil {
		t.Fatalf("seed LEASED: %v", err)
	}

	cmd := makeRenderCmd("job-leased", "w1", "lease-A", 1, time.Time{})
	err = repo.PR3RecordRenderFinished(ctx, cmd)
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("expected ErrTransitionConflict for LEASED→RENDER_FINISHED, got %v", err)
	}

	assertJobStatus(t, db, "job-leased", "LEASED")
}

func TestPR3RecordRenderFinished_JobNotFound(t *testing.T) {
	_, repo, _ := openPR3TestDB(t)
	ctx := context.Background()

	cmd := makeRenderCmd("nonexistent", "w1", "lease-A", 1, time.Time{})
	err := repo.PR3RecordRenderFinished(ctx, cmd)
	// Job not found: the function commits the read-only tx and returns nil.
	if err != nil {
		t.Fatalf("PR3RecordRenderFinished on missing job should return nil (commit no-op tx), got: %v", err)
	}
}

func TestPR3RecordRenderFinished_HistoryAndEventVerification(t *testing.T) {
	_, repo, db := openPR3TestDB(t)
	ctx := context.Background()

	seedRunningJob(t, db, "job-he-verify", "w99", "lease-XYZ", 3, 10)

	cmd := makeRenderCmd("job-he-verify", "w99", "lease-XYZ", 3, time.Time{})
	if err := repo.PR3RecordRenderFinished(ctx, cmd); err != nil {
		t.Fatalf("PR3RecordRenderFinished: %v", err)
	}

	// Verify history row.
	var hStatus, hWorkerID, hMessage string
	if err := db.QueryRow(
		`SELECT status, COALESCE(worker_id, ''), COALESCE(message, '') FROM job_history WHERE job_id = ?`,
		"job-he-verify",
	).Scan(&hStatus, &hWorkerID, &hMessage); err != nil {
		t.Fatalf("read history: %v", err)
	}
	if hStatus != "RENDER_FINISHED" {
		t.Errorf("history status: got %q, want RENDER_FINISHED", hStatus)
	}
	if hWorkerID != "w99" {
		t.Errorf("history worker_id: got %q, want w99", hWorkerID)
	}
	if hMessage == "" {
		t.Error("history message should not be empty")
	}

	// Verify event row.
	var eEvent string
	if err := db.QueryRow(
		`SELECT event FROM job_events WHERE job_id = ?`,
		"job-he-verify",
	).Scan(&eEvent); err != nil {
		t.Fatalf("read event: %v", err)
	}
	if eEvent != "render_finished" {
		t.Errorf("event type: got %q, want render_finished", eEvent)
	}
}

func TestPR3RecordRenderFinished_MultipleJobsSequential(t *testing.T) {
	_, repo, db := openPR3TestDB(t)
	ctx := context.Background()

	for i, jid := range []string{"job-seq-a", "job-seq-b", "job-seq-c"} {
		seedRunningJob(t, db, jid, fmt.Sprintf("w-%d", i), fmt.Sprintf("lease-%d", i), i+1, i*10)
		cmd := makeRenderCmd(jid, fmt.Sprintf("w-%d", i), fmt.Sprintf("lease-%d", i), i+1, time.Time{})
		if err := repo.PR3RecordRenderFinished(ctx, cmd); err != nil {
			t.Fatalf("job %d (%s): %v", i, jid, err)
		}
		assertJobStatus(t, db, jid, "RENDER_FINISHED")
	}
}

func TestPR3RecordRenderFinished_EmptyJobID(t *testing.T) {
	_, repo, _ := openPR3TestDB(t)
	ctx := context.Background()

	cmd := makeRenderCmd("", "w1", "lease-A", 1, time.Time{})
	err := repo.PR3RecordRenderFinished(ctx, cmd)
	if err == nil {
		t.Fatal("expected error for empty jobID")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// PR3Start — NULL revision handling
// ═══════════════════════════════════════════════════════════════════════════

func TestPR3Start_NullRevision(t *testing.T) {
	_, repo, db := openPR3TestDB(t)
	ctx := context.Background()

	now := time.Now().UTC().Format(time.RFC3339)
	_, err := db.Exec(
		`INSERT INTO jobs (job_id, status, assigned_to, lease_id, lease_expiry, revision, attempt, created_at, updated_at)
		 VALUES ('job-start-null', 'LEASED', 'w1', 'lease-A', ?, NULL, 1, ?, ?)`,
		time.Now().UTC().Add(30*time.Minute).Format(time.RFC3339), now, now,
	)
	if err != nil {
		t.Fatalf("seed LEASED with NULL revision: %v", err)
	}

	cmd := StartCommand{
		JobID:            "job-start-null",
		WorkerID:         "w1",
		LeaseID:          "lease-A",
		Attempt:          1,
		ExpectedRevision: 0, // COALESCE(revision,0)=0
	}
	if err := repo.PR3Start(ctx, cmd); err != nil {
		t.Fatalf("PR3Start with NULL revision: %v", err)
	}

	assertJobStatus(t, db, "job-start-null", "RUNNING")
}

func TestPR3Start_WrongRevisionCAS(t *testing.T) {
	_, repo, db := openPR3TestDB(t)
	ctx := context.Background()

	now := time.Now().UTC().Format(time.RFC3339)
	_, err := db.Exec(
		`INSERT INTO jobs (job_id, status, assigned_to, lease_id, lease_expiry, revision, attempt, created_at, updated_at)
		 VALUES ('job-start-cas', 'LEASED', 'w1', 'lease-A', ?, 7, 1, ?, ?)`,
		time.Now().UTC().Add(30*time.Minute).Format(time.RFC3339), now, now,
	)
	if err != nil {
		t.Fatalf("seed LEASED: %v", err)
	}

	// Use stale revision.
	cmd := StartCommand{
		JobID:            "job-start-cas",
		WorkerID:         "w1",
		LeaseID:          "lease-A",
		Attempt:          1,
		ExpectedRevision: 3, // stale: real = 7
	}
	err = repo.PR3Start(ctx, cmd)
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("expected ErrTransitionConflict for stale revision, got %v", err)
	}
	assertJobStatus(t, db, "job-start-cas", "LEASED")
}

// ═══════════════════════════════════════════════════════════════════════════
// PR3RenewLease — SkipRevisionCAS and NULL revision
// ═══════════════════════════════════════════════════════════════════════════

func TestPR3RenewLease_SkipRevisionCAS(t *testing.T) {
	_, repo, db := openPR3TestDB(t)
	ctx := context.Background()

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339)
	_, err := db.Exec(
		`INSERT INTO jobs (job_id, status, assigned_to, lease_id, lease_expiry, revision, attempt, created_at, updated_at)
		 VALUES ('job-renew-skip', 'LEASED', 'w1', 'lease-A', ?, 5, 1, ?, ?)`,
		now.Add(30*time.Minute).Format(time.RFC3339), nowStr, nowStr,
	)
	if err != nil {
		t.Fatalf("seed LEASED: %v", err)
	}

	cmd := RenewLeaseCommand{
		JobID:             "job-renew-skip",
		WorkerID:          "w1",
		LeaseID:           "lease-A",
		LeaseExpiry:       now.Add(60 * time.Minute),
		SkipRevisionCAS:   true,
		ExpectedRevision:  999, // Would fail, but SkipRevisionCAS=true bypasses it.
		EmitEvent:         false,
	}
	if err := repo.PR3RenewLease(ctx, cmd); err != nil {
		t.Fatalf("PR3RenewLease with SkipRevisionCAS: %v", err)
	}

	// Verify revision bumped despite CAS skip.
	var newRev int
	var newExpiry string
	if err := db.QueryRow(
		`SELECT COALESCE(revision,0), COALESCE(lease_expiry,'') FROM jobs WHERE job_id = ?`,
		"job-renew-skip",
	).Scan(&newRev, &newExpiry); err != nil {
		t.Fatalf("read after renew: %v", err)
	}
	if newRev != 6 {
		t.Errorf("expected revision 6 (5+1), got %d", newRev)
	}
	if newExpiry == "" {
		t.Error("lease_expiry should be set")
	}
}

func TestPR3RenewLease_StaleRevisionConflict(t *testing.T) {
	_, repo, db := openPR3TestDB(t)
	ctx := context.Background()

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339)
	_, err := db.Exec(
		`INSERT INTO jobs (job_id, status, assigned_to, lease_id, lease_expiry, revision, attempt, created_at, updated_at)
		 VALUES ('job-renew-conflict', 'LEASED', 'w1', 'lease-A', ?, 5, 1, ?, ?)`,
		now.Add(30*time.Minute).Format(time.RFC3339), nowStr, nowStr,
	)
	if err != nil {
		t.Fatalf("seed LEASED: %v", err)
	}

	// Stale revision without skip.
	cmd := RenewLeaseCommand{
		JobID:             "job-renew-conflict",
		WorkerID:          "w1",
		LeaseID:           "lease-A",
		LeaseExpiry:       now.Add(60 * time.Minute),
		SkipRevisionCAS:   false,
		ExpectedRevision:  3, // real = 5
	}
	err = repo.PR3RenewLease(ctx, cmd)
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("expected ErrTransitionConflict for stale revision, got %v", err)
	}
}

func TestPR3RenewLease_NullRevision(t *testing.T) {
	_, repo, db := openPR3TestDB(t)
	ctx := context.Background()

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339)
	_, err := db.Exec(
		`INSERT INTO jobs (job_id, status, assigned_to, lease_id, lease_expiry, revision, attempt, created_at, updated_at)
		 VALUES ('job-renew-null', 'LEASED', 'w1', 'lease-A', ?, NULL, 1, ?, ?)`,
		now.Add(30*time.Minute).Format(time.RFC3339), nowStr, nowStr,
	)
	if err != nil {
		t.Fatalf("seed LEASED with NULL revision: %v", err)
	}

	cmd := RenewLeaseCommand{
		JobID:             "job-renew-null",
		WorkerID:          "w1",
		LeaseID:           "lease-A",
		LeaseExpiry:       now.Add(60 * time.Minute),
		SkipRevisionCAS:   false,
		ExpectedRevision:  0, // COALESCE(NULL,0)=0
	}
	if err := repo.PR3RenewLease(ctx, cmd); err != nil {
		t.Fatalf("PR3RenewLease with NULL revision: %v", err)
	}
}

func TestPR3RenewLease_EmitEvent(t *testing.T) {
	_, repo, db := openPR3TestDB(t)
	ctx := context.Background()

	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339)
	_, err := db.Exec(
		`INSERT INTO jobs (job_id, status, assigned_to, lease_id, lease_expiry, revision, attempt, created_at, updated_at)
		 VALUES ('job-renew-event', 'LEASED', 'w1', 'lease-A', ?, 0, 1, ?, ?)`,
		now.Add(30*time.Minute).Format(time.RFC3339), nowStr, nowStr,
	)
	if err != nil {
		t.Fatalf("seed LEASED: %v", err)
	}

	cmd := RenewLeaseCommand{
		JobID:             "job-renew-event",
		WorkerID:          "w1",
		LeaseID:           "lease-A",
		LeaseExpiry:       now.Add(60 * time.Minute),
		SkipRevisionCAS:   true,
		ExpectedRevision:  0,
		EmitEvent:         true,
	}
	if err := repo.PR3RenewLease(ctx, cmd); err != nil {
		t.Fatalf("PR3RenewLease with EmitEvent: %v", err)
	}

	// Verify event was emitted.
	var eEvent string
	if err := db.QueryRow(
		`SELECT event FROM job_events WHERE job_id = ?`, "job-renew-event",
	).Scan(&eEvent); err != nil {
		t.Fatalf("read event: %v", err)
	}
	if eEvent != "lease_renewed" {
		t.Errorf("expected lease_renewed event, got %q", eEvent)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// PR3RecordRenderFinished — transactional atomicity
// ═══════════════════════════════════════════════════════════════════════════

func TestPR3RecordRenderFinished_Atomicity_NoOrphanEvents(t *testing.T) {
	// Verify that when the CAS UPDATE fails (0 rows), neither history
	// nor event rows are inserted. The tx rolls back entirely.
	_, repo, db := openPR3TestDB(t)
	ctx := context.Background()

	seedRunningJob(t, db, "job-atomic", "w1", "lease-A", 1, 5)

	// First call succeeds.
	cmd := makeRenderCmd("job-atomic", "w1", "lease-A", 1, time.Time{})
	if err := repo.PR3RecordRenderFinished(ctx, cmd); err != nil {
		t.Fatalf("first call: %v", err)
	}

	// Count history + events after success.
	hc1, ec1 := countHistoryAndEvents(t, db, "job-atomic")
	if hc1 != 1 || ec1 != 1 {
		t.Fatalf("expected 1 history + 1 event after success, got %d + %d", hc1, ec1)
	}

	// Bump the job back to RUNNING and change revision so it could match again,
	// but with a WRONG worker ID so the UPDATE fails.
	_, err := db.Exec(
		`UPDATE jobs SET status = 'RUNNING', assigned_to = 'w1', lease_id = 'lease-A',
		 revision = 10, attempt = 1 WHERE job_id = 'job-atomic'`,
	)
	if err != nil {
		t.Fatalf("reset job: %v", err)
	}

	// Now call with wrong worker — CAS should fail.
	cmd2 := makeRenderCmd("job-atomic", "w2-impostor", "lease-A", 1, time.Time{})
	err = repo.PR3RecordRenderFinished(ctx, cmd2)
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("expected ErrTransitionConflict, got %v", err)
	}

	// History + event count must NOT have changed (tx rolled back).
	hc2, ec2 := countHistoryAndEvents(t, db, "job-atomic")
	if hc2 != hc1 {
		t.Errorf("history count changed from %d to %d — tx should have rolled back", hc1, hc2)
	}
	if ec2 != ec1 {
		t.Errorf("event count changed from %d to %d — tx should have rolled back", ec1, ec2)
	}

	// Status must still be RUNNING (UPDATE failed, tx rolled back).
	assertJobStatus(t, db, "job-atomic", "RUNNING")
}
