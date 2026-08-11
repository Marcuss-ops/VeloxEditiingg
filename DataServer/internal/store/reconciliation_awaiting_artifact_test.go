package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"velox-server/internal/store/migrations"

	_ "github.com/mattn/go-sqlite3"
)

func openReconciliationTestDB(t *testing.T) (*SQLiteStore, *sql.DB) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "reconcile-a3.db")
	db, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := migrations.RunMigrations(db, migrations.SQLiteMigrationsFS(), "sqlite"); err != nil {
		t.Fatal(err)
	}
	return &SQLiteStore{db: db, path: dbPath}, db
}

// seedAwaitingArtifactJob inserts a job in AWAITING_ARTIFACT plus a
// fully-committed task/attempt/commit triplet (the normal shape of a
// job that reached the gate) so tests can layer the blocking
// conditions on top.
func seedAwaitingArtifactJob(t *testing.T, db *sql.DB, jobID string, old string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO jobs (job_id, status, created_at, updated_at, migrated_at, revision)
		VALUES (?, 'AWAITING_ARTIFACT', ?, ?, ?, 0)`, jobID, old, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO tasks (task_id, job_id, status, revision, attempt_count, created_at, updated_at)
		VALUES (?, ?, 'SUCCEEDED', 1, 1, ?, ?)`, "task-"+jobID, jobID, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO task_attempts (id, task_id, job_id, attempt_number, worker_id, lease_id, status, report_version, created_at, updated_at)
		VALUES (?, ?, ?, 1, 'w', 'l', 'SUCCEEDED', 0, ?, ?)`, "attempt-"+jobID, "task-"+jobID, jobID, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO attempt_commits (commit_id, task_id, attempt_id, job_id, worker_id, lease_id, task_revision, status, required_output_count, commit_token_hash, commit_deadline_at, last_progress_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, 'w', 'l', 1, 'COMMITTED', 1, 'tok', ?, ?, ?, ?)`,
		"commit-"+jobID, "task-"+jobID, "attempt-"+jobID, jobID, old, old, old, old); err != nil {
		t.Fatal(err)
	}
}

func newAwaitingArtifactReconciler(t *testing.T, store *SQLiteStore) *AwaitingArtifactReconciler {
	t.Helper()
	r, err := NewAwaitingArtifactReconciler(store)
	if err != nil {
		t.Fatal(err)
	}
	r.SetStaleAfter(24 * time.Hour)
	return r
}

func TestAwaitingArtifactReconciler_TerminalizesStaleJob(t *testing.T) {
	store, db := openReconciliationTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour).Format(time.RFC3339Nano)
	seedAwaitingArtifactJob(t, db, "job-stale", old)

	r := newAwaitingArtifactReconciler(t, store)
	if err := r.Reconcile(context.Background(), now); err != nil {
		t.Fatal(err)
	}

	var status, reason, reconciledAt, message string
	var version int
	if err := db.QueryRow(`SELECT status, reconciliation_reason, COALESCE(reconciled_at,''), COALESCE(error_message,''), COALESCE(reconciliation_version,0) FROM jobs WHERE job_id='job-stale'`).Scan(&status, &reason, &reconciledAt, &message, &version); err != nil {
		t.Fatal(err)
	}
	if status != "FAILED" {
		t.Fatalf("status = %q, want FAILED", status)
	}
	if reason != ReconciliationReasonStaleAwaitingArtifact {
		t.Fatalf("reconciliation_reason = %q, want %q", reason, ReconciliationReasonStaleAwaitingArtifact)
	}
	if reconciledAt == "" {
		t.Fatal("reconciled_at must be stamped")
	}
	if version != 1 {
		t.Fatalf("reconciliation_version = %d, want 1", version)
	}
	if !strings.Contains(message, ReconciliationReasonStaleAwaitingArtifact) {
		t.Fatalf("error_message = %q, want STALE_AWAITING_ARTIFACT", message)
	}

	var audits int
	if err := db.QueryRow(`SELECT COUNT(*) FROM audit_events WHERE action='STALE_EXECUTION_RECONCILED' AND resource_id='job-stale'`).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if audits != 1 {
		t.Fatalf("audit count = %d, want 1", audits)
	}
}

func TestAwaitingArtifactReconciler_IsIdempotent(t *testing.T) {
	store, db := openReconciliationTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour).Format(time.RFC3339Nano)
	seedAwaitingArtifactJob(t, db, "job-ido", old)

	r := newAwaitingArtifactReconciler(t, store)
	if err := r.Reconcile(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	var version int
	if err := db.QueryRow(`SELECT COALESCE(reconciliation_version,0) FROM jobs WHERE job_id='job-ido'`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 1 {
		t.Fatalf("version after first pass = %d, want 1", version)
	}

	// Second pass must be a no-op: job is FAILED now, not a candidate.
	if err := r.Reconcile(context.Background(), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	var versionAfter int
	if err := db.QueryRow(`SELECT COALESCE(reconciliation_version,0) FROM jobs WHERE job_id='job-ido'`).Scan(&versionAfter); err != nil {
		t.Fatal(err)
	}
	if versionAfter != 1 {
		t.Fatalf("version after second pass = %d, want 1 (idempotent)", versionAfter)
	}
	var audits int
	if err := db.QueryRow(`SELECT COUNT(*) FROM audit_events WHERE action='STALE_EXECUTION_RECONCILED' AND resource_id='job-ido'`).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if audits != 1 {
		t.Fatalf("audit count after second pass = %d, want 1", audits)
	}
}

// TestAwaitingArtifactReconciler_SkipsActiveTransfer pins the
// "nessun transfer attivo" precondition: an in-flight upload session
// keeps the job alive even when everything else is stale.
func TestAwaitingArtifactReconciler_SkipsActiveTransfer(t *testing.T) {
	store, db := openReconciliationTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour).Format(time.RFC3339Nano)
	seedAwaitingArtifactJob(t, db, "job-transfer", old)
	if _, err := db.Exec(`INSERT INTO artifact_uploads (upload_id, artifact_id, job_id, attempt_number, worker_id, lease_id, status, temporary_storage_key, created_at, expires_at)
		VALUES ('upload-transfer', 'art-transfer', 'job-transfer', 1, 'w', 'l', 'CREATED', 'tmp', ?, ?)`, old, now.Add(time.Hour).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}

	r := newAwaitingArtifactReconciler(t, store)
	candidates, err := r.Scan(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 0 {
		t.Fatalf("candidates = %d, want 0 (active transfer blocks terminalization)", len(candidates))
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE job_id='job-transfer'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "AWAITING_ARTIFACT" {
		t.Fatalf("status = %q, want AWAITING_ARTIFACT (untouched)", status)
	}
}

// TestAwaitingArtifactReconciler_SkipsReadyArtifact pins the
// "artifact non registrato" precondition.
func TestAwaitingArtifactReconciler_SkipsReadyArtifact(t *testing.T) {
	store, db := openReconciliationTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour).Format(time.RFC3339Nano)
	seedAwaitingArtifactJob(t, db, "job-art", old)
	if _, err := db.Exec(`INSERT INTO artifacts (id, job_id, status, created_at) VALUES ('art-ready', 'job-art', 'READY', ?)`, old); err != nil {
		t.Fatal(err)
	}

	r := newAwaitingArtifactReconciler(t, store)
	candidates, err := r.Scan(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 0 {
		t.Fatalf("candidates = %d, want 0 (READY artifact blocks terminalization)", len(candidates))
	}
}

// TestAwaitingArtifactReconciler_SkipsActiveAttempt pins the
// "worker attempt non più attivo" precondition.
func TestAwaitingArtifactReconciler_SkipsActiveAttempt(t *testing.T) {
	store, db := openReconciliationTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour).Format(time.RFC3339Nano)
	seedAwaitingArtifactJob(t, db, "job-attempt", old)
	if _, err := db.Exec(`UPDATE task_attempts SET status='RUNNING' WHERE id='attempt-job-attempt'`); err != nil {
		t.Fatal(err)
	}

	r := newAwaitingArtifactReconciler(t, store)
	candidates, err := r.Scan(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 0 {
		t.Fatalf("candidates = %d, want 0 (active attempt blocks terminalization)", len(candidates))
	}
}

// TestAwaitingArtifactReconciler_SkipsActiveCommit pins the commit
// protocol half of "nessun transfer attivo".
func TestAwaitingArtifactReconciler_SkipsActiveCommit(t *testing.T) {
	store, db := openReconciliationTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour).Format(time.RFC3339Nano)
	seedAwaitingArtifactJob(t, db, "job-commit", old)
	if _, err := db.Exec(`UPDATE attempt_commits SET status='DECLARED' WHERE commit_id='commit-job-commit'`); err != nil {
		t.Fatal(err)
	}

	r := newAwaitingArtifactReconciler(t, store)
	candidates, err := r.Scan(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 0 {
		t.Fatalf("candidates = %d, want 0 (active commit blocks terminalization)", len(candidates))
	}
}

// TestAwaitingArtifactReconciler_SkipsFreshJob pins the
// "timeout superato" precondition.
func TestAwaitingArtifactReconciler_SkipsFreshJob(t *testing.T) {
	store, db := openReconciliationTestDB(t)
	now := time.Now().UTC()
	fresh := now.Add(-time.Hour).Format(time.RFC3339Nano)
	seedAwaitingArtifactJob(t, db, "job-fresh", fresh)

	r := newAwaitingArtifactReconciler(t, store)
	candidates, err := r.Scan(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 0 {
		t.Fatalf("candidates = %d, want 0 (fresh job is not stale)", len(candidates))
	}
}

// TestAwaitingArtifactReconciler_NeverTouchesTerminalJob pins the
// hard rule: terminal jobs are immutable, even when stale and missing
// every artifact/transfer precondition.
func TestAwaitingArtifactReconciler_NeverTouchesTerminalJob(t *testing.T) {
	store, db := openReconciliationTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour).Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO jobs (job_id, status, created_at, updated_at, migrated_at, revision) VALUES ('job-terminal', 'FAILED', ?, ?, ?, 0)`, old, old, old); err != nil {
		t.Fatal(err)
	}

	r := newAwaitingArtifactReconciler(t, store)
	candidates, err := r.Scan(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 0 {
		t.Fatalf("candidates = %d, want 0 (terminal job must never be a candidate)", len(candidates))
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE job_id='job-terminal'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "FAILED" {
		t.Fatalf("status = %q, want FAILED (untouched)", status)
	}
}

// TestAwaitingArtifactReconciler_ExpiresLingeringCommitAndUpload
// exercises the defensive closes inside applyCandidate: when the
// reconciler commits, any lingering non-terminal commit row or upload
// session for the job is expired atomically with the job flip.
func TestAwaitingArtifactReconciler_ExpiresLingeringCommitAndUpload(t *testing.T) {
	store, db := openReconciliationTestDB(t)
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour).Format(time.RFC3339Nano)
	seedAwaitingArtifactJob(t, db, "job-linger", old)
	// Layer a second, still-DECLARED commit row plus an active-looking
	// upload session, then drive applyCandidate directly (the scan
	// would normally skip this job because of these rows).
	// attempt_commits is UNIQUE(task_id, attempt_id): the second row uses
	// a distinct attempt identity (the row needs no task_attempts row —
	// migration 061 declares no FK on attempt_id).
	if _, err := db.Exec(`INSERT INTO attempt_commits (commit_id, task_id, attempt_id, job_id, worker_id, lease_id, task_revision, status, required_output_count, commit_token_hash, commit_deadline_at, last_progress_at, created_at, updated_at)
		VALUES ('commit-linger2', 'task-job-linger', 'attempt-linger2', 'job-linger', 'w', 'l', 1, 'DECLARED', 1, 'tok', ?, ?, ?, ?)`, old, old, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO artifact_uploads (upload_id, artifact_id, job_id, attempt_number, worker_id, lease_id, status, temporary_storage_key, created_at, expires_at)
		VALUES ('upload-linger', 'art-linger', 'job-linger', 1, 'w', 'l', 'UPLOADING', 'tmp', ?, ?)`, old, now.Add(time.Hour).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}

	r := newAwaitingArtifactReconciler(t, store)
	c := AwaitingArtifactCandidate{JobID: "job-linger", OldStatus: "AWAITING_ARTIFACT", UpdatedAt: old}
	if err := r.applyCandidate(context.Background(), c, now); err != nil {
		t.Fatal(err)
	}

	var commitStatus, uploadStatus string
	if err := db.QueryRow(`SELECT status FROM attempt_commits WHERE commit_id='commit-linger2'`).Scan(&commitStatus); err != nil {
		t.Fatal(err)
	}
	if commitStatus != "EXPIRED" {
		t.Fatalf("commit status = %q, want EXPIRED", commitStatus)
	}
	if err := db.QueryRow(`SELECT status FROM artifact_uploads WHERE upload_id='upload-linger'`).Scan(&uploadStatus); err != nil {
		t.Fatal(err)
	}
	if uploadStatus != "EXPIRED" {
		t.Fatalf("upload status = %q, want EXPIRED", uploadStatus)
	}
}
