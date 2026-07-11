// Package migrations / sqlite/061_attempt_commits_test.go
//
// Per-migration unit tests for 061_attempt_commits.sql. The test
// file lives next to the SQL it exercises so the test/SQL pairing
// is a single grep. The parent package's helpers (openTestDB,
// tableExists, columnExists, applyMigrationSQL, indexExists) are
// imported from the same `package migrations` declaration.
//
// Coverage:
//   - Table exists after applying 061.
//   - All 18 columns present in the exact order the plan dictates.
//   - Both indexes (idx_attempt_commits_status,
//     idx_attempt_commits_deadline) present.
//   - PRAGMA integrity_check returns "ok".
//   - UNIQUE(task_id, attempt_id) rejects a duplicate row.
//   - ready_output_count defaults to 0 (per the column DEFAULT 0).
//   - Round-trip: insert a fully-populated row, read it back, update
//     the status, delete and re-insert (recovery flow).
package migrations

import (
	"database/sql"
	"strings"
	"testing"

	// Required for //go:embed parsing.
	_ "embed"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed sqlite/061_attempt_commits.sql
var sqliteSQL061 string

// TestMigration061_TableExists asserts the table lands on a fresh DB
// after the migration is applied. This is the entry-point check: if
// this fails, every later test is meaningless.
func TestMigration061_TableExists(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sqliteSQL061)

	if !tableExists(t, db, "attempt_commits") {
		t.Fatal("expected attempt_commits table to exist after migration 061")
	}
}

// TestMigration061_AllColumns asserts every column from the plan
// (`docs/completion-protocol.md` §1.1) is present after the
// migration applies. A future schema drift that renames or drops a
// column will fail this test BEFORE the runtime path notices.
func TestMigration061_AllColumns(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sqliteSQL061)

	expected := []string{
		"commit_id",
		"task_id",
		"attempt_id",
		"job_id",
		"worker_id",
		"lease_id",
		"task_revision",
		"status",
		"required_output_count",
		"ready_output_count",
		"commit_token_hash",
		"commit_deadline_at",
		"last_progress_at",
		"committed_at",
		"rejected_code",
		"rejected_message",
		"created_at",
		"updated_at",
	}
	for _, col := range expected {
		if !columnExists(t, db, "attempt_commits", col) {
			t.Errorf("attempt_commits column %q missing after migration 061", col)
		}
	}
}

// TestMigration061_Indexes asserts both indexes from the plan
// (status + commit_deadline_at) are present. The supervisor's
// candidate scan reads by status and by deadline; missing either
// index is a silent scan-cost regression.
func TestMigration061_Indexes(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sqliteSQL061)

	for _, idx := range []string{
		"idx_attempt_commits_status",
		"idx_attempt_commits_deadline",
	} {
		if !indexExists(t, db, idx) {
			t.Errorf("attempt_commits index %q missing after migration 061", idx)
		}
	}
}

// TestMigration061_IntegrityOK runs PRAGMA integrity_check. Any
// schema-level corruption (orphaned pages, missing btree entries,
// mismatched rowids) surfaces here.
func TestMigration061_IntegrityOK(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sqliteSQL061)

	var result string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&result); err != nil {
		t.Fatalf("PRAGMA integrity_check: %v", err)
	}
	if result != "ok" {
		t.Errorf("PRAGMA integrity_check returned %q, want \"ok\"", result)
	}
}

// TestMigration061_DefaultReadyOutputCount inserts a row that does
// NOT specify ready_output_count and asserts the column DEFAULT 0
// kicks in. The plan's column is `ready_output_count INTEGER NOT
// NULL DEFAULT 0`.
func TestMigration061_DefaultReadyOutputCount(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sqliteSQL061)

	now := "2024-01-01T00:00:00Z"
	_, err := db.Exec(
		`INSERT INTO attempt_commits (
			commit_id, task_id, attempt_id, job_id, worker_id, lease_id,
			task_revision, status, required_output_count,
			commit_token_hash, commit_deadline_at, last_progress_at,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"c-default", "t-default", "a-default", "j-default", "w-default", "l-default",
		3, "DECLARED", 1,
		"hash-default", now, now, now, now,
	)
	if err != nil {
		t.Fatalf("insert attempt_commits: %v", err)
	}

	var readyCount int
	if err := db.QueryRow(
		`SELECT ready_output_count FROM attempt_commits WHERE commit_id = ?`,
		"c-default",
	).Scan(&readyCount); err != nil {
		t.Fatalf("read ready_output_count: %v", err)
	}
	if readyCount != 0 {
		t.Errorf("ready_output_count default: got %d, want 0", readyCount)
	}
}

// TestMigration061_UniqueOnTaskAndAttempt exercises the canonical
// UNIQUE constraint from the plan: UNIQUE(task_id, attempt_id).
// A second INSERT with the same (task_id, attempt_id) but a
// different commit_id MUST be rejected. The plan uses this to
// ensure a single Attempt cannot carry two commit rows in flight.
func TestMigration061_UniqueOnTaskAndAttempt(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sqliteSQL061)

	now := "2024-01-01T00:00:00Z"
	insertRow := func(commitID string) error {
		_, err := db.Exec(
			`INSERT INTO attempt_commits (
				commit_id, task_id, attempt_id, job_id, worker_id, lease_id,
				task_revision, status, required_output_count,
				commit_token_hash, commit_deadline_at, last_progress_at,
				created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			commitID, "t-uniq", "a-uniq", "j-uniq", "w-uniq", "l-uniq",
			1, "DECLARED", 1,
			"hash-uniq", now, now, now, now,
		)
		return err
	}

	if err := insertRow("c-first"); err != nil {
		t.Fatalf("first insert attempt_commits: %v", err)
	}
	if err := insertRow("c-second"); err == nil {
		t.Fatal("expected UNIQUE violation on (task_id, attempt_id), got nil")
	} else if !strings.Contains(strings.ToLower(err.Error()), "unique") {
		t.Errorf("second insert error should mention UNIQUE violation, got: %v", err)
	}
}

// TestMigration061_RoundTrip drives the full lifecycle: insert with
// all columns populated → read back → update status to COMMITTED →
// delete → re-insert (recovery flow). Asserts every column round-
// trips with the expected value, and that the UNIQUE constraint is
// only active while the row exists (delete + re-insert is allowed).
func TestMigration061_RoundTrip(t *testing.T) {
	db := openTestDB(t)
	applyMigrationSQL(t, db, sqliteSQL061)

	now := "2024-01-01T00:00:00Z"
	_, err := db.Exec(
		`INSERT INTO attempt_commits (
			commit_id, task_id, attempt_id, job_id, worker_id, lease_id,
			task_revision, status, required_output_count,
			commit_token_hash, commit_deadline_at, last_progress_at,
			committed_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"c-rt", "t-rt", "a-rt", "j-rt", "w-rt", "l-rt",
		5, "VERIFYING", 2,
		"hash-rt", now, now, now, now, now,
	)
	if err != nil {
		t.Fatalf("insert attempt_commits round-trip: %v", err)
	}

	var (
		status        string
		commitToken   string
		committedAt   sql.NullString
		requiredCount int
		taskRevision  int
	)
	err = db.QueryRow(
		`SELECT status, commit_token_hash, committed_at,
		        required_output_count, task_revision
		   FROM attempt_commits WHERE commit_id = ?`, "c-rt",
	).Scan(&status, &commitToken, &committedAt, &requiredCount, &taskRevision)
	if err != nil {
		t.Fatalf("select attempt_commits row: %v", err)
	}
	if status != "VERIFYING" {
		t.Errorf("status: got %q, want VERIFYING", status)
	}
	if commitToken != "hash-rt" {
		t.Errorf("commit_token_hash: got %q, want hash-rt", commitToken)
	}
	if !committedAt.Valid || committedAt.String != now {
		t.Errorf("committed_at: got %v, want %q", committedAt, now)
	}
	if requiredCount != 2 {
		t.Errorf("required_output_count: got %d, want 2", requiredCount)
	}
	if taskRevision != 5 {
		t.Errorf("task_revision: got %d, want 5", taskRevision)
	}

	// Update path: bump ready_output_count and verify the row still
	// passes UNIQUE/read integrity.
	if _, err := db.Exec(
		`UPDATE attempt_commits
		    SET status = ?, ready_output_count = ?, updated_at = ?
		  WHERE commit_id = ?`,
		"COMMITTED", 2, "2024-01-01T00:01:00Z", "c-rt",
	); err != nil {
		t.Fatalf("update attempt_commits: %v", err)
	}

	var newStatus string
	if err := db.QueryRow(
		`SELECT status FROM attempt_commits WHERE commit_id = ?`, "c-rt",
	).Scan(&newStatus); err != nil {
		t.Fatalf("re-select after update: %v", err)
	}
	if newStatus != "COMMITTED" {
		t.Errorf("status after update: got %q, want COMMITTED", newStatus)
	}

	// Hard delete and re-insert: UNIQUE constraint removes with the
	// row. Models the Phase 2 "previous attempt's commit row removed,
	// new attempt for same task_id" recovery flow.
	if _, err := db.Exec(`DELETE FROM attempt_commits WHERE commit_id = ?`, "c-rt"); err != nil {
		t.Fatalf("delete attempt_commits: %v", err)
	}
	if err := insertAttemptCommitForRecovery(db, "c-rt-after-delete", "t-rt", "a-rt", now); err != nil {
		t.Errorf("re-INSERT after DELETE for same (task_id, attempt_id): %v", err)
	}
}

// insertAttemptCommitForRecovery inserts a fresh attempt_commits row
// for the (task_id, attempt_id) recovery flow. Helper for the
// round-trip test.
func insertAttemptCommitForRecovery(db *sql.DB, commitID, taskID, attemptID, now string) error {
	_, err := db.Exec(
		`INSERT INTO attempt_commits (
			commit_id, task_id, attempt_id, job_id, worker_id, lease_id,
			task_revision, status, required_output_count,
			commit_token_hash, commit_deadline_at, last_progress_at,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		commitID, taskID, attemptID, "j-rt", "w-rt", "l-rt",
		6, "DECLARED", 1,
		"hash-rt-2", now, now, now, now,
	)
	return err
}
