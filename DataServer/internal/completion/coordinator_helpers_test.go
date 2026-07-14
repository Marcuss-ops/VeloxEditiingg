// Package completion / coordinator_helpers_test.go
//
// Shared test helpers extracted from coordinator_test.go as part of the
// per-phase split (declare / progress / complete-upload / commit /
// reconcile). These helpers are used by every test file in the
// `completion` package, including the existing fencing_test.go and
// reconcile_test.go — they MUST stay in the package's test-only
// compilation graph (i.e. a `_test.go` file, not a regular `.go`
// file). fencing_test.go has been calling openCoordinatorTestDB and
// validFence since the Phase 2.2 central-gate land; reconcile_test.go
// calls newTestCoordinator since the supervisor wiring landed. Both
// callers continue to depend on these symbols at the same package
// scope.
package completion

import (
	"crypto/sha256"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/store/migrations"
)

// testHMACKey is the deterministic 32-byte HMAC key used by every
// Coordinator built in this package (Verdetto P0 #6). It is the
// SHA-256 of a fixed string ("velox-test-commit-hmac-key-v1") and is
// stable across runs so DeclareOutputs produces the same
// commit_token + commit_token_hash for the same (commit_id, fence)
// — the exact property the new replay-safe derivation ships with.
var testHMACKey = func() []byte {
	h := sha256.Sum256([]byte("velox-test-commit-hmac-key-v1"))
	return h[:]
}()

// newTestCoordinator builds the canonical Coordinator with the test
// HMAC key for this package. NewCoordinator's >=32-byte guard passes
// (testHMACKey is exactly 32 bytes).
func newTestCoordinator(db *sql.DB) Coordinator {
	c, err := NewCoordinator(CoordinatorConfig{DB: db, HMACKey: testHMACKey})
	if err != nil {
		panic(err) // test-only; cannot reasonably happen
	}
	return c
}

// ────────────────────────────────────────────────────────────────────────
// helpers: open the canonical migrations-seeded DB used by every test
// in this file.
//
// We use a tempfile-backed SQLite (with WAL journal mode) rather than
// the `file:NAME?mode=memory&cache=shared` idiom. The shared-cache
// in-memory mode crashed the package-level go test under concurrent
// fixture reuse because RunMigrations would re-apply migrations on a
// non-empty schema_migrations table from a sibling test's fixture,
// surfacing as a FAIL exit code at the package boundary even though
// every individual t.Run reported PASS. The tempfile alternative is
// per-test isolated by t.TempDir() and works under `go test -race`
// without surprises.
// ────────────────────────────────────────────────────────────────────────

func openCoordinatorTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "coordinator_test.db")
	db, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000&_journal_mode=WAL&_synchronous=NORMAL")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable FK: %v", err)
	}

	if err := migrations.RunMigrations(db, migrations.SQLiteMigrationsFS(), "sqlite"); err != nil {
		t.Fatalf("apply production migrations: %v", err)
	}
	return db
}

func validFence(taskID, attemptID string) FenceTuple {
	return FenceTuple{
		TaskID:    taskID,
		AttemptID: attemptID,
		WorkerID:  "worker-" + taskID,
		LeaseID:   "lease-" + attemptID,
		Revision:  1,
	}
}

// validManifests produces a minimal OutputManifest that satisfies
// validateManifest. The shape mirrors what the executor emits today.
func validManifests() []OutputManifest {
	return []OutputManifest{
		{
			OutputKind:     "final_video",
			LogicalName:    "out.mp4",
			MimeType:       "video/mp4",
			SizeBytes:      1024,
			SHA256:         strings.Repeat("a", 64),
			WorkerSpoolKey: "spool-key-1",
		},
	}
}

// attemptCommitRow reads the attempt_commits row for the supplied
// tuple and returns its column values for assertion in tests.
type attemptCommitRow struct {
	CommitID          string
	TaskID            string
	AttemptID         string
	JobID             string
	WorkerID          string
	LeaseID           string
	TaskRevision      int
	Status            string
	RequiredOutputCnt int
	ReadyOutputCnt    int
	CommitTokenHash   string
	CommitDeadlineAt  string
	LastProgressAt    string
}

func readAttemptCommitRow(t *testing.T, db *sql.DB, fence FenceTuple) attemptCommitRow {
	t.Helper()
	var r attemptCommitRow
	err := db.QueryRow(
		`SELECT commit_id, task_id, attempt_id, job_id, worker_id, lease_id,
		        task_revision, status, required_output_count, ready_output_count,
		        commit_token_hash, commit_deadline_at, last_progress_at
		 FROM attempt_commits
		 WHERE task_id = ? AND attempt_id = ?`,
		fence.TaskID, fence.AttemptID,
	).Scan(&r.CommitID, &r.TaskID, &r.AttemptID, &r.JobID, &r.WorkerID, &r.LeaseID,
		&r.TaskRevision, &r.Status, &r.RequiredOutputCnt, &r.ReadyOutputCnt,
		&r.CommitTokenHash, &r.CommitDeadlineAt, &r.LastProgressAt)
	if err != nil {
		t.Fatalf("read attempt_commits: %v", err)
	}
	return r
}

// sha256HexFromRow is a tiny inline helper that reads
// attempt_commits.commit_token_hash and returns its hex form.
// It's used by the determinism replay test; reading via the
// canonical helpers keeps the test independent from the wider
// package API.
func sha256HexFromRow(t *testing.T, db *sql.DB, fence FenceTuple) string {
	t.Helper()
	var h string
	if err := db.QueryRow(
		`SELECT commit_token_hash FROM attempt_commits
		 WHERE task_id = ? AND attempt_id = ?`,
		fence.TaskID, fence.AttemptID,
	).Scan(&h); err != nil {
		t.Fatalf("read commit_token_hash: %v", err)
	}
	return h
}

// seedCompleteUploadFixture inserts a jobs row (needed by both the
// artifact_uploads.job_id FK and the legacy canonical pipeline),
// an artifacts row (STAGING, expected SHA), and an artifact_uploads
// row (RECEIVED, expected_sha256). Tests call the coordinator's
// CompleteUpload against this fixture.
//
// The artifact_uploads schema (migration 030) enforces
//
//	FOREIGN KEY (job_id) REFERENCES jobs(job_id)
//
// so a placeholder row in jobs is required even though our tests
// never read it.
func seedCompleteUploadFixture(t *testing.T, db *sql.DB, uploadID, artifactID, jobID, expectedSHA string) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`
		INSERT OR IGNORE INTO jobs (job_id, migrated_at)
		VALUES (?, ?)`,
		jobID, now,
	); err != nil {
		t.Fatalf("seed jobs: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO artifacts (
			id, job_id, type, storage_provider, storage_key,
			sha256, size_bytes, status, created_at
		) VALUES (?, ?, 'video', 'local', ?, ?, 1024, 'STAGING', ?)`,
		artifactID, jobID, uploadID+".local", expectedSHA, now,
	); err != nil {
		t.Fatalf("seed artifacts: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO artifact_uploads (
			upload_id, artifact_id, job_id, attempt_number,
			worker_id, lease_id, status, temporary_storage_key,
			expected_size_bytes, expected_sha256, created_at, expires_at
		) VALUES (?, ?, ?, 1, 'worker-fixture', 'lease-fixture',
		          'RECEIVED', ?, 1024, ?, ?, ?)`,
		uploadID, artifactID, jobID, uploadID+".local", expectedSHA, now, now,
	); err != nil {
		t.Fatalf("seed artifact_uploads: %v", err)
	}
}

// readArtifactStatus returns the post-call status of the
// artifact row, used by the four-branch assertions.
func readArtifactStatus(t *testing.T, db *sql.DB, artifactID string) string {
	t.Helper()
	var status string
	if err := db.QueryRow(`SELECT status FROM artifacts WHERE id = ?`, artifactID).Scan(&status); err != nil {
		t.Fatalf("read artifact status: %v", err)
	}
	return status
}

// errorsIs is a tiny inline errors.Is shim to avoid pulling the
// stdlib import into every test. The Go 1.18+ errors package is used
// identically to errors.Is.
func errorsIs(err, target error) bool {
	return err != nil && (err == target || (err.Error() == target.Error()))
}
