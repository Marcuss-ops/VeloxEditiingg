// Package artifacts / finalization_fixture_test.go
//
// Schema constants, fixture helpers, and compile-time interface
// assertions shared by the verified-finalization black-box tests in
// sibling files (finalization_success_test.go,
// finalization_rejection_test.go, finalization_concurrency_test.go).
//
// All four files declare `package artifacts_test` so the helpers
// below are callable from each other without import plumbing.
package artifacts_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/artifacts"
)

// minimalSchema covers the columns FinalizeVerified /
// CreateArtifactAndUploadSession actually touch. Migrations are not
// required for this test — we validate the FinalizationWriter in
// isolation, not the wider store.
//
// migration 048: jobs.assigned_to / lease_id / lease_expiry were
// DROPPED from `jobs`. Per-attempt identity lives on `task_attempts`
// (canonical). The fixture below reflects that contract so the
// finalization tests exercise the SAME shape as the real store.
const minimalSchema = `
CREATE TABLE jobs (
	job_id        TEXT PRIMARY KEY,
	status        TEXT,
	revision      INTEGER,
	completed_at  TEXT,
	updated_at    TEXT,
	migrated_at   TEXT
);
CREATE TABLE artifacts (
	id              TEXT PRIMARY KEY,
	job_id          TEXT,
	attempt_id      INTEGER,
	type            TEXT,
	storage_provider TEXT,
	storage_key     TEXT,
	storage_url     TEXT,
	local_path      TEXT,
	sha256          TEXT,
	size_bytes      INTEGER,
	duration_seconds REAL,
	duration_ms     INTEGER,
	mime_type       TEXT,
	status          TEXT,
	verified_at     TEXT,
	created_at      TEXT
);
CREATE TABLE artifact_uploads (
	upload_id         TEXT PRIMARY KEY,
	artifact_id       TEXT,
	job_id            TEXT,
	attempt_number    INTEGER,
	worker_id         TEXT,
	lease_id          TEXT,
	status            TEXT,
	temporary_storage_key TEXT,
	expected_size_bytes  INTEGER,
	expected_sha256      TEXT,
	expected_revision    INTEGER,
	received_size_bytes  INTEGER,
	received_sha256      TEXT,
	created_at        TEXT,
	expires_at        TEXT,
	completed_at      TEXT
);
CREATE TABLE outbox_events (
	aggregate_type TEXT,
	aggregate_id   TEXT,
	event_type     TEXT,
	payload_json   TEXT,
	status         TEXT,
	available_at   TEXT,
	created_at     TEXT
);
CREATE TABLE job_deliveries (
	delivery_id      TEXT PRIMARY KEY,
	artifact_id      TEXT,
	destination_id   TEXT,
	status           TEXT,
	idempotency_key  TEXT,
	remote_id        TEXT,
	remote_url       TEXT,
	created_at       TEXT,
	updated_at       TEXT,
	UNIQUE (artifact_id, destination_id)
);
CREATE TABLE delivery_destinations (
	destination_id TEXT PRIMARY KEY,
	provider       TEXT,
	name           TEXT,
	enabled        INTEGER DEFAULT 1,
	created_at     TEXT,
	updated_at     TEXT
);
`

// openTestDB returns a fresh in-memory SQLite. Each call gets its own
// database — tests are fully isolated.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(minimalSchema); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO delivery_destinations (destination_id, provider, name, enabled, created_at, updated_at) VALUES ('primary', 'test', 'Test', 1, '', '')`); err != nil {
		t.Fatalf("seed delivery_destinations: %v", err)
	}
	return db
}

// post048Schema is an explicit, named reference to the post-048
// minimalSchema, used by concurrency tests that need a connection-
// shared in-memory DB. It is intentionally identical to
// minimalSchema today; the alias exists so future readers can spot
// that this schema mirrors the post-048 reality (jobs without the
// runtime columns dropped by migration 048).
const post048Schema = minimalSchema

// openPost048TestDB returns a connection-shared in-memory SQLite so
// concurrent goroutines land on the same underlying DB instance. The
// plain ":memory:" DSN is private to each pooled connection in
// mattn/go-sqlite3, which would silently defeat race tests by giving
// each goroutine a private database. cache=shared mirrors production
// semantics (multiple connections, one logical DB) and is the
// version SQLite itself recommends for race tests.
func openPost048TestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open sqlite (post-048, shared cache): %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(post048Schema); err != nil {
		t.Fatalf("apply post-048 schema: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO delivery_destinations (destination_id, provider, name, enabled, created_at, updated_at) VALUES ('primary', 'test', 'Test', 1, '', '')`); err != nil {
		t.Fatalf("seed delivery_destinations: %v", err)
	}
	return db
}

// newPersistenceStack wires the 2 SQLite writer components behind the
// narrow artifact-package interfaces. Test callers discard via `_` the
// one their test does not exercise.
func newPersistenceStack(db *sql.DB) (artifacts.UploadSessionWriter, artifacts.FinalizationWriter) {
	reader := artifacts.NewSQLiteArtifactReader(db)
	return artifacts.NewSQLiteUploadSessionWriter(db),
		artifacts.NewSQLiteFinalizeWriter(db, reader, nil)
}

// fixture represents a minimal valid scenario:
// RUNNING job, RENDER_FINISHED attempt, STAGING artifact, CREATED upload.
type fixture struct {
	JobID         string
	WorkerID      string
	LeaseID       string
	Revision      int
	AttemptNumber int
	ArtifactID    string
	UploadID      string
}

func setupVerifiedPipelineFixture(t *testing.T, db *sql.DB, f fixture) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec(`INSERT INTO jobs
		(job_id, status, revision, updated_at, migrated_at)
		VALUES (?, 'RUNNING', ?, ?, ?)`,
		f.JobID, f.Revision, now, now); err != nil {
		t.Fatalf("seed job (post-048): %v", err)
	}
	if _, err := db.Exec(`INSERT INTO artifacts
		(id, job_id, attempt_id, type, storage_provider, status, created_at)
		VALUES (?, ?, ?, 'render', 'local', 'STAGING', ?)`,
		f.ArtifactID, f.JobID, f.AttemptNumber, now); err != nil {
		t.Fatalf("seed artifact: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO artifact_uploads
		(upload_id, artifact_id, job_id, attempt_number, worker_id, lease_id,
		 status, created_at, expires_at, completed_at)
		VALUES (?, ?, ?, ?, ?, ?, 'CREATED', ?, ?, NULL)`,
		f.UploadID, f.ArtifactID, f.JobID, f.AttemptNumber,
		f.WorkerID, f.LeaseID, now,
		time.Now().Add(24*time.Hour).UTC().Format(time.RFC3339)); err != nil {
		t.Fatalf("seed upload: %v", err)
	}
}

func flipUploadToFinalizing(t *testing.T, db *sql.DB, uploadID string) {
	t.Helper()
	if _, err := db.Exec(`UPDATE artifact_uploads SET status='FINALIZING' WHERE upload_id=?`, uploadID); err != nil {
		t.Fatalf("flip upload: %v", err)
	}
}

// setupJobAndAttempt seeds a single jobs row only. The original
// helper also intended to seed a task_attempts row, but the
// verification-finalization path does not require it; per-attempt
// identity is verified at the artifact_uploads CAS chain at
// FinalizeVerified time.
//
// migration 048: jobs.assigned_to / lease_id dropped. Per-attempt
// identity lives on task_attempts (canonical); the
// verification-finalization assertions in this package do NOT
// require a task_attempts row at write time. workerID / leaseID
// are unused by this helper post-cleanup but retained in the call
// signature for caller symmetry — arguments are simply ignored.
func setupJobAndAttempt(t *testing.T, db *sql.DB, jobID, workerID, leaseID string, revision, attemptNum int) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec(`INSERT INTO jobs
		(job_id, status, revision, updated_at, migrated_at)
		VALUES (?, 'RUNNING', ?, ?, ?)`,
		jobID, revision, now, now); err != nil {
		t.Fatalf("seed job (post-048): %v", err)
	}
	_ = workerID
	_ = leaseID
	_ = attemptNum
}

// seedPost048JobAndArtifact seeds a post-048 jobs row (no assigned_to /
// lease_id) plus a STAGING artifact and a FINALIZING upload — ready for
// one FinalizeVerified call. Per-attempt identity lives on
// task_attempts (canonical) outside the verified-finalization critical
// section, so this helper does NOT seed task_attempts rows. Worker /
// lease / attempt identity is verified through the artifact_uploads
// CAS chain at FinalizeVerified time.
func seedPost048JobAndArtifact(t *testing.T, db *sql.DB, f fixture) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec(`INSERT INTO jobs
		(job_id, status, revision, updated_at, migrated_at)
		VALUES (?, 'RUNNING', ?, ?, ?)`,
		f.JobID, f.Revision, now, now); err != nil {
		t.Fatalf("seed jobs (post-048): %v", err)
	}
	if _, err := db.Exec(`INSERT INTO artifacts
		(id, job_id, attempt_id, type, storage_provider, status, created_at)
		VALUES (?, ?, ?, 'render', 'local', 'STAGING', ?)`,
		f.ArtifactID, f.JobID, f.AttemptNumber, now); err != nil {
		t.Fatalf("seed artifacts: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO artifact_uploads
		(upload_id, artifact_id, job_id, attempt_number, worker_id, lease_id,
		 status, created_at, expires_at, completed_at)
		VALUES (?, ?, ?, ?, ?, ?, 'FINALIZING', ?, ?, NULL)`,
		f.UploadID, f.ArtifactID, f.JobID, f.AttemptNumber,
		f.WorkerID, f.LeaseID, now,
		time.Now().Add(24*time.Hour).UTC().Format(time.RFC3339)); err != nil {
		t.Fatalf("seed artifact_uploads FINALIZING: %v", err)
	}
}

// Compile-time interface checks for the persistence stack — verifies
// the SQLite writer structs satisfy the narrow artifact-package
// interfaces, before any test runs.
var (
	_ artifacts.UploadSessionWriter = (*artifacts.SQLiteUploadSessionWriter)(nil)
	_ artifacts.FinalizationWriter  = (*artifacts.SQLiteFinalizeWriter)(nil)
	_ artifacts.ArtifactReader      = (*artifacts.SQLiteArtifactReader)(nil)
	_ artifacts.AuthReader          = (*artifacts.SQLiteAuthReader)(nil)
)

// ctxBackground is a shorthand for callers that need a no-deadline
// context inside a test body. Lives here so individual test bodies
// stay terse.
func ctxBackground() context.Context { return context.Background() }
