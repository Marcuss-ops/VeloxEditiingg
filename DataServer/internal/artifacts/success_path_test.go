// Package artifacts / success_path_test.go
//
// Black-box verification of the SPEC invariants that the verified
// finalization contract enforces:
//
//  1. JobResult success alone does NOT promote jobs.status to
//     SUCCEEDED — FinalizationWriter.FinalizeVerified is the ONLY
//     surface producing that transition (no JobResult handler
//     short-circuit on this side of the worker/master trust line).
//
//  2. LifecycleService's Start / Fail / Cancel / Renew paths cannot
//     produce jobs.status='SUCCEEDED'. Verified by absence of any
//     such method in the JobRepository interface and by the
//     finalization scan in scan_test.go (cross-file invariant).
//
//  3. Only the verified-artifact path (status='FINALIZING' upload
//     + receiving sha256 matching master-computed via Receive which
//     already executed before FinalizeVerified is called) promotes
//     jobs.status='SUCCEEDED'.
//
//  4. Without an artifact_uploads row pointing at the job, Finalize
//     refuses with ErrUploadNotFound and the tx rolls back — no
//     SUCCEEDED leak.
//
//  5. An artifact whose upload session status is anything other than
//     'FINALIZING' cannot promote — Step 1's 'FINALIZING only'
//     precondition rejects with ErrUploadStateInvalid.
//
// The "happy path" test (TestFinalizeVerified_HappyPath) covers the
// inverse: with all preconditions met, Finalize produces precisely
// one SUCCEEDED on jobs, READY on artifacts, COMPLETED on the
// upload, and one delivery row per destination — idempotent across
// concurrent finishers per the UNIQUE constraints.
package artifacts_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/artifacts"
)

// minimalSchema covers the columns FinalizeVerified /
// CreateArtifactAndUploadSession actually touch. Migrations are not
// required for this test — we are validating the
// FinalizationWriter in isolation, not the wider store.
//
// migration 048: assigned_to / lease_id / lease_expiry were DROPPED
// from `jobs`. Worker / lease identity for the upload pipeline now
// lives on `task_attempts`. The fixture below reflects that
// contract so the tests above exercise the SAME shape as the real
// store — fixing any future regression where someone re-adds the
// dropped columns to the jobs CAS chain by mistake.
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
	// migration 048: jobs.assigned_to / lease_id columns were
	// dropped; the jobs seed only carries the columns that still
	// exist. Worker / lease identity is seed-attached via the
	// task_attempts INSERT immediately below.
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

// =====================================================================
// SPEC 3: only verified artifact path produces SUCCEEDED
// =====================================================================

func TestFinalizeVerified_HappyPath(t *testing.T) {
	db := openTestDB(t)
	_, fin := newPersistenceStack(db)

	f := fixture{
		JobID: "J1", WorkerID: "worker-1", LeaseID: "lease-1",
		Revision:      7,
		AttemptNumber: 3,
		ArtifactID:    "art-J1-3", UploadID: "up-J1-3",
	}
	setupVerifiedPipelineFixture(t, db, f)
	flipUploadToFinalizing(t, db, f.UploadID)

	ctx := context.Background()
	art, err := fin.FinalizeVerified(ctx, artifacts.FinalizeVerifiedCommand{
		UploadID:         f.UploadID,
		ArtifactID:       f.ArtifactID,
		JobID:            f.JobID,
		WorkerID:         f.WorkerID,
		LeaseID:          f.LeaseID,
		AttemptNumber:    f.AttemptNumber,
		ExpectedRevision: f.Revision,
		StorageProvider:  "local",
		StorageKey:       "artifacts/sha256/deadbeef/J1-3",
		SHA256:           "deadbeef",
		SizeBytes:        1024,
		MIMEType:         "video/mp4",
		VerifiedAt:       time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("FinalizeVerified: %v", err)
	}
	if art == nil || art.Status != "READY" {
		t.Fatalf("artifact post-state wrong: %+v", art)
	}

	// The writer stamps the master-computed SHA256 / size /
	// storage_key / mime_type onto the artifact row before the tx
	// commits. The returned *store.Artifact is what callers should
	// observe, so verify the in-memory view matches what the caller
	// requested. (MimeType is not surfaced in the writer's SELECT; we
	// check it directly on the row below.)
	if art.SHA256 != "deadbeef" {
		t.Errorf("artifact.SHA256 = %q; want %q", art.SHA256, "deadbeef")
	}
	if art.SizeBytes != 1024 {
		t.Errorf("artifact.SizeBytes = %d; want 1024", art.SizeBytes)
	}
	if art.StorageKey != "artifacts/sha256/deadbeef/J1-3" {
		t.Errorf("artifact.StorageKey = %q; want %q",
			art.StorageKey, "artifacts/sha256/deadbeef/J1-3")
	}

	// The row on disk must also have mime_type stamped (writer's SELECT
	// doesn't surface it; verify directly).
	var mime string
	if err := db.QueryRow(`SELECT COALESCE(mime_type,'') FROM artifacts WHERE id=?`, f.ArtifactID).Scan(&mime); err != nil {
		t.Fatal(err)
	}
	if mime != "video/mp4" {
		t.Errorf("artifacts.mime_type = %q; want %q", mime, "video/mp4")
	}

	// jobs.status flipped exactly once to SUCCEEDED.
	var jobStatus string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE job_id=?`, f.JobID).Scan(&jobStatus); err != nil {
		t.Fatal(err)
	}
	if jobStatus != "SUCCEEDED" {
		t.Errorf("jobs.status = %s; want SUCCEEDED", jobStatus)
	}

	// The post-finalize assertion on job_attempts.status='SUCCEEDED'
	// was retired alongside the legacy CAS chain. Per-attempt close-out
	// is driven by the task_attempts layer (canonical). The
	// verified-finalization contract here is
	// [jobs.status='SUCCEEDED', artifact_uploads.status='COMPLETED',
	// artifacts.status='READY'].

	// artifact_uploads COMPLETED.
	var uploadStatus string
	if err := db.QueryRow(`SELECT status FROM artifact_uploads WHERE upload_id=?`, f.UploadID).Scan(&uploadStatus); err != nil {
		t.Fatal(err)
	}
	if uploadStatus != "COMPLETED" {
		t.Errorf("artifact_uploads.status = %s; want COMPLETED", uploadStatus)
	}

	// job_deliveries: exactly one row for (artifact, primary).
	var deliveryCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM job_deliveries
		WHERE artifact_id=? AND destination_id='primary'`, f.ArtifactID).Scan(&deliveryCount); err != nil {
		t.Fatal(err)
	}
	if deliveryCount != 1 {
		t.Errorf("job_deliveries primary count = %d; want 1", deliveryCount)
	}

	// Legacy outbox emissions (ARTIFACT_READY, JOB_SUCCEEDED,
	// DELIVERY_CREATED) are decommissioned. Verify the outbox table
	// is empty — no spurious emissions from the retired path.
	var nOutbox int
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox_events`).Scan(&nOutbox); err != nil {
		t.Fatal(err)
	}
	if nOutbox != 0 {
		t.Errorf("outbox events = %d; want 0 (legacy outbox emissions retired)", nOutbox)
	}
}

// =====================================================================
// SPEC 4: no artifact = no SUCCEEDED
// =====================================================================

func TestFinalizeVerified_NoArtifactUpload(t *testing.T) {
	db := openTestDB(t)
	_, fin := newPersistenceStack(db)

	f := fixture{
		JobID: "J4", WorkerID: "worker-1", LeaseID: "lease-1",
		Revision: 1, AttemptNumber: 1,
		ArtifactID: "missing-art", UploadID: "missing-up",
	}
	// Seed only the job + attempt — no artifact_uploads row.
	setupJobAndAttempt(t, db, f.JobID, f.WorkerID, f.LeaseID, f.Revision, f.AttemptNumber)

	ctx := context.Background()
	_, err := fin.FinalizeVerified(ctx, artifacts.FinalizeVerifiedCommand{
		UploadID:         f.UploadID,
		ArtifactID:       f.ArtifactID,
		JobID:            f.JobID,
		WorkerID:         f.WorkerID,
		LeaseID:          f.LeaseID,
		AttemptNumber:    f.AttemptNumber,
		ExpectedRevision: f.Revision,
	})
	if err == nil {
		t.Fatal("expected error for missing artifact_uploads row")
	}
	if !errors.Is(err, artifacts.ErrUploadNotFound) &&
		!strings.Contains(err.Error(), "not found") {
		t.Errorf("expected ErrUploadNotFound-style error, got %v", err)
	}

	// Verify no row was mutated: jobs.status still RUNNING, no outbox events.
	var jobStatus string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE job_id=?`, f.JobID).Scan(&jobStatus); err != nil {
		t.Fatal(err)
	}
	if jobStatus != "RUNNING" {
		t.Errorf("jobs.status = %s; want RUNNING (tx must roll back on missing upload)", jobStatus)
	}
	var nEvents int
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox_events`).Scan(&nEvents); err != nil {
		t.Fatal(err)
	}
	if nEvents != 0 {
		t.Errorf("outbox events = %d; want 0 (tx rolled back)", nEvents)
	}
}

// =====================================================================
// SPEC 5: STAGING artifact / non-FINALIZING upload = no SUCCEEDED
// =====================================================================

func TestFinalizeVerified_StagingArtifactCannotPromote(t *testing.T) {
	db := openTestDB(t)
	_, fin := newPersistenceStack(db)

	f := fixture{
		JobID: "J5", WorkerID: "worker-1", LeaseID: "lease-1",
		Revision: 1, AttemptNumber: 1,
		ArtifactID: "art-J5-still-staging", UploadID: "up-J5-still-staging",
	}
	// Note: artifact_uploads.status remains 'CREATED' (NOT flipped to
	// FINALIZING) — emulates the orchestration gap where the
	// pipeline did NOT complete Receive.
	setupVerifiedPipelineFixture(t, db, f)
	// Override: leave upload in CREATED, NOT FINALIZING.
	if _, err := db.Exec(`UPDATE artifact_uploads SET status='CREATED' WHERE upload_id=?`, f.UploadID); err != nil {
		t.Fatal(err)
	}

	_, err := fin.FinalizeVerified(context.Background(), artifacts.FinalizeVerifiedCommand{
		UploadID:         f.UploadID,
		ArtifactID:       f.ArtifactID,
		JobID:            f.JobID,
		WorkerID:         f.WorkerID,
		LeaseID:          f.LeaseID,
		AttemptNumber:    f.AttemptNumber,
		ExpectedRevision: f.Revision,
	})
	if err == nil {
		t.Fatal("expected error when artifact_uploads.status != FINALIZING")
	}
	if !errors.Is(err, artifacts.ErrUploadStateInvalid) {
		t.Errorf("expected ErrUploadStateInvalid; got %v", err)
	}

	// jobs.status untouched.
	var jobStatus string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE job_id=?`, f.JobID).Scan(&jobStatus); err != nil {
		t.Fatal(err)
	}
	if jobStatus != "RUNNING" {
		t.Errorf("jobs.status = %s; want RUNNING (STAGING artifact must not promote)", jobStatus)
	}
}

// =====================================================================
// The legacy job_attempts.status='RENDER_FINISHED' gate was retired;
// the corresponding Step 4 job_attempts CAS no longer exists in
// FinalizeVerified. Per-attempt close-out lives on task_attempts
// (canonical). The transaction rollback still holds for any of the
// surviving preconditions:
//   - artifact_uploads.status != FINALIZING (Step 1: ErrUploadStateInvalid)
//   - jobs.status not in (RUNNING, AWAITING_ARTIFACT) (Step 2: ErrTransitionConflict)
//   - artifacts.status != STAGING (Step 3: ErrTransitionConflict)
// so the rollback invariants are preserved.
// (TestFinalizeVerified_AttemptNotRenderFinishedRollsBack retired
// alongside the job_attempts CAS chain. See comment above.)
// =====================================================================

// =====================================================================
// Guard: empty-identity calls are rejected before any DB work.
// =====================================================================

func TestFinalizeVerified_RequiresUploadArtifactJobIDs(t *testing.T) {
	db := openTestDB(t)
	_, fin := newPersistenceStack(db)
	ctx := context.Background()

	cases := []artifacts.FinalizeVerifiedCommand{
		{},                                   // nothing set
		{UploadID: "up-x"},                   // missing artifact + job
		{UploadID: "up-x", ArtifactID: "ax"}, // missing job
	}
	for _, c := range cases {
		if _, err := fin.FinalizeVerified(ctx, c); err == nil {
			t.Errorf("expected identity-payload error for %+v", c)
		}
	}

	// No DB state must have changed.
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM jobs`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("jobs rows = %d; want 0 (early reject)", n)
	}
}

// =====================================================================
// CreateArtifactAndUploadSession — happy path & missing identity.
// =====================================================================

func TestCreateArtifactAndUploadSession_Atomic(t *testing.T) {
	db := openTestDB(t)
	uw, _ := newPersistenceStack(db)

	err := uw.CreateArtifactAndUploadSession(context.Background(),
		artifacts.CreateArtifactAndUploadSessionCommand{
			ArtifactID:    "art-c1",
			UploadID:      "up-c1",
			JobID:         "J-c1",
			Kind:          "render",
			WorkerID:      "worker-1",
			LeaseID:       "lease-1",
			AttemptNumber: 1,
		})
	if err != nil {
		t.Fatalf("CreateArtifactAndUploadSession: %v", err)
	}

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM artifacts WHERE id='art-c1' AND status='STAGING'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("artifact STAGING row = %d; want 1", n)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM artifact_uploads WHERE upload_id='up-c1' AND status='CREATED'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("artifact_uploads CREATED row = %d; want 1", n)
	}
}

func TestCreateArtifactAndUploadSession_RequiresIdentityFields(t *testing.T) {
	db := openTestDB(t)
	uw, _ := newPersistenceStack(db)

	if err := uw.CreateArtifactAndUploadSession(context.Background(),
		artifacts.CreateArtifactAndUploadSessionCommand{}); err == nil {
		t.Error("expected error for empty command")
	}
}

// =====================================================================
// Constructor guards.
// =====================================================================

func TestNewSQLiteArtifactComponentsPanicOnNilDB(t *testing.T) {
	cases := []struct {
		name string
		fn   func()
	}{
		{"ArtifactReader", func() { artifacts.NewSQLiteArtifactReader(nil) }},
		{"UploadSessionWriter", func() { artifacts.NewSQLiteUploadSessionWriter(nil) }},
		{"FinalizeWriter_NilDB", func() {
			r, err := sql.Open("sqlite3", ":memory:")
			if err != nil {
				t.Fatalf("open in-memory sqlite: %v", err)
			}
			defer r.Close()
			artifacts.NewSQLiteFinalizeWriter(nil, artifacts.NewSQLiteArtifactReader(r), nil)
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if rec := recover(); rec == nil {
					t.Errorf("[%s] expected panic on nil *sql.DB; constructor must fail loudly", c.name)
				}
			}()
			c.fn()
		})
	}
}

// =====================================================================
// Helpers
// =====================================================================

func setupJobAndAttempt(t *testing.T, db *sql.DB, jobID, workerID, leaseID string, revision, attemptNum int) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	// migration 048: jobs.assigned_to / lease_id dropped. Per-attempt
	// identity now lives on task_attempts (canonical); the
	// verification-finalization assertions in this package no longer
	// depend on a row in job_attempts. workerID / leaseID are unused
	// by this helper post-cleanup but retained in the call signature
	// for caller symmetry with newer helpers — arguments are simply
	// ignored.
	if _, err := db.Exec(`INSERT INTO jobs
		(job_id, status, revision, updated_at, migrated_at)
		VALUES (?, 'RUNNING', ?, ?, ?)`,
		jobID, revision, now, now); err != nil {
		t.Fatalf("seed job (post-048): %v", err)
	}
}

// Compile-time interface checks.
var (
	_ artifacts.UploadSessionWriter    = (*artifacts.SQLiteUploadSessionWriter)(nil)
	_ artifacts.FinalizationWriter     = (*artifacts.SQLiteFinalizeWriter)(nil)
	_ artifacts.ArtifactReader         = (*artifacts.SQLiteArtifactReader)(nil)
)

// =====================================================================
// post-migration-048 behavior — the existing tests above use the
// post-048 minimalSchema; the tests below additionally prove the CAS
// chain holds BOTH for a sequential re-finalize attempt (correctly
// rejects with ErrUploadStateInvalid) AND for two concurrent finalizers
// (exactly one wins, exactly one delivery row inserted).
// =====================================================================

// post048Schema is an explicit, named copy of minimalSchema used by the
// post-048-specific tests. It is intentionally identical to
// minimalSchema today but is named so that future readers can spot
// that this schema mirrors the post-048 reality (jobs without the
// runtime columns dropped by migration 048).
const post048Schema = minimalSchema

func openPost048TestDB(t *testing.T) *sql.DB {
	t.Helper()
	// migration 048: use a connection-shared in-memory DSN so concurrent
	// goroutines land on the same underlying DB instance. The plain
	// ":memory:" is private to each pooled connection in
	// mattn/go-sqlite3, which would silently defeat
	// TestArtifactFinalize_Post048RejectsConcurrentFinalize by giving
	// each goroutine a private database. cache=shared mirrors
	// production semantics (multiple connections, one logical DB) and
	// is the version SQLite itself recommends for race tests.
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

// seedPost048JobAndArtifact seeds a post-048 jobs row (no assigned_to /
// lease_id) plus a STAGING artifact and a FINALIZING upload — ready for
// one FinalizeVerified call. The historical job_attempts INSERT that
// lived here was retired because per-attempt identity is now the
// task_attempts canonical layer. Verification of worker/lease/attempt
// identity in this fixture is implicit — the orchestrator-driven
// FinalizeVerified path trusts the artifact_uploads CAS chain.
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

// TestArtifactFinalize_Post048SchemaIdempotent verifies:
//  1. FinalizeVerified runs cleanly against a post-migration-048
//     schema (jobs without assigned_to / lease_id / lease_expiry).
//  2. A second finalize attempt with matching worker / lease / revision
//     is correctly rejected with ErrUploadStateInvalid because step 1
//     preconditions fail (artifact_uploads is now COMPLETED, not
//     FINALIZING). This confirms the idempotency boundary holds after
//     migration 048 even when the caller's stale view happens to match.
//  3. Exactly one delivery row is inserted (UNIQUE (artifact_id, destination_id)).
func TestArtifactFinalize_Post048SchemaIdempotent(t *testing.T) {
	db := openPost048TestDB(t)
	_, fin := newPersistenceStack(db)

	f := fixture{
		JobID: "J-post-048", WorkerID: "worker-7", LeaseID: "lease-7",
		Revision:      4,
		AttemptNumber: 2,
		ArtifactID:    "art-post-048", UploadID: "up-post-048",
	}
	seedPost048JobAndArtifact(t, db, f)

	ctx := context.Background()
	art, err := fin.FinalizeVerified(ctx, artifacts.FinalizeVerifiedCommand{
		UploadID:         f.UploadID,
		ArtifactID:       f.ArtifactID,
		JobID:            f.JobID,
		WorkerID:         f.WorkerID,
		LeaseID:          f.LeaseID,
		AttemptNumber:    f.AttemptNumber,
		ExpectedRevision: f.Revision,
		StorageProvider:  "local",
		StorageKey:       "artifacts/sha256/post048/J-post-048-2",
		SHA256:           "post048cafe",
		SizeBytes:        2048,
		MIMEType:         "video/mp4",
		VerifiedAt:       time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("first FinalizeVerified (post-048): %v", err)
	}
	if art == nil || art.Status != "READY" {
		t.Fatalf("artifact post-state wrong: %+v", art)
	}

	var jobStatus string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE job_id=?`, f.JobID).Scan(&jobStatus); err != nil {
		t.Fatal(err)
	}
	if jobStatus != "SUCCEEDED" {
		t.Fatalf("jobs.status after first finalize = %s; want SUCCEEDED", jobStatus)
	}

	// Second finalize: re-issue with matching worker / lease /
	// original revision. ExpectedRevision=0 disables step 2's revision
	// CAS so the only gate being asserted here is step 1's
	// artifact_uploads status='FINALIZING' precondition — which step 7
	// broke by flipping the upload to COMPLETED. Cleaner intent: the
	// idempotency boundary under test is the artifact_uploads lock, not
	// the jobs revision.
	if _, err := fin.FinalizeVerified(ctx, artifacts.FinalizeVerifiedCommand{
		UploadID:      f.UploadID,
		ArtifactID:    f.ArtifactID,
		JobID:         f.JobID,
		WorkerID:      f.WorkerID,
		LeaseID:       f.LeaseID,
		AttemptNumber: f.AttemptNumber,
	}); err == nil {
		t.Fatal("expected ErrUploadStateInvalid on second finalize, got nil")
	} else if !errors.Is(err, artifacts.ErrUploadStateInvalid) {
		t.Errorf("expected ErrUploadStateInvalid on second finalize; got %v", err)
	}

	var deliveryCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM job_deliveries
		WHERE artifact_id=? AND destination_id='primary'`, f.ArtifactID).Scan(&deliveryCount); err != nil {
		t.Fatal(err)
	}
	if deliveryCount != 1 {
		t.Errorf("job_deliveries primary count = %d; want 1 (UNIQUE on (artifact_id, destination_id))", deliveryCount)
	}
}

// TestArtifactFinalize_Post048RejectsConcurrentFinalize confirms that
// two goroutines racing to FinalizeVerified against the same job /
// artifact produce exactly ONE SUCCEEDED row + ONE delivery row.
//
// The jobs CAS (status='RUNNING' only — post-048 identity-free) +
// the upload CAS (FINALIZING + worker + lease + attempt) +
// the UNIQUE (artifact_id, destination_id) on job_deliveries together
// serialize concurrent finishers without producing partial state.
//
// This is the explicit post-migration-048 version of the design-doc
// requirement "race test due finalize concorrenti (atteso un solo
// SUCCEEDED)".
func TestArtifactFinalize_Post048RejectsConcurrentFinalize(t *testing.T) {
	db := openPost048TestDB(t)
	_, fin := newPersistenceStack(db)

	f := fixture{
		JobID: "J-race", WorkerID: "worker-race", LeaseID: "lease-race",
		Revision:      1,
		AttemptNumber: 1,
		ArtifactID:    "art-race", UploadID: "up-race",
	}
	seedPost048JobAndArtifact(t, db, f)

	type outcome struct {
		err error
	}
	results := make(chan outcome, 2)
	ctx := context.Background()
	cmd := artifacts.FinalizeVerifiedCommand{
		UploadID:         f.UploadID,
		ArtifactID:       f.ArtifactID,
		JobID:            f.JobID,
		WorkerID:         f.WorkerID,
		LeaseID:          f.LeaseID,
		AttemptNumber:    f.AttemptNumber,
		ExpectedRevision: f.Revision,
		StorageProvider:  "local",
		StorageKey:       "artifacts/sha256/race",
		SHA256:           "racehash",
		SizeBytes:        4096,
		MIMEType:         "video/mp4",
		VerifiedAt:       time.Now().UTC(),
	}
	for i := 0; i < 2; i++ {
		go func() {
			_, err := fin.FinalizeVerified(ctx, cmd)
			results <- outcome{err: err}
		}()
	}

	var successes int
	for i := 0; i < 2; i++ {
		r := <-results
		if r.err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent finalize successes = %d; want exactly 1", successes)
	}

	var jobStatus string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE job_id=?`, f.JobID).Scan(&jobStatus); err != nil {
		t.Fatal(err)
	}
	if jobStatus != "SUCCEEDED" {
		t.Errorf("jobs.status = %s; want SUCCEEDED (one of two finalizers won)", jobStatus)
	}
	var succ int
	if err := db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE job_id=? AND status='SUCCEEDED'`, f.JobID).Scan(&succ); err != nil {
		t.Fatal(err)
	}
	if succ != 1 {
		t.Errorf("SUCCEEDED rows on jobs = %d; want 1 (no double promotion)", succ)
	}
	var deliveryCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM job_deliveries
		WHERE artifact_id=? AND destination_id='primary'`, f.ArtifactID).Scan(&deliveryCount); err != nil {
		t.Fatal(err)
	}
	if deliveryCount != 1 {
		t.Errorf("job_deliveries primary count = %d; want 1 (no double-deliver)", deliveryCount)
	}
}
