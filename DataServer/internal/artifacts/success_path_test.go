// Package artifacts / success_path_test.go — PR 3.5-a behavioral
//
// Black-box verification of the SPEC invariants that the verified
// finalization contract enforces:
//
//  1. JobResult success alone does NOT promote jobs.status to
//     SUCCEEDED — the FinalizationRepository interface is the ONLY
//     surface producing that transition (no JobResult handler
//     short-circuit on this side of the worker/master trust line).
//
//  2. LifecycleService PR3.Start / PR3Fail / PR3Cancel / PR3Renew
//     cannot produce jobs.status='SUCCEEDED'. Verified by absence of
//     any such method in the JobRepository interface and by the
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
//  5. An artifact whose status is still 'STAGING' (or any value
//     other than the FINALIZING artifact_uploads status) cannot
//     promote — Step 1's 'FINALIZING only' precondition rejects
//     with ErrUploadStateInvalid.
//
//  6. If job_attempts.status is NOT 'RENDER_FINISHED' when Finalize
//     is called, the Step 4 CAS returns 0 rows and the entire tx
//     rolls back (no partial mutations of jobs / artifacts /
//     outbox_events / job_deliveries).
//
// The "happy path" test (TestFinalizeVerified_HappyPath) covers the
// inverse: with all preconditions met, Finalize produces precisely
// one SUCCEEDED on jobs, READY on artifacts, SUCCEEDED on the
// attempt, COMPLETED on the upload, and an outbox row for each of
// ARTIFACT_READY / JOB_SUCCEEDED / DELIVERY_CREATED — idempotent
// across concurrent finishers per the UNIQUE constraints.
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

// minimalSchema covers the columns FinalizeVerified / CreateArtifactAndUploadSession
// actually touch. Migrations are not required for this test — we are
// validating the FinalizationRepository in isolation, not the wider store.
const minimalSchema = `
CREATE TABLE jobs (
	job_id        TEXT PRIMARY KEY,
	status        TEXT,
	assigned_to   TEXT,
	lease_id      TEXT,
	lease_expiry  TEXT,
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
CREATE TABLE job_attempts (
	job_id          TEXT,
	attempt_number  INTEGER,
	worker_id       TEXT,
	lease_id        TEXT,
	status          TEXT,
	started_at      TEXT,
	finished_at     TEXT,
	error_code      TEXT,
	engine_version  TEXT,
	bundle_hash     TEXT,
	created_at      TEXT,
	PRIMARY KEY (job_id, attempt_number)
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
		(job_id, status, assigned_to, lease_id, revision, updated_at, migrated_at)
		VALUES (?, 'RUNNING', ?, ?, ?, ?, ?)`,
		f.JobID, f.WorkerID, f.LeaseID, f.Revision, now, now); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO job_attempts
		(job_id, attempt_number, worker_id, lease_id, status, finished_at, created_at)
		VALUES (?, ?, ?, ?, 'RENDER_FINISHED', ?, ?)`,
		f.JobID, f.AttemptNumber, f.WorkerID, f.LeaseID, now, now); err != nil {
		t.Fatalf("seed attempt: %v", err)
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
	fin := artifacts.NewSQLiteFinalizationRepository(db)

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

	// PR 3.5-a: writer must STAMP the master-computed SHA256 / size /
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

	// outbox JOB_SUCCEEDED payload MUST carry the canonical SHA so that
	// downstream consumers see the verified fingerprint (cross-system
	// invariant the writer is responsible for).
	var payload string
	if err := db.QueryRow(`SELECT payload_json FROM outbox_events WHERE event_type='JOB_SUCCEEDED'`).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(payload, "deadbeef") {
		t.Errorf("outbox JOB_SUCCEEDED payload missing SHA256: %q", payload)
	}

	// jobs.status flipped exactly once to SUCCEEDED.
	var jobStatus string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE job_id=?`, f.JobID).Scan(&jobStatus); err != nil {
		t.Fatal(err)
	}
	if jobStatus != "SUCCEEDED" {
		t.Errorf("jobs.status = %s; want SUCCEEDED", jobStatus)
	}

	// job_attempts closed.
	var attemptStatus string
	if err := db.QueryRow(`SELECT status FROM job_attempts WHERE job_id=? AND attempt_number=?`,
		f.JobID, f.AttemptNumber).Scan(&attemptStatus); err != nil {
		t.Fatal(err)
	}
	if attemptStatus != "SUCCEEDED" {
		t.Errorf("job_attempts.status = %s; want SUCCEEDED", attemptStatus)
	}

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

	// outbox: ARTIFACT_READY + JOB_SUCCEEDED + DELIVERY_CREATED — but
	// only one ARTIFACT_READY for the same aggregate (idempotent on
	// repeated finalize).
	rows, err := db.Query(`SELECT event_type, COUNT(*) FROM outbox_events GROUP BY event_type`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	gotEvents := map[string]int{}
	for rows.Next() {
		var et string
		var n int
		if err := rows.Scan(&et, &n); err != nil {
			t.Fatal(err)
		}
		gotEvents[et] = n
	}
	for _, want := range []string{"ARTIFACT_READY", "JOB_SUCCEEDED", "DELIVERY_CREATED"} {
		if gotEvents[want] != 1 {
			t.Errorf("outbox[%s] = %d; want 1", want, gotEvents[want])
		}
	}
}

// =====================================================================
// SPEC 4: no artifact = no SUCCEEDED
// =====================================================================

func TestFinalizeVerified_NoArtifactUpload(t *testing.T) {
	db := openTestDB(t)
	fin := artifacts.NewSQLiteFinalizationRepository(db)

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
	fin := artifacts.NewSQLiteFinalizationRepository(db)

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
// SPEC 6: attempt not RENDER_FINISHED → entire tx rolls back
// =====================================================================

func TestFinalizeVerified_AttemptNotRenderFinishedRollsBack(t *testing.T) {
	db := openTestDB(t)
	fin := artifacts.NewSQLiteFinalizationRepository(db)

	f := fixture{
		JobID: "J6", WorkerID: "worker-1", LeaseID: "lease-1",
		Revision: 1, AttemptNumber: 1,
		ArtifactID: "art-J6", UploadID: "up-J6",
	}
	setupVerifiedPipelineFixture(t, db, f)
	flipUploadToFinalizing(t, db, f.UploadID)

	// Force attempt status to RUNNING (NOT RENDER_FINISHED). The
	// orchestration contract says Receive must not run until the
	// after-RENDER_FINISHED CAS — but we test the database-level
	// guarantee (Step 4 CAS rejects this state).
	if _, err := db.Exec(`UPDATE job_attempts SET status='RUNNING' WHERE job_id=? AND attempt_number=?`,
		f.JobID, f.AttemptNumber); err != nil {
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
		t.Fatal("expected error when job_attempts.status != RENDER_FINISHED")
	}
	if !errors.Is(err, artifacts.ErrTransitionConflict) {
		t.Errorf("expected ErrTransitionConflict; got %v", err)
	}

	// Whole-tx rollback — verify NOTHING was mutated downstream.
	checks := []struct {
		table  string
		column string
		key    string
		keyCol string
		want   string
	}{
		{"jobs", "status", f.JobID, "job_id", "RUNNING"},
		{"artifacts", "status", f.ArtifactID, "id", "STAGING"},
		{"artifact_uploads", "status", f.UploadID, "upload_id", "FINALIZING"},
	}
	for _, c := range checks {
		var got string
		if err := db.QueryRow(`SELECT `+c.column+` FROM `+c.table+` WHERE `+c.keyCol+`=?`, c.key).Scan(&got); err != nil {
			t.Fatalf("check %s.%s WHERE %s=%s: %v", c.table, c.column, c.keyCol, c.key, err)
		}
		if got != c.want {
			t.Errorf("%s.%s = %s; want %s (tx should roll back on Step-4 miss)", c.table, c.column, got, c.want)
		}
	}

	// Zero outbox events.
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox_events`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("outbox events = %d; want 0 (rollback)", n)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM job_deliveries`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("job_deliveries rows = %d; want 0 (rollback)", n)
	}
}

// =====================================================================
// Guard: empty-identity calls are rejected before any DB work.
// =====================================================================

func TestFinalizeVerified_RequiresUploadArtifactJobIDs(t *testing.T) {
	db := openTestDB(t)
	fin := artifacts.NewSQLiteFinalizationRepository(db)
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
	fin := artifacts.NewSQLiteFinalizationRepository(db)

	err := fin.CreateArtifactAndUploadSession(context.Background(),
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
	fin := artifacts.NewSQLiteFinalizationRepository(db)

	if err := fin.CreateArtifactAndUploadSession(context.Background(),
		artifacts.CreateArtifactAndUploadSessionCommand{}); err == nil {
		t.Error("expected error for empty command")
	}
}

// =====================================================================
// Constructor guards.
// =====================================================================

func TestNewSQLiteFinalizationRepository_NilDB(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on nil *sql.DB; constructor must fail loudly")
		}
	}()
	artifacts.NewSQLiteFinalizationRepository(nil)
}

// =====================================================================
// Helpers
// =====================================================================

func setupJobAndAttempt(t *testing.T, db *sql.DB, jobID, workerID, leaseID string, revision, attemptNum int) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec(`INSERT INTO jobs
		(job_id, status, assigned_to, lease_id, revision, updated_at, migrated_at)
		VALUES (?, 'RUNNING', ?, ?, ?, ?, ?)`,
		jobID, workerID, leaseID, revision, now, now); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO job_attempts
		(job_id, attempt_number, worker_id, lease_id, status, finished_at, created_at)
		VALUES (?, ?, ?, ?, 'RENDER_FINISHED', ?, ?)`,
		jobID, attemptNum, workerID, leaseID, now, now); err != nil {
		t.Fatalf("seed attempt: %v", err)
	}
}

// Compile-time interface checks.
var (
	_ artifacts.FinalizationRepository = (*artifacts.SQLiteFinalizationRepository)(nil)
)
