// Package artifacts / finalization_rejection_test.go
//
// Rejection + identity-guard tests for the verified-finalization
// contract. Each test asserts a single negative invariant: the
// FinalizationWriter / UploadSessionWriter MUST reject illegal
// transitions or invalid payloads, and the constructor guards MUST
// fail loudly on a misconfigured dependency.
package artifacts_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/artifacts"
)

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
	// Seed only the job — no artifact_uploads row.
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
// post-048 finalization boundaries (per-attempt close-out on task_attempts):
//
// Per-attempt close-out lives on task_attempts (canonical), driven by
// the task-attempts lifecycle outside the verified-finalization
// critical section. The transaction rollback holds for any of the
// surviving preconditions:
//   - artifact_uploads.status != FINALIZING (Step 1: ErrUploadStateInvalid)
//   - jobs.status not in (RUNNING, AWAITING_ARTIFACT) (Step 2: ErrTransitionConflict)
//   - artifacts.status != STAGING (Step 3: ErrTransitionConflict)
// (No per-attempt status gate is asserted here; that ownership
// moved to the task-attempts layer post-migration 048.)
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
			r, err := openSharedMemory()
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

// openSharedMemory is a tiny helper for the NilDB test path that
// needs a valid reader. It returns a cheap shared-cache in-memory DB
// so the constructor under test panics on the (nil) primary arg
// before any DB work happens.
func openSharedMemory() (*sql.DB, error) {
	return sql.Open("sqlite3", "file::memory:?cache=shared&_busy_timeout=5000")
}
