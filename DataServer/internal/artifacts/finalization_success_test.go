// Package artifacts / finalization_success_test.go
//
// Happy-path success tests for the verified-finalization contract.
// Each test asserts a single positive invariant: with all
// preconditions met, FinalizeVerified produces precisely one
// SUCCEEDED jobs row, one READY artifact, one COMPLETED upload, and
// one delivery row per destination — idempotent across retries that
// arrive after the prior tx committed.
package artifacts_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"velox-server/internal/artifacts"
)

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

	// Verified-finalization contract (post-048):
	//   jobs.status='SUCCEEDED' (single source of truth; CAS in
	//     FinalizeVerified).
	//   artifacts.status='READY' (CAS in the same tx).
	//   artifact_uploads.status='COMPLETED' (in-tx flip in the
	//     Finalizing step inside FinalizeVerified).
	//   Per-destination job_deliveries row (UNIQUE on
	//     (artifact_id, destination_id) makes the dup no-op).
	// Per-attempt close-out lives on task_attempts (canonical)
	// outside this critical section.

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
