// Package completion / coordinator_complete_upload_test.go
//
// Per-phase split (declare / progress / complete-upload / commit /
// reconcile) extracted from coordinator_test.go. This file owns the
// CompleteUpload phase — the worker's "bytes transferred" signal that
// verifies the worker-supplied SHA against the master-declared
// expected_sha256, advances artifact_uploads → COMPLETED + artifacts
// STAGING/VERIFYING → READY|VERIFYING in one tx, and bumps
// attempt_commits.ready_output_count.
//
// Coverage is the Verdetto P0 #5 four-branch authoritative-SHA gate:
//
//	A. ServerSHA="" AND effectiveExpected=""  -> artifact stays VERIFYING
//	B. ServerSHA="" AND effectiveExpected!="" -> artifact stays VERIFYING
//	C. ServerSHA matches effectiveExpected     -> artifact STAGING/VERIFYING -> READY
//	D. ServerSHA!="" AND differs               -> ErrStaleReport (no row change)
package completion

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// ────────────────────────────────────────────────────────────────────
// Verdetto P0 #5: ServerSHA256 authoritative gate for CompleteUpload.
//
// Four branches must be exercised end-to-end against the
// artifact_uploads + artifacts schema:
//   A. ServerSHA="" AND effectiveExpected=""  -> artifact stays VERIFYING
//   B. ServerSHA="" AND effectiveExpected!="" -> artifact stays VERIFYING
//   C. ServerSHA matches effectiveExpected     -> artifact STAGING/VERIFYING -> READY
//   D. ServerSHA!="" AND differs               -> ErrStaleReport (no row change)
// ────────────────────────────────────────────────────────────────────

func TestCoordinator_CompleteUpload_BranchA_NoServerSHA_NoExpected_StaysVerifying(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db)
	fence := validFence("task-branch-a", "attempt-branch-a")

	// Seed artifact + upload without setting expected_sha256.
	seedCompleteUploadFixture(t, db, "up-branch-a", "art-branch-a", "job-branch-a", "")
	// DeclareOutputs is required for the Fence.Read gate.
	if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence: fence, JobID: "job-branch-a", OutputManifests: []OutputManifest{
			{OutputKind: "final_video", LogicalName: "out.mp4",
				MimeType: "video/mp4", SizeBytes: 1024,
				SHA256: strings.Repeat("a", 64)},
		},
	}); err != nil {
		t.Fatalf("DeclareOutputs: %v", err)
	}

	if err := c.CompleteUpload(context.Background(), CompleteUploadCommand{
		Fence:        fence,
		UploadID:     "up-branch-a",
		WorkerSHA256: strings.Repeat("a", 64),
		ServerSHA256: "", // Branch A
	}); err != nil {
		t.Fatalf("Branch A CompleteUpload: %v", err)
	}
	if got := readArtifactStatus(t, db, "art-branch-a"); got != "VERIFYING" {
		t.Errorf("Branch A artifact.status: got %q, want VERIFYING (no master SHA, no declarative SHA)", got)
	}
}

func TestCoordinator_CompleteUpload_BranchB_NoServerSHA_HasExpected_StaysVerifying(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db)
	fence := validFence("task-branch-b", "attempt-branch-b")
	expected := strings.Repeat("b", 64)
	seedCompleteUploadFixture(t, db, "up-branch-b", "art-branch-b", "job-branch-b", expected)
	if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence: fence, JobID: "job-branch-b", OutputManifests: []OutputManifest{
			{OutputKind: "final_video", LogicalName: "out.mp4",
				MimeType: "video/mp4", SizeBytes: 1024,
				SHA256: expected},
		},
	}); err != nil {
		t.Fatalf("DeclareOutputs: %v", err)
	}

	if err := c.CompleteUpload(context.Background(), CompleteUploadCommand{
		Fence:        fence,
		UploadID:     "up-branch-b",
		WorkerSHA256: expected,
		ServerSHA256: "", // Branch B
	}); err != nil {
		t.Fatalf("Branch B CompleteUpload: %v", err)
	}
	if got := readArtifactStatus(t, db, "art-branch-b"); got != "VERIFYING" {
		t.Errorf("Branch B artifact.status: got %q, want VERIFYING (no master SHA despite declarative SHA)", got)
	}
}

func TestCoordinator_CompleteUpload_BranchC_ServerSHAMatch_PromotesToReady(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db)
	fence := validFence("task-branch-c", "attempt-branch-c")
	expected := strings.Repeat("c", 64)
	seedCompleteUploadFixture(t, db, "up-branch-c", "art-branch-c", "job-branch-c", expected)
	if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence: fence, JobID: "job-branch-c", OutputManifests: []OutputManifest{
			{OutputKind: "final_video", LogicalName: "out.mp4",
				MimeType: "video/mp4", SizeBytes: 1024,
				SHA256: expected},
		},
	}); err != nil {
		t.Fatalf("DeclareOutputs: %v", err)
	}

	if err := c.CompleteUpload(context.Background(), CompleteUploadCommand{
		Fence:        fence,
		UploadID:     "up-branch-c",
		WorkerSHA256: expected,
		ServerSHA256: expected, // Branch C (match)
	}); err != nil {
		t.Fatalf("Branch C CompleteUpload: %v", err)
	}
	if got := readArtifactStatus(t, db, "art-branch-c"); got != "READY" {
		t.Errorf("Branch C artifact.status: got %q, want READY (server SHA matches declarative)", got)
	}
	// received_sha256 must be the server-derived SHA, NOT the
	// worker self-report. This is the canonical ledger entry; if
	// the worker ever wrote it independently the ledger would be
	// forgeable.
	var receivedSHA string
	if err := db.QueryRow(`SELECT received_sha256 FROM artifact_uploads WHERE upload_id = ?`,
		"up-branch-c").Scan(&receivedSHA); err != nil {
		t.Fatalf("read received_sha256: %v", err)
	}
	if receivedSHA != expected {
		t.Errorf("artifact_uploads.received_sha256: got %q, want %q (master-derived only, never worker self-report)",
			receivedSHA, expected)
	}
}

func TestCoordinator_CompleteUpload_BranchD_ServerSHAMismatch_ErrStaleReport(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db)
	fence := validFence("task-branch-d", "attempt-branch-d")
	expected := strings.Repeat("d", 64)
	other := strings.Repeat("e", 64)
	seedCompleteUploadFixture(t, db, "up-branch-d", "art-branch-d", "job-branch-d", expected)
	if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence: fence, JobID: "job-branch-d", OutputManifests: []OutputManifest{
			{OutputKind: "final_video", LogicalName: "out.mp4",
				MimeType: "video/mp4", SizeBytes: 1024,
				SHA256: expected},
		},
	}); err != nil {
		t.Fatalf("DeclareOutputs: %v", err)
	}

	err := c.CompleteUpload(context.Background(), CompleteUploadCommand{
		Fence:        fence,
		UploadID:     "up-branch-d",
		WorkerSHA256: expected,
		ServerSHA256: other, // Branch D (mismatch)
	})
	if !errors.Is(err, ErrStaleReport) {
		t.Fatalf("Branch D CompleteUpload: expected ErrStaleReport, got %v", err)
	}
	// Branch D must roll back: artifact stays STAGING (no
	// advancement), artifact_uploads stays RECEIVED.
	if got := readArtifactStatus(t, db, "art-branch-d"); got != "STAGING" {
		t.Errorf("Branch D artifact.status after rollback: got %q, want STAGING (rollback preserves pre-call state)", got)
	}
	var upStatus string
	if err := db.QueryRow(`SELECT status FROM artifact_uploads WHERE upload_id = ?`,
		"up-branch-d").Scan(&upStatus); err != nil {
		t.Fatalf("read artifact_uploads status: %v", err)
	}
	if upStatus != "RECEIVED" {
		t.Errorf("Branch D artifact_uploads.status after rollback: got %q, want RECEIVED", upStatus)
	}
}
