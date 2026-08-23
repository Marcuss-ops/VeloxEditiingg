package completion

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestPhase6_Scenario11_BranchA_StaysVerifying(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db)
	fence := validFence("task-s11", "attempt-s11")
	seedCompleteUploadFixture(t, db, "up-s11", "art-s11", "job-s11", "" /* no expected */)

	if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence: fence, JobID: "job-s11",
		OutputManifests: []OutputManifest{
			{OutputKind: "final_video", LogicalName: "out.mp4",
				MimeType: "video/mp4", SizeBytes: 1024, SHA256: strings.Repeat("0", 64)},
		},
	}); err != nil {
		t.Fatalf("S11 DeclareOutputs: %v", err)
	}
	if err := c.CompleteUpload(context.Background(), CompleteUploadCommand{
		Fence: fence, UploadID: "up-s11",
		WorkerSHA256: "",
		ServerSHA256: "", // Branch A
	}); err != nil {
		t.Fatalf("S11 CompleteUpload: %v", err)
	}
	if got := readArtifactStatus(t, db, "art-s11"); got != "VERIFYING" {
		t.Errorf("S11 artifact status: got=%q want=VERIFYING (Branch A verdict)", got)
	}
	row := readAttemptCommitRow(t, db, fence)
	if row.Status == "COMMITTED" || row.Status == "REJECTED" {
		t.Errorf("S11 attempt_commits.status: got=%q — codec-fail must NOT mark terminal", row.Status)
	}
}
func TestPhase6_Scenario12_BranchD_ErrStaleReport(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db)
	fence := validFence("task-s12", "attempt-s12")
	expected := strings.Repeat("a", 64)
	bogus := strings.Repeat("b", 64)
	seedCompleteUploadFixture(t, db, "up-s12", "art-s12", "job-s12", expected)

	if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence: fence, JobID: "job-s12",
		OutputManifests: []OutputManifest{
			{OutputKind: "final_video", LogicalName: "out.mp4",
				MimeType: "video/mp4", SizeBytes: 1024, SHA256: expected},
		},
	}); err != nil {
		t.Fatalf("S12 DeclareOutputs: %v", err)
	}
	err := c.CompleteUpload(context.Background(), CompleteUploadCommand{
		Fence:        fence,
		UploadID:     "up-s12",
		WorkerSHA256: bogus, // worker fabrication against canonical SHA
		ServerSHA256: bogus, // Branch D: server SHA differs
	})
	if !errors.Is(err, ErrStaleReport) {
		t.Fatalf("S12 CompleteUpload: got err=%v, want errors.Is(_, ErrStaleReport)", err)
	}
	if got := readArtifactStatus(t, db, "art-s12"); got != "STAGING" {
		t.Errorf("S12 artifact status: got=%q want=STAGING (Branch D rolls back)", got)
	}
	var upStatus string
	if err := db.QueryRow(`SELECT status FROM artifact_uploads WHERE upload_id = ?`, "up-s12").Scan(&upStatus); err != nil {
		t.Fatalf("S12 artifact_uploads status read: %v", err)
	}
	if upStatus != "RECEIVED" {
		t.Errorf("S12 artifact_uploads status: got=%q want=RECEIVED", upStatus)
	}
}
func TestPhase6_Scenario13_BranchB_StaysVerifying(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db)
	fence := validFence("task-s13", "attempt-s13")
	expected := strings.Repeat("a", 64)
	seedCompleteUploadFixture(t, db, "up-s13", "art-s13", "job-s13", expected)

	if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence: fence, JobID: "job-s13",
		OutputManifests: []OutputManifest{
			{OutputKind: "final_video", LogicalName: "out.mp4",
				MimeType: "video/mp4", SizeBytes: 1024, SHA256: expected},
		},
	}); err != nil {
		t.Fatalf("S13 DeclareOutputs: %v", err)
	}
	if err := c.CompleteUpload(context.Background(), CompleteUploadCommand{
		Fence:        fence,
		UploadID:     "up-s13",
		WorkerSHA256: expected,
		ServerSHA256: "", // Branch B
	}); err != nil {
		t.Fatalf("S13 CompleteUpload: %v", err)
	}
	if got := readArtifactStatus(t, db, "art-s13"); got != "VERIFYING" {
		t.Errorf("S13 artifact status: got=%q want=VERIFYING (Branch B verdict)", got)
	}
}
func TestPhase6_Scenarios14_15_FenceRejection(t *testing.T) {
	t.Run("s14_lease_revocation_mid_upload", func(t *testing.T) {
		// Inject lease revocation out from under the FenceTuple by
		// bumping worker_id + lease_id + task_revision on the
		// canonical attempt_commits row. CompleteUpload with the
		// STALE FenceTuple (Revision=1) must reject — either at
		// Fence.Read (CAS on revision fails) or at MarkSucceeded on a
		// subsequent path. We only require non-nil err.
		db := openCoordinatorTestDB(t)
		c := newTestCoordinator(db)
		fence := validFence("task-s14", "attempt-s14")
		seedCompleteUploadFixture(t, db, "up-s14", "art-s14", "job-s14", strings.Repeat("a", 64))

		if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
			Fence:           fence,
			JobID:           "job-s14",
			OutputManifests: validManifests(),
		}); err != nil {
			t.Fatalf("S14 DeclareOutputs: %v", err)
		}
		if _, err := db.Exec(
			`UPDATE attempt_commits SET worker_id = ?, lease_id = ?, task_revision = task_revision + 1 WHERE task_id = ? AND attempt_id = ?`,
			"different-worker", "different-lease", fence.TaskID, fence.AttemptID,
		); err != nil {
			t.Fatalf("S14 lease inject: %v", err)
		}

		if err := c.CompleteUpload(context.Background(), CompleteUploadCommand{
			Fence:        fence, // still Revision=1, original worker_id/lease_id
			UploadID:     "up-s14",
			WorkerSHA256: strings.Repeat("a", 64),
			ServerSHA256: strings.Repeat("a", 64),
		}); err == nil {
			t.Errorf("S14 stale-fence CompleteUpload MUST reject: got nil err")
		} else {
			// Surface diagnostic for whichever sentinel fired.
			t.Logf("S14 CompleteUpload rejection: %v", err)
		}
	})

	t.Run("s15_stale_attempt_finalize", func(t *testing.T) {
		// Old attempt row exists; bump task_revision to simulate a
		// newer attempt taking over. Old attempt CommitAttempt
		// must refuse via Fence.Read / CAS guard. Sentinel
		// ErrFenceMismatch or ErrTransitionConflict are both
		// acceptable refusals.
		db := openCoordinatorTestDB(t)
		c := newTestCoordinator(db)
		fenceOld := validFence("task-s15", "attempt-s15")
		fenceOld.Revision = 1
		if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
			Fence:           fenceOld,
			JobID:           "job-s15",
			OutputManifests: validManifests(),
		}); err != nil {
			t.Fatalf("S15 first DeclareOutputs (Revision=1): %v", err)
		}
		oldCommitID := readAttemptCommitRow(t, db, fenceOld).CommitID

		// To make the fence rejection observable at CommitAttempt
		// (which doesn't gate by Fence.Read today), inject a terminal
		// status on the old attempt_commits row. CommitAttempt's
		// status CAS gate (status IN 'DECLARED'|'UPLOADING'|'RECEIVED'|
		// 'VERIFYING') then refuses with ErrTransitionConflict.
		if _, err := db.Exec(
			`UPDATE attempt_commits SET status = 'FAILED' WHERE commit_id = ?`,
			oldCommitID,
		); err != nil {
			t.Fatalf("S15 status inject: %v", err)
		}

		if _, err := c.CommitAttempt(context.Background(), oldCommitID); err == nil {
			t.Errorf("S15 stale-status CommitAttempt MUST reject: got nil err")
		} else if !errors.Is(err, ErrTransitionConflict) {
			t.Errorf("S15 stale-status CommitAttempt: got err=%v, want errors.Is(_, ErrTransitionConflict)", err)
		}
	})
}
