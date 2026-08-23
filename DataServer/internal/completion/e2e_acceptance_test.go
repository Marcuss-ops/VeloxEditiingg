package completion

import (
	"context"
	"strings"
	"testing"
)

func TestPhase6_ArtifactContractJobPromotesFromRunning(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db)
	fence := validFence("task-artifact-gate", "attempt-artifact-gate")
	jobID := "job-artifact-gate"
	seedCompleteUploadFixture(t, db, "up-artifact-gate", "art-artifact-gate", jobID, strings.Repeat("a", 64))

	if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence: fence, JobID: jobID, OutputManifests: validManifests(),
	}); err != nil {
		t.Fatalf("DeclareOutputs: %v", err)
	}
	if _, err := db.Exec(`UPDATE jobs SET status = 'RUNNING' WHERE job_id = ?`, jobID); err != nil {
		t.Fatalf("set RUNNING: %v", err)
	}

	// Fail-closed: while the declared artifact is still STAGING the
	// commit must be blocked (ready_output_count < required_output_count)
	// and the contract-bound job must remain RUNNING.
	if _, err := c.CommitAttempt(context.Background(), readAttemptCommitRow(t, db, fence).CommitID); err == nil {
		t.Fatal("artifact-contract RUNNING job must fail closed while artifact is not READY")
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE job_id = ?`, jobID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "RUNNING" {
		t.Fatalf("job status after blocked commit=%q, want RUNNING", status)
	}

	// Once the artifact set is provably READY (scheduleRowReady bumps
	// ready_output_count and binds the declaration), the finalizer
	// promotes the RUNNING job through AWAITING_ARTIFACT to SUCCEEDED
	// atomically.
	commitID := scheduleRowReady(t, db, fence, "art-artifact-gate")
	if _, err := c.CommitAttempt(context.Background(), commitID); err != nil {
		t.Fatalf("artifact-contract RUNNING job with READY artifact must promote: %v", err)
	}
	if err := db.QueryRow(`SELECT status FROM jobs WHERE job_id = ?`, jobID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "SUCCEEDED" {
		t.Fatalf("job status after promoted commit=%q, want SUCCEEDED", status)
	}
}
func TestPhase6_Acceptance_GoldenPath(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db)
	fence := validFence("task-acc", "attempt-acc")
	jobID := "job-acc"
	seedCompleteUploadFixture(t, db, "up-acc", "art-acc", jobID, strings.Repeat("a", 64))
	seedDeliveryDestination(t, db, "dest-acc", "drive")
	seedJobDeliveryPlan(t, db, jobID, "dest-acc")

	_, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence:           fence,
		JobID:           jobID,
		OutputManifests: validManifests(),
	})
	if err != nil {
		t.Fatalf("acc DeclareOutputs: %v", err)
	}
	_ = c.RecordUploadProgress(context.Background(), RecordUploadProgressCommand{
		Fence: fence, UploadID: "up-acc", UploadedBytes: 1024,
	})
	if err := c.CompleteUpload(context.Background(), CompleteUploadCommand{
		Fence:        fence,
		UploadID:     "up-acc",
		WorkerSHA256: strings.Repeat("a", 64),
		ServerSHA256: strings.Repeat("a", 64),
	}); err != nil {
		t.Fatalf("acc CompleteUpload: %v", err)
	}
	if got := readArtifactStatus(t, db, "art-acc"); got != "READY" {
		t.Errorf("acc artifact status: got=%q want=READY", got)
	}
	commitID := scheduleRowReady(t, db, fence, "art-acc")

	// Pre-set jobs.status at the artifact gate. The completion protocol
	// must never promote an artifact-contract job directly from RUNNING.
	if _, err := db.Exec(`UPDATE jobs SET status = 'AWAITING_ARTIFACT' WHERE job_id = ?`, jobID); err != nil {
		t.Fatalf("acc jobs.status pre-set: %v", err)
	}

	res, err := c.CommitAttempt(context.Background(), commitID)
	if err != nil {
		t.Fatalf("acc CommitAttempt: %v", err)
	}
	row := readAttemptCommitRow(t, db, fence)
	if row.Status != "COMMITTED" {
		t.Errorf("acc attempt_commits.status: got=%q want=COMMITTED", row.Status)
	}
	var jobStatus string
	if err := db.QueryRow(`SELECT status FROM jobs WHERE job_id = ?`, jobID).Scan(&jobStatus); err != nil {
		t.Fatalf("acc jobs.status read: %v", err)
	}
	if jobStatus != "SUCCEEDED" {
		t.Errorf("acc jobs.status: got=%q want=SUCCEEDED (MarkSucceededIfTasksDone must fire because the only task SUCCEEDED)", jobStatus)
	}
	var deliveriesCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM job_deliveries WHERE artifact_id = ?`, "art-acc").Scan(&deliveriesCount); err != nil {
		t.Fatalf("acc deliveries count: %v", err)
	}
	if deliveriesCount < 1 {
		t.Errorf("acc job_deliveries count: got=%d want>=1 (durable cross-join)", deliveriesCount)
	}
	if res != nil && res.JobStatus != jobStatus {
		t.Errorf("acc CommitResult.JobStatus drift: result=%q db=%q", res.JobStatus, jobStatus)
	}
}
