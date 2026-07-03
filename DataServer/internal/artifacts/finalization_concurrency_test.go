// Package artifacts / finalization_concurrency_test.go
//
// Concurrency / race tests for the verified-finalization contract.
// The CAS chain on jobs.status (single source of truth) +
// artifact_uploads.status (FINALIZING) + UNIQUE on
// (artifact_id, destination_id) on job_deliveries must serialize
// concurrent finishers without producing partial state.
package artifacts_test

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/artifacts"
)

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
