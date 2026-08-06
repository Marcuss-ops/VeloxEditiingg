package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func insertDeliveryTestJob(t *testing.T, db *SQLiteStore, jobID, status string) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.DB().ExecContext(context.Background(), `
		INSERT INTO jobs (job_id, status, revision, max_retries, created_at, updated_at, migrated_at, request_json)
		VALUES (?, ?, 0, 3, ?, ?, ?, '{}')`, jobID, status, now, now, now); err != nil {
		t.Fatalf("insert job %s: %v", jobID, err)
	}
}

func TestClaimDeliveries_FairAcrossParentJobs(t *testing.T) {
	db := setupDeliveryTestDB(t)
	ctx := context.Background()
	insertTestDeliveryDestination(t, db, "dest-fair", "drive")

	// One large parent must not consume the whole claim batch before the
	// smaller parents get a chance to run.
	for _, jobID := range []string{"job-large", "job-small-a", "job-small-b"} {
		insertDeliveryTestJob(t, db, jobID, "DELIVERING")
	}
	for i := 0; i < 6; i++ {
		artifactID := "artifact-large-" + string(rune('a'+i))
		deliveryID := "delivery-large-" + string(rune('a'+i))
		insertTestArtifact(t, db, artifactID, "job-large", filepath.Join(t.TempDir(), artifactID+".mp4"))
		insertTestJobDelivery(t, db, deliveryID, artifactID, "dest-fair")
	}
	for _, jobID := range []string{"job-small-a", "job-small-b"} {
		artifactID := "artifact-" + jobID
		deliveryID := "delivery-" + jobID
		insertTestArtifact(t, db, artifactID, jobID, filepath.Join(t.TempDir(), artifactID+".mp4"))
		insertTestJobDelivery(t, db, deliveryID, artifactID, "dest-fair")
	}

	leases, err := db.ClaimDeliveries(ctx, "fair-runner", 5*time.Minute, 4)
	if err != nil {
		t.Fatalf("ClaimDeliveries: %v", err)
	}
	if len(leases) != 4 {
		t.Fatalf("claimed %d deliveries, want 4", len(leases))
	}

	counts := map[string]int{}
	for _, lease := range leases {
		var jobID string
		if err := db.DB().QueryRowContext(ctx,
			`SELECT job_id FROM artifacts WHERE id = ?`, lease.ArtifactID).Scan(&jobID); err != nil {
			t.Fatalf("resolve parent for %s: %v", lease.DeliveryID, err)
		}
		counts[jobID]++
	}
	if counts["job-small-a"] != 1 || counts["job-small-b"] != 1 {
		t.Fatalf("small parents were starved: claims by parent = %#v", counts)
	}
	if counts["job-large"] > 2 {
		t.Fatalf("large parent consumed too many initial slots: claims by parent = %#v", counts)
	}
}

func TestDeliveryChildren_RetryIsolatedAndParentAggregatesAfterAllTerminal(t *testing.T) {
	db := setupDeliveryTestDB(t)
	ctx := context.Background()
	insertDeliveryTestJob(t, db, "job-aggregate", "DELIVERING")
	insertTestDeliveryDestination(t, db, "dest-aggregate-a", "drive")
	insertTestDeliveryDestination(t, db, "dest-aggregate-b", "drive")

	now := time.Now().UTC().Format(time.RFC3339)
	insertTestArtifact(t, db, "artifact-aggregate-a", "job-aggregate", filepath.Join(t.TempDir(), "a.mp4"))
	insertTestArtifact(t, db, "artifact-aggregate-b", "job-aggregate", filepath.Join(t.TempDir(), "b.mp4"))
	insertTestJobDelivery(t, db, "delivery-aggregate-a", "artifact-aggregate-a", "dest-aggregate-a")
	insertTestJobDelivery(t, db, "delivery-aggregate-b", "artifact-aggregate-b", "dest-aggregate-b")

	leases, err := db.ClaimDeliveries(ctx, "aggregate-runner", 5*time.Minute, 2)
	if err != nil || len(leases) != 2 {
		t.Fatalf("claim: %v leases=%d", err, len(leases))
	}
	byDestination := make(map[string]DeliveryLease, len(leases))
	for _, lease := range leases {
		byDestination[lease.DestinationID] = lease
	}

	// A retry on child A must release only A's lease and leave child B
	// independently RUNNING. The parent must remain DELIVERING while A is
	// waiting and B has not reached a terminal state.
	nextAttempt := time.Now().UTC().Add(time.Hour)
	if err := db.MarkDeliveryRetry(ctx,
		byDestination["dest-aggregate-a"].DeliveryID,
		"aggregate-runner",
		byDestination["dest-aggregate-a"].LeaseID,
		"TRANSIENT", "provider timeout", nextAttempt); err != nil {
		t.Fatalf("retry child A: %v", err)
	}
	rowA, err := db.GetJobDelivery(ctx, "delivery-aggregate-a")
	if err != nil {
		t.Fatal(err)
	}
	if rowA.Status != "RETRY_WAIT" || rowA.NextAttemptAt == "" || rowA.LastError != "TRANSIENT" {
		t.Fatalf("child A retry state = %+v", rowA)
	}
	rowB, err := db.GetJobDelivery(ctx, "delivery-aggregate-b")
	if err != nil {
		t.Fatal(err)
	}
	if rowB.Status != "RUNNING" || rowB.LeaseID == "" {
		t.Fatalf("child B was affected by child A retry: %+v", rowB)
	}
	var parentStatus string
	if err := db.DB().QueryRowContext(ctx,
		`SELECT status FROM jobs WHERE job_id = 'job-aggregate'`).Scan(&parentStatus); err != nil {
		t.Fatal(err)
	}
	if parentStatus != "DELIVERING" {
		t.Fatalf("parent status after isolated retry = %q, want DELIVERING", parentStatus)
	}

	// Completing B while A is still retryable must not fail or complete the
	// parent. Once A is re-claimed and completes, the parent succeeds.
	if err := db.MarkDeliverySucceeded(ctx,
		byDestination["dest-aggregate-b"].DeliveryID,
		"aggregate-runner",
		byDestination["dest-aggregate-b"].LeaseID,
		"remote-b", "https://example.test/b"); err != nil {
		t.Fatalf("succeed child B: %v", err)
	}
	if err := db.DB().QueryRowContext(ctx,
		`SELECT status FROM jobs WHERE job_id = 'job-aggregate'`).Scan(&parentStatus); err != nil {
		t.Fatal(err)
	}
	if parentStatus != "DELIVERING" {
		t.Fatalf("parent completed before retry child: %q", parentStatus)
	}

	if _, err := db.DB().ExecContext(ctx,
		`UPDATE job_deliveries SET next_attempt_at = ? WHERE delivery_id = ?`,
		now, "delivery-aggregate-a"); err != nil {
		t.Fatal(err)
	}
	retryLeases, err := db.ClaimDeliveries(ctx, "aggregate-runner", 5*time.Minute, 1)
	if err != nil || len(retryLeases) != 1 || retryLeases[0].DeliveryID != "delivery-aggregate-a" {
		t.Fatalf("reclaim child A: %v leases=%#v", err, retryLeases)
	}
	if err := db.MarkDeliverySucceeded(ctx, retryLeases[0].DeliveryID, "aggregate-runner", retryLeases[0].LeaseID, "remote-a", "https://example.test/a"); err != nil {
		t.Fatalf("succeed retried child A: %v", err)
	}
	if err := db.DB().QueryRowContext(ctx,
		`SELECT status FROM jobs WHERE job_id = 'job-aggregate'`).Scan(&parentStatus); err != nil {
		t.Fatal(err)
	}
	if parentStatus != "SUCCEEDED" {
		t.Fatalf("parent status after all children succeeded = %q, want SUCCEEDED", parentStatus)
	}
}

func TestDeliveryChildren_FailedChildDoesNotFailParentUntilSiblingsTerminal(t *testing.T) {
	db := setupDeliveryTestDB(t)
	ctx := context.Background()
	insertDeliveryTestJob(t, db, "job-failed-child", "DELIVERING")
	insertTestDeliveryDestination(t, db, "dest-failed-a", "drive")
	insertTestDeliveryDestination(t, db, "dest-failed-b", "drive")
	insertTestArtifact(t, db, "artifact-failed-a", "job-failed-child", filepath.Join(t.TempDir(), "a.mp4"))
	insertTestArtifact(t, db, "artifact-failed-b", "job-failed-child", filepath.Join(t.TempDir(), "b.mp4"))
	insertTestJobDelivery(t, db, "delivery-failed-a", "artifact-failed-a", "dest-failed-a")
	insertTestJobDelivery(t, db, "delivery-failed-b", "artifact-failed-b", "dest-failed-b")

	leases, err := db.ClaimDeliveries(ctx, "failed-runner", 5*time.Minute, 2)
	if err != nil || len(leases) != 2 {
		t.Fatalf("claim: %v leases=%d", err, len(leases))
	}
	var failed, sibling DeliveryLease
	for _, lease := range leases {
		if lease.DestinationID == "dest-failed-a" {
			failed = lease
		} else {
			sibling = lease
		}
	}
	if err := db.MarkDeliveryFailed(ctx, failed.DeliveryID, failed.RunnerID, failed.LeaseID, "PERMANENT", "invalid target"); err != nil {
		t.Fatalf("fail child: %v", err)
	}
	var parentStatus string
	if err := db.DB().QueryRowContext(ctx,
		`SELECT status FROM jobs WHERE job_id = 'job-failed-child'`).Scan(&parentStatus); err != nil {
		t.Fatal(err)
	}
	if parentStatus != "DELIVERING" {
		t.Fatalf("parent failed before sibling terminal: %q", parentStatus)
	}
	if err := db.MarkDeliverySucceeded(ctx, sibling.DeliveryID, sibling.RunnerID, sibling.LeaseID, "remote-sibling", "https://example.test/sibling"); err != nil {
		t.Fatalf("succeed sibling: %v", err)
	}
	if err := db.DB().QueryRowContext(ctx,
		`SELECT status FROM jobs WHERE job_id = 'job-failed-child'`).Scan(&parentStatus); err != nil {
		t.Fatal(err)
	}
	if parentStatus != "FAILED" {
		t.Fatalf("parent status after failed child and terminal sibling = %q, want FAILED", parentStatus)
	}
}
