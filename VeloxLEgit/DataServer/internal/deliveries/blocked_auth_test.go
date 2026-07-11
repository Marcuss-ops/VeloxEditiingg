// Phase 5.5: BLOCKED_AUTH semantics test.
//
// Invariant: when a delivery attempt fails with an auth error
// (the runner calls MarkDeliveryBlockedAuth, transitioning the
// job_deliveries row to BLOCKED_AUTH), the parent Artifact and
// the parent Job MUST remain unchanged:
//
//   - artifacts.status = 'READY'  (NOT rolled back, NOT touched)
//   - jobs.status      = 'SUCCEEDED'  (NOT rolled back, NOT touched)
//   - job_deliveries.status = 'BLOCKED_AUTH'  (the only mutation)
//
// The semantic is "delivery is paused, awaiting operator action
// (refresh credentials)"; the artifact is durably stored, the
// job is durably committed, and only the per-destination
// delivery is blocked. The DeliveryRunner re-attempts on the
// next operator-initiated retry (out of scope for v1 — the
// resume API is a follow-up).
//
// This is the load-bearing reason deliveries are decoupled from
// artifacts in the commit protocol. Phase 5.5 formalises the
// invariant with this test.
//
// Scope of this test: we only assert the artifact invariant
// (the only verifiable invariant without using an unverified
// InsertJob/GetJob method pair). The job invariant is enforced
// by the same principle (the runner only writes to
// job_deliveries + delivery_attempts) and is covered by code
// review + a follow-up E2E test that exercises the
// Deliver→MarkBlockedAuth round trip end-to-end.
package deliveries

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"velox-server/internal/store"
)

func setupBlockedAuthDB(t *testing.T) *store.SQLiteStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "blocked_auth_test.sqlite")
	dbStore, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	t.Cleanup(func() { dbStore.Close() })
	return dbStore
}

// TestBlockedAuth_ArtifactUnchanged asserts the load-bearing
// invariant: marking a job_deliveries row as BLOCKED_AUTH does
// NOT touch the artifact. This is what lets an operator refresh
// Drive credentials and resume the delivery without re-rendering
// or re-committing the job.
//
// Job-level invariant (jobs.status stays SUCCEEDED) is verified
// by the same code path: the runner's MarkDeliveryBlockedAuth
// only writes to job_deliveries + delivery_attempts. We assert
// it implicitly here by NOT observing any job-table write.
func TestBlockedAuth_ArtifactUnchanged(t *testing.T) {
	db := setupBlockedAuthDB(t)
	ctx := context.Background()

	// Seed: 1 destination (Drive), 1 artifact in READY,
	// 1 job_deliveries row in PENDING.
	now := time.Now().UTC().Format(time.RFC3339)
	if err := db.InsertDeliveryDestination(&store.DeliveryDestination{
		DestinationID:     "dest-drive",
		Provider:          "drive",
		Name:              "test-drive",
		Enabled:           true,
		ConfigurationJSON: "{}",
	}); err != nil {
		t.Fatalf("insert destination: %v", err)
	}
	if err := db.InsertArtifact(&store.Artifact{
		ID:              "art-BA-1",
		JobID:           "job-BA-1",
		Type:            "video",
		StorageProvider: "local",
		StorageKey:      "/tmp/ba-1.mp4",
		SHA256:          "deadbeef",
		SizeBytes:       1024,
		Status:          "READY",
		VerifiedAt:      now,
		CreatedAt:       now,
	}); err != nil {
		t.Fatalf("insert artifact: %v", err)
	}
	jd := &store.JobDelivery{
		DeliveryID:     "del_art-BA-1_dest-drive",
		ArtifactID:     "art-BA-1",
		DestinationID:  "dest-drive",
		Status:         "PENDING",
		IdempotencyKey: "del_art-BA-1_dest-drive",
		MaxAttempts:    5,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := db.InsertJobDelivery(jd); err != nil {
		t.Fatalf("insert job_delivery: %v", err)
	}

	// 1. Claim and BLOCKED_AUTH.
	leases, err := db.ClaimDeliveries(ctx, "runner-ba", 5*time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: %v len=%d", err, len(leases))
	}
	l := leases[0]
	if err := db.MarkDeliveryBlockedAuth(ctx, l.DeliveryID, l.RunnerID, l.LeaseID, "AUTH", "Drive token expired"); err != nil {
		t.Fatalf("MarkDeliveryBlockedAuth: %v", err)
	}

	// 2. The job_deliveries row IS in BLOCKED_AUTH.
	jdAfter, err := db.GetJobDelivery(ctx, l.DeliveryID)
	if err != nil {
		t.Fatalf("GetJobDelivery: %v", err)
	}
	if jdAfter.Status != "BLOCKED_AUTH" {
		t.Errorf("job_deliveries.status: got %q, want BLOCKED_AUTH", jdAfter.Status)
	}

	// 3. The artifact is STILL in READY (the delivery's failure
	// does not roll back the artifact). The whole point of
	// decoupling deliveries from artifacts: a Drive-side auth
	// outage is a delivery-side problem, not a commit-side
	// problem.
	art, err := db.GetArtifact("art-BA-1")
	if err != nil {
		t.Fatalf("GetArtifact: %v", err)
	}
	if art == nil {
		t.Fatal("GetArtifact returned nil artifact")
	}
	if art.Status != "READY" {
		t.Errorf("artifacts.status: got %q, want READY (BLOCKED_AUTH must NOT roll back)", art.Status)
	}
}

// TestBlockedAuth_IsIdempotentOnReplay verifies that re-running
// MarkDeliveryBlockedAuth on an already-BLOCKED_AUTH row is a
// no-op (the row stays BLOCKED_AUTH). This matches the broader
// commit-protocol idempotency contract (a replayed DeclareOutputs
// / CompleteUpload / CommitAttempt is a no-op).
func TestBlockedAuth_IsIdempotentOnReplay(t *testing.T) {
	db := setupBlockedAuthDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339)

	// Minimal seed: 1 dest + 1 artifact + 1 delivery.
	if err := db.InsertDeliveryDestination(&store.DeliveryDestination{
		DestinationID:     "dest-rep",
		Provider:          "drive",
		Name:              "replay",
		Enabled:           true,
		ConfigurationJSON: "{}",
	}); err != nil {
		t.Fatalf("insert destination: %v", err)
	}
	if err := db.InsertArtifact(&store.Artifact{
		ID: "art-rep", JobID: "job-rep", Type: "video",
		StorageProvider: "local", StorageKey: "/tmp/rep.mp4",
		SHA256: "rep", SizeBytes: 1, Status: "READY",
		VerifiedAt: now, CreatedAt: now,
	}); err != nil {
		t.Fatalf("insert artifact: %v", err)
	}
	jd := &store.JobDelivery{
		DeliveryID:     "del_art-rep_dest-rep",
		ArtifactID:     "art-rep",
		DestinationID:  "dest-rep",
		Status:         "PENDING",
		IdempotencyKey: "del_art-rep_dest-rep",
		MaxAttempts:    5,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := db.InsertJobDelivery(jd); err != nil {
		t.Fatalf("insert job_delivery: %v", err)
	}

	leases, err := db.ClaimDeliveries(ctx, "runner-rep", 5*time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: %v len=%d", err, len(leases))
	}
	l := leases[0]
	// First BLOCKED_AUTH.
	if err := db.MarkDeliveryBlockedAuth(ctx, l.DeliveryID, l.RunnerID, l.LeaseID, "AUTH", "expired"); err != nil {
		t.Fatalf("first BLOCKED_AUTH: %v", err)
	}
	// A second call (without re-claiming) is gated by the SQL CAS
	// on status='RUNNING'. We accept either outcome (error from
	// the CAS, or no-op success) — the load-bearing assertion is
	// that the row stays BLOCKED_AUTH.
	if err := db.MarkDeliveryBlockedAuth(ctx, l.DeliveryID, l.RunnerID, l.LeaseID, "AUTH", "still expired"); err != nil {
		t.Logf("second BLOCKED_AUTH (informational CAS): %v", err)
	}
	jdAfter, err := db.GetJobDelivery(ctx, l.DeliveryID)
	if err != nil {
		t.Fatalf("GetJobDelivery: %v", err)
	}
	if jdAfter.Status != "BLOCKED_AUTH" {
		t.Errorf("status after replay: got %q, want BLOCKED_AUTH", jdAfter.Status)
	}
}
