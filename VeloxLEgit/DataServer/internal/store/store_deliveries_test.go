package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func setupDeliveryTestDB(t *testing.T) *SQLiteStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "delivery_lease_test.sqlite")
	dbStore, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	t.Cleanup(func() { dbStore.Close() })
	return dbStore
}

func insertTestDeliveryDestination(t *testing.T, db *SQLiteStore, destID, provider string) {
	t.Helper()
	err := db.InsertDeliveryDestination(&DeliveryDestination{
		DestinationID:     destID,
		Provider:          provider,
		Name:              "test-" + destID,
		Enabled:           true,
		ConfigurationJSON: "{}",
	})
	if err != nil {
		t.Fatalf("insert delivery destination: %v", err)
	}
}

func insertTestArtifact(t *testing.T, db *SQLiteStore, artifactID, jobID, storageKey string) {
	t.Helper()
	err := db.InsertArtifact(&Artifact{
		ID:              artifactID,
		JobID:           jobID,
		Type:            "video",
		StorageProvider: "local",
		StorageKey:      storageKey,
		SHA256:          "abc123",
		SizeBytes:       1024,
		Status:          "READY",
		VerifiedAt:      time.Now().UTC().Format(time.RFC3339),
		CreatedAt:       time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("insert artifact: %v", err)
	}
}

func insertTestJobDelivery(t *testing.T, db *SQLiteStore, deliveryID, artifactID, destID string) {
	t.Helper()
	jd := &JobDelivery{
		DeliveryID:     deliveryID,
		ArtifactID:     artifactID,
		DestinationID:  destID,
		Status:         "PENDING",
		IdempotencyKey: deliveryID,
		MaxAttempts:    5,
		CreatedAt:      time.Now().UTC().Format(time.RFC3339),
		UpdatedAt:      time.Now().UTC().Format(time.RFC3339),
	}
	if err := db.InsertJobDelivery(jd); err != nil {
		t.Fatalf("insert job delivery: %v", err)
	}
}

func TestClaimDeliveries_BasicClaim(t *testing.T) {
	db := setupDeliveryTestDB(t)
	ctx := context.Background()

	insertTestDeliveryDestination(t, db, "dest-yt", "youtube")
	insertTestArtifact(t, db, "art-1", "job-1", "/tmp/test.mp4")
	insertTestJobDelivery(t, db, "del_art-1_dest-yt", "art-1", "dest-yt")

	leases, err := db.ClaimDeliveries(ctx, "runner-1", 5*time.Minute, 4)
	if err != nil {
		t.Fatalf("ClaimDeliveries: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("expected 1 lease, got %d", len(leases))
	}
	if leases[0].DeliveryID != "del_art-1_dest-yt" {
		t.Errorf("delivery_id = %q, want %q", leases[0].DeliveryID, "del_art-1_dest-yt")
	}
	if leases[0].RunnerID != "runner-1" {
		t.Errorf("runner_id = %q, want %q", leases[0].RunnerID, "runner-1")
	}
	if leases[0].LeaseID == "" {
		t.Error("lease_id should not be empty")
	}
	if leases[0].AttemptNumber != 1 {
		t.Errorf("attempt_number = %d, want 1", leases[0].AttemptNumber)
	}
	if leases[0].Provider != "youtube" {
		t.Errorf("provider = %q, want %q", leases[0].Provider, "youtube")
	}
}

func TestClaimDeliveries_ConcurrentRunners(t *testing.T) {
	db := setupDeliveryTestDB(t)
	ctx := context.Background()

	insertTestDeliveryDestination(t, db, "dest-d", "drive")
	for i := 0; i < 10; i++ {
		suffix := string(rune('0' + i))
		insertTestArtifact(t, db, "art-conc-"+suffix, "job-conc", "/tmp/f-"+suffix+".mp4")
		insertTestJobDelivery(t, db, "del_art-conc-"+suffix+"_dest-d",
			"art-conc-"+suffix, "dest-d")
	}

	// Two runners claim 5 each
	leases1, err := db.ClaimDeliveries(ctx, "runner-A", 5*time.Minute, 5)
	if err != nil {
		t.Fatalf("runner-A claim: %v", err)
	}
	leases2, err := db.ClaimDeliveries(ctx, "runner-B", 5*time.Minute, 5)
	if err != nil {
		t.Fatalf("runner-B claim: %v", err)
	}

	total := len(leases1) + len(leases2)
	if total != 10 {
		t.Errorf("expected 10 total claims, got %d", total)
	}

	// No duplicates
	seen := make(map[string]bool)
	for _, l := range append(leases1, leases2...) {
		if seen[l.DeliveryID] {
			t.Errorf("duplicate claim: %s", l.DeliveryID)
		}
		seen[l.DeliveryID] = true
	}
}

func TestClaimDeliveries_ZombieReclaim(t *testing.T) {
	db := setupDeliveryTestDB(t)
	ctx := context.Background()

	insertTestDeliveryDestination(t, db, "dest-z", "drive")
	insertTestArtifact(t, db, "art-z", "job-z", "/tmp/z.mp4")
	insertTestJobDelivery(t, db, "del_art-z_dest-z", "art-z", "dest-z")

	// Claim and let the lease expire
	leases, err := db.ClaimDeliveries(ctx, "runner-old", 1*time.Second, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("initial claim: %v len=%d", err, len(leases))
	}

	// Wait for lease to expire
	time.Sleep(2100 * time.Millisecond)

	// A new runner should reclaim the zombie
	leases2, err := db.ClaimDeliveries(ctx, "runner-new", 5*time.Minute, 1)
	if err != nil {
		t.Fatalf("zombie reclaim: %v", err)
	}
	if len(leases2) != 1 {
		t.Fatalf("expected 1 reclaimed lease, got %d", len(leases2))
	}
	if leases2[0].RunnerID != "runner-new" {
		t.Errorf("reclaimed runner_id = %q, want %q", leases2[0].RunnerID, "runner-new")
	}
}

func TestClaimDeliveries_MaxAttemptsExhausted(t *testing.T) {
	db := setupDeliveryTestDB(t)
	ctx := context.Background()

	insertTestDeliveryDestination(t, db, "dest-m", "drive")
	insertTestArtifact(t, db, "art-m", "job-m", "/tmp/m.mp4")
	jd := &JobDelivery{
		DeliveryID:     "del_art-m_dest-m",
		ArtifactID:     "art-m",
		DestinationID:  "dest-m",
		Status:         "RETRY_WAIT",
		AttemptCount:   5,
		MaxAttempts:    5,
		IdempotencyKey: "del_art-m_dest-m",
		CreatedAt:      time.Now().UTC().Format(time.RFC3339),
		UpdatedAt:      time.Now().UTC().Format(time.RFC3339),
	}
	if err := db.InsertJobDelivery(jd); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Should not claim because attempt_count >= max_attempts check is done
	// in the runner, not in ClaimDeliveries (the claim just flips status).
	// But RETRY_WAIT with next_attempt_at=NULL should still be claimable
	// per the SQL. This is correct — the runner enforces max_attempts.
	leases, err := db.ClaimDeliveries(ctx, "runner-x", 5*time.Minute, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	// The claim should succeed (the SQL doesn't filter by max_attempts).
	// The runner's processLease enforces max_attempts before calling MarkDelivery*.
	if len(leases) != 1 {
		t.Fatalf("expected 1 lease, got %d", len(leases))
	}
}

func TestMarkDeliverySucceeded(t *testing.T) {
	db := setupDeliveryTestDB(t)
	ctx := context.Background()

	insertTestDeliveryDestination(t, db, "dest-s", "youtube")
	insertTestArtifact(t, db, "art-s", "job-s", "/tmp/s.mp4")
	insertTestJobDelivery(t, db, "del_art-s_dest-s", "art-s", "dest-s")

	leases, err := db.ClaimDeliveries(ctx, "runner-s", 5*time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: %v len=%d", err, len(leases))
	}

	l := leases[0]
	err = db.MarkDeliverySucceeded(ctx, l.DeliveryID, l.RunnerID, l.LeaseID, "yt-video-123", "https://youtube.com/watch?v=123")
	if err != nil {
		t.Fatalf("MarkDeliverySucceeded: %v", err)
	}

	jd, err := db.GetJobDelivery(ctx, l.DeliveryID)
	if err != nil {
		t.Fatalf("GetJobDelivery: %v", err)
	}
	if jd.Status != "SUCCEEDED" {
		t.Errorf("status = %q, want SUCCEEDED", jd.Status)
	}
	if jd.RemoteID != "yt-video-123" {
		t.Errorf("remote_id = %q, want yt-video-123", jd.RemoteID)
	}
	if jd.CompletedAt == "" {
		t.Error("completed_at should be set")
	}
}

func TestMarkDeliveryRetry(t *testing.T) {
	db := setupDeliveryTestDB(t)
	ctx := context.Background()

	insertTestDeliveryDestination(t, db, "dest-r", "drive")
	insertTestArtifact(t, db, "art-r", "job-r", "/tmp/r.mp4")
	insertTestJobDelivery(t, db, "del_art-r_dest-r", "art-r", "dest-r")

	leases, err := db.ClaimDeliveries(ctx, "runner-r", 5*time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: %v len=%d", err, len(leases))
	}

	l := leases[0]
	nextAttempt := time.Now().UTC().Add(2 * time.Minute)
	err = db.MarkDeliveryRetry(ctx, l.DeliveryID, l.RunnerID, l.LeaseID, "TRANSIENT", "timeout", nextAttempt)
	if err != nil {
		t.Fatalf("MarkDeliveryRetry: %v", err)
	}

	jd, err := db.GetJobDelivery(ctx, l.DeliveryID)
	if err != nil {
		t.Fatalf("GetJobDelivery: %v", err)
	}
	if jd.Status != "RETRY_WAIT" {
		t.Errorf("status = %q, want RETRY_WAIT", jd.Status)
	}
	if jd.LockedBy != "" {
		t.Errorf("locked_by = %q, want empty", jd.LockedBy)
	}
	if jd.LeaseID != "" {
		t.Errorf("lease_id = %q, want empty", jd.LeaseID)
	}
	if jd.NextAttemptAt == "" {
		t.Error("next_attempt_at should be set")
	}
	if jd.LastError != "TRANSIENT" {
		t.Errorf("last_error_code = %q, want TRANSIENT", jd.LastError)
	}
}

func TestMarkDeliveryFailed(t *testing.T) {
	db := setupDeliveryTestDB(t)
	ctx := context.Background()

	insertTestDeliveryDestination(t, db, "dest-f", "youtube")
	insertTestArtifact(t, db, "art-f", "job-f", "/tmp/f.mp4")
	insertTestJobDelivery(t, db, "del_art-f_dest-f", "art-f", "dest-f")

	leases, err := db.ClaimDeliveries(ctx, "runner-f", 5*time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: %v len=%d", err, len(leases))
	}

	l := leases[0]
	err = db.MarkDeliveryFailed(ctx, l.DeliveryID, l.RunnerID, l.LeaseID, "PERMANENT", "invalid file")
	if err != nil {
		t.Fatalf("MarkDeliveryFailed: %v", err)
	}

	jd, err := db.GetJobDelivery(ctx, l.DeliveryID)
	if err != nil {
		t.Fatalf("GetJobDelivery: %v", err)
	}
	if jd.Status != "FAILED" {
		t.Errorf("status = %q, want FAILED", jd.Status)
	}
	if jd.CompletedAt == "" {
		t.Error("completed_at should be set")
	}
}

func TestMarkDeliveryBlockedAuth(t *testing.T) {
	db := setupDeliveryTestDB(t)
	ctx := context.Background()

	insertTestDeliveryDestination(t, db, "dest-a", "youtube")
	insertTestArtifact(t, db, "art-a", "job-a", "/tmp/a.mp4")
	insertTestJobDelivery(t, db, "del_art-a_dest-a", "art-a", "dest-a")

	leases, err := db.ClaimDeliveries(ctx, "runner-a", 5*time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: %v len=%d", err, len(leases))
	}

	l := leases[0]
	err = db.MarkDeliveryBlockedAuth(ctx, l.DeliveryID, l.RunnerID, l.LeaseID, "AUTH", "token expired")
	if err != nil {
		t.Fatalf("MarkDeliveryBlockedAuth: %v", err)
	}

	jd, err := db.GetJobDelivery(ctx, l.DeliveryID)
	if err != nil {
		t.Fatalf("GetJobDelivery: %v", err)
	}
	if jd.Status != "BLOCKED_AUTH" {
		t.Errorf("status = %q, want BLOCKED_AUTH", jd.Status)
	}
}

func TestRenewDeliveryLease(t *testing.T) {
	db := setupDeliveryTestDB(t)
	ctx := context.Background()

	insertTestDeliveryDestination(t, db, "dest-ren", "drive")
	insertTestArtifact(t, db, "art-ren", "job-ren", "/tmp/ren.mp4")
	insertTestJobDelivery(t, db, "del_art-ren_dest-ren", "art-ren", "dest-ren")

	leases, err := db.ClaimDeliveries(ctx, "runner-ren", 1*time.Second, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: %v len=%d", err, len(leases))
	}

	l := leases[0]
	newExpiry := time.Now().UTC().Add(10 * time.Minute)
	err = db.RenewDeliveryLease(ctx, l.DeliveryID, l.RunnerID, l.LeaseID, newExpiry)
	if err != nil {
		t.Fatalf("RenewDeliveryLease: %v", err)
	}

	// Verify the delivery is still RUNNING and lease extended.
	jd, err := db.GetJobDelivery(ctx, l.DeliveryID)
	if err != nil {
		t.Fatalf("GetJobDelivery: %v", err)
	}
	if jd.Status != "RUNNING" {
		t.Errorf("status = %q, want RUNNING", jd.Status)
	}
}

func TestRenewDeliveryLease_CASGuard(t *testing.T) {
	db := setupDeliveryTestDB(t)
	ctx := context.Background()

	insertTestDeliveryDestination(t, db, "dest-guard", "drive")
	insertTestArtifact(t, db, "art-guard", "job-guard", "/tmp/guard.mp4")
	insertTestJobDelivery(t, db, "del_art-guard_dest-guard", "art-guard", "dest-guard")

	leases, err := db.ClaimDeliveries(ctx, "runner-real", 5*time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: %v len=%d", err, len(leases))
	}

	l := leases[0]
	// Try renewing with wrong runner_id — should fail with conflict.
	wrongExpiry := time.Now().UTC().Add(10 * time.Minute)
	err = db.RenewDeliveryLease(ctx, l.DeliveryID, "wrong-runner", l.LeaseID, wrongExpiry)
	if err != ErrTransitionConflict {
		t.Errorf("expected ErrTransitionConflict, got %v", err)
	}
}

func TestClaimDeliveries_RetryWaitWithNextAttemptAt(t *testing.T) {
	db := setupDeliveryTestDB(t)
	ctx := context.Background()

	insertTestDeliveryDestination(t, db, "dest-nxt", "drive")
	insertTestArtifact(t, db, "art-nxt", "job-nxt", "/tmp/nxt.mp4")

	// Insert a RETRY_WAIT delivery with next_attempt_at in the future.
	future := time.Now().UTC().Add(1 * time.Hour)
	jd := &JobDelivery{
		DeliveryID:     "del_art-nxt_dest-nxt",
		ArtifactID:     "art-nxt",
		DestinationID:  "dest-nxt",
		Status:         "RETRY_WAIT",
		NextAttemptAt:  future.Format(time.RFC3339),
		AttemptCount:   2,
		MaxAttempts:    5,
		IdempotencyKey: "del_art-nxt_dest-nxt",
		CreatedAt:      time.Now().UTC().Format(time.RFC3339),
		UpdatedAt:      time.Now().UTC().Format(time.RFC3339),
	}
	if err := db.InsertJobDelivery(jd); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Should NOT be claimed because next_attempt_at is in the future.
	leases, err := db.ClaimDeliveries(ctx, "runner-nxt", 5*time.Minute, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(leases) != 0 {
		t.Errorf("expected 0 claims (future next_attempt_at), got %d", len(leases))
	}
}

func TestClaimDeliveries_SucceededDoubleClaim(t *testing.T) {
	db := setupDeliveryTestDB(t)
	ctx := context.Background()

	insertTestDeliveryDestination(t, db, "dest-2x", "youtube")
	insertTestArtifact(t, db, "art-2x", "job-2x", "/tmp/2x.mp4")
	insertTestJobDelivery(t, db, "del_art-2x_dest-2x", "art-2x", "dest-2x")

	// Claim and succeed
	leases, err := db.ClaimDeliveries(ctx, "runner-2x", 5*time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: %v len=%d", err, len(leases))
	}
	l := leases[0]
	if err := db.MarkDeliverySucceeded(ctx, l.DeliveryID, l.RunnerID, l.LeaseID, "vid-1", "https://yt/1"); err != nil {
		t.Fatalf("succeed: %v", err)
	}

	// Second claim should get nothing (delivery is SUCCEEDED, not PENDING/RETRY_WAIT)
	leases2, err := db.ClaimDeliveries(ctx, "runner-2x-b", 5*time.Minute, 1)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if len(leases2) != 0 {
		t.Errorf("expected 0 claims on succeeded delivery, got %d", len(leases2))
	}
}
