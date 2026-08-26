package store

import (
	"context"
	"testing"
	"time"

	"velox-server/internal/forwardingcontract"
	"velox-server/internal/forwardingstore"
)

func TestClaimForwardings_BasicClaim(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertTestForwarding(t, db, "cf-claim-1", "openai", "creator-job-10", "scene.composite.v1", "PENDING")

	leases, err := db.Forwarding().ClaimCreatorForwardings(ctx, "runner-1", "cf", 5*time.Minute, 4)
	if err != nil {
		t.Fatalf("ClaimCreatorForwardings: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("expected 1 lease, got %d", len(leases))
	}
	if leases[0].ForwardingID != "cf-claim-1" {
		t.Errorf("forwarding_id = %q, want cf-claim-1", leases[0].ForwardingID)
	}
	if leases[0].RunnerID != "runner-1" {
		t.Errorf("runner_id = %q, want runner-1", leases[0].RunnerID)
	}
	if leases[0].LeaseID == "" {
		t.Error("lease_id should not be empty")
	}
	if leases[0].AttemptCount != 1 {
		t.Errorf("attempt_count = %d, want 1", leases[0].AttemptCount)
	}
	if leases[0].SourceProvider != "openai" {
		t.Errorf("source_provider = %q, want openai", leases[0].SourceProvider)
	}
}

func TestClaimForwardings_ConcurrentRunners(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		suffix := string(rune('a' + i))
		insertTestForwarding(t, db, "cf-conc-"+suffix, "openai", "creator-conc-"+suffix, "scene.composite.v1", "PENDING")
	}

	leases1, err := db.Forwarding().ClaimCreatorForwardings(ctx, "runner-A", "cf", 5*time.Minute, 5)
	if err != nil {
		t.Fatalf("runner-A claim: %v", err)
	}
	leases2, err := db.Forwarding().ClaimCreatorForwardings(ctx, "runner-B", "cf", 5*time.Minute, 5)
	if err != nil {
		t.Fatalf("runner-B claim: %v", err)
	}

	total := len(leases1) + len(leases2)
	if total != 10 {
		t.Errorf("expected 10 total claims, got %d", total)
	}

	seen := make(map[string]bool)
	for _, l := range append(leases1, leases2...) {
		if seen[l.ForwardingID] {
			t.Errorf("duplicate claim: %s", l.ForwardingID)
		}
		seen[l.ForwardingID] = true
	}
}

func TestClaimForwardings_ZombieReclaim(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertTestForwarding(t, db, "cf-zombie", "openai", "creator-zombie", "scene.composite.v1", "PENDING")

	leases, err := db.Forwarding().ClaimCreatorForwardings(ctx, "runner-old", "cf", 5*time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("initial claim: %v len=%d", err, len(leases))
	}
	if _, err := db.DB().ExecContext(ctx, `UPDATE creator_forwardings SET lease_expires_at = ? WHERE forwarding_id = ?`, time.Now().UTC().Add(-time.Minute).Format(time.RFC3339), leases[0].ForwardingID); err != nil {
		t.Fatalf("expire lease: %v", err)
	}

	// New runner should reclaim the zombie
	leases2, err := db.Forwarding().ClaimCreatorForwardings(ctx, "runner-new", "cf", 5*time.Minute, 1)
	if err != nil {
		t.Fatalf("zombie reclaim: %v", err)
	}
	if len(leases2) != 1 {
		t.Fatalf("expected 1 reclaimed lease, got %d", len(leases2))
	}
	if leases2[0].RunnerID != "runner-new" {
		t.Errorf("reclaimed runner_id = %q, want runner-new", leases2[0].RunnerID)
	}
	if leases2[0].ForwardingID != "cf-zombie" {
		t.Errorf("reclaimed forwarding_id = %q, want cf-zombie", leases2[0].ForwardingID)
	}
}

func TestClaimForwardings_RetryWaitWithFutureNextAttempt(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	future := time.Now().UTC().Add(1 * time.Hour)
	cf := &forwardingcontract.CreatorForwarding{
		ForwardingID:     "cf-future",
		SourceProvider:   "openai",
		SourceJobID:      "creator-future",
		TargetExecutorID: "scene.composite.v1",
		Status:           "RETRY_WAIT",
		NextAttemptAt:    future.Format(time.RFC3339),
		AttemptCount:     2,
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
		UpdatedAt:        time.Now().UTC().Format(time.RFC3339),
	}
	if _, err := db.Forwarding().InsertCreatorForwarding(ctx, cf); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Should NOT be claimed because next_attempt_at is in the future
	leases, err := db.Forwarding().ClaimCreatorForwardings(ctx, "runner-x", "cf", 5*time.Minute, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(leases) != 0 {
		t.Errorf("expected 0 claims (future next_attempt_at), got %d", len(leases))
	}
}

func TestClaimForwardings_RetryWaitWithPastNextAttempt(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	past := time.Now().UTC().Add(-1 * time.Hour)
	cf := &forwardingcontract.CreatorForwarding{
		ForwardingID:     "cf-past",
		SourceProvider:   "openai",
		SourceJobID:      "creator-past",
		TargetExecutorID: "scene.composite.v1",
		Status:           "RETRY_WAIT",
		NextAttemptAt:    past.Format(time.RFC3339),
		AttemptCount:     2,
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
		UpdatedAt:        time.Now().UTC().Format(time.RFC3339),
	}
	if _, err := db.Forwarding().InsertCreatorForwarding(ctx, cf); err != nil {
		t.Fatalf("insert: %v", err)
	}

	leases, err := db.Forwarding().ClaimCreatorForwardings(ctx, "runner-x", "cf", 5*time.Minute, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("expected 1 claim, got %d", len(leases))
	}
	if leases[0].ForwardingID != "cf-past" {
		t.Errorf("forwarding_id = %q, want cf-past", leases[0].ForwardingID)
	}
}

func TestRenewForwardingLease(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertTestForwarding(t, db, "cf-renew", "openai", "creator-renew", "scene.composite.v1", "PENDING")

	leases, err := db.Forwarding().ClaimCreatorForwardings(ctx, "runner-renew", "cf", 5*time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: %v len=%d", err, len(leases))
	}

	l := leases[0]
	newExpiry := time.Now().UTC().Add(10 * time.Minute)
	err = db.Forwarding().RenewCreatorForwardingLease(ctx, l.ForwardingID, l.RunnerID, l.LeaseID, newExpiry)
	if err != nil {
		t.Fatalf("RenewCreatorForwardingLease: %v", err)
	}

	// Verify still in POLLING
	cf, err := db.Forwarding().GetCreatorForwarding(ctx, l.ForwardingID)
	if err != nil {
		t.Fatalf("GetCreatorForwarding: %v", err)
	}
	if cf.Status != "POLLING" {
		t.Errorf("status = %q, want POLLING", cf.Status)
	}
}

func TestRenewForwardingLease_CASGuard(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertTestForwarding(t, db, "cf-guard", "openai", "creator-guard", "scene.composite.v1", "PENDING")

	leases, err := db.Forwarding().ClaimCreatorForwardings(ctx, "runner-real", "cf", 5*time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: %v len=%d", err, len(leases))
	}

	l := leases[0]
	wrongExpiry := time.Now().UTC().Add(10 * time.Minute)
	err = db.Forwarding().RenewCreatorForwardingLease(ctx, l.ForwardingID, "wrong-runner", l.LeaseID, wrongExpiry)
	if err != forwardingstore.ErrTransitionConflict {
		t.Errorf("expected forwardingstore.ErrTransitionConflict, got %v", err)
	}
}
