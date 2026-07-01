package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func setupForwardingTestDB(t *testing.T) *SQLiteStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "cf_lease_test.sqlite")
	dbStore, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	t.Cleanup(func() { dbStore.Close() })
	return dbStore
}

func insertTestForwarding(t *testing.T, db *SQLiteStore, forwardingID, provider, sourceJobID, executorID, status string) {
	t.Helper()
	cf := &CreatorForwarding{
		ForwardingID:     forwardingID,
		SourceProvider:   provider,
		SourceJobID:      sourceJobID,
		TargetExecutorID: executorID,
		Status:           status,
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
		UpdatedAt:        time.Now().UTC().Format(time.RFC3339),
	}
	if err := db.InsertCreatorForwarding(cf); err != nil {
		t.Fatalf("insert forwarding: %v", err)
	}
}

func insertTestForwardingWithPayload(t *testing.T, db *SQLiteStore, forwardingID, provider, sourceJobID, executorID, status, payloadJSON, payloadSHA256 string) {
	t.Helper()
	cf := &CreatorForwarding{
		ForwardingID:     forwardingID,
		SourceProvider:   provider,
		SourceJobID:      sourceJobID,
		TargetExecutorID: executorID,
		Status:           status,
		PayloadJSON:      payloadJSON,
		PayloadSHA256:    payloadSHA256,
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
		UpdatedAt:        time.Now().UTC().Format(time.RFC3339),
	}
	if err := db.InsertCreatorForwarding(cf); err != nil {
		t.Fatalf("insert forwarding with payload: %v", err)
	}
}

// ── Insert + Get ────────────────────────────────────────────────────────

func TestInsertAndGetForwarding(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertTestForwarding(t, db, "cf-001", "openai", "creator-job-1", "scene.composite.v1", "PENDING")

	cf, err := db.GetCreatorForwarding(ctx, "cf-001")
	if err != nil {
		t.Fatalf("GetCreatorForwarding: %v", err)
	}
	if cf.Status != "PENDING" {
		t.Errorf("status = %q, want PENDING", cf.Status)
	}
	if cf.SourceProvider != "openai" {
		t.Errorf("source_provider = %q, want openai", cf.SourceProvider)
	}
	if cf.AttemptCount != 0 {
		t.Errorf("attempt_count = %d, want 0", cf.AttemptCount)
	}
}

func TestInsertForwarding_Idempotent(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	// First insert
	insertTestForwarding(t, db, "cf-idem", "openai", "creator-job-2", "scene.composite.v1", "PENDING")

	// Second insert with same unique key should be ignored
	cf2 := &CreatorForwarding{
		ForwardingID:     "cf-idem-2",
		SourceProvider:   "openai",
		SourceJobID:      "creator-job-2",
		TargetExecutorID: "scene.composite.v1",
		Status:           "PENDING",
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
		UpdatedAt:        time.Now().UTC().Format(time.RFC3339),
	}
	if err := db.InsertCreatorForwarding(cf2); err != nil {
		t.Fatalf("second insert: %v", err)
	}

	// First record should still exist
	cf, err := db.GetCreatorForwarding(ctx, "cf-idem")
	if err != nil {
		t.Fatalf("GetCreatorForwarding: %v", err)
	}
	if cf.ForwardingID != "cf-idem" {
		t.Errorf("forwarding_id = %q, want cf-idem", cf.ForwardingID)
	}

	// Second record should NOT exist (ignored by UNIQUE)
	_, err = db.GetCreatorForwarding(ctx, "cf-idem-2")
	if err != ErrCreatorForwardingNoRow {
		t.Errorf("expected ErrCreatorForwardingNoRow, got %v", err)
	}
}

func TestGetForwardingBySource(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertTestForwarding(t, db, "cf-src", "openai", "creator-job-3", "scene.composite.v1", "PENDING")

	cf, err := db.GetCreatorForwardingBySource(ctx, "openai", "creator-job-3", "scene.composite.v1")
	if err != nil {
		t.Fatalf("GetCreatorForwardingBySource: %v", err)
	}
	if cf.ForwardingID != "cf-src" {
		t.Errorf("forwarding_id = %q, want cf-src", cf.ForwardingID)
	}
}

func TestGetForwardingMising(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	_, err := db.GetCreatorForwarding(ctx, "nonexistent")
	if err != ErrCreatorForwardingNoRow {
		t.Errorf("expected ErrCreatorForwardingNoRow, got %v", err)
	}
}

// ── Claim ────────────────────────────────────────────────────────────────

func TestClaimForwardings_BasicClaim(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertTestForwarding(t, db, "cf-claim-1", "openai", "creator-job-10", "scene.composite.v1", "PENDING")

	leases, err := db.ClaimCreatorForwardings(ctx, "runner-1", "cf", 5*time.Minute, 4)
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

	leases1, err := db.ClaimCreatorForwardings(ctx, "runner-A", "cf", 5*time.Minute, 5)
	if err != nil {
		t.Fatalf("runner-A claim: %v", err)
	}
	leases2, err := db.ClaimCreatorForwardings(ctx, "runner-B", "cf", 5*time.Minute, 5)
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

	// Claim with short lease
	leases, err := db.ClaimCreatorForwardings(ctx, "runner-old", "cf", 1*time.Second, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("initial claim: %v len=%d", err, len(leases))
	}

	// Wait for lease to expire
	time.Sleep(2100 * time.Millisecond)

	// New runner should reclaim the zombie
	leases2, err := db.ClaimCreatorForwardings(ctx, "runner-new", "cf", 5*time.Minute, 1)
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
	cf := &CreatorForwarding{
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
	if err := db.InsertCreatorForwarding(cf); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Should NOT be claimed because next_attempt_at is in the future
	leases, err := db.ClaimCreatorForwardings(ctx, "runner-x", "cf", 5*time.Minute, 1)
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
	cf := &CreatorForwarding{
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
	if err := db.InsertCreatorForwarding(cf); err != nil {
		t.Fatalf("insert: %v", err)
	}

	leases, err := db.ClaimCreatorForwardings(ctx, "runner-x", "cf", 5*time.Minute, 1)
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

// ── Renew ────────────────────────────────────────────────────────────────

func TestRenewForwardingLease(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertTestForwarding(t, db, "cf-renew", "openai", "creator-renew", "scene.composite.v1", "PENDING")

	leases, err := db.ClaimCreatorForwardings(ctx, "runner-renew", "cf", 5*time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: %v len=%d", err, len(leases))
	}

	l := leases[0]
	newExpiry := time.Now().UTC().Add(10 * time.Minute)
	err = db.RenewCreatorForwardingLease(ctx, l.ForwardingID, l.RunnerID, l.LeaseID, newExpiry)
	if err != nil {
		t.Fatalf("RenewCreatorForwardingLease: %v", err)
	}

	// Verify still in POLLING
	cf, err := db.GetCreatorForwarding(ctx, l.ForwardingID)
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

	leases, err := db.ClaimCreatorForwardings(ctx, "runner-real", "cf", 5*time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: %v len=%d", err, len(leases))
	}

	l := leases[0]
	wrongExpiry := time.Now().UTC().Add(10 * time.Minute)
	err = db.RenewCreatorForwardingLease(ctx, l.ForwardingID, "wrong-runner", l.LeaseID, wrongExpiry)
	if err != ErrTransitionConflict {
		t.Errorf("expected ErrTransitionConflict, got %v", err)
	}
}

// ── State Transitions ──────────────────────────────────────────────────

func TestMarkReadyToForward(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertTestForwarding(t, db, "cf-r2f", "openai", "creator-r2f", "scene.composite.v1", "PENDING")

	leases, err := db.ClaimCreatorForwardings(ctx, "runner-r2f", "cf", 5*time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: %v len=%d", err, len(leases))
	}

	l := leases[0]
	err = db.MarkCreatorForwardingReadyToForward(ctx, l.ForwardingID, l.RunnerID, l.LeaseID, `{"video":"test"}`, "abc123")
	if err != nil {
		t.Fatalf("MarkCreatorForwardingReadyToForward: %v", err)
	}

	cf, err := db.GetCreatorForwarding(ctx, l.ForwardingID)
	if err != nil {
		t.Fatalf("GetCreatorForwarding: %v", err)
	}
	if cf.Status != "READY_TO_FORWARD" {
		t.Errorf("status = %q, want READY_TO_FORWARD", cf.Status)
	}
	if cf.SourceStatus != "completed" {
		t.Errorf("source_status = %q, want completed", cf.SourceStatus)
	}
	if cf.PayloadJSON != `{"video":"test"}` {
		t.Errorf("payload_json = %q", cf.PayloadJSON)
	}
	if cf.PayloadSHA256 != "abc123" {
		t.Errorf("payload_sha256 = %q, want abc123", cf.PayloadSHA256)
	}
	if cf.LockedBy != "" {
		t.Error("locked_by should be cleared")
	}
}

func TestMarkForwardingForwarded(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertTestForwardingWithPayload(t, db, "cf-fwd", "openai", "creator-fwd", "scene.composite.v1", "READY_TO_FORWARD", `{"video":"test"}`, "abc")

	// READY_TO_FORWARD → FORWARDING
	if err := db.MarkCreatorForwardingForwarding(ctx, "cf-fwd"); err != nil {
		t.Fatalf("MarkCreatorForwardingForwarding: %v", err)
	}

	// FORWARDING → FORWARDED
	if err := db.MarkCreatorForwardingForwarded(ctx, "cf-fwd", "target-job-123"); err != nil {
		t.Fatalf("MarkCreatorForwardingForwarded: %v", err)
	}

	cf, err := db.GetCreatorForwarding(ctx, "cf-fwd")
	if err != nil {
		t.Fatalf("GetCreatorForwarding: %v", err)
	}
	if cf.Status != "FORWARDED" {
		t.Errorf("status = %q, want FORWARDED", cf.Status)
	}
	if cf.TargetJobID != "target-job-123" {
		t.Errorf("target_job_id = %q, want target-job-123", cf.TargetJobID)
	}
	if cf.ForwardedAt == "" {
		t.Error("forwarded_at should be set")
	}
}

func TestMarkForwardingRetry(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertTestForwarding(t, db, "cf-retry", "openai", "creator-retry", "scene.composite.v1", "PENDING")

	leases, err := db.ClaimCreatorForwardings(ctx, "runner-retry", "cf", 5*time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: %v len=%d", err, len(leases))
	}

	l := leases[0]
	nextAttempt := time.Now().UTC().Add(2 * time.Minute)
	err = db.MarkCreatorForwardingRetry(ctx, l.ForwardingID, l.RunnerID, l.LeaseID, "POLL_FAILED", "connection refused", nextAttempt)
	if err != nil {
		t.Fatalf("MarkCreatorForwardingRetry: %v", err)
	}

	cf, err := db.GetCreatorForwarding(ctx, l.ForwardingID)
	if err != nil {
		t.Fatalf("GetCreatorForwarding: %v", err)
	}
	if cf.Status != "RETRY_WAIT" {
		t.Errorf("status = %q, want RETRY_WAIT", cf.Status)
	}
	if cf.LockedBy != "" {
		t.Errorf("locked_by = %q, want empty", cf.LockedBy)
	}
	if cf.LeaseID != "" {
		t.Errorf("lease_id = %q, want empty", cf.LeaseID)
	}
	if cf.NextAttemptAt == "" {
		t.Error("next_attempt_at should be set")
	}
	if cf.LastErrorCode != "POLL_FAILED" {
		t.Errorf("last_error_code = %q, want POLL_FAILED", cf.LastErrorCode)
	}
}

func TestMarkForwardingFailed(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertTestForwarding(t, db, "cf-fail", "openai", "creator-fail", "scene.composite.v1", "RETRY_WAIT")

	err := db.MarkCreatorForwardingFailed(ctx, "cf-fail", "MAX_ATTEMPTS", "exhausted retries")
	if err != nil {
		t.Fatalf("MarkCreatorForwardingFailed: %v", err)
	}

	cf, err := db.GetCreatorForwarding(ctx, "cf-fail")
	if err != nil {
		t.Fatalf("GetCreatorForwarding: %v", err)
	}
	if cf.Status != "FAILED" {
		t.Errorf("status = %q, want FAILED", cf.Status)
	}
	if cf.LastErrorCode != "MAX_ATTEMPTS" {
		t.Errorf("last_error_code = %q, want MAX_ATTEMPTS", cf.LastErrorCode)
	}
}

func TestMarkForwardingBlocked(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertTestForwarding(t, db, "cf-block", "openai", "creator-block", "scene.composite.v1", "PENDING")

	err := db.MarkCreatorForwardingBlocked(ctx, "cf-block", "INVALID_PAYLOAD", "bad schema")
	if err != nil {
		t.Fatalf("MarkCreatorForwardingBlocked: %v", err)
	}

	cf, err := db.GetCreatorForwarding(ctx, "cf-block")
	if err != nil {
		t.Fatalf("GetCreatorForwarding: %v", err)
	}
	if cf.Status != "BLOCKED" {
		t.Errorf("status = %q, want BLOCKED", cf.Status)
	}
}

// ── Recovery ────────────────────────────────────────────────────────────

func TestExpiredForwardingLeases(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertTestForwarding(t, db, "cf-exp-1", "openai", "creator-exp-1", "scene.composite.v1", "PENDING")
	insertTestForwarding(t, db, "cf-exp-2", "openai", "creator-exp-2", "scene.composite.v1", "PENDING")

	// Claim both with short lease
	leases, err := db.ClaimCreatorForwardings(ctx, "runner-exp", "cf", 1*time.Second, 4)
	if err != nil || len(leases) != 2 {
		t.Fatalf("claim: %v len=%d", err, len(leases))
	}

	// Wait for lease to expire
	time.Sleep(2100 * time.Millisecond)

	expired, err := db.ExpiredCreatorForwardingLeases(ctx, time.Now().UTC().Format(time.RFC3339), 10)
	if err != nil {
		t.Fatalf("ExpiredCreatorForwardingLeases: %v", err)
	}
	if len(expired) != 2 {
		t.Errorf("expected 2 expired, got %d", len(expired))
	}
}

func TestListReadyToForward(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertTestForwardingWithPayload(t, db, "cf-ready-1", "openai", "creator-r-1", "scene.composite.v1", "READY_TO_FORWARD", `{"v":"1"}`, "sha1")
	insertTestForwardingWithPayload(t, db, "cf-ready-2", "openai", "creator-r-2", "scene.composite.v1", "READY_TO_FORWARD", `{"v":"2"}`, "sha2")
	insertTestForwarding(t, db, "cf-pending", "openai", "creator-r-3", "scene.composite.v1", "PENDING")

	ready, err := db.ListReadyToForward(ctx, 10)
	if err != nil {
		t.Fatalf("ListReadyToForward: %v", err)
	}
	if len(ready) != 2 {
		t.Errorf("expected 2 ready, got %d", len(ready))
	}
}

func TestCannotClaimTerminalForwarding(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertTestForwarding(t, db, "cf-final", "openai", "creator-final", "scene.composite.v1", "FORWARDED")

	leases, err := db.ClaimCreatorForwardings(ctx, "runner-x", "cf", 5*time.Minute, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(leases) != 0 {
		t.Errorf("expected 0 claims on FORWARDED forwarding, got %d", len(leases))
	}
}
