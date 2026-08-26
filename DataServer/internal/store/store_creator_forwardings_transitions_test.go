package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"velox-server/internal/forwardingcontract"
	"velox-server/internal/forwardingstore"
)

func TestMarkReadyToForward(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertTestForwarding(t, db, "cf-r2f", "openai", "creator-r2f", "scene.composite.v1", "PENDING")

	leases, err := db.Forwarding().ClaimCreatorForwardings(ctx, "runner-r2f", "cf", 5*time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: %v len=%d", err, len(leases))
	}

	l := leases[0]
	err = db.Forwarding().MarkCreatorForwardingReadyToForward(ctx, l.ForwardingID, l.RunnerID, l.LeaseID, `{"video":"test"}`, "abc123")
	if err != nil {
		t.Fatalf("MarkCreatorForwardingReadyToForward: %v", err)
	}

	cf, err := db.Forwarding().GetCreatorForwarding(ctx, l.ForwardingID)
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
	if cf.LockedBy != l.RunnerID || cf.LeaseID != l.LeaseID {
		t.Errorf("runner lease = (%q,%q), want retained (%q,%q)", cf.LockedBy, cf.LeaseID, l.RunnerID, l.LeaseID)
	}
}

func TestMarkForwardingForwarded(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertTestForwardingWithPayload(t, db, "cf-fwd", "openai", "creator-fwd", "scene.composite.v1", "READY_TO_FORWARD", `{"video":"test"}`, "abc")

	// READY_TO_FORWARD → FORWARDING
	if err := db.Forwarding().MarkCreatorForwardingForwarding(ctx, "cf-fwd"); err != nil {
		t.Fatalf("MarkCreatorForwardingForwarding: %v", err)
	}

	// FORWARDING → FORWARDED
	if err := db.Forwarding().MarkCreatorForwardingForwarded(ctx, "cf-fwd", "target-job-123"); err != nil {
		t.Fatalf("MarkCreatorForwardingForwarded: %v", err)
	}

	cf, err := db.Forwarding().GetCreatorForwarding(ctx, "cf-fwd")
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

	leases, err := db.Forwarding().ClaimCreatorForwardings(ctx, "runner-retry", "cf", 5*time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: %v len=%d", err, len(leases))
	}

	l := leases[0]
	nextAttempt := time.Now().UTC().Add(2 * time.Minute)
	err = db.Forwarding().MarkCreatorForwardingRetry(ctx, l.ForwardingID, l.RunnerID, l.LeaseID, "POLL_FAILED", "connection refused", "TRANSIENT", nextAttempt)
	if err != nil {
		t.Fatalf("MarkCreatorForwardingRetry: %v", err)
	}

	cf, err := db.Forwarding().GetCreatorForwarding(ctx, l.ForwardingID)
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

func TestMarkCreatorForwardingReadySyncDoesNotStealLease(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()
	insertTestForwarding(t, db, "cf-sync-fence", "openai", "creator-sync-fence", "scene.composite.v1", "PENDING")
	leaseExpiry := time.Now().UTC().Add(time.Minute).Format(time.RFC3339)
	if _, err := db.db.ExecContext(ctx, `UPDATE creator_forwardings
		SET locked_by=?, lease_id=?, lease_expires_at=? WHERE forwarding_id=?`,
		"runner-owner", "lease-owner", leaseExpiry, "cf-sync-fence"); err != nil {
		t.Fatalf("seed forwarding lease: %v", err)
	}

	err := db.Forwarding().MarkCreatorForwardingReadySync(ctx, "cf-sync-fence", `{"complete":true}`, "sha-complete")
	if err != forwardingstore.ErrTransitionConflict {
		t.Fatalf("ReadySync error=%v, want forwardingstore.ErrTransitionConflict", err)
	}
	saved, err := db.Forwarding().GetCreatorForwarding(ctx, "cf-sync-fence")
	if err != nil {
		t.Fatalf("GetCreatorForwarding: %v", err)
	}
	if saved.Status != "PENDING" || saved.LockedBy != "runner-owner" || saved.LeaseID != "lease-owner" {
		t.Fatalf("sync path stole lease or changed status: status=%q owner=(%q,%q)", saved.Status, saved.LockedBy, saved.LeaseID)
	}
	if saved.PayloadJSON != "" || saved.PayloadSHA256 != "" {
		t.Fatalf("sync path wrote payload despite lease conflict: payload=%q sha=%q", saved.PayloadJSON, saved.PayloadSHA256)
	}
}

func TestMarkForwardingFailed(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertTestForwarding(t, db, "cf-fail", "openai", "creator-fail", "scene.composite.v1", "RETRY_WAIT")

	err := db.Forwarding().MarkCreatorForwardingFailed(ctx, "cf-fail", "", "", "MAX_ATTEMPTS", "exhausted retries", "PERMANENT")
	if err != nil {
		t.Fatalf("MarkCreatorForwardingFailed: %v", err)
	}

	cf, err := db.Forwarding().GetCreatorForwarding(ctx, "cf-fail")
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

	err := db.Forwarding().MarkCreatorForwardingBlocked(ctx, "cf-block", "", "", "INVALID_PAYLOAD", "bad schema")
	if err != nil {
		t.Fatalf("MarkCreatorForwardingBlocked: %v", err)
	}

	cf, err := db.Forwarding().GetCreatorForwarding(ctx, "cf-block")
	if err != nil {
		t.Fatalf("GetCreatorForwarding: %v", err)
	}
	if cf.Status != "BLOCKED" {
		t.Errorf("status = %q, want BLOCKED", cf.Status)
	}
}

func TestExpiredForwardingLeases(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertTestForwarding(t, db, "cf-exp-1", "openai", "creator-exp-1", "scene.composite.v1", "PENDING")
	insertTestForwarding(t, db, "cf-exp-2", "openai", "creator-exp-2", "scene.composite.v1", "PENDING")

	// Claim both with short lease
	leases, err := db.Forwarding().ClaimCreatorForwardings(ctx, "runner-exp", "cf", 1*time.Second, 4)
	if err != nil || len(leases) != 2 {
		t.Fatalf("claim: %v len=%d", err, len(leases))
	}

	// Wait for lease to expire
	time.Sleep(2100 * time.Millisecond)

	expired, err := db.Forwarding().ExpiredCreatorForwardingLeases(ctx, time.Now().UTC().Format(time.RFC3339), 10)
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

	ready, err := db.Forwarding().ListReadyToForward(ctx, 10)
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

	leases, err := db.Forwarding().ClaimCreatorForwardings(ctx, "runner-x", "cf", 5*time.Minute, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(leases) != 0 {
		t.Errorf("expected 0 claims on FORWARDED forwarding, got %d", len(leases))
	}
}

func TestMarkCreatorForwardingCancelledForClient(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertClientForwarding := func(forwardingID, clientID, status string) {
		t.Helper()
		cf := &forwardingcontract.CreatorForwarding{
			ForwardingID:     forwardingID,
			ExternalClientID: clientID,
			SourceProvider:   "external_api",
			SourceJobID:      forwardingID + "-source",
			TargetExecutorID: "scene.composite.v1",
			Status:           status,
			CreatedAt:        time.Now().UTC().Format(time.RFC3339),
			UpdatedAt:        time.Now().UTC().Format(time.RFC3339),
		}
		if _, err := db.Forwarding().InsertCreatorForwarding(ctx, cf); err != nil {
			t.Fatalf("insert forwarding %s: %v", forwardingID, err)
		}
	}

	// 1. Matching client cancels the row.
	insertClientForwarding("cf-client-cancel", "client-a", "PENDING")
	if err := db.Forwarding().MarkCreatorForwardingCancelledForClient(ctx, "cf-client-cancel", "client-a", "CANCELLED_BY_USER", "cancelled by user"); err != nil {
		t.Fatalf("cancel matching client: %v", err)
	}
	row, err := db.Forwarding().GetCreatorForwarding(ctx, "cf-client-cancel")
	if err != nil {
		t.Fatalf("get cancelled forwarding: %v", err)
	}
	if row.Status != "CANCELLED" || row.LastErrorCode != "CANCELLED_BY_USER" || row.LastErrorMessage != "cancelled by user" {
		t.Errorf("cancelled row = status=%q err=%q msg=%q", row.Status, row.LastErrorCode, row.LastErrorMessage)
	}

	// 2. Wrong client is fenced and leaves the row untouched.
	insertClientForwarding("cf-client-fence", "client-a", "PENDING")
	if err := db.Forwarding().MarkCreatorForwardingCancelledForClient(ctx, "cf-client-fence", "client-b", "X", "y"); !errors.Is(err, forwardingstore.ErrCreatorForwardingNoRow) {
		t.Fatalf("wrong client error = %v, want forwardingstore.ErrCreatorForwardingNoRow", err)
	}
	fenced, err := db.Forwarding().GetCreatorForwarding(ctx, "cf-client-fence")
	if err != nil {
		t.Fatalf("get fenced forwarding: %v", err)
	}
	if fenced.Status != "PENDING" {
		t.Errorf("wrong-client cancel mutated status to %q", fenced.Status)
	}

	// 3. Terminal rows cannot be cancelled by a client.
	insertClientForwarding("cf-client-terminal", "client-a", "FORWARDED")
	if err := db.Forwarding().MarkCreatorForwardingCancelledForClient(ctx, "cf-client-terminal", "client-a", "X", "y"); !errors.Is(err, forwardingstore.ErrCreatorForwardingNoRow) {
		t.Fatalf("terminal cancel error = %v, want forwardingstore.ErrCreatorForwardingNoRow", err)
	}

	// 4. Empty client ID is fenced.
	if err := db.Forwarding().MarkCreatorForwardingCancelledForClient(ctx, "cf-client-cancel", "", "X", "y"); !errors.Is(err, forwardingstore.ErrCreatorForwardingNoRow) {
		t.Fatalf("empty client error = %v, want forwardingstore.ErrCreatorForwardingNoRow", err)
	}
}
