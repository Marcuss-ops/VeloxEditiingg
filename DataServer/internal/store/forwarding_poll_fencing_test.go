package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"velox-server/internal/forwardingstore"
)

func TestRecordCreatorForwardingPoll_FencedSuccess(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()
	insertTestForwarding(t, db, "cf-poll-success", "openai", "poll-success", "scene.composite.v1", "PENDING")

	leases, err := db.Forwarding().ClaimCreatorForwardings(ctx, "runner-poll", "cf", 5*time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: err=%v len=%d", err, len(leases))
	}
	lease := leases[0]
	nextPoll := time.Now().UTC().Add(time.Minute)
	if err := db.Forwarding().RecordCreatorForwardingPoll(ctx, lease.ForwardingID, lease.RunnerID, lease.LeaseID, "running", nextPoll); err != nil {
		t.Fatalf("RecordCreatorForwardingPoll: %v", err)
	}

	row, err := db.Forwarding().GetCreatorForwarding(ctx, lease.ForwardingID)
	if err != nil {
		t.Fatalf("get forwarding: %v", err)
	}
	if row.PollAttempts != 1 || row.LastRemoteStatus != "running" || row.NextPollAt == "" {
		t.Fatalf("poll fields = attempts=%d status=%q next=%q", row.PollAttempts, row.LastRemoteStatus, row.NextPollAt)
	}
}

func TestRecordCreatorForwardingPoll_RejectsWrongRunner(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()
	insertTestForwarding(t, db, "cf-poll-runner", "openai", "poll-runner", "scene.composite.v1", "PENDING")
	leases, err := db.Forwarding().ClaimCreatorForwardings(ctx, "runner-owner", "cf", 5*time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: err=%v len=%d", err, len(leases))
	}
	lease := leases[0]
	if err := db.Forwarding().RecordCreatorForwardingPoll(ctx, lease.ForwardingID, "runner-other", lease.LeaseID, "running", time.Now().UTC()); !errors.Is(err, forwardingstore.ErrLeaseLost) {
		t.Fatalf("wrong runner error = %v, want forwardingstore.ErrLeaseLost", err)
	}
	assertPollUnchanged(t, db, lease.ForwardingID, 0, "")
}

func TestRecordCreatorForwardingPoll_RejectsWrongLease(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()
	insertTestForwarding(t, db, "cf-poll-lease", "openai", "poll-lease", "scene.composite.v1", "PENDING")
	leases, err := db.Forwarding().ClaimCreatorForwardings(ctx, "runner-owner", "cf", 5*time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: err=%v len=%d", err, len(leases))
	}
	lease := leases[0]
	if err := db.Forwarding().RecordCreatorForwardingPoll(ctx, lease.ForwardingID, lease.RunnerID, "lease-stale", "running", time.Now().UTC()); !errors.Is(err, forwardingstore.ErrLeaseLost) {
		t.Fatalf("wrong lease error = %v, want forwardingstore.ErrLeaseLost", err)
	}
	assertPollUnchanged(t, db, lease.ForwardingID, 0, "")
}

func TestRecordCreatorForwardingPoll_RejectsExpiredLeaseAndPreservesNextPoll(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()
	insertTestForwarding(t, db, "cf-poll-expired", "openai", "poll-expired", "scene.composite.v1", "PENDING")
	leases, err := db.Forwarding().ClaimCreatorForwardings(ctx, "runner-expired", "cf", -time.Second, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: err=%v len=%d", err, len(leases))
	}
	lease := leases[0]
	const sentinelNextPoll = "2030-01-01T00:00:00Z"
	if _, err := db.db.ExecContext(ctx, `UPDATE creator_forwardings SET next_poll_at = ? WHERE forwarding_id = ?`, sentinelNextPoll, lease.ForwardingID); err != nil {
		t.Fatalf("seed next_poll_at: %v", err)
	}
	if err := db.Forwarding().RecordCreatorForwardingPoll(ctx, lease.ForwardingID, lease.RunnerID, lease.LeaseID, "running", time.Now().UTC()); !errors.Is(err, forwardingstore.ErrLeaseLost) {
		t.Fatalf("expired lease error = %v, want forwardingstore.ErrLeaseLost", err)
	}
	row, err := db.Forwarding().GetCreatorForwarding(ctx, lease.ForwardingID)
	if err != nil {
		t.Fatalf("get forwarding: %v", err)
	}
	if row.PollAttempts != 0 || row.NextPollAt != sentinelNextPoll || row.LastRemoteStatus != "" {
		t.Fatalf("expired poll mutated row: attempts=%d next=%q status=%q", row.PollAttempts, row.NextPollAt, row.LastRemoteStatus)
	}
}

func TestRecordCreatorForwardingPoll_TakeoverRejectsStaleLease(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()
	insertTestForwarding(t, db, "cf-poll-takeover", "openai", "poll-takeover", "scene.composite.v1", "PENDING")
	oldLeases, err := db.Forwarding().ClaimCreatorForwardings(ctx, "runner-old", "cf", -time.Second, 1)
	if err != nil || len(oldLeases) != 1 {
		t.Fatalf("old claim: err=%v len=%d", err, len(oldLeases))
	}
	oldLease := oldLeases[0]
	newLeases, err := db.Forwarding().ClaimCreatorForwardings(ctx, "runner-new", "cf", 5*time.Minute, 1)
	if err != nil || len(newLeases) != 1 {
		t.Fatalf("takeover claim: err=%v len=%d", err, len(newLeases))
	}
	if err := db.Forwarding().RecordCreatorForwardingPoll(ctx, oldLease.ForwardingID, oldLease.RunnerID, oldLease.LeaseID, "stale", time.Now().UTC()); !errors.Is(err, forwardingstore.ErrLeaseLost) {
		t.Fatalf("takeover error = %v, want forwardingstore.ErrLeaseLost", err)
	}
	row, err := db.Forwarding().GetCreatorForwarding(ctx, oldLease.ForwardingID)
	if err != nil {
		t.Fatalf("get forwarding: %v", err)
	}
	if row.LockedBy != newLeases[0].RunnerID || row.LeaseID != newLeases[0].LeaseID || row.PollAttempts != 0 || row.LastRemoteStatus != "" {
		t.Fatalf("takeover row was mutated by stale poll: locked_by=%q lease=%q attempts=%d status=%q", row.LockedBy, row.LeaseID, row.PollAttempts, row.LastRemoteStatus)
	}
}

func assertPollUnchanged(t *testing.T, db *SQLiteStore, forwardingID string, wantAttempts int, wantStatus string) {
	t.Helper()
	row, err := db.Forwarding().GetCreatorForwarding(context.Background(), forwardingID)
	if err != nil {
		t.Fatalf("get forwarding: %v", err)
	}
	if row.PollAttempts != wantAttempts || row.LastRemoteStatus != wantStatus {
		t.Fatalf("poll row changed: attempts=%d status=%q", row.PollAttempts, row.LastRemoteStatus)
	}
}
