package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMarkCreatorForwardingReadyToForward_ExpiredLeaseIsFenced(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()
	insertTestForwarding(t, db, "cf-ready-expired", "openai", "ready-expired", "scene.composite.v1", "PENDING")

	leases, err := db.ClaimCreatorForwardings(ctx, "runner-ready", "cf", time.Minute, 1)
	if err != nil || len(leases) != 1 {
		t.Fatalf("claim: err=%v len=%d", err, len(leases))
	}
	lease := leases[0]
	const sentinelNextPoll = "2030-01-01T00:00:00Z"
	if _, err := db.DB().ExecContext(ctx,
		`UPDATE creator_forwardings SET lease_expires_at = ?, next_poll_at = ? WHERE forwarding_id = ?`,
		time.Now().UTC().Add(-time.Minute).Format(time.RFC3339), sentinelNextPoll, lease.ForwardingID); err != nil {
		t.Fatalf("expire lease: %v", err)
	}

	err = db.MarkCreatorForwardingReadyToForward(ctx, lease.ForwardingID, lease.RunnerID, lease.LeaseID, `{"video":"stale"}`, "sha-stale")
	if err == nil {
		t.Fatal("expired MarkReadyToForward unexpectedly succeeded")
	}
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("expired MarkReadyToForward error = %v, want ErrTransitionConflict", err)
	}

	row, err := db.GetCreatorForwarding(ctx, lease.ForwardingID)
	if err != nil {
		t.Fatalf("get forwarding: %v", err)
	}
	if row.Status != "POLLING" || row.PayloadJSON != "" || row.PollAttempts != 0 || row.NextPollAt != sentinelNextPoll {
		t.Fatalf("expired MarkReady mutated row: status=%q payload=%q polls=%d next=%q", row.Status, row.PayloadJSON, row.PollAttempts, row.NextPollAt)
	}
}

func TestLeaseFencedTransitionsRejectPartialIdentity(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		call func(*SQLiteStore) error
	}{
		{
			name: "failed_runner_only",
			call: func(db *SQLiteStore) error {
				return db.MarkCreatorForwardingFailed(ctx, "missing-runner", "runner-only", "", "STALE", "stale", "")
			},
		},
		{
			name: "blocked_lease_only",
			call: func(db *SQLiteStore) error {
				return db.MarkCreatorForwardingBlocked(ctx, "missing-lease", "", "lease-only", "STALE", "stale")
			},
		},
		{
			name: "cancelled_runner_only",
			call: func(db *SQLiteStore) error {
				return db.MarkCreatorForwardingCancelled(ctx, "missing-cancel-runner", "runner-only", "", "STALE", "stale")
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := setupForwardingTestDB(t)
			if err := tc.call(db); err == nil {
				t.Fatal("partial lease identity unexpectedly succeeded")
			}
		})
	}
}

func TestLeaseFencedTransitionsRejectExpiredLease(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		call func(*SQLiteStore, CreatorForwardingLease) error
	}{
		{
			name: "retry",
			call: func(db *SQLiteStore, lease CreatorForwardingLease) error {
				return db.MarkCreatorForwardingRetry(ctx, lease.ForwardingID, lease.RunnerID, lease.LeaseID, "STALE", "stale", "", time.Now().UTC())
			},
		},
		{
			name: "failed",
			call: func(db *SQLiteStore, lease CreatorForwardingLease) error {
				return db.MarkCreatorForwardingFailed(ctx, lease.ForwardingID, lease.RunnerID, lease.LeaseID, "STALE", "stale", "")
			},
		},
		{
			name: "blocked",
			call: func(db *SQLiteStore, lease CreatorForwardingLease) error {
				return db.MarkCreatorForwardingBlocked(ctx, lease.ForwardingID, lease.RunnerID, lease.LeaseID, "STALE", "stale")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := setupForwardingTestDB(t)
			forwardingID := "cf-expired-" + tc.name
			insertTestForwarding(t, db, forwardingID, "openai", forwardingID+"-source", "scene.composite.v1", "PENDING")

			leases, err := db.ClaimCreatorForwardings(ctx, "runner-expired", "cf", time.Minute, 1)
			if err != nil || len(leases) != 1 {
				t.Fatalf("claim: err=%v len=%d", err, len(leases))
			}
			lease := leases[0]
			if _, err := db.DB().ExecContext(ctx,
				`UPDATE creator_forwardings SET lease_expires_at = ? WHERE forwarding_id = ?`,
				time.Now().UTC().Add(-time.Minute).Format(time.RFC3339), forwardingID); err != nil {
				t.Fatalf("expire lease: %v", err)
			}

			if err := tc.call(db, lease); !errors.Is(err, ErrTransitionConflict) {
				t.Fatalf("expired %s transition error = %v, want ErrTransitionConflict", tc.name, err)
			}
			row, err := db.GetCreatorForwarding(ctx, forwardingID)
			if err != nil {
				t.Fatalf("get forwarding: %v", err)
			}
			if row.Status != "POLLING" {
				t.Fatalf("expired %s transition changed status to %q", tc.name, row.Status)
			}
		})
	}
}
