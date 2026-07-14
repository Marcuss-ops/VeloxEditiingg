// Package store / forwarding_renew.go
//
// Lease RENEWAL path for the creator_forwardings table. Split out of
// store_creator_forwardings_lease.go as part of the per-concern refactor
// (claim / renew / transitions). The renew method is the smallest in
// the forwarding lease surface — a single CAS UPDATE that extends
// lease_expires_at on a row that is still owned by the same (runner_id,
// lease_id) pair.
//
// The CAS guard (status=POLLING, locked_by, lease_id) is what makes the
// renew safe against preemption: if a different runner has reclaimed
// the row, RowsAffected==0 and we surface ErrTransitionConflict so the
// runner knows to drop the lease and stop polling.
package store

import (
	"context"
	"fmt"
	"time"
)

// ── Renew ────────────────────────────────────────────────────────────────

// RenewCreatorForwardingLease extends the lease on a POLLING forwarding record.
// CAS guard verifies (forwarding_id, status=POLLING, locked_by, lease_id) to
// prevent stale renewals. Returns ErrTransitionConflict if the guard fails.
func (s *SQLiteStore) RenewCreatorForwardingLease(ctx context.Context, forwardingID, runnerID, leaseID string, newExpiry time.Time) error {
	if forwardingID == "" || runnerID == "" || leaseID == "" {
		return fmt.Errorf("store: RenewCreatorForwardingLease: missing required fields")
	}
	iso := newExpiry.UTC().Format(time.RFC3339)
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := s.db.ExecContext(ctx,
		`UPDATE creator_forwardings
		 SET lease_expires_at = ?, updated_at = ?
		 WHERE forwarding_id = ?
		   AND status = 'POLLING'
		   AND locked_by = ?
		   AND lease_id = ?`,
		iso, now, forwardingID, runnerID, leaseID,
	)
	if err != nil {
		return fmt.Errorf("store: RenewCreatorForwardingLease: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return ErrTransitionConflict
	}
	return nil
}
