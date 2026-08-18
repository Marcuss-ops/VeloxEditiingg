// Package store / forwarding_renew.go
//
// Lease RENEWAL path for the creator_forwardings table. The renew SQL now
// lives in the internal/forwardingstore leaf; this file keeps the historical
// store method name for existing callers.
package store

import (
	"context"
	"time"
)

// RenewCreatorForwardingLease extends the lease on a POLLING forwarding record.
// CAS guard verifies (forwarding_id, status=POLLING, locked_by, lease_id) to
// prevent stale renewals. Returns ErrTransitionConflict if the guard fails.
func (s *SQLiteStore) RenewCreatorForwardingLease(ctx context.Context, forwardingID, runnerID, leaseID string, newExpiry time.Time) error {
	return s.forwarding.RenewCreatorForwardingLease(ctx, forwardingID, runnerID, leaseID, newExpiry)
}
