// Package store / forwarding_claim.go
//
// Lease-based CLAIM path for the creator_forwardings table. The atomic
// claim SQL now lives in the internal/forwardingstore leaf; this file keeps
// the historical store method name for existing callers.
package store

import (
	"context"
	"time"
)

// ClaimCreatorForwardings atomically claims up to `batch` claimable forwarding
// records for a runner. Returns typed CreatorForwardingLease values for the
// runner to dispatch.
func (s *SQLiteStore) ClaimCreatorForwardings(ctx context.Context, runnerID, leaseProvisionalPrefix string, lease time.Duration, batch int) ([]CreatorForwardingLease, error) {
	return s.forwarding.ClaimCreatorForwardings(ctx, runnerID, leaseProvisionalPrefix, lease, batch)
}
