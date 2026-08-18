package store

import (
	"context"
)

// forwarding_queries.go delegates the read-only forwarding recovery/sweep
// queries to the internal/forwardingstore leaf. The SQL lives in the leaf.

// ExpiredCreatorForwardingLeases returns forwarding records whose lease has
// expired (zombie reclaim candidates). SELECT-only — the caller is expected
// to re-claim via ClaimCreatorForwardings or transition via Mark* methods.
func (s *SQLiteStore) ExpiredCreatorForwardingLeases(ctx context.Context, nowRFC3339 string, limit int) ([]CreatorForwarding, error) {
	return s.forwarding.ExpiredCreatorForwardingLeases(ctx, nowRFC3339, limit)
}

// ListReadyToForward returns forwardings in READY_TO_FORWARD state that are
// ready to be enqueued.
func (s *SQLiteStore) ListReadyToForward(ctx context.Context, limit int) ([]CreatorForwarding, error) {
	return s.forwarding.ListReadyToForward(ctx, limit)
}
