package store

import (
	"context"
	"time"

	"velox-server/internal/deliverystore"
)

// ── Typed lease methods (PR4e) — facade over the deliverystore leaf ────────

// ClaimDeliveries atomically claims up to `batch` claimable deliveries for a
// runner. The SQL/CAS lives in internal/deliverystore; this facade preserves
// the historical store.ClaimDeliveries call site.
func (s *SQLiteStore) ClaimDeliveries(ctx context.Context, runnerID string, lease time.Duration, batch int) ([]deliverystore.DeliveryLease, error) {
	return s.deliveryStore().ClaimDeliveries(ctx, runnerID, lease, batch)
}

// RenewDeliveryLease extends the lease on a RUNNING delivery. The SQL/CAS
// lives in internal/deliverystore; this facade preserves the historical
// store.RenewDeliveryLease call site.
func (s *SQLiteStore) RenewDeliveryLease(ctx context.Context, deliveryID, runnerID, leaseID string, newExpiry time.Time) error {
	return s.deliveryStore().RenewDeliveryLease(ctx, deliveryID, runnerID, leaseID, newExpiry)
}
