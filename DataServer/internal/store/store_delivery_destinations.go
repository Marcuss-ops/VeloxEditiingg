// Package store / store_delivery_destinations.go
//
// CRUD for delivery_destinations (reusable, per-provider configuration).
// The SQL/CAS lives in the deliverystore leaf; these methods are delegating
// facades that preserve the historical store.<Method> call sites.
package store

import (
	"context"

	"velox-server/internal/deliverystore"
)

// DeliveryDestinationStatus is the 3-state verdict returned by
// BatchDeliveryDestinationsStatus. Re-exported as an alias from the
// deliverystore leaf (the canonical declaration).
type DeliveryDestinationStatus = deliverystore.DeliveryDestinationStatus

// The three buckets are re-exported so existing store.<Const> call sites keep
// compiling while the canonical constants live in the leaf.
const (
	DeliveryDestinationNotFound = deliverystore.DeliveryDestinationNotFound
	DeliveryDestinationDisabled = deliverystore.DeliveryDestinationDisabled
	DeliveryDestinationEnabled  = deliverystore.DeliveryDestinationEnabled
)

// ── Destination CRUD — facade over the deliverystore leaf ───────────────────

// InsertDeliveryDestination persists a delivery destination (idempotent
// on destination_id via INSERT OR IGNORE so retries are safe).
func (s *SQLiteStore) InsertDeliveryDestination(dest *deliverystore.DeliveryDestination) error {
	return s.deliveryStore().InsertDeliveryDestination(dest)
}

// ListDeliveryDestinations returns all enabled destinations, optionally
// filtered by provider. Returns at most `limit` rows (zero means default).
func (s *SQLiteStore) ListDeliveryDestinations(provider string, limit int) ([]deliverystore.DeliveryDestination, error) {
	return s.deliveryStore().ListDeliveryDestinations(provider, limit)
}

// GetDeliveryDestinationByExternalID returns a destination by its
// opaque InstaEdit identifier.
func (s *SQLiteStore) GetDeliveryDestinationByExternalID(ctx context.Context, externalID string) (*deliverystore.DeliveryDestination, error) {
	return s.deliveryStore().GetDeliveryDestinationByExternalID(ctx, externalID)
}

// BatchDeliveryDestinationsStatus buckets each destination_id into
// NOT_FOUND / DISABLED / ENABLED.
func (s *SQLiteStore) BatchDeliveryDestinationsStatus(ctx context.Context, ids []string) (map[string]deliverystore.DeliveryDestinationStatus, error) {
	return s.deliveryStore().BatchDeliveryDestinationsStatus(ctx, ids)
}

// BatchDeliveryDestinations returns a point-in-time local registry snapshot
// for the requested destination IDs.
func (s *SQLiteStore) BatchDeliveryDestinations(ctx context.Context, ids []string) (map[string]*deliverystore.DeliveryDestination, error) {
	return s.deliveryStore().BatchDeliveryDestinations(ctx, ids)
}

// GetDeliveryDestination returns a single destination by id, or
// ErrDeliveryNoRow when missing.
func (s *SQLiteStore) GetDeliveryDestination(ctx context.Context, destID string) (*deliverystore.DeliveryDestination, error) {
	return s.deliveryStore().GetDeliveryDestination(ctx, destID)
}
