// Package store / store_deliveries.go
//
// Typed delivery shapes, re-exported as aliases from the deliverystore leaf,
// plus the plan-metadata read facade. The canonical persistence surface
// (claim/lease/marks) lives in internal/deliverystore; the destination and
// job-delivery CRUD methods remain in store_delivery_destinations.go and
// store_job_deliveries.go.
package store

import (
	"context"

	"velox-server/internal/deliverystore"
)

// DeliveryDestination, JobDelivery and DeliveryLease are re-exported as
// aliases from the deliverystore leaf so existing store.<Type> call sites
// keep compiling while the canonical declaration lives in the leaf.
type (
	DeliveryDestination = deliverystore.DeliveryDestination
	JobDelivery         = deliverystore.JobDelivery
	DeliveryLease       = deliverystore.DeliveryLease
)

// GetDeliveryPlanMetadata returns the immutable per-destination metadata
// snapshot associated with an artifact's job delivery plan. Missing metadata
// is represented as an empty JSON object so providers can safely apply their
// defaults. Delegates to the deliverystore leaf.
func (s *SQLiteStore) GetDeliveryPlanMetadata(ctx context.Context, artifactID, destinationID string) (string, error) {
	return s.deliveryStore().GetDeliveryPlanMetadata(ctx, artifactID, destinationID)
}

// ErrDeliveryNoRow (the shared delivery sentinel) is re-exported from
// internal/storecore via db_errors.go.
