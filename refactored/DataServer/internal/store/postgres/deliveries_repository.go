package postgres

import (
	"context"

	"velox-server/internal/store"
)

// DeliveryRepository is the upcoming Postgres implementation of
// store.DeliveryRepository. Scaffolding only — see package doc.
type DeliveryRepository struct {
	dsn string
}

// NewDeliveryRepository constructs a Postgres delivery repository stub.
func NewDeliveryRepository(dsn string) *DeliveryRepository {
	return &DeliveryRepository{dsn: dsn}
}

// CreateDeliveriesForArtifact returns ErrNotImplemented until the driver is wired in.
func (r *DeliveryRepository) CreateDeliveriesForArtifact(ctx context.Context, artifactID string) error {
	_, _ = ctx, artifactID
	return store.ErrNotImplemented
}

// ClaimNextDelivery returns ErrNotImplemented. Returning (nil, ErrNotImplemented)
// rather than (nil, nil) keeps the empty-queue semantics aligned with the
// SQLite impl only when real backing is added.
func (r *DeliveryRepository) ClaimNextDelivery(ctx context.Context, workerID string) (*store.DeliveryTarget, error) {
	_, _ = ctx, workerID
	return nil, store.ErrNotImplemented
}

// CompleteDelivery returns ErrNotImplemented.
func (r *DeliveryRepository) CompleteDelivery(ctx context.Context, targetID int, result store.DeliveryResult) error {
	_, _, _ = ctx, targetID, result
	return store.ErrNotImplemented
}

// FailDelivery returns ErrNotImplemented.
func (r *DeliveryRepository) FailDelivery(ctx context.Context, targetID int, failure store.DeliveryFailure) error {
	_, _, _ = ctx, targetID, failure
	return store.ErrNotImplemented
}

var _ store.DeliveryRepository = (*DeliveryRepository)(nil)
