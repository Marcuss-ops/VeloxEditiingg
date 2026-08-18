// Package store / store_job_deliveries.go
//
// CRUD for job_deliveries (per-artifact × per-destination join rows).
// The SQL/CAS lives in the deliverystore leaf; these methods are delegating
// facades that preserve the historical store.<Method> call sites.
package store

import (
	"context"
)

// ── Job Delivery CRUD — facade over the deliverystore leaf ─────────────────

// InsertJobDelivery persists a new per-(artifact, destination) row.
func (s *SQLiteStore) InsertJobDelivery(jobD *JobDelivery) error {
	return s.deliveryStore().InsertJobDelivery(jobD)
}

// ListJobDeliveriesByJob returns all deliveries for a job's READY artifacts.
func (s *SQLiteStore) ListJobDeliveriesByJob(jobID string) ([]JobDelivery, error) {
	return s.deliveryStore().ListJobDeliveriesByJob(jobID)
}

// GetJobDelivery retrieves a single job_delivery by ID.
func (s *SQLiteStore) GetJobDelivery(ctx context.Context, deliveryID string) (*JobDelivery, error) {
	return s.deliveryStore().GetJobDelivery(ctx, deliveryID)
}

// ListDeliveryReconciliationCandidates delegates the reconciliation sweep
// read to the deliverystore leaf.
func (s *SQLiteStore) ListDeliveryReconciliationCandidates(ctx context.Context, limit int) ([]JobDelivery, error) {
	return s.deliveryStore().ListDeliveryReconciliationCandidates(ctx, limit)
}

// ApplyReconciledDelivery delegates the reconciliation verdict projection to
// the deliverystore leaf.
func (s *SQLiteStore) ApplyReconciledDelivery(ctx context.Context, deliveryID, status, remoteID, remoteURL, errorCode, errorMessage string) error {
	return s.deliveryStore().ApplyReconciledDelivery(ctx, deliveryID, status, remoteID, remoteURL, errorCode, errorMessage)
}
