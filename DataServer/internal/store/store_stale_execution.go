package store

// store_stale_execution.go — re-export + delegation shim for the
// stalereconcile leaf package (internal/stalereconcile), which owns the
// stale-execution reconciler extracted from this god-package. The type
// surface and Scan/Reconcile behaviour moved out of the god-package into the
// leaf; the *SQLiteStore-backed constructor below wires the two repository
// seams so callers (the admin CLI + reconciliation registry) keep the
// store.StaleExecutionReconciler surface unchanged.

import (
	"fmt"

	"velox-server/internal/stalereconcile"
)

// StaleExecutionCategory + categories re-exported from the leaf.
type StaleExecutionCategory = stalereconcile.StaleExecutionCategory

const (
	StaleLeaseExpired      = stalereconcile.StaleLeaseExpired
	StaleOrphanTask        = stalereconcile.StaleOrphanTask
	StaleCommittedArtifact = stalereconcile.StaleCommittedArtifact
	StaleUnconfirmedSpool  = stalereconcile.StaleUnconfirmedSpool
	StaleWorkerOffline     = stalereconcile.StaleWorkerOffline
	StaleOrphanAttempt     = stalereconcile.StaleOrphanAttempt
	StaleAwaitingArtifact  = stalereconcile.StaleAwaitingArtifact
	StaleDeliveryPending   = stalereconcile.StaleDeliveryPending
	StaleWorkerLost        = stalereconcile.StaleWorkerLost
)

// StaleExecutionFinding + StaleExecutionReport + StaleExecutionReconciler
// re-exported from the leaf.
type StaleExecutionFinding = stalereconcile.StaleExecutionFinding
type StaleExecutionReport = stalereconcile.StaleExecutionReport
type StaleExecutionReconciler = stalereconcile.StaleExecutionReconciler

// NewStaleExecutionReconciler builds the reconciler over a SQLiteStore,
// wiring the concrete task/job repositories into the leaf's interfaces.
func NewStaleExecutionReconciler(s *SQLiteStore) (*StaleExecutionReconciler, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("stale execution reconciler: store is not initialized")
	}
	return stalereconcile.New(s.db, NewSQLiteTaskRepository(s), NewSQLiteJobRepository(s)), nil
}
