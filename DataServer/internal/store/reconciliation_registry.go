package store

import (
	"context"
	"fmt"
	"log"
	"time"

	"velox-server/internal/reconcile"
	"velox-server/internal/stalereconcile"
)

// staleExecutionRegistryEntry adapts the canonical stalereconcile reconciler
// (whose Reconcile keeps its dry-run/apply arguments explicit for the admin
// CLI) to the canonical reconcile.Reconciler surface. Production wiring
// always uses apply=true and remains bounded/idempotent through the store's
// CAS transitions.
type staleExecutionRegistryEntry struct {
	reconciler *stalereconcile.StaleExecutionReconciler
	limit      int
	actor      string
}

func (e staleExecutionRegistryEntry) Reconcile(ctx context.Context, now time.Time) error {
	if e.reconciler == nil {
		return fmt.Errorf("stale execution reconciler is not initialized")
	}
	limit := e.limit
	if limit <= 0 {
		limit = 500
	}
	report, err := e.reconciler.Reconcile(ctx, now, limit, true, e.actor)
	if err != nil {
		return err
	}
	if len(report.Applied) > 0 {
		log.Printf("[RECONCILIATION] entry=%s applied=%d skipped=%d findings=%d", reconcile.NameStaleExecution, len(report.Applied), report.Skipped, len(report.Findings))
	}
	return nil
}

// BuildReconciliationRegistry assembles the canonical reconciliation entries
// (AWAITING_ARTIFACT, DELIVERY_PENDING, WORKER_LOST, STALE_EXECUTION) from
// the concrete store and returns the registered registry. It is the single
// owner of the production wiring so the composition root no longer reaches
// into each reconciler constructor.
func BuildReconciliationRegistry(db *SQLiteStore, staleThresholdSeconds, partitionThresholdSeconds, staleExecutionLimit int, actor string) (*reconcile.Registry, error) {
	registry := reconcile.NewRegistry()

	awaiting, err := NewAwaitingArtifactReconciler(db)
	if err != nil {
		return nil, fmt.Errorf("awaiting artifact reconciler: %w", err)
	}
	delivery, err := NewDeliveryPendingReconciler(db)
	if err != nil {
		return nil, fmt.Errorf("delivery pending reconciler: %w", err)
	}
	workerLost, err := NewWorkerLostReconciler(db, staleThresholdSeconds, partitionThresholdSeconds)
	if err != nil {
		return nil, fmt.Errorf("worker lost reconciler: %w", err)
	}
	staleExecution := stalereconcile.New(db.DB(), NewSQLiteTaskRepository(db), NewSQLiteJobRepository(db))

	if err := registry.Register(reconcile.NameAwaitingArtifact, awaiting); err != nil {
		return nil, err
	}
	if err := registry.Register(reconcile.NameDeliveryPending, delivery); err != nil {
		return nil, err
	}
	if err := registry.Register(reconcile.NameWorkerLost, workerLost); err != nil {
		return nil, err
	}
	if err := registry.Register(reconcile.NameStaleExecution, staleExecutionRegistryEntry{
		reconciler: staleExecution,
		limit:      staleExecutionLimit,
		actor:      actor,
	}); err != nil {
		return nil, err
	}
	return registry, nil
}
