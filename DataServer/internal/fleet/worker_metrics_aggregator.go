package fleet

// worker_metrics_aggregator.go coordinates the fleet metrics snapshot job.
// SQL ownership belongs to internal/store; this package only coordinates the
// typed repository contract and persists the resulting snapshot.

import (
	"context"
	"fmt"
	"time"

	"velox-server/internal/store"
)

// AggregatorDataSource is the narrow consumer-side interface used by the
// scheduler. Production wires a typed store adapter; tests inject a fake.
type AggregatorDataSource interface {
	WorkerIDs(ctx context.Context) ([]string, error)
	ComputeWorkerMetricsSnapshot(ctx context.Context, workerID string, now time.Time) (store.WorkerMetricsSnapshot, error)
	InsertWorkerMetricsSnapshot(ctx context.Context, snap store.WorkerMetricsSnapshot) error
}

// ComputeAndPersistSnapshot computes and persists one typed snapshot per
// known worker. Query semantics and source-table access live in internal/store.
func ComputeAndPersistSnapshot(ctx context.Context, ds AggregatorDataSource, now time.Time) (int, error) {
	if ds == nil {
		return 0, fmt.Errorf("aggregator: data source is nil")
	}
	workerIDs, err := ds.WorkerIDs(ctx)
	if err != nil {
		return 0, fmt.Errorf("aggregator: listing workers: %w", err)
	}
	persisted := 0
	for _, workerID := range workerIDs {
		snap, err := ds.ComputeWorkerMetricsSnapshot(ctx, workerID, now)
		if err != nil {
			return persisted, fmt.Errorf("aggregator: worker=%s: %w", workerID, err)
		}
		if err := ds.InsertWorkerMetricsSnapshot(ctx, snap); err != nil {
			return persisted, fmt.Errorf("aggregator: insert worker=%s: %w", workerID, err)
		}
		persisted++
	}
	return persisted, nil
}

// WorkerMetricsAggregatorDataSource is the production binding used by the
// bootstrap. The store interface intentionally exposes only typed operations.
type WorkerMetricsAggregatorDataSource struct {
	Store interface {
		WorkerIDs(ctx context.Context) ([]string, error)
		ComputeWorkerMetricsSnapshot(ctx context.Context, workerID string, now time.Time) (store.WorkerMetricsSnapshot, error)
		InsertWorkerMetricsSnapshot(ctx context.Context, snap store.WorkerMetricsSnapshot) error
		ListLatestWorkerMetrics(ctx context.Context, limit int) ([]store.WorkerMetricsSnapshot, error)
		GetLatestWorkerMetricsForWorker(ctx context.Context, workerID string) (store.WorkerMetricsSnapshot, error)
	}
}

func (s WorkerMetricsAggregatorDataSource) WorkerIDs(ctx context.Context) ([]string, error) {
	if s.Store == nil {
		return nil, fmt.Errorf("aggregator: store is nil")
	}
	return s.Store.WorkerIDs(ctx)
}

func (s WorkerMetricsAggregatorDataSource) ComputeWorkerMetricsSnapshot(ctx context.Context, workerID string, now time.Time) (store.WorkerMetricsSnapshot, error) {
	if s.Store == nil {
		return store.WorkerMetricsSnapshot{}, fmt.Errorf("aggregator: store is nil")
	}
	return s.Store.ComputeWorkerMetricsSnapshot(ctx, workerID, now)
}

func (s WorkerMetricsAggregatorDataSource) InsertWorkerMetricsSnapshot(ctx context.Context, snap store.WorkerMetricsSnapshot) error {
	if s.Store == nil {
		return fmt.Errorf("aggregator: store is nil")
	}
	return s.Store.InsertWorkerMetricsSnapshot(ctx, snap)
}
